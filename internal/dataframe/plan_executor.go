package dataframe

import (
	"fmt"
	"log"
	"sync"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/errors"
	"github.com/go-sif/sif/internal/partition"
	pb "github.com/go-sif/sif/internal/rpc"
	"github.com/go-sif/sif/internal/stats"
	itypes "github.com/go-sif/sif/internal/types"
	uuid "github.com/gofrs/uuid"
	"github.com/hashicorp/go-multierror"
)

// planExecutorImpl executes a plan, on a master or on workers
type planExecutorImpl struct {
	id                   string
	plan                 itypes.Plan
	conf                 *itypes.PlanExecutorConfig
	nextStage            int
	partitionLoaders     []sif.PartitionLoader
	partitionLoadersLock sync.Mutex
	assignedBucket       uint64
	shuffleReady         bool
	shuffleTrees         map[uint64]*pTreeRoot // staging area for partitions before shuffle // TODO replace with partially-disked-back map with automated paging
	shuffleTreesLock     sync.Mutex
	shuffleIterators     map[uint64]sif.PartitionIterator // used to serve partitions from the shuffleTree
	shuffleIteratorsLock sync.Mutex
	collectCache         map[string]sif.Partition // staging area used for collection when there has been no shuffle
	collectCacheLock     sync.Mutex
	accumulateReady      bool
	statsTracker         *stats.RunStatistics
}

// CreatePlanExecutor is a factory for planExecutors
func CreatePlanExecutor(plan itypes.Plan, conf *itypes.PlanExecutorConfig, statsTracker *stats.RunStatistics) itypes.PlanExecutor {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID: %v", err)
	}
	return &planExecutorImpl{
		id:               id.String(),
		plan:             plan,
		conf:             conf,
		partitionLoaders: make([]sif.PartitionLoader, 0),
		shuffleTrees:     make(map[uint64]*pTreeRoot),
		shuffleIterators: make(map[uint64]sif.PartitionIterator),
		collectCache:     make(map[string]sif.Partition),
		statsTracker:     statsTracker,
	}
}

// ID returns the configuration for this PlanExecutor
func (pe *planExecutorImpl) ID() string {
	return pe.id
}

// GetConf returns the configuration for this PlanExecutor
func (pe *planExecutorImpl) GetConf() *itypes.PlanExecutorConfig {
	return pe.conf
}

// HasNextStage forms an iterator for planExecutor Stages
func (pe *planExecutorImpl) HasNextStage() bool {
	return pe.nextStage < (pe.plan.Size())
}

// NextStage forms an iterator for planExecutor Stages
func (pe *planExecutorImpl) GetNextStage() itypes.Stage {
	if pe.nextStage >= pe.plan.Size() {
		return nil
	}
	s := pe.plan.GetStage(pe.nextStage)
	pe.nextStage++
	if pe.conf.Streaming {
		pe.nextStage = pe.nextStage % pe.plan.Size()
	}
	pe.shuffleReady = false
	return s
}

func (pe *planExecutorImpl) GetNumStages() int {
	return pe.plan.Size()
}

// peekNextStage returns the next stage without advancing the iterator, or nil if there isn't one
func (pe *planExecutorImpl) peekNextStage() itypes.Stage {
	if pe.HasNextStage() {
		return pe.plan.GetStage(pe.nextStage)
	}
	return nil
}

// GetCurrentStage returns the current stage without advancing the iterator, or nil if the iterator has never been advanced
func (pe *planExecutorImpl) GetCurrentStage() itypes.Stage {
	if pe.nextStage == 0 {
		return pe.plan.GetStage(pe.nextStage)
	}
	return pe.plan.GetStage(pe.nextStage - 1)
}

// onFirstStage returns true iff we're past the first stage
func (pe *planExecutorImpl) onFirstStage() bool {
	return pe.nextStage == 1
}

// hasPartitionLoaders returns true iff we have assigned PartitionLoaders
func (pe *planExecutorImpl) hasPartitionLoaders() bool {
	pe.partitionLoadersLock.Lock()
	defer pe.partitionLoadersLock.Unlock()
	return len(pe.partitionLoaders) > 0
}

// GetPartitionSource returns the the source of partitions for the current stage
func (pe *planExecutorImpl) GetPartitionSource() sif.PartitionIterator {
	var parts sif.PartitionIterator
	// we only load partitions if we're on the first stage, and if they're available to load
	if pe.onFirstStage() && pe.hasPartitionLoaders() {
		parts = createPartitionLoaderIterator(pe.partitionLoaders, pe.plan.Parser(), pe.plan.GetStage(0).WidestInitialPrivateSchema())
		// in the non-streaming context, the partition loader won't offer more partitions
		// after we're done iterating through it, so we can safely get rid of it.
		if !pe.conf.Streaming {
			pe.partitionLoadersLock.Lock()
			pe.partitionLoaders = make([]sif.PartitionLoader, 0) // clear partition loaders
			pe.partitionLoadersLock.Unlock()
		}
	} else {
		pe.shuffleTreesLock.Lock()
		parts = createPTreeIterator(pe.shuffleTrees[pe.assignedBucket], true)
		// once we consume a completed shuffle, we don't need a record of it anymore.
		pe.shuffleTrees = make(map[uint64]*pTreeRoot)
		pe.shuffleIterators = make(map[uint64]sif.PartitionIterator)
		pe.shuffleReady = false
		pe.accumulateReady = false
		pe.shuffleTreesLock.Unlock()
	}
	return parts
}

// IsShuffleReady returns true iff a shuffle has been prepared in this planExecutor's shuffle trees
func (pe *planExecutorImpl) IsShuffleReady() bool {
	return pe.shuffleReady
}

// IsAccumulatorReady returns true iff an Accumulator has been prepared in this planExecutor
func (pe *planExecutorImpl) IsAccumulatorReady() bool {
	return pe.accumulateReady
}

// AssignPartitionLoader assigns a serialized PartitionLoader to this executor
func (pe *planExecutorImpl) AssignPartitionLoader(sLoader []byte) error {
	loader, err := pe.plan.Source().DeserializeLoader(sLoader[0:])
	if err != nil {
		return err
	}
	pe.partitionLoadersLock.Lock()
	defer pe.partitionLoadersLock.Unlock()
	pe.partitionLoaders = append(pe.partitionLoaders, loader)
	return nil
}

// FlatMapPartitions applies a Partition operation to all partitions in this plan, regardless of where they come from
func (pe *planExecutorImpl) FlatMapPartitions(fn func(sif.OperablePartition) ([]sif.OperablePartition, error), req *pb.MRunStageRequest, onRowError func(error) error) error {
	if pe.plan.Size() == 0 {
		return fmt.Errorf("Plan has no stages")
	}
	currentStageID := pe.GetCurrentStage().ID()
	parts := pe.GetPartitionSource()

	for parts.HasNextPartition() {
		part, err := parts.NextPartition()
		if _, ok := err.(errors.NoMorePartitionsError); ok {
			// It's ok for a data source to throw this once, as HasNextPartition is just a hint
			break
		} else if err != nil {
			return err
		}
		pe.statsTracker.StartPartition()
		opart := part.(sif.OperablePartition)
		newParts, err := fn(opart)
		if err := onRowError(err); err != nil {
			return err
		}
		// Prepare resulting partitions for transfer to next stage
		for _, newPart := range newParts {
			if req.RunShuffle {
				tNewPart := newPart.(itypes.TransferrablePartition)
				if !tNewPart.GetIsKeyed() {
					return fmt.Errorf("Cannot prepare a shuffle for non-keyed partitions")
				}
				err = pe.PrepareShuffle(tNewPart, req.Buckets)
				if err := onRowError(err); err != nil {
					return err
				}
			} else if req.PrepCollect {
				tNewPart := newPart.(itypes.TransferrablePartition)
				if pe.collectCache[tNewPart.ID()] != nil {
					return fmt.Errorf("Partition ID collision")
				}
				// only collect partitions that have rows
				if tNewPart.GetNumRows() > 0 {
					pe.collectCache[tNewPart.ID()] = tNewPart
				}
			}
		}
		pe.statsTracker.EndPartition(currentStageID, part.GetNumRows())
	}
	if req.RunShuffle || req.PrepCollect {
		// We're only ready to shuffle if none of our trees are currently swapping to disk
		pe.shuffleReady = true
	} else if req.PrepAccumulate {
		pe.accumulateReady = true
	}
	return nil
}

// PrepareShuffle appropriately caches and sorts a Partition before making it available for shuffling
func (pe *planExecutorImpl) PrepareShuffle(part itypes.TransferrablePartition, buckets []uint64) error {
	if part.GetNumRows() == 0 {
		// no need to handle empty partitions
		return nil
	}
	var multierr *multierror.Error
	currentStage := pe.plan.GetStage(pe.nextStage - 1)
	targetPartitionSize := part.GetMaxRows()
	if currentStage.TargetPartitionSize() > 0 {
		targetPartitionSize = currentStage.TargetPartitionSize()
	}

	// we might need to repack the partition before reducing, if the next stage starts with AddColumns
	nextStage := pe.peekNextStage()
	nextStageWidestInitialSchema := nextStage.WidestInitialPrivateSchema()
	if !part.GetPrivateSchema().Equals(nextStageWidestInitialSchema) {
		repackedPart, err := part.(sif.OperablePartition).Repack(nextStageWidestInitialSchema)
		if err != nil {
			return err
		}
		part = repackedPart.(itypes.TransferrablePartition)
	}

	// merge rows into our tree
	i := 0
	tempRow := partition.CreateTempRow()
	err := part.ForEachRow(func(row sif.Row) error {
		key, err := part.GetKey(i)
		if err != nil {
			return err
		}
		bucket := pe.keyToBuckets(key, buckets)
		pe.shuffleTreesLock.Lock()
		if _, ok := pe.shuffleTrees[buckets[bucket]]; !ok {
			pe.shuffleTrees[buckets[bucket]] = createPTreeNode(pe.conf, targetPartitionSize, nextStage.WidestInitialPrivateSchema(), nextStage.IncomingPublicSchema())
		}
		pe.shuffleTreesLock.Unlock()
		err = pe.shuffleTrees[buckets[bucket]].mergeRow(tempRow, row, currentStage.KeyingOperation(), currentStage.ReductionOperation())
		if err != nil {
			multierr = multierror.Append(multierr, err)
		}
		i++
		return nil
	})
	if err != nil {
		return err
	}
	return multierr.ErrorOrNil()
}

func (pe *planExecutorImpl) keyToBuckets(key uint64, buckets []uint64) int {
	for i, b := range buckets {
		if key < b {
			return i
		}
	}
	// should never reach here
	log.Panicf("Key must fall within a bucket")
	return 0
}

// AssignShuffleBucket assigns a ShuffleBucket to this executor
func (pe *planExecutorImpl) AssignShuffleBucket(assignedBucket uint64) {
	pe.assignedBucket = assignedBucket
}

// GetShufflePartitionIterator serves up an iterator for partitions to shuffle
func (pe *planExecutorImpl) GetShufflePartitionIterator(bucket uint64) (sif.PartitionIterator, error) {
	if len(pe.collectCache) > 0 {
		// if we're collecting, and we never reduced, then the collectCache will be used instead of a tree
		return createPartitionCacheIterator(pe.collectCache, true), nil
	}
	// otherwise create an iterator to grab partitions from a tree
	// Note: the tree may be null if we never encountered rows belonging to a bucket
	pe.shuffleIteratorsLock.Lock()
	pe.shuffleTreesLock.Lock()
	defer pe.shuffleIteratorsLock.Unlock()
	defer pe.shuffleTreesLock.Unlock()
	if _, ok := pe.shuffleIterators[bucket]; !ok {
		pe.shuffleIterators[bucket] = createPTreeIterator(pe.shuffleTrees[bucket], true)
	}
	return pe.shuffleIterators[bucket], nil
}

// GetAccumulator returns this plan executor's Accumulator, if any
func (pe *planExecutorImpl) GetAccumulator() sif.Accumulator {
	return pe.GetCurrentStage().Accumulator()
}

// AcceptShuffledPartition receives a Partition that belongs on this worker and merges it into the local shuffle tree
func (pe *planExecutorImpl) AcceptShuffledPartition(mpart *pb.MPartitionMeta, dataStream pb.PartitionsService_TransferPartitionDataClient) error {
	// merge partition into appropriate shuffle tree
	pe.shuffleTreesLock.Lock()
	defer pe.shuffleTreesLock.Unlock()
	// if we're the last stage, then the incoming data should match our outgoing schema
	// but if there is a following stage, the data should match that stage.
	var incomingDataSchema sif.Schema
	if pe.HasNextStage() {
		incomingDataSchema = pe.peekNextStage().WidestInitialPrivateSchema()
	} else {
		incomingDataSchema = pe.GetCurrentStage().OutgoingPrivateSchema()
	}
	if _, ok := pe.shuffleTrees[pe.assignedBucket]; !ok {
		pe.shuffleTrees[pe.assignedBucket] = createPTreeNode(pe.conf, int(mpart.GetMaxRows()), incomingDataSchema, pe.GetCurrentStage().IncomingPublicSchema())
	}
	part := partition.FromMetaMessage(mpart, incomingDataSchema)
	err := part.ReceiveStreamedData(dataStream, incomingDataSchema, mpart)
	if err != nil {
		return err
	}
	// merge data into our tree
	var multierr *multierror.Error
	tempRow := partition.CreateTempRow()
	part.ForEachRow(func(row sif.Row) error {
		err = pe.shuffleTrees[pe.assignedBucket].mergeRow(tempRow, row, pe.plan.GetStage(pe.nextStage-1).KeyingOperation(), pe.plan.GetStage(pe.nextStage-1).ReductionOperation())
		if err != nil {
			multierr = multierror.Append(multierr, err)
		}
		return nil
	})
	return multierr.ErrorOrNil()
}
