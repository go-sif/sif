package dataframe

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

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
	id   string
	plan itypes.Plan
	conf *itypes.PlanExecutorConfig
	done context.CancelFunc

	nextStage            int
	partitionLoaders     []sif.PartitionLoader
	partitionLoadersLock sync.Mutex
	assignedBucket       uint64
	shuffleReady         bool
	shuffleTrees         map[uint64]*pTreeRoot // staging area for partitions before shuffle // TODO replace with partially-disked-back map with automated paging
	shuffleTreesLock     sync.Mutex
	shuffleIterators     map[uint64]itypes.SerializedPartitionIterator // used to serve partitions from the shuffleTree
	shuffleIteratorsLock sync.Mutex
	collectCache         map[string]sif.Partition // staging area used for collection when there has been no shuffle
	collectCacheLock     sync.Mutex
	accumulateReady      bool
	statsTracker         *stats.RunStatistics
}

// CreatePlanExecutor is a factory for planExecutors
func CreatePlanExecutor(ctx context.Context, plan itypes.Plan, conf *itypes.PlanExecutorConfig, statsTracker *stats.RunStatistics, isCoordinator bool) itypes.PlanExecutor {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID: %v", err)
	}
	executor := &planExecutorImpl{
		id:               id.String(),
		plan:             plan,
		conf:             conf,
		done:             func() {},
		partitionLoaders: make([]sif.PartitionLoader, 0),
		shuffleTrees:     make(map[uint64]*pTreeRoot),
		shuffleIterators: make(map[uint64]itypes.SerializedPartitionIterator),
		collectCache:     make(map[string]sif.Partition),
		statsTracker:     statsTracker,
	}
	if !isCoordinator {
		monitorCtx, canceller := context.WithCancel(ctx)
		executor.done = canceller
		go executor.monitorMemoryUsage(monitorCtx)
	}
	return executor
}

// ID returns the configuration for this PlanExecutor
func (pe *planExecutorImpl) ID() string {
	return pe.id
}

// GetConf returns the configuration for this PlanExecutor
func (pe *planExecutorImpl) GetConf() *itypes.PlanExecutorConfig {
	return pe.conf
}

func (pe *planExecutorImpl) Stop() {
	pe.done()
	pe.shuffleTreesLock.Lock()
	for _, tree := range pe.shuffleTrees {
		tree.clearCaches()
	}
	pe.shuffleTreesLock.Unlock()
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
		parts = createPartitionLoaderIterator(pe.partitionLoaders, pe.plan.Parser(), pe.plan.GetStage(0).WidestInitialSchema())
		// in the non-streaming context, the partition loader won't offer more partitions
		// after we're done iterating through it, so we can safely get rid of it.
		if !pe.conf.Streaming {
			pe.partitionLoadersLock.Lock()
			pe.partitionLoaders = make([]sif.PartitionLoader, 0) // clear partition loaders
			pe.partitionLoadersLock.Unlock()
		}
	} else {
		pe.shuffleTreesLock.Lock()
		parts = createPreloadingPartitionIterator(createPTreeIterator(pe.shuffleTrees[pe.assignedBucket], true), 3)
		// parts = createPTreeIterator(pe.shuffleTrees[pe.assignedBucket], true)
		pe.shuffleTrees = make(map[uint64]*pTreeRoot)
		pe.shuffleIterators = make(map[uint64]itypes.SerializedPartitionIterator)
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
		part, unlockPartition, err := parts.NextPartition()
		if _, ok := err.(errors.NoMorePartitionsError); ok {
			if unlockPartition != nil {
				unlockPartition()
			}
			// It's ok for a data source to throw this once, as HasNextPartition is just a hint
			break
		} else if err != nil {
			if unlockPartition != nil {
				unlockPartition()
			}
			return err
		}
		pe.statsTracker.StartPartition()
		opart := part.(sif.OperablePartition)
		newParts, err := fn(opart)
		if err := onRowError(err); err != nil {
			if unlockPartition != nil {
				unlockPartition()
			}
			return err
		}
		// we're done with the source partition now
		if unlockPartition != nil {
			unlockPartition()
		}
		// Prepare resulting partitions for transfer to next stage
		for _, newPart := range newParts {
			tNewPart := newPart.(itypes.ReduceablePartition)
			if req.RunShuffle {
				if !tNewPart.GetIsKeyed() {
					return fmt.Errorf("Cannot prepare a shuffle for non-keyed partitions")
				}
				err = pe.PrepareShuffle(tNewPart, req.Buckets)
				if err := onRowError(err); err != nil {
					return err
				}
			} else if req.PrepCollect {
				if pe.collectCache[tNewPart.ID()] != nil {
					return fmt.Errorf("Partition ID collision")
				}
				// only collect partitions that have rows
				if tNewPart.GetNumRows() > 0 {
					// repack if any columns have been removed
					if tNewPart.GetSchema().NumRemovedColumns() > 0 {
						repackedSchema := tNewPart.GetSchema().Repack()
						repackedPart, err := tNewPart.(sif.OperablePartition).Repack(repackedSchema)
						if err != nil {
							return err
						}
						tNewPart = repackedPart.(itypes.ReduceablePartition)
					}
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
func (pe *planExecutorImpl) PrepareShuffle(part itypes.ReduceablePartition, buckets []uint64) error {
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

	// we might need to repack the partition before shuffling,
	// if this stage removed columns and/or the next stage adds any
	nextStage := pe.peekNextStage()
	nextStageWidestInitialSchema := nextStage.WidestInitialSchema()
	if part.GetSchema().NumRemovedColumns() > 0 || part.GetSchema().Equals(nextStageWidestInitialSchema) != nil {
		repackedPart, err := part.(sif.OperablePartition).Repack(nextStageWidestInitialSchema)
		if err != nil {
			return err
		}
		part = repackedPart.(itypes.ReduceablePartition)
	}

	// merge rows into our trees
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
			pe.shuffleTrees[buckets[bucket]] = createPTreeNode(pe.conf, targetPartitionSize, nextStage.WidestInitialSchema())
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
func (pe *planExecutorImpl) GetShufflePartitionIterator(bucket uint64) (itypes.SerializedPartitionIterator, error) {
	if len(pe.collectCache) > 0 {
		// if we're collecting, and we never reduced, then the collectCache will be used instead of a tree
		pci := createPartitionCacheIterator(pe.collectCache, true)
		return createPartitionSerializingIterator(pci), nil
	}
	// otherwise create an iterator to grab partitions from a tree
	// Note: the tree may be null if we never encountered rows belonging to a bucket
	pe.shuffleIteratorsLock.Lock()
	pe.shuffleTreesLock.Lock()
	defer pe.shuffleIteratorsLock.Unlock()
	defer pe.shuffleTreesLock.Unlock()
	if _, ok := pe.shuffleIterators[bucket]; !ok {
		pe.shuffleIterators[bucket] = createPTreeSerializedIterator(pe.shuffleTrees[bucket], true)
	}
	return pe.shuffleIterators[bucket], nil
}

// GetAccumulator returns this plan executor's Accumulator, if any
func (pe *planExecutorImpl) GetAccumulator() sif.Accumulator {
	return pe.GetCurrentStage().Accumulator()
}

// ShufflePartitionData receives a Partition that belongs on this worker and merges it into the local shuffle tree
func (pe *planExecutorImpl) ShufflePartitionData(wg *sync.WaitGroup, partMerger chan<- itypes.ReduceablePartition, asyncErrors chan<- error, mpart *pb.MPartitionMeta, dataStream pb.PartitionsService_TransferPartitionDataClient) {
	defer wg.Done()
	// if we're the last stage, then the incoming data should match our outgoing schema
	// but if there is a following stage, the data should match that stage.
	var incomingDataSchema sif.Schema
	if pe.HasNextStage() {
		incomingDataSchema = pe.peekNextStage().WidestInitialSchema()
	} else {
		incomingDataSchema = pe.GetCurrentStage().OutgoingSchema()
	}
	if _, ok := pe.shuffleTrees[pe.assignedBucket]; !ok {
		pe.shuffleTreesLock.Lock()
		pe.shuffleTrees[pe.assignedBucket] = createPTreeNode(pe.conf, pe.GetCurrentStage().TargetPartitionSize(), incomingDataSchema)
		pe.shuffleTreesLock.Unlock()
	}
	part, err := partition.FromStreamedData(dataStream, mpart, incomingDataSchema, pe.conf.PartitionCompressor)
	if err != nil {
		asyncErrors <- err
		return
	}
	rpart, ok := part.(itypes.ReduceablePartition)
	if !ok {
		asyncErrors <- fmt.Errorf("Deserialized partition is not Reduceable")
		return
	}
	partMerger <- rpart
}

func (pe *planExecutorImpl) MergeShuffledPartitions(wg *sync.WaitGroup, partMerger <-chan itypes.ReduceablePartition, asyncErrors chan<- error) {
	defer wg.Done()
	for part := range partMerger {
		pe.shuffleTreesLock.Lock()
		// merge partition into appropriate shuffle tree
		var multierr *multierror.Error
		tempRow := partition.CreateTempRow()
		destinationTree := pe.shuffleTrees[pe.assignedBucket]
		areEqualSchemas := destinationTree.shared.nextStageSchema.Equals(part.GetSchema())
		if areEqualSchemas != nil {
			asyncErrors <- fmt.Errorf("Incoming shuffled partition schema does not match expected schema")
			pe.shuffleTreesLock.Unlock()
			break
		}
		part.ForEachRow(func(row sif.Row) error {
			err := destinationTree.mergeRow(tempRow, row, pe.plan.GetStage(pe.nextStage-1).KeyingOperation(), pe.plan.GetStage(pe.nextStage-1).ReductionOperation())
			if err != nil {
				multierr = multierror.Append(multierr, err)
			}
			return nil
		})
		errors := multierr.ErrorOrNil()
		if errors != nil {
			asyncErrors <- errors
			pe.shuffleTreesLock.Unlock()
			break
		}
		pe.shuffleTreesLock.Unlock()
	}
}

func (pe *planExecutorImpl) monitorMemoryUsage(ctx context.Context) {
	var m runtime.MemStats
	usage := make([]uint64, 4)
	usageHead := 0
	var avg uint64
	for range time.Tick(250 * time.Millisecond) {
		select {
		case <-ctx.Done():
			log.Printf("Finished monitoring memory usage")
			return
		default:
			// record current memory usage
			runtime.ReadMemStats(&m)
			usage[usageHead] = m.Alloc / uint64(len(usage))
			usageHead = (usageHead + 1) % len(usage)
			// compute rolling average
			avg = 0
			for _, v := range usage {
				if v == 0 {
					// we haven't captured enough data yet
					continue
				}
				avg += v
			}
			// check watermarks
			if avg > pe.conf.CacheMemoryHighWatermark && len(pe.shuffleTrees) > 0 {
				shrinkFac := (float64(pe.conf.CacheMemoryHighWatermark) / float64(avg)) - 0.05
				// pe.shuffleTreesLock.Lock() // we won't be modifying the map at this point
				resized := true
				for _, tree := range pe.shuffleTrees {
					resized = resized && tree.resizeCaches(shrinkFac)
				}
				if resized {
					log.Printf("Memory usage %d is greater than high watermark %d. Shrunk partition caches by %f", avg, pe.conf.CacheMemoryHighWatermark, shrinkFac)
				}
				// pe.shuffleTreesLock.Unlock()
			}
		}
	}
}
