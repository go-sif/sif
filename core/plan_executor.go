package core

import (
	"context"
	"fmt"
	"log"
	"sync"

	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	iutil "github.com/go-sif/sif/internal/util"
	logging "github.com/go-sif/sif/logging"
	"github.com/go-sif/sif/partition"
	"github.com/go-sif/sif/types"
	uuid "github.com/gofrs/uuid"
	"github.com/hashicorp/go-multierror"
)

// planExecutor executes a plan, on a master or on workers
type planExecutor struct {
	id                   string
	plan                 *plan
	conf                 *PlanExecutorConfig
	nextStage            int
	partitionLoaders     []types.PartitionLoader
	partitionLoadersLock sync.Mutex
	assignedBucket       uint64
	shuffleReady         bool
	shuffleTrees         map[uint64]*pTreeRoot // staging area for partitions before shuffle // TODO replace with partially-disked-back map with automated paging
	shuffleTreesLock     sync.Mutex
	shuffleIterators     map[uint64]types.PartitionIterator // used to serve partitions from the shuffleTree
	shuffleIteratorsLock sync.Mutex
	collectCache         map[string]types.Partition // staging area used for collection when there has been no shuffle
	collectCacheLock     sync.Mutex
}

// PlanExecutorConfig configures the execution of a plan
type PlanExecutorConfig struct {
	tempFilePath       string // the directory to use as on-disk swap space for partitions
	inMemoryPartitions int    // the number of partitions to retain in memory before swapping to disk
	streaming          bool   // whether or not this executor is operating on streaming data
	ignoreRowErrors    bool   // iff true, log row transformation errors instead of crashing immediately
}

// createplanExecutor is a factory for planExecutors
func createplanExecutor(plan *plan, conf *PlanExecutorConfig) *planExecutor {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID: %v", err)
	}
	return &planExecutor{
		id:               id.String(),
		plan:             plan,
		conf:             conf,
		partitionLoaders: make([]types.PartitionLoader, 0),
		shuffleTrees:     make(map[uint64]*pTreeRoot),
		shuffleIterators: make(map[uint64]types.PartitionIterator),
		collectCache:     make(map[string]types.Partition),
	}
}

// hasNextStage forms an iterator for planExecutor Stages
func (pe *planExecutor) hasNextStage() bool {
	return pe.nextStage < (len(pe.plan.stages))
}

// nextStage forms an iterator for planExecutor Stages
func (pe *planExecutor) getNextStage() *stage {
	if pe.nextStage >= len(pe.plan.stages) {
		return nil
	}
	s := pe.plan.stages[pe.nextStage]
	pe.nextStage++
	if pe.conf.streaming {
		pe.nextStage = pe.nextStage % len(pe.plan.stages)
	}
	pe.shuffleReady = false
	return s
}

// peekNextStage returns the next stage without advancing the iterator, or nil if there isn't one
func (pe *planExecutor) peekNextStage() *stage {
	if pe.hasNextStage() {
		return pe.plan.stages[pe.nextStage]
	}
	return nil
}

// getCurrentStage returns the current stage without advancing the iterator, or nil if the iterator has never been advanced
func (pe *planExecutor) getCurrentStage() *stage {
	if pe.nextStage == 0 {
		return nil
	}
	return pe.plan.stages[pe.nextStage-1]
}

// onFirstStage returns true iff we're past the first stage
func (pe *planExecutor) onFirstStage() bool {
	return pe.nextStage == 1
}

// hasPartitionLoaders returns true iff we have assigned PartitionLoaders
func (pe *planExecutor) hasPartitionLoaders() bool {
	pe.partitionLoadersLock.Lock()
	defer pe.partitionLoadersLock.Unlock()
	return len(pe.partitionLoaders) > 0
}

// getPartitionSource returns the the source of partitions for the current stage
func (pe *planExecutor) getPartitionSource() types.PartitionIterator {
	var parts types.PartitionIterator
	// we only load partitions if we're on the first stage, and if they're available to load
	if pe.onFirstStage() && pe.hasPartitionLoaders() {
		parts = createPartitionLoaderIterator(pe.partitionLoaders, pe.plan.parser, pe.plan.stages[0].widestInitialSchema())
		// in the non-streaming context, the partition loader won't offer more partitions
		// after we're done iterating through it, so we can safely get rid of it.
		if !pe.conf.streaming {
			pe.partitionLoadersLock.Lock()
			pe.partitionLoaders = make([]types.PartitionLoader, 0) // clear partition loaders
			pe.partitionLoadersLock.Unlock()
		}
	} else {
		pe.shuffleTreesLock.Lock()
		parts = createPTreeIterator(pe.shuffleTrees[pe.assignedBucket], true)
		// once we consume a completed shuffle, we don't need a record of it anymore.
		pe.shuffleTrees = make(map[uint64]*pTreeRoot)
		pe.shuffleIterators = make(map[uint64]types.PartitionIterator)
		pe.shuffleReady = false
		pe.shuffleTreesLock.Unlock()
	}
	return parts
}

// isShuffleReady returns true iff a shuffle has been prepared in this planExecutor's shuffle trees
func (pe *planExecutor) isShuffleReady() bool {
	return pe.shuffleReady
}

// assignPartitionLoader assigns a serialized PartitionLoader to this executor
func (pe *planExecutor) assignPartitionLoader(sLoader []byte) error {
	loader, err := pe.plan.source.DeserializeLoader(sLoader[0:])
	if err != nil {
		return err
	}
	pe.partitionLoadersLock.Lock()
	defer pe.partitionLoadersLock.Unlock()
	pe.partitionLoaders = append(pe.partitionLoaders, loader)
	return nil
}

// flatMapPartitions applies a Partition operation to all partitions in this plan, regardless of where they come from
func (pe *planExecutor) flatMapPartitions(ctx context.Context, fn func(types.OperablePartition) ([]types.OperablePartition, error), logClient pb.LogServiceClient, runShuffle bool, prepCollect bool, buckets []uint64, workers []*pb.MWorkerDescriptor) error {
	if len(pe.plan.stages) == 0 {
		return fmt.Errorf("Plan has no stages")
	}
	parts := pe.getPartitionSource()

	for parts.HasNextPartition() {
		part, err := parts.NextPartition()
		if err != nil {
			return err
		}
		opart := part.(types.OperablePartition)
		newParts, err := fn(opart)
		if err != nil {
			// TODO eliminate this duplication from s_execution
			// if this is a multierror, it's from a row transformation, which we might want to ignore
			if multierr, ok := err.(*multierror.Error); pe.conf.ignoreRowErrors && ok {
				multierr.ErrorFormat = iutil.FormatMultiError
				// log errors and carry on
				logger, err := logClient.Log(ctx)
				if err != nil {
					return err
				}
				err = logger.Send(&pb.MLogMsg{
					Level:   logging.ErrorLevel,
					Source:  pe.id,
					Message: fmt.Sprintf("Map error in stage %s:\n%s", pe.getCurrentStage().id, multierr.Error()),
				})
				if err != nil {
					return err
				}
				_, err = logger.CloseAndRecv()
				if err != nil {
					return err
				}
			} else {
				// otherwise, crash immediately
				return err
			}
		}
		for _, newPart := range newParts {
			tNewPart := newPart.(itypes.TransferrablePartition)
			if runShuffle {
				if !tNewPart.GetIsKeyed() {
					return fmt.Errorf("Cannot prepare a shuffle for non-keyed partitions")
				}
				err = pe.prepareShuffle(tNewPart, buckets)
				if err != nil {
					return err
				}
			} else if prepCollect {
				if pe.collectCache[tNewPart.ID()] != nil {
					return fmt.Errorf("Partition ID collision")
				}
				pe.collectCache[tNewPart.ID()] = tNewPart
			}
		}
	}
	if runShuffle || prepCollect {
		pe.shuffleReady = true
	}
	return nil
}

// prepareShuffle appropriately caches and sorts a Partition before making it available for shuffling
func (pe *planExecutor) prepareShuffle(part itypes.TransferrablePartition, buckets []uint64) error {
	for i := 0; i < part.GetNumRows(); i++ {
		key, err := part.GetKey(i)
		if err != nil {
			return err
		}
		bucket := pe.keyToBuckets(key, buckets)
		pe.shuffleTreesLock.Lock()
		if _, ok := pe.shuffleTrees[buckets[bucket]]; !ok {
			nextStage := pe.peekNextStage()
			pe.shuffleTrees[buckets[bucket]] = createPTreeNode(pe.conf, part.GetMaxRows(), nextStage.widestInitialSchema(), nextStage.incomingSchema)
		}
		pe.shuffleTreesLock.Unlock()
		pe.shuffleTrees[buckets[bucket]].mergeRow(part.GetRow(i), pe.plan.stages[pe.nextStage-1].keyFn, pe.plan.stages[pe.nextStage-1].reduceFn)
	}
	return nil
}

func (pe *planExecutor) keyToBuckets(key uint64, buckets []uint64) int {
	for i, b := range buckets {
		if key < b {
			return i
		}
	}
	// should never reach here
	log.Panicf("Key must fall within a bucket")
	return 0
}

// assignShuffleBucket assigns a ShuffleBucket to this executor
func (pe *planExecutor) assignShuffleBucket(assignedBucket uint64) {
	pe.assignedBucket = assignedBucket
}

// getShufflePartitionIterator serves up an iterator for partitions to shuffle
func (pe *planExecutor) getShufflePartitionIterator(bucket uint64) (types.PartitionIterator, error) {
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

// acceptShuffledPartition receives a Partition that belongs on this worker and merges it into the local shuffle tree
func (pe *planExecutor) acceptShuffledPartition(mpart *pb.MPartitionMeta, dataStream pb.PartitionsService_TransferPartitionDataClient) error {
	// merge partition into appropriate shuffle tree
	pe.shuffleTreesLock.Lock()
	defer pe.shuffleTreesLock.Unlock()
	if _, ok := pe.shuffleTrees[pe.assignedBucket]; !ok {
		// pe.plan.stages[pe.nextStage-1].widestSchema
		pe.shuffleTrees[pe.assignedBucket] = createPTreeNode(pe.conf, int(mpart.GetMaxRows()), pe.getCurrentStage().outgoingSchema, pe.getCurrentStage().finalSchema())
	}
	part := partition.FromMetaMessage(mpart, pe.shuffleTrees[pe.assignedBucket].nextStageWidestSchema, pe.shuffleTrees[pe.assignedBucket].nextStageIncomingSchema)
	err := part.ReceiveStreamedData(dataStream, pe.getCurrentStage().outgoingSchema)
	if err != nil {
		return err
	}
	var multierr *multierror.Error
	for i := 0; i < part.GetNumRows(); i++ {
		err = pe.shuffleTrees[pe.assignedBucket].mergeRow(part.GetRow(i), pe.plan.stages[pe.nextStage-1].keyFn, pe.plan.stages[pe.nextStage-1].reduceFn)
		if err != nil {
			multierr = multierror.Append(multierr, err)
		}
	}
	return multierr.ErrorOrNil()
}
