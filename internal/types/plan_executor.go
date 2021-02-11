package types

import (
	"sync"

	"github.com/go-sif/sif"
	pb "github.com/go-sif/sif/internal/rpc"
)

// A PlanExecutor manages the execution of a DataFrame Plan
type PlanExecutor interface {
	ID() string                   // ID returns the ID for this PlanExecutor
	GetConf() *PlanExecutorConfig // GetConf returns the configuration for this PlanExecutor
	Stop()                        // ends a PlanExecutor execution
	HasNextStage() bool           // HasNextStage forms an iterator for planExecutor Stages
	GetNextStage() Stage          // NextStage forms an iterator for planExecutor Stages
	GetNumStages() int            // GetNumStages returns the total number of stages
	// PeekNextStage() Stage      // PeekNextStage returns the next stage without advancing the iterator, or nil if there isn't one
	GetCurrentStage() Stage // GetCurrentStage returns the current stage without advancing the iterator, or nil if the iterator has never been advanced
	// OnFirstStage() bool                                                                                                                                                        // OnFirstStage returns true iff we're past the first stage
	// HasPartitionLoaders() bool                                                                                                                                                 // HasPartitionLoaders returns true iff we have assigned PartitionLoaders
	IsShuffleReady() bool                                                                                                                                                                                // IsShuffleReady returns true iff a shuffle has been prepared in this planExecutor's shuffle trees
	IsAccumulatorReady() bool                                                                                                                                                                            // IsAccumulatorReady returns true iff an accumulator has been prepared in this planExecutor
	AssignPartitionLoader(sLoader []byte) error                                                                                                                                                          // AssignPartitionLoader assigns a serialized PartitionLoader to this executor
	InitStageContext(sctx sif.StageContext, stage Stage) error                                                                                                                                           // InitStageContext initializes a StageContext
	TransformPartitions(sctx sif.StageContext, fn PartitionTransform, onRowError func(error) error) error                                                                                                // FlatMapPartitions applies a Partition operation to all partitions in this plan, regardless of where they come from
	AssignShuffleBucket(assignedBucket uint64)                                                                                                                                                           // AssignShuffleBucket assigns a ShuffleBucket to this executor
	GetShufflePartitionIterator(bucket uint64) (sif.SerializedPartitionIterator, error)                                                                                                                  // GetShufflePartitionIterator serves up an iterator for partitions to shuffle
	ShufflePartitionData(wg *sync.WaitGroup, partMerger chan<- sif.ReduceablePartition, asyncErrors chan<- error, mpart *pb.MPartitionMeta, dataStream pb.PartitionsService_TransferPartitionDataClient) // AcceptShuffledPartition receives a Partition that belongs on this worker and merges it into the local shuffle tree
	MergeShuffledPartitions(sctx sif.StageContext, wg *sync.WaitGroup, partMerger <-chan sif.ReduceablePartition, asyncErrors chan<- error)                                                              // MergeShuffledPartitions activates this PlanExecutor's shuffled partition merger, until the given channel is closed
	GetAccumulator() (sif.Accumulator, error)                                                                                                                                                            // GetAccumulator returns this planExecutor's Accumulator, returning an error if one is not available/ready
}
