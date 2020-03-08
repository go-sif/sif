package types

import (
	"context"

	"github.com/go-sif/sif"
	pb "github.com/go-sif/sif/internal/rpc"
)

// A PlanExecutor manages the execution of a DataFrame Plan
type PlanExecutor interface {
	ID() string                   // ID returns the ID for this PlanExecutor
	GetConf() *PlanExecutorConfig // GetConf returns the configuration for this PlanExecutor
	HasNextStage() bool           // HasNextStage forms an iterator for planExecutor Stages
	GetNextStage() Stage          // NextStage forms an iterator for planExecutor Stages
	// PeekNextStage() Stage   // PeekNextStage returns the next stage without advancing the iterator, or nil if there isn't one
	GetCurrentStage() Stage // GetCurrentStage returns the current stage without advancing the iterator, or nil if the iterator has never been advanced
	// OnFirstStage() bool                                                                                                                                                                                                                // OnFirstStage returns true iff we're past the first stage
	// HasPartitionLoaders() bool                                                                                                                                                                                                         // HasPartitionLoaders returns true iff we have assigned PartitionLoaders
	GetPartitionSource() sif.PartitionIterator                                                                                                                             // GetPartitionSource returns the the source of partitions for the current stage
	IsShuffleReady() bool                                                                                                                                                  // IsShuffleReady returns true iff a shuffle has been prepared in this planExecutor's shuffle trees
	AssignPartitionLoader(sLoader []byte) error                                                                                                                            // AssignPartitionLoader assigns a serialized PartitionLoader to this executor
	FlatMapPartitions(ctx context.Context, fn func(sif.OperablePartition) ([]sif.OperablePartition, error), logClient pb.LogServiceClient, req *pb.MRunStageRequest) error // FlatMapPartitions applies a Partition operation to all partitions in this plan, regardless of where they come from
	PrepareShuffle(part TransferrablePartition, buckets []uint64) error                                                                                                    // PrepareShuffle appropriately caches and sorts a Partition before making it available for shuffling
	AssignShuffleBucket(assignedBucket uint64)                                                                                                                             // AssignShuffleBucket assigns a ShuffleBucket to this executor
	GetShufflePartitionIterator(bucket uint64) (sif.PartitionIterator, error)                                                                                              // GetShufflePartitionIterator serves up an iterator for partitions to shuffle
	AcceptShuffledPartition(mpart *pb.MPartitionMeta, dataStream pb.PartitionsService_TransferPartitionDataClient) error                                                   // AcceptShuffledPartition receives a Partition that belongs on this worker and merges it into the local shuffle tree

}
