package sif

import "context"

// A StageContext is a Context enhanced to store Stage state during execution of a Stage
type StageContext interface {
	context.Context
	ShuffleBuckets() []uint64                                // ShuffleBuckets retusn the shuffle buckets for this stage, or an empty slice if there are none
	SetShuffleBuckets([]uint64) error                        // SetShuffleBuckets configures the shuffle buckets for this Stage
	NextStageWidestInitialSchema() Schema                    // NextStageWidestInitialSchema returns the initial underlying data schema for the next stage, or nil if there is no next stage
	SetNextStageWidestInitialSchema(schema Schema) error     // SetNextStageWidestInitialSchema sets the initial underlying data schema for the next stage within this StageContext
	PartitionCache() PartitionCache                          // PartitionCache returns the configured PartitionCache for this Stage, or nil if none exists
	SetPartitionCache(cache PartitionCache) error            // SetPartitionCache configures the PartitionCache for this Stage, returning an error if one is already set
	PartitionIndex() PartitionIndex                          // PartitionIndex returns the PartitionIndex for this StageContext, or nil if one has not been set
	SetPartitionIndex(idx PartitionIndex) error              // SetPartitionIndex sets the PartitionIndex for this StageContext. An error is returned if one has already been set.
	IncomingPartitionIterator() PartitionIterator            // IncomingPartitionIndex returns the incoming PartitionIterator for this StageContext, or nil if one has not been set
	SetIncomingPartitionIterator(i PartitionIterator) error  // SetIncomingPartitionIndex sets the incoming PartitionIterator for this StageContext. An error is returned if one has already been set.
	KeyingOperation() KeyingOperation                        // KeyingOperation retrieves the KeyingOperation for this Stage (if it exists)
	SetKeyingOperation(keyFn KeyingOperation) error          // Configure the keying operation for the end of this stage
	ReductionOperation() ReductionOperation                  // ReductionOperation retrieves the ReductionOperation for this Stage (if it exists)
	SetReductionOperation(reduceFn ReductionOperation) error // Configure the reduction operation for the end of this stage
	Accumulator() Accumulator                                // Accumulator retrieves the Accumulator for this Stage (if it exists)
	SetAccumulator(acc Accumulator) error                    // Configure the accumulator for the end of this stage
	TargetPartitionSize() int                                // TargetPartitionSize returns the intended Partition maxSize for outgoing Partitions
	SetTargetPartitionSize(TargetPartitionSize int) error    // SetTargetPartitionSize configures the intended Partition maxSize for outgoing Partitions
}
