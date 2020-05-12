package types

import "github.com/go-sif/sif"

// Stage is a group of tasks which share a common schema.
// stages block the execution of further stages until they
// are complete.
type Stage interface {
	ID() int                                                                   // ID returns the ID for this stage
	IncomingSchema() sif.Schema                                                // IncomingSchema is the Schema for data entering this Stage
	OutgoingSchema() sif.Schema                                                // OutgoingSchema returns the schema from the final task of the stage, or the nil if there are no tasks. This represents the true, underlying structure of Partitions exiting this Stage.
	WidestInitialSchema() sif.Schema                                           // WidestInitialPrivateSchema returns final schema before a repack in a stage. This represents how much space needs to be allocated for a Partition.
	WorkerExecute(part sif.OperablePartition) ([]sif.OperablePartition, error) // Or turned into multiple Partitions)
	EndsInAccumulate() bool                                                    // EndsInAccumulate returns true iff this Stage ends with an accumulation task
	EndsInShuffle() bool                                                       // EndsInShuffle returns true iff this Stage ends with a reduction task
	EndsInCollect() bool                                                       // EndsInCollect returns true iff this Stage represents a collect task
	GetCollectionLimit() int64                                                 // GetCollectionLimit returns the maximum number of Partitions to collect
	KeyingOperation() sif.KeyingOperation                                      // KeyingOperation retrieves the KeyingOperation for this Stage (if it exists)
	SetKeyingOperation(keyFn sif.KeyingOperation)                              // Configure the keying operation for the end of this stage
	ReductionOperation() sif.ReductionOperation                                // ReductionOperation retrieves the ReductionOperation for this Stage (if it exists)
	SetReductionOperation(reduceFn sif.ReductionOperation)                     // Configure the reduction operation for the end of this stage
	Accumulator() sif.Accumulator                                              // Accumulator retrieves the Accumulator for this Stage (if it exists)
	SetAccumulator(acc sif.Accumulator)                                        // Configure the accumulator for the end of this stage
	TargetPartitionSize() int                                                  // TargetPartitionSize returns the intended Partition maxSize for outgoing Partitions
	SetTargetPartitionSize(TargetPartitionSize int)                            // SetTargetPartitionSize configures the intended Partition maxSize for outgoing Partitions
}
