package types

import "github.com/go-sif/sif"

// Stage is a group of tasks which share a common schema.
// stages block the execution of further stages until they
// are complete.
type Stage interface {
	ID() int                                                                                          // ID returns the ID for this stage
	IncomingSchema() sif.Schema                                                                       // IncomingSchema is the Schema for data entering this Stage
	OutgoingSchema() sif.Schema                                                                       // OutgoingSchema returns the schema from the final task of the stage, or the nil if there are no tasks. This represents the true, underlying structure of Partitions exiting this Stage.
	WidestInitialSchema() sif.Schema                                                                  // widestInitialSchema returns final schema before a repack in a stage. This represents how much space needs to be allocated for a Partition.
	WorkerInitialize(sctx sif.StageContext) error                                                     // WorkerInitialize initializes a Stage on a worker
	WorkerExecute(sctx sif.StageContext, part sif.OperablePartition) ([]sif.OperablePartition, error) // WorkerExecute runs a Stage on a worker
	EndsInAccumulate() bool                                                                           // EndsInAccumulate returns true iff this Stage ends with an accumulation task
	EndsInShuffle() bool                                                                              // EndsInShuffle returns true iff this Stage ends with a reduction task
	EndsInCollect() bool                                                                              // EndsInCollect returns true iff this Stage represents a collect task
}
