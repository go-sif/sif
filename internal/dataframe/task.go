package dataframe

import (
	"github.com/go-sif/sif"
)

// A shuffleTask is a task that represents a key-based shuffled, with potential aggregation
type shuffleTask interface {
	sif.Task
	GetKeyingOperation() sif.KeyingOperation
	GetReductionOperation() sif.ReductionOperation // Might be nil
	GetTargetPartitionSize() int
}

// A collectionTask is a task that represents collecting data to the coordinator
type collectionTask interface {
	sif.Task
	GetCollectionLimit() int32
}

// A accumulationTask is a task that represents a user-defined aggregation
type accumulationTask interface {
	sif.Task
	GetAccumulator() sif.Accumulator
}

// noOpTask is a task that does nothing
type noOpTask struct{}

// RunWorker for noOpTask does nothing
func (s *noOpTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}
