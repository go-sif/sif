package core

import "github.com/go-sif/sif/types"

// A reductionTask is a task that represents an aggregation
type reductionTask interface {
	types.Task
	GetKeyingOperation() types.KeyingOperation
	GetReductionOperation() types.ReductionOperation
}

// A collectionTask is a task that represents collecting data to the coordinator
type collectionTask interface {
	types.Task
	GetCollectionLimit() int64
}

// noOpTask is a task that does nothing
type noOpTask struct{}

// RunWorker for noOpTask does nothing
func (s *noOpTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	return []types.OperablePartition{previous}, nil
}
