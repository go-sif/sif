package core

import "github.com/go-sif/sif"

// A reductionTask is a task that represents an aggregation
type reductionTask interface {
	sif.Task
	GetKeyingOperation() sif.KeyingOperation
	GetReductionOperation() sif.ReductionOperation
}

// A collectionTask is a task that represents collecting data to the coordinator
type collectionTask interface {
	sif.Task
	GetCollectionLimit() int64
}

// noOpTask is a task that does nothing
type noOpTask struct{}

// RunWorker for noOpTask does nothing
func (s *noOpTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}
