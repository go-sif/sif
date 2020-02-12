package core

// A Task is an action or transformation applied
// to Partitions of columnar data.
type Task interface {
	RunWorker(previous *Partition) ([]*Partition, error)
}

// A reductionTask is a task that represents an aggregation
type reductionTask interface {
	Task
	GetKeyingOperation() KeyingOperation
	GetReductionOperation() ReductionOperation
}

// A collectionTask is a task that represents collecting data to the coordinator
type collectionTask interface {
	Task
	GetCollectionLimit() int64
}

// noOpTask is a task that does nothing
type noOpTask struct{}

// RunWorker for noOpTask does nothing
func (s *noOpTask) RunWorker(previous *Partition) ([]*Partition, error) {
	return []*Partition{previous}, nil
}
