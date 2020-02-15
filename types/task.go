package types

// A Task is an action or transformation applied
// to Partitions of columnar data.
type Task interface {
	RunWorker(previous OperablePartition) ([]OperablePartition, error)
}
