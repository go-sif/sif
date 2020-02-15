package types

// PartitionIterator is a generalized interface for iterating over Partitions, regardless of where they come from
type PartitionIterator interface {
	HasNextPartition() bool
	NextPartition() (Partition, error)
	OnEnd(onEnd func())
}
