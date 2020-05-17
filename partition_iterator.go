package sif

// PartitionIterator is a generalized interface for iterating over Partitions, regardless of where they come from
type PartitionIterator interface {
	HasNextPartition() bool
	// if unlockPartition is not nil, it must be called when one is finished with the returned Partition
	NextPartition() (part Partition, unlockPartition func(), err error)
	OnEnd(onEnd func())
}
