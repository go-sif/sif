package sif

// SerializedPartitionIterator is a generalized interface for iterating over SerializedPartitions, regardless of where they come from
type SerializedPartitionIterator interface {
	HasNextSerializedPartition() bool
	NextSerializedPartition() (id string, spart []byte, done func(), err error)
	OnEnd(onEnd func())
}
