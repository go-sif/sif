package types

// SerializedPartitionIterator is a generalized interface for iterating over SerializedPartitions, regardless of where they come from
type SerializedPartitionIterator interface {
	HasNextSerializedPartition() bool
	NextSerializedPartition() (spart []byte, err error)
	OnEnd(onEnd func())
}
