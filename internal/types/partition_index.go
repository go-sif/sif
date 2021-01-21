package types

import (
	"github.com/go-sif/sif"
)

// A PartitionIndex is an index for Partitions, useful for shuffling, sorting and/or reducing.
// Leverages an underlying PartitionCache for Partition storage, rather than storing Partition data itself.
type PartitionIndex interface {
	GetNextStageSchema() sif.Schema                                                                            // Returns the Schema for the Stage which will *read* from this index
	MergePartition(part ReduceablePartition, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error // Merges all the Rows within a Partition into this PartitionIndex. reducefn may be nil, indicating that reduction is not intended.
	MergeRow(tempRow sif.Row, row sif.Row, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error   // Merges a Row of data into the PartitionIndex, possibly appending it to an existing/new Partition, or combining it with an existing Row. reducefn may be nil, indicating that reduction is not intended.
	GetPartitionIterator(destructive bool) sif.PartitionIterator                                               // Returns a PartitionIterator for this PartitionIndex
	GetSerializedPartitionIterator(destructive bool) SerializedPartitionIterator                               // Returns a SerializedPartitionIterator for this PartitionIndex
	NumPartitions() uint64                                                                                     // Returns the number of Partitions in this PartitionIndex
	CacheSize() int                                                                                            // Returns the in-memory size (in Partitions) of the underlying PartitionCache
	ResizeCache(frac float64) bool                                                                             // Resizes the underlying PartitionCache
	Destroy()                                                                                                  // Destroys the index, and underlying PartitionCache
}
