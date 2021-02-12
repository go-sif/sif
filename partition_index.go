package sif

// A PartitionIndex is an index for Partitions, useful for shuffling, sorting and/or reducing.
// An implementation of PartitionIndex permits the indexing of Partitions as well as individual rows,
// and provides a PartitionIterator/SerializedPartitionIterator to iterate over the indexed partitions
// in a particular order unique to the implementation (e.g. sorted order for an index which sorts Rows).
// Leverages an underlying PartitionCache for Partition storage, rather than storing Partition data itself.
type PartitionIndex interface {
	SetMaxRows(maxRows int)                                                                           // Change the maxRows for future partitions created by this index
	GetNextStageSchema() Schema                                                                       // Returns the Schema for the Stage which will *read* from this index
	MergePartition(part BuildablePartition, keyfn KeyingOperation, reducefn ReductionOperation) error // Merges all the Rows within a Partition into this PartitionIndex. reducefn may be nil, indicating that reduction is not intended.
	MergeRow(tempRow Row, row Row, keyfn KeyingOperation, reducefn ReductionOperation) error          // Merges a Row of data into the PartitionIndex, possibly appending it to an existing/new Partition, or combining it with an existing Row. reducefn may be nil, indicating that reduction is not intended.
	GetPartitionIterator(destructive bool) PartitionIterator                                          // Returns a PartitionIterator for this PartitionIndex
	GetSerializedPartitionIterator(destructive bool) SerializedPartitionIterator                      // Returns a SerializedPartitionIterator for this PartitionIndex
	NumPartitions() uint64                                                                            // Returns the number of Partitions in this PartitionIndex
	CacheSize() int                                                                                   // Returns the in-memory size (in Partitions) of the underlying PartitionCache
	ResizeCache(frac float64) bool                                                                    // Resizes the underlying PartitionCache
	Destroy()                                                                                         // Destroys the index
}

// A BucketedPartitionIndex is a PartitionIndex divided into buckets, which are indexed by uint64s
type BucketedPartitionIndex interface {
	PartitionIndex
	GetBucket(bucket uint64) PartitionIndex                                                             // return the PartitionIndex associated with the given bucket
	ReducePartition(part ReduceablePartition, keyfn KeyingOperation, reducefn ReductionOperation) error // Merge rows from an already-keyed partition into the appropriate buckets
}
