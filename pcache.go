package sif

import "bytes"

// PartitionCache is a cache for Partitions
type PartitionCache interface {
	Destroy()
	Add(key string, value ReduceablePartition)
	Get(key string) (value ReduceablePartition, err error) // removes the partition from the cache and returns it, if present. Returns an error otherwise.
	GetSerialized(key string, result *bytes.Buffer) error  // removes the partition from the cache in a serialized format and copies it to the supplied buffer, if present. Returns an error otherwise.
	CurrentSize() int
	Resize(frac float64) bool // resize by a fraction RELATIVE TO THE CURRENT NUMBER OF ITEMS IN THE CACHE
}
