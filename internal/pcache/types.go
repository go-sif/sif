package pcache

import (
	itypes "github.com/go-sif/sif/internal/types"
)

// PartitionCache is a cache for Partitions
type PartitionCache interface {
	Destroy()
	Add(key string, value itypes.ReduceablePartition)
	Get(key string) (value itypes.ReduceablePartition, err error) // removes the partition from the cache and returns it, if present. Returns an error otherwise.
	Resize(size int)
}
