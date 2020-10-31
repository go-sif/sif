package pcache

import (
	itypes "github.com/go-sif/sif/internal/types"
)

// PartitionCache is a cache for Partitions
type PartitionCache interface {
	Destroy()
	Add(key string, value itypes.ReduceablePartition)
	Get(key string) (value itypes.ReduceablePartition, err error) // removes the partition from the cache and returns it, if present. Returns an error otherwise.
	GetSerialized(key string) (value []byte, err error)           // removes the partition from the cache in a serialized format adn returns it, if present. Returns an error otherwise.
	CurrentSize() int
	Resize(frac float64) bool // resize by a fraction RELATIVE TO THE CURRENT NUMBER OF ITEMS IN THE CACHE
}
