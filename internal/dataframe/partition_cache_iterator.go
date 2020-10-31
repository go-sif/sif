package dataframe

import (
	"sync"

	"github.com/go-sif/sif"
	errors "github.com/go-sif/sif/errors"
)

// PartitionCacheIterator produces Partitions non-sorted, cached data
type partitionCacheIterator struct {
	partitions   map[string]sif.Partition // partition id -> partition
	keys         []string
	next         int
	destructive  bool
	lock         sync.Mutex
	endListeners []func()
}

func createPartitionCacheIterator(partitions map[string]sif.Partition, destructive bool) sif.PartitionIterator {
	keys := make([]string, len(partitions))
	i := 0
	for k := range partitions {
		keys[i] = k
		i++
	}
	return &partitionCacheIterator{
		partitions:   partitions,
		keys:         keys,
		next:         0,
		destructive:  destructive,
		endListeners: []func(){},
	}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (pci *partitionCacheIterator) OnEnd(onEnd func()) {
	pci.lock.Lock()
	defer pci.lock.Unlock()
	pci.endListeners = append(pci.endListeners, onEnd)
}

func (pci *partitionCacheIterator) HasNextPartition() bool {
	pci.lock.Lock()
	defer pci.lock.Unlock()
	return pci.next < len(pci.keys)
}

func (pci *partitionCacheIterator) NextPartition() (sif.Partition, func(), error) {
	pci.lock.Lock()
	defer pci.lock.Unlock()
	if pci.next >= len(pci.keys) {
		for _, l := range pci.endListeners {
			l()
		}
		pci.endListeners = []func(){}
		return nil, nil, errors.NoMorePartitionsError{}
	}
	p := pci.partitions[pci.keys[pci.next]]
	if pci.destructive {
		delete(pci.partitions, pci.keys[pci.next])
	}
	pci.next++
	return p, nil, nil
}
