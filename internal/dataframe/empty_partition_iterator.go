package dataframe

import (
	"sync"

	"github.com/go-sif/sif"
	errors "github.com/go-sif/sif/errors"
)

type emptyPartitionIterator struct {
	lock         sync.Mutex
	endListeners []func()
}

// createEmptyPartitionIterator produces an empty PartitionIterator
func createEmptyPartitionIterator() sif.PartitionIterator {
	return &emptyPartitionIterator{
		endListeners: []func(){},
	}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (epi *emptyPartitionIterator) OnEnd(onEnd func()) {
	epi.lock.Lock()
	defer epi.lock.Unlock()
	epi.endListeners = append(epi.endListeners, onEnd)
}

// HasNextPartition returns true iff this PartitionIterator can produce another Partition
func (epi *emptyPartitionIterator) HasNextPartition() bool {
	return false
}

// NextPartition returns the next Partition if one is available, or an error
func (epi *emptyPartitionIterator) NextPartition() (sif.Partition, func(), error) {
	epi.lock.Lock()
	defer epi.lock.Unlock()
	for _, l := range epi.endListeners {
		l()
	}
	epi.endListeners = []func(){}
	return nil, nil, errors.NoMorePartitionsError{}
}
