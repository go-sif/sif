package dataframe

import (
	"sync"

	errors "github.com/go-sif/sif/errors"
	itypes "github.com/go-sif/sif/internal/types"
)

type emptySerializedIterator struct {
	lock         sync.Mutex
	endListeners []func()
}

// createEmptySerializedIterator returns an empty SerializedPartitionIterator
func createEmptySerializedIterator() itypes.SerializedPartitionIterator {
	return &emptySerializedIterator{
		endListeners: []func(){},
	}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (npi *emptySerializedIterator) OnEnd(onEnd func()) {
	npi.lock.Lock()
	defer npi.lock.Unlock()
	npi.endListeners = append(npi.endListeners, onEnd)
}

func (npi *emptySerializedIterator) HasNextSerializedPartition() bool {
	npi.lock.Lock()
	defer npi.lock.Unlock()
	for _, l := range npi.endListeners {
		l()
	}
	npi.endListeners = []func(){}
	return false
}

func (npi *emptySerializedIterator) NextSerializedPartition() (string, []byte, func(), error) {
	npi.lock.Lock()
	defer npi.lock.Unlock()
	return "", nil, func() {}, errors.NoMorePartitionsError{}
}
