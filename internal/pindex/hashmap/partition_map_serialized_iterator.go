package hashmap

import (
	"bytes"
	"sync"

	"github.com/go-sif/sif"
	errors "github.com/go-sif/sif/errors"
)

// partitionMapSerializedIterator is a SerializedPartitionIterator for PartitionMap
type partitionMapSerializedIterator struct {
	ids                []string
	cache              sif.PartitionCache
	next               int
	destructive        bool
	lock               sync.Mutex
	reusableReadBuffer *bytes.Buffer
	endListeners       []func()
}

func createPartitionMapSerializedIterator(ids []string, cache sif.PartitionCache, destructive bool) sif.SerializedPartitionIterator {
	if !destructive {
		panic("partitionMapSerializedIterator must always be destructive")
	}
	return &partitionMapSerializedIterator{
		ids:                ids,
		cache:              cache,
		next:               0,
		destructive:        destructive,
		reusableReadBuffer: new(bytes.Buffer),
		endListeners:       []func(){},
	}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (pmsi *partitionMapSerializedIterator) OnEnd(onEnd func()) {
	pmsi.lock.Lock()
	defer pmsi.lock.Unlock()
	pmsi.endListeners = append(pmsi.endListeners, onEnd)
}

func (pmsi *partitionMapSerializedIterator) HasNextSerializedPartition() bool {
	pmsi.lock.Lock()
	defer pmsi.lock.Unlock()
	return pmsi.next < len(pmsi.ids)
}

func (pmsi *partitionMapSerializedIterator) NextSerializedPartition() (id string, spart []byte, done func(), err error) {
	pmsi.lock.Lock()
	defer pmsi.lock.Unlock()
	if pmsi.next >= len(pmsi.ids) {
		for _, l := range pmsi.endListeners {
			l()
		}
		pmsi.endListeners = []func(){}
		return "", nil, nil, errors.NoMorePartitionsError{}
	}
	partID := pmsi.ids[pmsi.next]
	pmsi.reusableReadBuffer.Reset()
	err = pmsi.cache.GetSerialized(partID, pmsi.reusableReadBuffer)
	if err != nil {
		return "", nil, nil, err
	}
	pmsi.next++
	return partID, pmsi.reusableReadBuffer.Bytes(), func() {}, nil
}
