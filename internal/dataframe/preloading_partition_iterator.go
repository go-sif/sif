package dataframe

import (
	"sync"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/errors"
)

type preloadedPartition struct {
	part   sif.Partition
	unlock func()
}

type preloadingPartitionIterator struct {
	lock            sync.Mutex
	iterator        sif.PartitionIterator
	pchan           chan *preloadedPartition
	errChan         chan error
	endListeners    []func()
	finishedLoading bool
}

// createPreloadingPartitionIterator creates a new preloadingPartitionIterator
func createPreloadingPartitionIterator(iterator sif.PartitionIterator, preloadCount int) sif.PartitionIterator {
	result := &preloadingPartitionIterator{
		iterator: iterator,
		pchan:    make(chan *preloadedPartition, preloadCount),
		errChan:  make(chan error),
	}
	go result.loadPartitions()
	return result
}

func (ppi *preloadingPartitionIterator) HasNextPartition() bool {
	ppi.lock.Lock()
	defer ppi.lock.Unlock()
	ended := len(ppi.pchan) == 0 && !ppi.finishedLoading
	if ended {
		ppi.doEnd()
	}
	return ended
}

// if unlockPartition is not nil, it must be called when one is finished with the returned Partition
func (ppi *preloadingPartitionIterator) NextPartition() (part sif.Partition, unlockPartition func(), err error) {
	ppi.lock.Lock()
	defer ppi.lock.Unlock()
	// check for errors (unblocking)
	select {
	case loadErr := <-ppi.errChan:
		close(ppi.errChan)
		return nil, nil, loadErr
	default:
	}
	// load partition (blocking)
	next := <-ppi.pchan
	// if channel was closed, we'll get nil back
	if next == nil {
		ppi.doEnd()
		return nil, nil, errors.NoMorePartitionsError{}
	}
	return next.part, next.unlock, nil
}

func (ppi *preloadingPartitionIterator) OnEnd(onEnd func()) {
	ppi.lock.Lock()
	defer ppi.lock.Unlock()
	ppi.endListeners = append(ppi.endListeners, onEnd)
}

func (ppi *preloadingPartitionIterator) loadPartitions() {
	for ppi.iterator.HasNextPartition() {
		part, unlock, err := ppi.iterator.NextPartition()
		if err != nil {
			ppi.errChan <- err
			break
		}
		ppi.pchan <- &preloadedPartition{
			part:   part,
			unlock: unlock,
		}
	}
	close(ppi.pchan) // safe to close buffered channel
	ppi.lock.Lock()
	ppi.finishedLoading = true
	ppi.lock.Unlock()
}

func (ppi *preloadingPartitionIterator) doEnd() {
	for _, l := range ppi.endListeners {
		l()
	}
	ppi.endListeners = []func(){}
}
