package dataframe

import (
	"sync"

	"github.com/go-sif/sif"
	errors "github.com/go-sif/sif/errors"
)

// pTreePartitionIterator iterates over Partitions in a pTree, starting at the bottom left.
// It optionally destroys the tree as it does this.
type pTreePartitionIterator struct {
	root         *pTreeNode
	next         *pTreeNode
	destructive  bool
	lock         sync.Mutex
	endListeners []func()
}

func createPTreeIterator(tree *pTreeRoot, destructive bool) sif.PartitionIterator {
	if tree == nil {
		return &pTreePartitionIterator{root: nil, next: nil, destructive: destructive, endListeners: []func(){}}
	}
	return &pTreePartitionIterator{root: tree, next: tree.firstNode(), destructive: destructive, endListeners: []func(){}}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (tpi *pTreePartitionIterator) OnEnd(onEnd func()) {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	tpi.endListeners = append(tpi.endListeners, onEnd)
}

func (tpi *pTreePartitionIterator) HasNextPartition() bool {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	if tpi.next == nil && tpi.root != nil {
		tpi.root.Destroy()
		tpi.root = nil
	}
	return tpi.next != nil
}

func (tpi *pTreePartitionIterator) NextPartition() (sif.Partition, func(), error) {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	if tpi.next == nil {
		for _, l := range tpi.endListeners {
			l()
		}
		if tpi.next == nil && tpi.root != nil {
			tpi.root.Destroy()
			tpi.root = nil
		}
		tpi.endListeners = []func(){}
		return nil, nil, errors.NoMorePartitionsError{}
	}
	part, unlockPartition, err := tpi.next.loadPartition() // temp var for partition
	if err != nil {
		return nil, nil, err
	}
	tpi.next = tpi.next.next // advance iterator
	return part, unlockPartition, nil
}
