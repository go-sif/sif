package dataframe

import (
	"sync"

	errors "github.com/go-sif/sif/errors"
	itypes "github.com/go-sif/sif/internal/types"
)

// pTreeSerializedPartitionIterator iterates over Partitions in a pTree, starting at the bottom left.
// It optionally destroys the tree as it does this.
type pTreeSerializedPartitionIterator struct {
	root         *pTreeNode
	next         *pTreeNode
	destructive  bool
	lock         sync.Mutex
	endListeners []func()
}

func createPTreeSerializedIterator(tree *pTreeRoot, destructive bool) itypes.SerializedPartitionIterator {
	if tree == nil {
		return &pTreeSerializedPartitionIterator{root: nil, next: nil, destructive: destructive, endListeners: []func(){}}
	}
	return &pTreeSerializedPartitionIterator{root: tree, next: tree.firstNode(), destructive: destructive, endListeners: []func(){}}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (tpi *pTreeSerializedPartitionIterator) OnEnd(onEnd func()) {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	tpi.endListeners = append(tpi.endListeners, onEnd)
}

func (tpi *pTreeSerializedPartitionIterator) HasNextSerializedPartition() bool {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	if tpi.next == nil && tpi.root != nil {
		tpi.root.clearCaches()
		tpi.root = nil
	}
	return tpi.next != nil
}

func (tpi *pTreeSerializedPartitionIterator) NextSerializedPartition() ([]byte, error) {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	if tpi.next == nil {
		for _, l := range tpi.endListeners {
			l()
		}
		if tpi.next == nil && tpi.root != nil {
			tpi.root.clearCaches()
			tpi.root = nil
		}
		tpi.endListeners = []func(){}
		return nil, errors.NoMorePartitionsError{}
	}
	part, err := tpi.next.fetchSerializedPartition() // temp var for partition
	if err != nil {
		return nil, err
	}
	tpi.next = tpi.next.next // advance iterator
	return part, nil
}
