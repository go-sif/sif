package dataframe

import (
	"bytes"
	"sync"

	errors "github.com/go-sif/sif/errors"
	itypes "github.com/go-sif/sif/internal/types"
)

// pTreeSerializedPartitionIterator iterates over Partitions in a pTree, starting at the bottom left.
// It optionally destroys the tree as it does this.
type pTreeSerializedPartitionIterator struct {
	root             *pTreeNode
	next             *pTreeNode
	destructive      bool
	lock             sync.Mutex
	endListeners     []func()
	reusableBuffPool *sync.Pool
}

func createPTreeSerializedIterator(tree *pTreeRoot, destructive bool) itypes.SerializedPartitionIterator {
	var pool = &sync.Pool{
		New: func() interface{} {
			// The Pool's New function should generally only return pointer
			// types, since a pointer can be put into the return interface
			// value without an allocation:
			return new(bytes.Buffer)
		},
	}
	if tree == nil {
		return &pTreeSerializedPartitionIterator{
			root:             nil,
			next:             nil,
			destructive:      destructive,
			endListeners:     []func(){},
			reusableBuffPool: pool,
		}
	}
	return &pTreeSerializedPartitionIterator{
		root:             tree,
		next:             tree.firstNode(),
		destructive:      destructive,
		endListeners:     []func(){},
		reusableBuffPool: pool,
	}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (tpi *pTreeSerializedPartitionIterator) OnEnd(onEnd func()) {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	tpi.endListeners = append(tpi.endListeners,
		onEnd)
}

func (tpi *pTreeSerializedPartitionIterator) HasNextSerializedPartition() bool {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	if tpi.next == nil && tpi.root != nil {
		tpi.root.Destroy()
		tpi.root = nil
	}
	return tpi.next != nil
}

func (tpi *pTreeSerializedPartitionIterator) NextSerializedPartition() (string, []byte, func(), error) {
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
		return "", nil, func() {}, errors.NoMorePartitionsError{}
	}
	// try to allocate a buffer based on how big things were last time
	buff := tpi.reusableBuffPool.Get().(*bytes.Buffer)
	err := tpi.next.fetchSerializedPartition(buff) // temp var for partition
	id := tpi.next.partID
	if err != nil {
		tpi.reusableBuffPool.Put(buff)
		return "", nil, func() {}, err
	}
	done := func() {
		tpi.reusableBuffPool.Put(buff)
	}
	tpi.next = tpi.next.next // advance iterator
	return id, buff.Bytes(), done, nil
}
