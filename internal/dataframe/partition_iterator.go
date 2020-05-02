package dataframe

import (
	"sync"

	"github.com/go-sif/sif"
	errors "github.com/go-sif/sif/errors"
)

// partitionSliceIterator produces a simple iterator for Partitions stored in a slice
type partitionSliceIterator struct {
	partitions   []sif.Partition
	next         int
	lock         sync.Mutex
	endListeners []func()
}

// createPartitionSliceIterator produces a new PartitionIterator for iterating over a slice of Partitions
func createPartitionSliceIterator(partitions []sif.Partition) sif.PartitionIterator {
	return &partitionSliceIterator{
		partitions:   partitions,
		next:         0,
		endListeners: []func(){},
	}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (psi *partitionSliceIterator) OnEnd(onEnd func()) {
	psi.lock.Lock()
	defer psi.lock.Unlock()
	psi.endListeners = append(psi.endListeners, onEnd)
}

// HasNextPartition returns true iff this PartitionIterator can produce another Partition
func (psi *partitionSliceIterator) HasNextPartition() bool {
	psi.lock.Lock()
	defer psi.lock.Unlock()
	return psi.next < len(psi.partitions)
}

// NextPartition returns the next Partition if one is available, or an error
func (psi *partitionSliceIterator) NextPartition() (sif.Partition, error) {
	psi.lock.Lock()
	defer psi.lock.Unlock()
	if psi.next >= len(psi.partitions) {
		for _, l := range psi.endListeners {
			l()
		}
		psi.endListeners = []func(){}
		return nil, errors.NoMorePartitionsError{}
	}
	part := psi.partitions[psi.next]
	psi.next++
	return part, nil
}

// PartitionLoaderIterator produces Partitions from PartitionLoaders
type partitionLoaderIterator struct {
	partitionLoaders           []sif.PartitionLoader
	partitionGroup             sif.PartitionIterator
	parser                     sif.DataSourceParser
	widestInitialPrivateSchema sif.Schema
	next                       int
	lock                       sync.Mutex
	endListeners               []func()
}

func createPartitionLoaderIterator(partitionLoaders []sif.PartitionLoader, parser sif.DataSourceParser, widestInitialSchema sif.Schema) sif.PartitionIterator {
	return &partitionLoaderIterator{
		partitionLoaders:           partitionLoaders,
		partitionGroup:             nil,
		parser:                     parser,
		widestInitialPrivateSchema: widestInitialSchema,
		next:                       0,
		endListeners:               []func(){},
	}
}

// OnEnd registers a listener which fires when this iterator runs out of Partitions
func (pli *partitionLoaderIterator) OnEnd(onEnd func()) {
	pli.lock.Lock()
	defer pli.lock.Unlock()
	pli.endListeners = append(pli.endListeners, onEnd)
}

func (pli *partitionLoaderIterator) HasNextPartition() bool {
	pli.lock.Lock()
	defer pli.lock.Unlock()
	return pli.next < len(pli.partitionLoaders) || pli.partitionGroup.HasNextPartition()
}

func (pli *partitionLoaderIterator) NextPartition() (sif.Partition, error) {
	pli.lock.Lock()
	defer pli.lock.Unlock()
	// TODO switch to round robin across all loaders, for streaming data
	// grab the next group of partitions from the Loader iterator if necessary
	if pli.partitionGroup == nil || !pli.partitionGroup.HasNextPartition() {
		if pli.next >= len(pli.partitionLoaders) {
			for _, l := range pli.endListeners {
				l()
			}
			pli.endListeners = []func(){}
			return nil, errors.NoMorePartitionsError{}
		}
		l := pli.partitionLoaders[pli.next]
		pli.next++
		partGroup, err := l.Load(pli.parser, pli.widestInitialPrivateSchema) // load data into a partition wide enough to accommodate upcoming column adds
		if err != nil {
			return nil, err
		}
		pli.partitionGroup = partGroup
	}
	// return the next partition from the existing group
	part, err := pli.partitionGroup.NextPartition()
	if err != nil {
		return nil, err
	}
	return part, nil
}

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

func (pci *partitionCacheIterator) NextPartition() (sif.Partition, error) {
	pci.lock.Lock()
	defer pci.lock.Unlock()
	if pci.next >= len(pci.keys) {
		for _, l := range pci.endListeners {
			l()
		}
		pci.endListeners = []func(){}
		return nil, errors.NoMorePartitionsError{}
	}
	p := pci.partitions[pci.keys[pci.next]]
	if pci.destructive {
		delete(pci.partitions, pci.keys[pci.next])
	}
	pci.next++
	return p, nil
}

// pTreePartitionIterator iterates over Partitions in a pTree, starting at the bottom left.
// It optionally destroys the tree as it does this.
type pTreePartitionIterator struct {
	next         *pTreeNode
	destructive  bool
	lock         sync.Mutex
	endListeners []func()
}

func createPTreeIterator(tree *pTreeRoot, destructive bool) sif.PartitionIterator {
	if tree == nil {
		return &pTreePartitionIterator{next: nil, destructive: destructive, endListeners: []func(){}}
	}
	return &pTreePartitionIterator{next: tree.firstNode(), destructive: destructive, endListeners: []func(){}}
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
	return tpi.next != nil
}

func (tpi *pTreePartitionIterator) NextPartition() (sif.Partition, error) {
	tpi.lock.Lock()
	defer tpi.lock.Unlock()
	if tpi.next == nil {
		for _, l := range tpi.endListeners {
			l()
		}
		tpi.endListeners = []func(){}
		return nil, errors.NoMorePartitionsError{}
	}
	part, err := tpi.next.loadPartition() // temp var for partition
	if err != nil {
		return nil, err
	}
	toDestroy := tpi.next
	tpi.next = tpi.next.next // advance iterator
	// garbage collection
	if tpi.destructive {
		toDestroy.lruCache.Remove(*toDestroy.partID)
		toDestroy.partID = nil // delete reference to partition
		// walk up the tree and wipe out empty nodes
		for n := toDestroy; n != nil && n.left == nil && n.right == nil && n.center == nil; {
			toDestroy := n
			n = n.parent
			if n == nil {
				break
			}
			if n.left == toDestroy {
				n.left = nil
			} else {
				n.right = nil
			}
		}
		toDestroy = nil // delete node
	}
	return part, nil
}
