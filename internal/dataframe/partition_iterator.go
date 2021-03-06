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
func (psi *partitionSliceIterator) NextPartition() (sif.Partition, func(), error) {
	psi.lock.Lock()
	defer psi.lock.Unlock()
	if psi.next >= len(psi.partitions) {
		for _, l := range psi.endListeners {
			l()
		}
		psi.endListeners = []func(){}
		return nil, nil, errors.NoMorePartitionsError{}
	}
	part := psi.partitions[psi.next]
	psi.next++
	return part, nil, nil
}

// PartitionLoaderIterator produces Partitions from PartitionLoaders
type partitionLoaderIterator struct {
	partitionLoaders    []sif.PartitionLoader
	partitionGroup      sif.PartitionIterator
	parser              sif.DataSourceParser
	widestInitialSchema sif.Schema
	next                int
	lock                sync.Mutex
	endListeners        []func()
}

func createPartitionLoaderIterator(partitionLoaders []sif.PartitionLoader, parser sif.DataSourceParser, widestInitialSchema sif.Schema) sif.PartitionIterator {
	return &partitionLoaderIterator{
		partitionLoaders:    partitionLoaders,
		partitionGroup:      nil,
		parser:              parser,
		widestInitialSchema: widestInitialSchema,
		next:                0,
		endListeners:        []func(){},
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

func (pli *partitionLoaderIterator) NextPartition() (sif.Partition, func(), error) {
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
			return nil, nil, errors.NoMorePartitionsError{}
		}
		l := pli.partitionLoaders[pli.next]
		pli.next++
		partGroup, err := l.Load(pli.parser, pli.widestInitialSchema) // load data into a partition wide enough to accommodate upcoming column adds
		if err != nil {
			return nil, nil, err
		}
		pli.partitionGroup = partGroup
	}
	// return the next partition from the existing group
	part, unlockPartition, err := pli.partitionGroup.NextPartition()
	if err != nil {
		return nil, nil, err
	}
	return part, unlockPartition, nil
}
