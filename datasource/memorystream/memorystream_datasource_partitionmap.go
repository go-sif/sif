package memorystream

import "github.com/go-sif/sif/types"

// PartitionMap is an iterator producing a sequence of PartitionLoaders
type PartitionMap struct {
	idx    int
	source *DataSource
}

// HasNext returns true iff there is another PartitionLoader remaining
func (pm *PartitionMap) HasNext() bool {
	return pm.idx < len(pm.source.generators)
}

// Next returns the next PartitionLoader for a data generator
func (pm *PartitionMap) Next() types.PartitionLoader {
	result := &PartitionLoader{idx: pm.idx, source: pm.source}
	pm.idx++
	return result
}
