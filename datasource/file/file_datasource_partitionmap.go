package file

import (
	core "github.com/go-sif/sif/v0.0.1/core"
)

// PartitionMap is an iterator producing a sequence of PartitionLoaders
type PartitionMap struct {
	files  []string
	source *DataSource
}

// HasNext returns true iff there is another PartitionLoader remaining
func (pm *PartitionMap) HasNext() bool {
	return len(pm.files) > 0
}

// Next returns the next PartitionLoader for a file
func (pm *PartitionMap) Next() core.PartitionLoader {
	result := &PartitionLoader{path: pm.files[0], source: pm.source}
	pm.files = pm.files[1:]
	return result
}
