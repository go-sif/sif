package hashmap

import (
	"github.com/go-sif/sif"
)

type pMap struct {
	ids             []string
	cache           sif.PartitionCache
	nextStageSchema sif.Schema
}

// CreateMapPartitionIndex creates a new Map-based PartitionIndex suitable for reduction
func CreateMapPartitionIndex(cache sif.PartitionCache, nextStageSchema sif.Schema) sif.PartitionIndex {
	return &pMap{
		ids:             make([]string, 0, 10),
		cache:           cache,
		nextStageSchema: nextStageSchema,
	}
}

func (m *pMap) SetMaxRows(maxRows int) {
	// do nothing, since pMap never creates partitions
}

func (m *pMap) GetNextStageSchema() sif.Schema {
	return m.nextStageSchema
}

func (m *pMap) MergePartition(part sif.BuildablePartition, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error {
	m.cache.Add(part.ID(), part.(sif.ReduceablePartition))
	m.ids = append(m.ids, part.ID())
	return nil
}

func (m *pMap) MergeRow(tempRow sif.Row, row sif.Row, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error {
	panic("not implemented")
}

func (m *pMap) GetPartitionIterator(destructive bool) sif.PartitionIterator {
	panic("not implemented")
}

func (m *pMap) GetSerializedPartitionIterator(destructive bool) sif.SerializedPartitionIterator {
	return createPartitionMapSerializedIterator(m.ids, m.cache, true)
}

func (m *pMap) NumPartitions() uint64 {
	return uint64(len(m.ids))
}

func (m *pMap) CacheSize() int {
	return m.cache.CurrentSize()
}

func (m *pMap) ResizeCache(frac float64) bool {
	panic("not implemented")
}

func (m *pMap) Destroy() {
	m.cache.Destroy()
}
