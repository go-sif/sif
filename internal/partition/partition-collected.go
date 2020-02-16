package partition

import "github.com/go-sif/sif"

// createCollectedPartitionPartition creates a new Partition containing an empty byte array and a schema
func createCollectedPartitionPartition(maxRows int, widestSchema sif.Schema, currentSchema sif.Schema) sif.CollectedPartition {
	return createPartitionImpl(maxRows, widestSchema, currentSchema)
}

// ForEachRow runs a MapOperation on each row in this Partition, erroring immediately if an error occurs
func (p *partitionImpl) ForEachRow(fn sif.MapOperation) error {
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.GetRow(i)
		err := fn(row)
		if err != nil {
			return err
		}
	}
	return nil
}
