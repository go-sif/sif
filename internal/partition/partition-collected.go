package partition

import "github.com/go-sif/sif"

// createCollectedPartition creates a new Partition containing an empty byte array and a schema
func createCollectedPartition(maxRows int, schema sif.Schema) sif.CollectedPartition {
	return createPartitionImpl(maxRows, defaultCapacity, schema)
}

// ForEachRow runs a MapOperation on each row in this Partition, erroring immediately if an error occurs
func (p *partitionImpl) ForEachRow(fn sif.MapOperation) error {
	row := &rowImpl{}
	for i := 0; i < p.GetNumRows(); i++ {
		err := fn(p.getRow(row, i))
		if err != nil {
			return err
		}
	}
	return nil
}
