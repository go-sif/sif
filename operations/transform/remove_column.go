package transform

import (
	"github.com/go-sif/sif"
)

// removeColumnTask is a task that does nothing
type removeColumnTask struct{}

// RunWorker for removeColumnTask does nothing
func (s *removeColumnTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}

// RemoveColumn removes existing columns
func RemoveColumn(oldNames ...string) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.RemoveColumnTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			newSchema := d.GetPublicSchema().Clone()
			for _, oldName := range oldNames {
				newSchema, _ = newSchema.RemoveColumn(oldName)
			}
			return &sif.DataFrameOperationResult{
				Task:          &removeColumnTask{},
				PublicSchema:  newSchema,
				PrivateSchema: d.GetPrivateSchema().Clone(), // removing a column doesn't affect the private schema
			}, nil
		},
	}
}
