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

// RemoveColumn marks existing columns for removal at the end of the current stage
func RemoveColumn(oldNames ...string) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.RemoveColumnTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			newSchema := d.GetSchema().Clone()
			for _, oldName := range oldNames {
				newSchema, _ = newSchema.RemoveColumn(oldName)
			}
			return &sif.DataFrameOperationResult{
				Task:       &removeColumnTask{},
				DataSchema: newSchema,
			}, nil
		},
	}
}
