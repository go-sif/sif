package transform

import types "github.com/go-sif/sif/types"

// removeColumnTask is a task that does nothing
type removeColumnTask struct{}

// RunWorker for removeColumnTask does nothing
func (s *removeColumnTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	return []types.OperablePartition{previous}, nil
}

// RemoveColumn removes existing columns
func RemoveColumn(oldNames ...string) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		newSchema := d.GetSchema().Clone()
		for _, oldName := range oldNames {
			newSchema, _ = newSchema.RemoveColumn(oldName)
		}
		nextTask := &removeColumnTask{}
		return nextTask, "remove_column", newSchema, nil
	}
}
