package transform

import "github.com/go-sif/sif"

// removeColumnTask is a task that does nothing
type removeColumnTask struct{}

// RunWorker for removeColumnTask does nothing
func (s *removeColumnTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}

// RemoveColumn removes existing columns
func RemoveColumn(oldNames ...string) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, string, sif.Schema, error) {
		newSchema := d.GetSchema().Clone()
		for _, oldName := range oldNames {
			newSchema, _ = newSchema.RemoveColumn(oldName)
		}
		nextTask := &removeColumnTask{}
		return nextTask, "remove_column", newSchema, nil
	}
}
