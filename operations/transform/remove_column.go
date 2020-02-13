package transform

import (
	core "github.com/go-sif/sif/core"
)

// removeColumnTask is a task that does nothing
type removeColumnTask struct{}

// RunWorker for removeColumnTask does nothing
func (s *removeColumnTask) RunWorker(previous *core.Partition) ([]*core.Partition, error) {
	return []*core.Partition{previous}, nil
}

// RemoveColumn removes existing columns
func RemoveColumn(oldNames ...string) core.DataFrameOperation {
	return func(d *core.DataFrame) (core.Task, string, *core.Schema, error) {
		newSchema := d.GetSchema().Clone()
		for _, oldName := range oldNames {
			newSchema, _ = newSchema.RemoveColumn(oldName)
		}
		nextTask := &removeColumnTask{}
		return nextTask, "remove_column", newSchema, nil
	}
}
