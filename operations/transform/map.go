package transform

import (
	core "github.com/go-sif/sif/v0.0.1/core"
)

type mapTask struct {
	fn core.MapOperation
}

func (s *mapTask) RunWorker(previous *core.Partition) ([]*core.Partition, error) {
	next, err := previous.MapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []*core.Partition{next}, nil
}

// Map transforms a Row in-place
func Map(fn core.MapOperation) core.DataFrameOperation {
	return func(d *core.DataFrame) (core.Task, string, *core.Schema, error) {
		nextTask := mapTask{fn: core.SafeMapOperation(fn)}
		return &nextTask, "map", d.GetSchema().Clone(), nil
	}
}
