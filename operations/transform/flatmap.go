package transform

import (
	core "github.com/go-sif/sif/core"
)

type flatMapTask struct {
	fn core.FlatMapOperation
}

func (s *flatMapTask) RunWorker(previous *core.Partition) ([]*core.Partition, error) {
	results, err := previous.FlatMapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// FlatMap transforms a Row, potentially producing new rows
func FlatMap(fn core.FlatMapOperation) core.DataFrameOperation {
	return func(d *core.DataFrame) (core.Task, string, *core.Schema, error) {
		nextTask := flatMapTask{fn: core.SafeFlatMapOperation(fn)}
		return &nextTask, "flatmap", d.GetSchema().Clone(), nil
	}
}
