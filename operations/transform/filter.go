package transform

import (
	core "github.com/go-sif/sif/core"
)

type filterTask struct {
	fn core.FilterOperation
}

func (s *filterTask) RunWorker(previous *core.Partition) ([]*core.Partition, error) {
	result, err := previous.FilterRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []*core.Partition{result}, nil
}

// Filter filters Rows out of a Partition, creating a new one
func Filter(fn core.FilterOperation) core.DataFrameOperation {
	return func(d core.DataFrame) (core.Task, string, *core.Schema, error) {
		nextTask := filterTask{fn: core.SafeFilterOperation(fn)}
		return &nextTask, "filter", d.GetSchema().Clone(), nil
	}
}
