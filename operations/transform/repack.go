package transform

import (
	core "github.com/go-sif/sif/core"
)

type repackTask struct {
	newSchema *core.Schema
}

func (s *repackTask) RunWorker(previous *core.Partition) ([]*core.Partition, error) {
	part, err := previous.Repack(s.newSchema)
	if err != nil {
		return nil, err
	}
	return []*core.Partition{part}, nil
}

// Repack rearranges memory layout of rows to respect a new schema
func Repack() core.DataFrameOperation {
	return func(d *core.DataFrame) (core.Task, string, *core.Schema, error) {
		nextTask := repackTask{d.GetSchema().Repack()}
		return &nextTask, "repack", nextTask.newSchema, nil
	}
}
