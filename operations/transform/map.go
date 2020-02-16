package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type mapTask struct {
	fn sif.MapOperation
}

func (s *mapTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	next, err := previous.MapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{next}, nil
}

// Map transforms a Row in-place
func Map(fn sif.MapOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, string, sif.Schema, error) {
		nextTask := mapTask{fn: iutil.SafeMapOperation(fn)}
		return &nextTask, "map", d.GetSchema().Clone(), nil
	}
}
