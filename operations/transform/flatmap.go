package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

type flatMapTask struct {
	fn sif.FlatMapOperation
}

func (s *flatMapTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	results, err := previous.FlatMapRows(s.fn)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// FlatMap transforms a Row, potentially producing new rows
func FlatMap(fn sif.FlatMapOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, string, sif.Schema, error) {
		nextTask := flatMapTask{fn: iutil.SafeFlatMapOperation(fn)}
		return &nextTask, "flatmap", d.GetSchema().Clone(), nil
	}
}
