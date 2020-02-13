package transform

import (
	core "github.com/go-sif/sif/core"
)

// With inspiration from:
// https://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/
// https://github.com/cespare/xxhash

type reduceTask struct {
	kfn core.KeyingOperation
	fn  core.ReductionOperation
}

func (s *reduceTask) RunWorker(previous core.OperablePTition) ([]core.OperablePTition, error) {
	// Start by keying the rows in the partition
	part, err := previous.KeyRows(s.kfn)
	if err != nil {
		return nil, err
	}
	return []core.OperablePTition{part}, nil
}

func (s *reduceTask) GetKeyingOperation() core.KeyingOperation {
	return s.kfn
}

func (s *reduceTask) GetReductionOperation() core.ReductionOperation {
	return s.fn
}

// Reduce combines rows across workers, using a key
func Reduce(kfn core.KeyingOperation, fn core.ReductionOperation) core.DataFrameOperation {
	return func(d core.DataFrame) (core.Task, string, *core.Schema, error) {
		nextTask := reduceTask{
			kfn: core.SafeKeyingOperation(kfn),
			fn:  core.SafeReductionOperation(fn),
		}
		return &nextTask, "reduce", d.GetSchema().Clone(), nil
	}
}
