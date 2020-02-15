package transform

import (
	iutil "github.com/go-sif/sif/internal/util"
	types "github.com/go-sif/sif/types"
)

// With inspiration from:
// https://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/
// https://github.com/cespare/xxhash

type reduceTask struct {
	kfn types.KeyingOperation
	fn  types.ReductionOperation
}

func (s *reduceTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	// Start by keying the rows in the partition
	part, err := previous.KeyRows(s.kfn)
	if err != nil {
		return nil, err
	}
	return []types.OperablePartition{part}, nil
}

func (s *reduceTask) GetKeyingOperation() types.KeyingOperation {
	return s.kfn
}

func (s *reduceTask) GetReductionOperation() types.ReductionOperation {
	return s.fn
}

// Reduce combines rows across workers, using a key
func Reduce(kfn types.KeyingOperation, fn types.ReductionOperation) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		nextTask := reduceTask{
			kfn: iutil.SafeKeyingOperation(kfn),
			fn:  iutil.SafeReductionOperation(fn),
		}
		return &nextTask, "reduce", d.GetSchema().Clone(), nil
	}
}
