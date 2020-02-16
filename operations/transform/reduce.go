package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// With inspiration from:
// https://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/
// https://github.com/cespare/xxhash

type reduceTask struct {
	kfn sif.KeyingOperation
	fn  sif.ReductionOperation
}

func (s *reduceTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	// Start by keying the rows in the partition
	part, err := previous.KeyRows(s.kfn)
	if err != nil {
		return nil, err
	}
	return []sif.OperablePartition{part}, nil
}

func (s *reduceTask) GetKeyingOperation() sif.KeyingOperation {
	return s.kfn
}

func (s *reduceTask) GetReductionOperation() sif.ReductionOperation {
	return s.fn
}

// Reduce combines rows across workers, using a key
func Reduce(kfn sif.KeyingOperation, fn sif.ReductionOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, string, sif.Schema, error) {
		nextTask := reduceTask{
			kfn: iutil.SafeKeyingOperation(kfn),
			fn:  iutil.SafeReductionOperation(fn),
		}
		return &nextTask, "reduce", d.GetSchema().Clone(), nil
	}
}
