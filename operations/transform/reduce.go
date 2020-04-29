package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// With inspiration from:
// https://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/
// https://github.com/cespare/xxhash/v2

type reduceTask struct {
	kfn                 sif.KeyingOperation
	fn                  sif.ReductionOperation
	targetPartitionSize int
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

func (s *reduceTask) GetTargetPartitionSize() int {
	return s.targetPartitionSize
}

// Reduce combines rows across workers, using a key
func Reduce(kfn sif.KeyingOperation, fn sif.ReductionOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
		return &sif.DataFrameOperationResult{
			Task: &reduceTask{
				kfn:                 iutil.SafeKeyingOperation(kfn),
				fn:                  iutil.SafeReductionOperation(fn),
				targetPartitionSize: -1,
			},
			TaskType:      sif.ShuffleTaskType,
			PublicSchema:  d.GetPublicSchema().Clone(),
			PrivateSchema: d.GetPrivateSchema().Clone(),
		}, nil
	}
}
