package transform

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
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
		return nil, fmt.Errorf("Unable to key rows: %e", err)
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
func Reduce(kfn sif.KeyingOperation, fn sif.ReductionOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.ShuffleTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &reduceTask{
					kfn:                 iutil.SafeKeyingOperation(kfn),
					fn:                  iutil.SafeReductionOperation(fn),
					targetPartitionSize: -1,
				},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}

// KeyColumns creates a KeyingOperation which uses multiple column values to produce a compound key.
func KeyColumns(colNames ...string) sif.KeyingOperation {
	return func(row sif.Row) ([]byte, error) {
		irow := row.(itypes.AccessibleRow) // access row internals
		hasher := xxhash.New()
		for _, cname := range colNames {
			data, err := irow.GetColData(cname)
			if err != nil {
				return nil, err
			}
			hasher.Write(data)
		}
		return hasher.Sum(nil), nil
	}
}
