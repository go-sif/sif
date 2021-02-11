package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// Group shuffles rows across workers, using a key - useful for grouping buckets of data together on single workers
func Group(kfn sif.KeyingOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.ShuffleTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &reduceTask{
					kfn:                 iutil.SafeKeyingOperation(kfn),
					fn:                  nil,
					targetPartitionSize: -1,
				},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
