package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// Repartition is identical to Group, with the added ability to change the
// number of rows per partition during the shuffle
func Repartition(targetPartitionSize int, kfn sif.KeyingOperation) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.ShuffleTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task: &reduceTask{
					kfn:                 iutil.SafeKeyingOperation(kfn),
					fn:                  nil,
					targetPartitionSize: targetPartitionSize,
				},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
