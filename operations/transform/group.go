package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// Group shuffles rows across workers, using a key - useful for grouping buckets of data together on single workers
func Group(kfn sif.KeyingOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
		return &sif.DataFrameOperationResult{
			Task: &repartitionTask{
				kfn:                 iutil.SafeKeyingOperation(kfn),
				targetPartitionSize: -1,
			},
			TaskType:      sif.ShuffleTaskType,
			PublicSchema:  d.GetPublicSchema().Clone(),
			PrivateSchema: d.GetPrivateSchema().Clone(),
		}, nil
	}
}
