package transform

import (
	"github.com/go-sif/sif"
	iutil "github.com/go-sif/sif/internal/util"
)

// Group shuffles rows across workers, using a key - useful for grouping buckets of data together on single workers
func Group(kfn sif.KeyingOperation) sif.DataFrameOperation {
	return func(d sif.DataFrame) (sif.Task, sif.TaskType, sif.Schema, error) {
		nextTask := repartitionTask{
			kfn:                 iutil.SafeKeyingOperation(kfn),
			targetPartitionSize: -1,
		}
		return &nextTask, sif.ShuffleTaskType, d.GetSchema().Clone(), nil
	}
}
