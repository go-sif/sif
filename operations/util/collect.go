package util

import (
	"fmt"

	"github.com/go-sif/sif"
)

type collectTask struct {
	collectionLimit int32
}

func (s *collectTask) Name() string {
	return "collect"
}

func (s *collectTask) RunWorker(previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	// do nothing
	return []sif.OperablePartition{previous}, nil
}

func (s *collectTask) GetCollectionLimit() int32 {
	return s.collectionLimit
}

// Collect declares that data should be shufled to the Coordinator
// upon completion of the previous stage. This also signals
// the end of a Dataframe's tasks.
func Collect(collectionLimit int32) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.CollectTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			if d.GetDataSource().IsStreaming() {
				return nil, fmt.Errorf("Cannot collect() from a streaming DataSource")
			}
			return &sif.DataFrameOperationResult{
				Task:       &collectTask{collectionLimit},
				DataSchema: d.GetSchema().Clone(),
			}, nil
		},
	}
}
