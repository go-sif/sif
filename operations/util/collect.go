package util

import (
	"fmt"

	"github.com/go-sif/sif/types"
)

type collectTask struct {
	collectionLimit int64
}

func (s *collectTask) Name() string {
	return "collect"
}

func (s *collectTask) RunWorker(previous types.OperablePartition) ([]types.OperablePartition, error) {
	// do nothing
	return []types.OperablePartition{previous}, nil
}

func (s *collectTask) GetCollectionLimit() int64 {
	return s.collectionLimit
}

// Collect declares that data should be shufled to the Coordinator
// upon completion of the previous stage. This also signals
// the end of a Dataframe's tasks.
func Collect(collectionLimit int64) types.DataFrameOperation {
	return func(d types.DataFrame) (types.Task, string, types.Schema, error) {
		if d.GetDataSource().IsStreaming() {
			return nil, "collect", nil, fmt.Errorf("Cannot collect() from a streaming DataSource")
		}
		newSchema := d.GetSchema().Clone()
		nextTask := &collectTask{collectionLimit}
		return nextTask, "collect", newSchema, nil
	}
}
