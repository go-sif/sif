package util

import (
	"fmt"

	core "github.com/go-sif/sif/core"
)

type collectTask struct {
	collectionLimit int64
}

func (s *collectTask) Name() string {
	return "collect"
}

func (s *collectTask) RunWorker(previous core.OperablePTition) ([]core.OperablePTition, error) {
	// do nothing
	return []core.OperablePTition{previous}, nil
}

func (s *collectTask) GetCollectionLimit() int64 {
	return s.collectionLimit
}

// Collect declares that data should be shufled to the Coordinator
// upon completion of the previous stage. This also signals
// the end of a Dataframe's tasks.
func Collect(collectionLimit int64) core.DataFrameOperation {
	return func(d core.DataFrame) (core.Task, string, *core.Schema, error) {
		if d.GetDataSource().IsStreaming() {
			return nil, "collect", nil, fmt.Errorf("Cannot collect() from a streaming DataSource")
		}
		newSchema := d.GetSchema().Clone()
		nextTask := &collectTask{collectionLimit}
		return nextTask, "collect", newSchema, nil
	}
}
