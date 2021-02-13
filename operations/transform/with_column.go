package transform

import (
	"github.com/go-sif/sif"
)

// addColumnTask is a task that does nothing
type addColumnTask struct{}

func (s *addColumnTask) RunInitialize(sctx sif.StageContext) error {
	return nil
}

// RunWorker for addColumnTask does nothing
func (s *addColumnTask) RunWorker(sctx sif.StageContext, previous sif.OperablePartition) ([]sif.OperablePartition, error) {
	return []sif.OperablePartition{previous}, nil
}

// AddColumn declares that a new (empty) column with a
// specific type and name should be available to the
// next Task of the DataFrame pipeline
func AddColumn(colName string, colType sif.ColumnType) *sif.DataFrameOperation {
	return &sif.DataFrameOperation{
		TaskType: sif.WithColumnTaskType,
		Do: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			newSchema, err := d.GetSchema().Clone().CreateColumn(colName, colType)
			if err != nil {
				return nil, err
			}
			return &sif.DataFrameOperationResult{
				Task:       &addColumnTask{},
				DataSchema: newSchema,
			}, nil
		},
	}
}
