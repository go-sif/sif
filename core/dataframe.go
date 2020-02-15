package core

import (
	"github.com/go-sif/sif/types"
)

// A dataFrameImpl implements DataFrame internally for Sif
type dataFrameImpl struct {
	parent   *dataFrameImpl         // the parent DataFrame. Nil if this is the root.
	task     types.Task             // the task represented by this DataFrame, executed to produce the next one
	taskType string                 // a unique name for the type of task this DataFrame represents
	source   types.DataSource       // the source of the data
	parser   types.DataSourceParser // the parser for the source data
	schema   types.Schema           // the current schema of the data at this task
}

// CreateDataFrame is a factory for DataFrames. This function is not intended to be used directly,
// as DataFrames are returned by DataSource packages.
func CreateDataFrame(source types.DataSource, parser types.DataSourceParser, schema types.Schema) types.DataFrame {
	return &dataFrameImpl{
		parent:   nil,
		task:     &noOpTask{},
		taskType: "extract",
		source:   source,
		parser:   parser,
		schema:   schema,
	}
}

// GetSchema returns the Schema of a DataFrame
func (df *dataFrameImpl) GetSchema() types.Schema {
	return df.schema
}

// GetDataSource returns the DataSource of a DataFrame
func (df *dataFrameImpl) GetDataSource() types.DataSource {
	return df.source
}

// GetParser returns the DataSourceParser of a DataFrame
func (df *dataFrameImpl) GetParser() types.DataSourceParser {
	return df.parser
}

// To is a "functional operations" factory method for DataFrames,
// chaining operations onto the current one(s).
func (df *dataFrameImpl) To(ops ...types.DataFrameOperation) (types.DataFrame, error) {
	next := df
	// See https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis for details of approach
	for _, op := range ops {
		nextTask, nextTaskType, nextSchema, err := op(next)
		if err != nil {
			return nil, err
		}
		next = &dataFrameImpl{
			parent:   next,
			source:   df.source,
			task:     nextTask,
			taskType: nextTaskType,
			parser:   df.parser,
			schema:   nextSchema,
		}
	}
	return next, nil
}
