package dataframe

import (
	"github.com/go-sif/sif"
)

// A dataFrameImpl implements DataFrame internally for Sif
type dataFrameImpl struct {
	parent   *dataFrameImpl // the parent DataFrame. Nil if this is the root.
	apply    func(sif.DataFrame) (*sif.DataFrameOperationResult, error)
	taskType sif.TaskType         // a unique name for the type of task this DataFrame represents
	source   sif.DataSource       // the source of the data
	parser   sif.DataSourceParser // the parser for the source data
	// to be set by apply
	task          sif.Task
	publicSchema  sif.Schema
	privateSchema sif.Schema
}

// CreateDataFrame is a factory for DataFrames. This function is not intended to be used directly,
// as DataFrames are returned by DataSource packages.
func CreateDataFrame(source sif.DataSource, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	return &dataFrameImpl{
		parent: nil,
		apply: func(d sif.DataFrame) (*sif.DataFrameOperationResult, error) {
			return &sif.DataFrameOperationResult{
				Task:          &noOpTask{},
				PublicSchema:  schema,
				PrivateSchema: schema,
			}, nil
		},
		taskType: sif.ExtractTaskType,
		source:   source,
		parser:   parser,
	}
}

// Clone clones this dataframe. Must call df.apply() again on the result.
func (df *dataFrameImpl) Clone() *dataFrameImpl {
	var parent *dataFrameImpl
	if df.parent != nil {
		parent = df.parent.Clone()
	}
	return &dataFrameImpl{
		parent:   parent,
		apply:    df.apply,
		taskType: df.taskType,
		source:   df.source,
		parser:   df.parser,
	}
}

// GetPublicSchema returns the public Schema of a DataFrame
func (df *dataFrameImpl) GetPublicSchema() sif.Schema {
	return df.publicSchema
}

// GetPrivateSchema returns the private Schema of a DataFrame
func (df *dataFrameImpl) GetPrivateSchema() sif.Schema {
	return df.privateSchema
}

// GetDataSource returns the DataSource of a DataFrame
func (df *dataFrameImpl) GetDataSource() sif.DataSource {
	return df.source
}

// GetParser returns the DataSourceParser of a DataFrame
func (df *dataFrameImpl) GetParser() sif.DataSourceParser {
	return df.parser
}

// To is a "functional operations" factory method for DataFrames,
// chaining operations onto the current one(s).
func (df *dataFrameImpl) To(ops ...*sif.DataFrameOperation) (sif.DataFrame, error) {
	next := df
	// See https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis for details of approach
	for _, op := range ops {
		next = &dataFrameImpl{
			parent:   next,
			apply:    op.Do,
			taskType: op.TaskType,
			source:   df.source,
			parser:   df.parser,
		}
	}
	return next, nil
}
