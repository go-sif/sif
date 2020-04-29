package dataframe

import (
	"github.com/go-sif/sif"
)

// A dataFrameImpl implements DataFrame internally for Sif
type dataFrameImpl struct {
	parent        *dataFrameImpl       // the parent DataFrame. Nil if this is the root.
	task          sif.Task             // the task represented by this DataFrame, executed to produce the next one
	taskType      sif.TaskType         // a unique name for the type of task this DataFrame represents
	source        sif.DataSource       // the source of the data
	parser        sif.DataSourceParser // the parser for the source data
	publicSchema  sif.Schema           // the operation-facing schema of the data at this task. will omit columns which have been removed.
	privateSchema sif.Schema           // the internal schema of the data at this task.  will include columns which have been removed (until a repack).
}

// CreateDataFrame is a factory for DataFrames. This function is not intended to be used directly,
// as DataFrames are returned by DataSource packages.
func CreateDataFrame(source sif.DataSource, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	return &dataFrameImpl{
		parent:        nil,
		task:          &noOpTask{},
		taskType:      sif.ExtractTaskType,
		source:        source,
		parser:        parser,
		publicSchema:  schema,
		privateSchema: schema,
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
func (df *dataFrameImpl) To(ops ...sif.DataFrameOperation) (sif.DataFrame, error) {
	next := df
	// See https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis for details of approach
	for _, op := range ops {
		result, err := op(next)
		if err != nil {
			return nil, err
		}
		next = &dataFrameImpl{
			parent:        next,
			source:        df.source,
			task:          result.Task,
			taskType:      result.TaskType,
			parser:        df.parser,
			publicSchema:  result.PublicSchema,
			privateSchema: result.PrivateSchema,
		}
	}
	return next, nil
}
