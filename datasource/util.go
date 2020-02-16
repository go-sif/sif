package datasource

import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/dataframe"
)

// CreateDataFrame produces a fresh DataFrame (useful for the implementation of DataSources)
func CreateDataFrame(source sif.DataSource, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	return dataframe.CreateDataFrame(source, parser, schema)
}
