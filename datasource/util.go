package datasource

import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/dataframe"
	"github.com/go-sif/sif/internal/partition"
)

// CreateDataFrame produces a fresh DataFrame (useful for the implementation of DataSources)
func CreateDataFrame(source sif.DataSource, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	return dataframe.CreateDataFrame(source, parser, schema)
}

// CreateBuildablePartition creates a new Partition containing empty data and a Schema
func CreateBuildablePartition(maxRows int, widestSchema sif.Schema, currentSchema sif.Schema) sif.BuildablePartition {
	return partition.CreateBuildablePartition(maxRows, widestSchema, currentSchema)
}
