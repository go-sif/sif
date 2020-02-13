package memory

import (
	core "github.com/go-sif/sif/core"
)

// DataSource is a buffer containing data which will be manipulating according to a DataFrame
type DataSource struct {
	data   [][]byte
	schema *core.Schema
}

// CreateDataFrame is a factory for DataSources
func CreateDataFrame(data [][]byte, parser core.DataSourceParser, schema *core.Schema) *core.DataFrame {
	source := &DataSource{data, schema}
	df := core.CreateDataFrame(source, parser, schema)
	return df
}

// Analyze returns a PartitionMap, describing how the source data will be divided into Partitions
func (fs *DataSource) Analyze() (core.PartitionMap, error) {
	return &PartitionMap{
		source: fs,
	}, nil
}

// DeserializeLoader creates a PartitionLoader for this DataSource from a serialized representation
func (fs *DataSource) DeserializeLoader(bytes []byte) (core.PartitionLoader, error) {
	pl := PartitionLoader{idx: 0, source: fs}
	err := pl.GobDecode(bytes)
	if err != nil {
		return nil, err
	}
	return &pl, nil
}

// IsStreaming returns true iff this DataSource provides a continuous stream of data
func (fs *DataSource) IsStreaming() bool {
	return false
}
