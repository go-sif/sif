package memorystream

import (
	core "github.com/go-sif/sif/v0.0.1/core"
)

// DataSource is a buffer containing data which will be manipulating according to a DataFrame
type DataSource struct {
	generators []func() []byte
	batchSize  int // the number of records to process at one time
	schema     *core.Schema
}

// CreateDataFrame is a factory for DataSources
func CreateDataFrame(generators []func() []byte, batchSize int, parser core.DataSourceParser, schema *core.Schema) *core.DataFrame {
	source := &DataSource{generators, batchSize, schema}
	df := core.CreateDataFrame(source, parser, schema)
	return df
}

// Analyze returns a PartitionMap, describing how the source data will be divided into Partitions
func (ms *DataSource) Analyze() (core.PartitionMap, error) {
	return &PartitionMap{
		source: ms,
	}, nil
}

// DeserializeLoader creates a PartitionLoader for this DataSource from a serialized representation
func (ms *DataSource) DeserializeLoader(bytes []byte) (core.PartitionLoader, error) {
	// idx represents an index into the list of generators. Each loader represents a generator.
	pl := PartitionLoader{idx: 0, source: ms}
	err := pl.GobDecode(bytes)
	if err != nil {
		return nil, err
	}
	return &pl, nil
}

// IsStreaming returns true iff this DataSource provides a continuous stream of data
func (ms *DataSource) IsStreaming() bool {
	return true
}
