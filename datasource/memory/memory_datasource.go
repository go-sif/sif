package memory

import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/dataframe"
)

// DataSource is a buffer containing data which will be manipulating according to a DataFrame
type DataSource struct {
	data   [][]byte
	schema sif.Schema
}

// CreateDataFrame is a factory for DataSources
func CreateDataFrame(data [][]byte, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	source := &DataSource{data, schema}
	df := dataframe.CreateDataFrame(source, parser, schema)
	return df
}

// Analyze returns a PartitionMap, describing how the source data will be divided into Partitions
func (fs *DataSource) Analyze() (sif.PartitionMap, error) {
	return &PartitionMap{
		source: fs,
	}, nil
}

// DeserializeLoader creates a PartitionLoader for this DataSource from a serialized representation
func (fs *DataSource) DeserializeLoader(bytes []byte) (sif.PartitionLoader, error) {
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
