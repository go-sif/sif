package file

import (
	"fmt"
	"path/filepath"

	core "github.com/go-sif/sif/core"
	"github.com/go-sif/sif/types"
)

// DataSource is a file containing data which will be manipulating according to a DataFrame
type DataSource struct {
	glob   string
	schema types.Schema
}

// CreateDataFrame is a factory for DataSources
func CreateDataFrame(glob string, parser types.DataSourceParser, schema types.Schema) types.DataFrame {
	source := &DataSource{glob, schema}
	df := core.CreateDataFrame(source, parser, schema)
	return df
}

// Analyze returns a PartitionMap, describing how the source file will be divided into Partitions
func (fs *DataSource) Analyze() (types.PartitionMap, error) {
	matches, err := filepath.Glob(fs.glob)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("glob %s produced 0 files", fs.glob)
	}
	var toRead []string
	for _, path := range matches {
		toRead = append(toRead, path)
	}
	return &PartitionMap{
		files:  toRead,
		source: fs,
	}, nil
}

// DeserializeLoader creates a PartitionLoader for this DataSource from a serialized representation
func (fs *DataSource) DeserializeLoader(bytes []byte) (types.PartitionLoader, error) {
	pl := PartitionLoader{path: "", source: fs}
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
