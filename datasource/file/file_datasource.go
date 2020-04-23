package file

import (
	"fmt"
	"path/filepath"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/datasource"
)

// DataSource is a set of files containing data which will be manipulating according to a DataFrame
type DataSource struct {
	conf   *DataSourceConf
	schema sif.Schema
	parser sif.DataSourceParser
}

// DataSourceConf configures a file DataSource
type DataSourceConf struct {
	Glob    string
	Decoder func([]byte) ([]byte, error)
}

// CreateDataFrame is a factory for DataSources
func CreateDataFrame(conf *DataSourceConf, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	source := &DataSource{conf: conf, parser: parser, schema: schema}
	df := datasource.CreateDataFrame(source, parser, schema)
	return df
}

// Analyze returns a PartitionMap, describing how the source file will be divided into Partitions
func (fs *DataSource) Analyze() (sif.PartitionMap, error) {
	matches, err := filepath.Glob(fs.conf.Glob)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("glob %s produced 0 files", fs.conf.Glob)
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
func (fs *DataSource) DeserializeLoader(bytes []byte) (sif.PartitionLoader, error) {
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
