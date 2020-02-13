package file

import (
	"fmt"
	"log"
	"os"

	core "github.com/go-sif/sif/core"
)

// PartitionLoader is capable of loading partitions of data from a file
type PartitionLoader struct {
	path   string
	source *DataSource
}

// ToString returns a string representation of this PartitionLoader
func (pl *PartitionLoader) ToString() string {
	return fmt.Sprintf("File loader filename: %s", pl.path)
}

// Load is capable of loading partitions of data from a file
func (pl *PartitionLoader) Load(parser core.DataSourceParser, widestInitialSchema *core.Schema) (core.PartitionIterator, error) {
	f, err := os.Open(pl.path)
	if err != nil {
		return nil, err
	}
	pi, err := parser.Parse(f, pl.source, pl.source.schema, widestInitialSchema, func() {
		err := f.Close()
		if err != nil {
			log.Printf("WARNING: couldn't close file %e", err)
		}
	})
	if err != nil {
		return nil, err
	}
	return pi, nil
}

// GobEncode serializes a PartitionLoader
func (pl *PartitionLoader) GobEncode() ([]byte, error) {
	return []byte(pl.path), nil
}

// GobDecode deserializes a PartitionLoader
func (pl *PartitionLoader) GobDecode(in []byte) error {
	pl.path = string(in)
	return nil
}
