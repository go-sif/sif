package file

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/go-sif/sif"
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
func (pl *PartitionLoader) Load(parser sif.DataSourceParser, widestInitialSchema sif.Schema) (sif.PartitionIterator, error) {
	var reader io.Reader
	var onIteratorEnd func()
	if pl.source.conf.Decoder != nil {
		buf, err := ioutil.ReadFile(pl.path)
		if err != nil {
			return nil, err
		}
		buf, err = pl.source.conf.Decoder(buf)
		if err != nil {
			return nil, fmt.Errorf("WARNING: couldn't decode file %s: %e", pl.path, err)
		}
		reader = bytes.NewReader(buf)
		onIteratorEnd = func() {}
	} else {
		f, err := os.Open(pl.path)
		if err != nil {
			return nil, err
		}
		reader = f
		onIteratorEnd = func() {
			err := f.Close()
			if err != nil {
				log.Printf("WARNING: couldn't close file %e", err)
			}
		}
	}
	pi, err := parser.Parse(reader, pl.source, pl.source.schema, widestInitialSchema, onIteratorEnd)
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
