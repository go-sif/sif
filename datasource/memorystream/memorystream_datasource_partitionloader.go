package memorystream

import (
	"bytes"
	"encoding/binary"
	"fmt"

	core "github.com/go-sif/sif/v0.0.1/core"
)

// PartitionLoader is capable of loading partitions of data from a file
type PartitionLoader struct {
	idx    int
	source *DataSource
}

// ToString returns a string representation of this PartitionLoader
func (pl *PartitionLoader) ToString() string {
	return fmt.Sprintf("Memory loader index: %d", pl.idx)
}

// Load is capable of loading partitions of data from a file
func (pl *PartitionLoader) Load(parser core.DataSourceParser, widestInitialSchema *core.Schema) (core.PartitionIterator, error) {
	// Load batch of data from generator to pass to parser
	data := make([]byte, 0, 1024)
	for i := 0; i < pl.source.batchSize; i++ {
		row := pl.source.generators[pl.idx]()
		data = append(data, row...)
	}
	r := bytes.NewReader(data)
	pi, err := parser.Parse(r, pl.source, pl.source.schema, widestInitialSchema, nil)
	if err != nil {
		return nil, err
	}
	return pi, nil
}

// GobEncode serializes a PartitionLoader
func (pl *PartitionLoader) GobEncode() ([]byte, error) {
	buff := make([]byte, 32)
	binary.LittleEndian.PutUint32(buff, uint32(pl.idx))
	return buff, nil
}

// GobDecode deserializes a PartitionLoader
func (pl *PartitionLoader) GobDecode(in []byte) error {
	pl.idx = int(binary.LittleEndian.Uint32(in))
	return nil
}
