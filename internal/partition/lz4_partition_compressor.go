package partition

import (
	"bytes"
	"io"
	"log"

	"github.com/go-sif/sif"
	"github.com/pierrec/lz4"
)

// LZ4PartitionSerializer is a partition compressor which uses the lz4 compression algorithm
type LZ4PartitionSerializer struct {
	compressor         *lz4.Writer
	decompressor       *lz4.Reader
	reusableReadBuffer *bytes.Buffer
}

// NewLZ4PartitionSerializer instantiates a new LZ4PartitionSerializer
func NewLZ4PartitionSerializer() sif.PartitionSerializer {
	compressor := lz4.NewWriter(new(bytes.Buffer))
	decompressor := lz4.NewReader(new(bytes.Buffer))
	return &LZ4PartitionSerializer{
		compressor:         compressor,
		decompressor:       decompressor,
		reusableReadBuffer: new(bytes.Buffer),
	}
}

// Compress serializes and compresses partition data to a write stream
func (lz4pc *LZ4PartitionSerializer) Compress(w io.Writer, part sif.Partition) error {
	panic("not implemented") // TODO: Implement
}

// Decompress decompresses and deserializes partition data from a read stream
func (lz4pc *LZ4PartitionSerializer) Decompress(r io.Reader, schema sif.Schema) (sif.Partition, error) {
	lz4pc.decompressor.Reset(r)
	lz4pc.reusableReadBuffer.Reset()
	_, err := lz4pc.reusableReadBuffer.ReadFrom(lz4pc.decompressor)
	if err != nil {
		log.Panicf("Unable to decompress partition data: %e", err)
	}
	return FromBytes(lz4pc.reusableReadBuffer.Bytes(), schema)
}
