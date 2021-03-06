package partition

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/pierrec/lz4"
)

// LZ4PartitionCompressor is a partition compressor which uses the lz4 compression algorithm
type LZ4PartitionCompressor struct {
	compressor         *lz4.Writer
	decompressor       *lz4.Reader
	reusableReadBuffer *bytes.Buffer
	lock               sync.Mutex
}

// NewLZ4PartitionCompressor instantiates a new LZ4PartitionCompressor
func NewLZ4PartitionCompressor() itypes.PartitionCompressor {
	compressor := lz4.NewWriter(new(bytes.Buffer))
	decompressor := lz4.NewReader(new(bytes.Buffer))
	return &LZ4PartitionCompressor{
		compressor:         compressor,
		decompressor:       decompressor,
		reusableReadBuffer: new(bytes.Buffer),
	}
}

// Compress serializes and compresses partition data to a write stream
func (lz4ps *LZ4PartitionCompressor) Compress(w io.Writer, part sif.ReduceablePartition) error {
	lz4ps.lock.Lock()
	defer lz4ps.lock.Unlock()
	bytes, err := part.ToBytes()
	if err != nil {
		return fmt.Errorf("Unable to convert partition to buffer %w", err)
	}
	lz4ps.compressor.Reset(w)
	n, err := lz4ps.compressor.Write(bytes)
	if err != nil || n == 0 {
		return fmt.Errorf("Unable to write partition to stream: %w", err)
	}
	err = lz4ps.compressor.Close()
	if err != nil {
		return fmt.Errorf("Unable to close compressor for stream: %w", err)
	}
	return nil
}

// Decompress decompresses and deserializes partition data from a read stream
func (lz4ps *LZ4PartitionCompressor) Decompress(r io.Reader, schema sif.Schema) (sif.ReduceablePartition, error) {
	lz4ps.lock.Lock()
	defer lz4ps.lock.Unlock()
	lz4ps.decompressor.Reset(r)
	lz4ps.reusableReadBuffer.Reset()
	_, err := lz4ps.reusableReadBuffer.ReadFrom(lz4ps.decompressor)
	if err != nil {
		log.Panicf("Unable to decompress partition data: %e", err)
	}
	return FromBytes(lz4ps.reusableReadBuffer.Bytes(), schema)
}

// Destroy cleans up anything relevant when the Serializer is no longer needed
func (lz4ps *LZ4PartitionCompressor) Destroy() {
	lz4ps.lock.Lock()
	defer lz4ps.lock.Unlock()
	lz4ps.compressor.Close()
	lz4ps.decompressor.Reset(new(bytes.Buffer))
}
