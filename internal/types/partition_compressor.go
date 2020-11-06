package types

import (
	"io"

	"github.com/go-sif/sif"
)

// A PartitionCompressor serializes and compresses partition data (and the inverse)
type PartitionCompressor interface {
	Compress(w io.Writer, part ReduceablePartition) error                   // Compress serializes and compresses partition data to a write stream
	Decompress(r io.Reader, schema sif.Schema) (ReduceablePartition, error) // Decompress decompresses and deserializes partition data from a read stream
	Destroy()                                                               // Destroy cleans up anything relevant when the Serializer is no longer needed
}
