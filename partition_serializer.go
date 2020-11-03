package sif

import "io"

// A PartitionSerializer serializes and compresses partition data (and the inverse)
type PartitionSerializer interface {
	Compress(w io.Writer, part Partition) error               // Compress serializes and compresses partition data to a write stream
	Decompress(r io.Reader, schema Schema) (Partition, error) // Decompress decompresses and deserializes partition data from a read stream
}
