package sif

// An Accumulator is an alternative reduction technique, which siphons data from
// Partitions into a custom data structure. The result is itself an Accumulator,
// rather than a series of Partitions, thus ending the job. The advantage,
// however, is full control over the reduction technique, which can yield
// substantial performance benefits.
type Accumulator interface {
	Accumulate(row Row) error                  // Accumulate adds a row to this Accumulator
	Merge(o Accumulator) error                 // Merge merges another Accumulator into this one
	ToBytes() ([]byte, error)                  // ToBytes serializes this Accumulator
	FromBytes(buf []byte) (Accumulator, error) // FromBytes produce a new Accumulator from serialized data
}
