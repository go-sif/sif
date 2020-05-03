package sif

// An Accumulator is an alternative reduction technique, which siphons data from
// Partitions into a custom data structure. The result is itself an Accumulator,
// rather than a series of Partitions, thus ending the job (no more operations may)
// be performed against the data. The advantage, however, is full control over the
// reduction technique, which can yield substantial performance benefits.
// As reduction is performed locally on all workers, then worker results are
// all reduced on the Coordinator, Accumulators are best utilized for smaller
// results. Distributed reductions via Reduce() are more efficient when
// there is a large reduction result (e.g. a large number of buckets).
type Accumulator interface {
	Accumulate(row Row) error                  // Accumulate adds a row to this Accumulator
	Merge(o Accumulator) error                 // Merge merges another Accumulator into this one
	ToBytes() ([]byte, error)                  // ToBytes serializes this Accumulator
	FromBytes(buf []byte) (Accumulator, error) // FromBytes produce a new Accumulator from serialized data
}
