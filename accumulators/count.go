package accumulators

import (
	"encoding/binary"
	"fmt"

	"github.com/go-sif/sif"
)

// Counter returns a new Count Accumulator
func Counter() sif.Accumulator {
	return new(Count)
}

// Count counts records
type Count struct {
	count uint64
}

// GetCount returns the row count from this Accumulator
func (a *Count) GetCount() uint64 {
	return a.count
}

// Accumulate adds a row to this Accumulator
func (a *Count) Accumulate(row sif.Row) error {
	a.count++
	return nil
}

// Merge merges another Accumulator into this one
func (a *Count) Merge(o sif.Accumulator) error {
	ca, ok := o.(*Count)
	if !ok {
		return fmt.Errorf("Incoming accumulator is not a Count Accumulator")
	}
	a.count += ca.count
	return nil
}

// ToBytes serializes this Accumulator
func (a *Count) ToBytes() ([]byte, error) {
	buff := make([]byte, 8)
	binary.LittleEndian.PutUint64(buff, a.count)
	return buff, nil
}

// FromBytes produce a new Accumulator from serialized data
func (a *Count) FromBytes(buff []byte) (sif.Accumulator, error) {
	return &Count{count: binary.LittleEndian.Uint64(buff)}, nil
}
