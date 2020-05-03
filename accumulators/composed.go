package accumulators

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/go-sif/sif"
)

// Compose returns a new Composed Accumulator
func Compose(faccs ...func() sif.Accumulator) func() sif.Accumulator {
	return func() sif.Accumulator {
		accs := make([]sif.Accumulator, len(faccs))
		for i, f := range faccs {
			accs[i] = f()
		}
		return &Composed{accs: accs}
	}
}

// Composed composes other Accumulators
type Composed struct {
	accs []sif.Accumulator
}

// GetResults returns the contained Accumulators, so that their results may be accessed
func (c *Composed) GetResults() []sif.Accumulator {
	return c.accs
}

// Accumulate adds a row to all contained Accumulators
func (c *Composed) Accumulate(row sif.Row) error {
	for _, a := range c.accs {
		err := a.Accumulate(row)
		if err != nil {
			return err
		}
	}
	return nil
}

// Merge merges another Composed Accumulator into this one, merging all contained Accumulators
func (c *Composed) Merge(o sif.Accumulator) error {
	compa, ok := o.(*Composed)
	if !ok {
		return fmt.Errorf("Incoming accumulator is not a Composed Accumulator")
	}
	for i, a := range c.accs {
		err := a.Merge(compa.accs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// ToBytes serializes this Accumulator
func (c *Composed) ToBytes() ([]byte, error) {
	result := make([][]byte, len(c.accs))
	totalSize := 0
	for i, a := range c.accs {
		buff, err := a.ToBytes()
		if err != nil {
			return nil, err
		}
		result[i] = buff
		totalSize += len(buff)
	}
	buff := new(bytes.Buffer)
	e := gob.NewEncoder(buff)
	err := e.Encode(result)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// FromBytes produce a new Accumulator from serialized data
func (c *Composed) FromBytes(buff []byte) (sif.Accumulator, error) {
	var deser [][]byte
	d := gob.NewDecoder(bytes.NewBuffer(buff))
	err := d.Decode(&deser)
	if err != nil {
		return nil, err
	}
	newAcs := make([]sif.Accumulator, len(c.accs))
	for i, b := range deser {
		a, err := c.accs[i].FromBytes(b)
		if err != nil {
			return nil, err
		}
		newAcs[i] = a
	}
	return &Composed{accs: newAcs}, nil
}
