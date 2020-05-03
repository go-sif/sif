package accumulators

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/go-sif/sif"
)

// Adder returns a new Sum Accumulator
func Adder(colName string) func() sif.Accumulator {
	return func() sif.Accumulator {
		return &Sum{colName: colName}
	}
}

// Sum Sums records
type Sum struct {
	colName string
	sum     float64
}

// GetSum returns the row Sum from this Accumulator
func (a *Sum) GetSum() float64 {
	return a.sum
}

// Accumulate adds a row to this Accumulator
func (a *Sum) Accumulate(row sif.Row) error {
	offset, err := row.Schema().GetOffset(a.colName)
	if err != nil {
		return err
	}
	switch offset.Type().(type) {
	case *sif.Int8ColumnType:
		v, err := row.GetInt8(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Int16ColumnType:
		v, err := row.GetInt16(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Int32ColumnType:
		v, err := row.GetInt32(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Int64ColumnType:
		v, err := row.GetInt64(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Uint8ColumnType:
		v, err := row.GetUint8(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Uint16ColumnType:
		v, err := row.GetUint16(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Uint32ColumnType:
		v, err := row.GetUint32(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Uint64ColumnType:
		v, err := row.GetUint64(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Float32ColumnType:
		v, err := row.GetFloat32(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	case *sif.Float64ColumnType:
		v, err := row.GetFloat64(a.colName)
		if err != nil {
			return err
		}
		a.sum += float64(v)
	}
	return nil
}

// Merge merges another Accumulator into this one
func (a *Sum) Merge(o sif.Accumulator) error {
	ca, ok := o.(*Sum)
	if !ok {
		return fmt.Errorf("Incoming accumulator is not a Sum Accumulator")
	}
	a.sum += ca.sum
	return nil
}

// ToBytes serializes this Accumulator
func (a *Sum) ToBytes() ([]byte, error) {
	buff := make([]byte, 8)
	binary.LittleEndian.PutUint64(buff, math.Float64bits(a.sum))
	return buff, nil
}

// FromBytes produce a new Accumulator from serialized data
func (a *Sum) FromBytes(buff []byte) (sif.Accumulator, error) {
	return &Sum{colName: a.colName, sum: math.Float64frombits(binary.LittleEndian.Uint64(buff))}, nil
}
