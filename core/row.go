package core

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	time "time"

	types "github.com/go-sif/sif/columntype"
	errors "github.com/go-sif/sif/errors"
)

const (
	colValueIsNilFlag = 1 << iota
)

// Row is a representation of a single row of columnar data,
// (a slice of a Partition), along with a reference to the
// Schema for that row (a mapping of column names to byte
// offsets). In practice, users of Row will call its
// getter and setter methods to retrieve, manipulate and store data
type Row struct {
	meta              []byte
	data              []byte                 // likely a slice of a partition array
	varData           map[string]interface{} // variable-length data
	serializedVarData map[string][]byte      // variable-length data
	schema            *Schema                // schema lets us pick the values we need out of the row
}

// Schema returns a read-only copy of the schema for a row
func (r *Row) Schema() *Schema {
	return r.schema.Clone() // TODO expensive but safe?
}

// ToString returns a string representation of this row
func (r *Row) ToString() string {
	var res strings.Builder
	fmt.Fprint(&res, "{")
	r.schema.ForEachColumn(func(name string, col *column) error {
		var val string
		if r.IsNil(name) {
			val = "nil"
		} else {
			v, err := r.Get(name)
			if err != nil {
				return err
			}
			val = col.colType.ToString(v)
		}
		fmt.Fprintf(&res, "\"%s\": %s,", name, val)
		return nil
	})
	fmt.Fprint(&res, "}")
	return res.String()
}

// IsNil returns true iff the given column value is nil in this row. If an error occurs, this function will return false.
func (r *Row) IsNil(colName string) bool {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		return false
	}
	if !isVariableLength(offset.colType) {
		return r.meta[offset.idx]&colValueIsNilFlag > 0
	}
	_, ok := r.varData[colName]
	return !ok
}

// SetNil sets the given column value to nil within this row
func (r *Row) SetNil(colName string) error {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return err
	}
	if !isVariableLength(offset.colType) {
		r.meta[offset.idx] = r.meta[offset.idx] & colValueIsNilFlag
	} else {
		r.varData[colName] = nil
	}
	return nil
}

// checkIsNil is for internal use only
func (r *Row) checkIsNil(colName string, offset *column) error {
	if !isVariableLength(offset.colType) && r.meta[offset.idx]&colValueIsNilFlag > 0 {
		return errors.NilValueError{Name: colName}
	} else if isVariableLength(offset.colType) {
		if _, ok := r.varData[colName]; !ok {
			return errors.NilValueError{Name: colName}
		}
	}
	return nil
}

// setNotNil is for internal use only
func (r *Row) setNotNil(offset *column) {
	if !isVariableLength(offset.colType) {
		r.meta[offset.idx] = r.meta[offset.idx] &^ colValueIsNilFlag
	}
}

// Get returns the value of any column as an interface{}, if it exists
func (r *Row) Get(colName string) (col interface{}, err error) {
	offset, err := r.Schema().getOffset(colName)
	if err != nil {
		return nil, err
	} else if isVariableLength(offset.colType) {
		switch offset.colType.(type) {
		case *types.VarStringColumnType:
			return r.GetVarString(colName)
		case *types.VarBytesColumnType:
			return r.GetVarBytes(colName)
		default:
			return r.GetVarCustomData(colName)
		}
	} else {
		switch offset.colType.(type) {
		case *types.ByteColumnType:
			return r.GetByte(colName)
		case *types.BytesColumnType:
			return r.GetBytes(colName)
		case *types.BoolColumnType:
			return r.GetBool(colName)
		case *types.Uint8ColumnType:
			return r.GetUint8(colName)
		case *types.Uint16ColumnType:
			return r.GetUint16(colName)
		case *types.Uint32ColumnType:
			return r.GetUint32(colName)
		case *types.Uint64ColumnType:
			return r.GetUint64(colName)
		case *types.Int8ColumnType:
			return r.GetInt8(colName)
		case *types.Int16ColumnType:
			return r.GetInt16(colName)
		case *types.Int32ColumnType:
			return r.GetInt32(colName)
		case *types.Int64ColumnType:
			return r.GetInt64(colName)
		case *types.Float32ColumnType:
			return r.GetFloat32(colName)
		case *types.Float64ColumnType:
			return r.GetFloat64(colName)
		case *types.TimeColumnType:
			return r.GetTime(colName)
		case *types.StringColumnType:
			return r.GetString(colName)
		default:
			return nil, fmt.Errorf("Cannot fetch value for unknown column type")
		}
	}
}

// GetByte retrieves a single byte from the column with the given name.
func (r *Row) GetByte(colName string) (col byte, err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return
	}
	col = r.data[offset.start]
	return
}

// GetBytes retrieves a multiple byte from the column with the given name.
func (r *Row) GetBytes(colName string) (col []byte, err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return
	}
	col = r.data[offset.start : offset.start+offset.colType.Size()]
	return
}

// GetBool retrieves a single bool from the column with the given name.
func (r *Row) GetBool(colName string) (col bool, err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return
	}
	col = r.data[offset.start] > 0
	return
}

// GetUint8 retrieves a single uint8 from the column with the given name.
func (r *Row) GetUint8(colName string) (col uint8, err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return
	}
	col = uint8(r.data[offset.start])
	return
}

// GetUint16 retrieves a single uint16 from the column with the given name
func (r *Row) GetUint16(colName string) (col uint16, err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return
	}
	col = binary.LittleEndian.Uint16(r.data[offset.start:])
	return
}

// GetUint32 retrieves a single uint32 from the column with the given name
func (r *Row) GetUint32(colName string) (col uint32, err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	e = r.checkIsNil(colName, offset)
	if e != nil {
		return
	}
	col = binary.LittleEndian.Uint32(r.data[offset.start:])
	return
}

// GetUint64 retrieves a single uint64 from the column with the given name
func (r *Row) GetUint64(colName string) (col uint64, err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return
	}
	col = binary.LittleEndian.Uint64(r.data[offset.start:])
	return
}

// GetInt8 retrieves a single int8 from the column with the given name
func (r *Row) GetInt8(colName string) (col int8, err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return
	}
	col = int8(r.data[offset.start])
	return
}

// GetInt16 retrieves a single int16 from the column with the given name
func (r *Row) GetInt16(colName string) (col int16, err error) {
	result, err := r.GetUint16(colName)
	if err != nil {
		return
	}
	col = int16(result)
	return
}

// GetInt32 retrieves a single int32 from the column with the given name
func (r *Row) GetInt32(colName string) (col int32, err error) {
	result, err := r.GetUint32(colName)
	if err != nil {
		return
	}
	col = int32(result)
	return
}

// GetInt64 retrieves a single int64 from the column with the given name
func (r *Row) GetInt64(colName string) (col int64, err error) {
	result, err := r.GetUint64(colName)
	if err != nil {
		return
	}
	col = int64(result)
	return
}

// GetFloat32 retrieves a single float32 from the column with the given name
func (r *Row) GetFloat32(colName string) (col float32, err error) {
	bits, err := r.GetUint32(colName)
	if err != nil {
		return
	}
	col = math.Float32frombits(bits)
	return
}

// GetFloat64 retrieves a single float64 from the column with the given name
func (r *Row) GetFloat64(colName string) (col float64, err error) {
	bits, err := r.GetUint64(colName)
	if err != nil {
		return
	}
	col = math.Float64frombits(bits)
	return
}

// GetTime retrieves a single Time from the column with the given name
func (r *Row) GetTime(colName string) (col time.Time, err error) {
	bits, err := r.GetBytes(colName)
	if err != nil {
		return
	}
	col = time.Now()
	err = col.UnmarshalBinary(bits)
	if err != nil {
		return
	}
	return
}

// GetString returns a single, fixed-length string value from the column with the given name
func (r *Row) GetString(colName string) (string, error) {
	bits, err := r.GetBytes(colName)
	if err != nil {
		return "", err
	}
	return string(bits), nil
}

// GetVarCustomData retrieves variable-length data of a custom type from the column with the given name
func (r *Row) GetVarCustomData(colName string) (interface{}, error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return nil, err
	}
	// deserialize serialized data if present
	if ser, ok := r.serializedVarData[colName]; ok {
		vcol, ok := offset.colType.(VarColumnType)
		if !ok {
			return nil, fmt.Errorf("Column %s is not a VarColumnType", colName)
		}
		deser, err := vcol.Deserialize(ser)
		if err != nil {
			return nil, err
		}
		r.varData[colName] = deser
		delete(r.serializedVarData, colName)
		return r.varData[colName], nil
	}
	err = r.checkIsNil(colName, offset)
	if err != nil {
		return nil, err
	}
	return r.varData[colName], nil
}

// GetVarBytes retrieves a variable-length byte array from the column with the given name
func (r *Row) GetVarBytes(colName string) (col []byte, err error) {
	val, err := r.GetVarCustomData(colName)
	if err != nil {
		return nil, err
	}
	return val.([]byte), nil
}

// GetVarString retrieves a single string from the column with the given name
func (r *Row) GetVarString(colName string) (col string, err error) {
	val, err := r.GetVarCustomData(colName)
	if err != nil {
		return "", err
	}
	return val.(string), nil
}

// SetByte modifies a single byte from the column with the given name.
func (r *Row) SetByte(colName string, value byte) (err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.setNotNil(offset)
	r.data[offset.start] = value
	return
}

// SetBytes overwrites multiple byte from the column with the given name.
func (r *Row) SetBytes(colName string, value []byte) (err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	if len(value) > offset.colType.Size() {
		err = fmt.Errorf("Value is wider than column: %d/%d", offset.colType.Size(), len(value))
	}
	r.setNotNil(offset)
	copy(r.data[offset.start:offset.start+offset.colType.Size()], value)
	return
}

// SetBool modifies a single bool from the column with the given name.
func (r *Row) SetBool(colName string, value bool) (err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	var newVal byte
	if value {
		newVal = 1
	}
	r.setNotNil(offset)
	r.data[offset.start] = newVal
	return
}

// SetUint8 modifies a single uint8 from the column with the given name.
func (r *Row) SetUint8(colName string, value uint8) (err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.setNotNil(offset)
	r.data[offset.start] = uint8(value)
	return
}

// SetUint16 modifies a single uint16 from the column with the given name.
func (r *Row) SetUint16(colName string, value uint16) (err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.setNotNil(offset)
	buff := make([]byte, offset.colType.Size())
	binary.LittleEndian.PutUint16(buff, value)
	copy(r.data[offset.start:offset.start+offset.colType.Size()], buff)
	return
}

// SetUint32 modifies a single uint32 from the column with the given name.
func (r *Row) SetUint32(colName string, value uint32) (err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.setNotNil(offset)
	buff := make([]byte, offset.colType.Size())
	binary.LittleEndian.PutUint32(buff, value)
	copy(r.data[offset.start:offset.start+offset.colType.Size()], buff)
	return
}

// SetUint64 modifies a single uint64 from the column with the given name.
func (r *Row) SetUint64(colName string, value uint64) (err error) {
	offset, e := r.schema.getOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.setNotNil(offset)
	buff := make([]byte, offset.colType.Size())
	binary.LittleEndian.PutUint64(buff, value)
	copy(r.data[offset.start:offset.start+offset.colType.Size()], buff)
	return
}

// SetInt8 modifies a single int8 from the column with the given name.
func (r *Row) SetInt8(colName string, value int8) (err error) {
	return r.SetUint8(colName, uint8(value))
}

// SetInt16 modifies a single int16 from the column with the given name.
func (r *Row) SetInt16(colName string, value int16) (err error) {
	return r.SetUint16(colName, uint16(value))
}

// SetInt32 modifies a single int32 from the column with the given name.
func (r *Row) SetInt32(colName string, value int32) (err error) {
	return r.SetUint32(colName, uint32(value))
}

// SetInt64 modifies a single int64 from the column with the given name.
func (r *Row) SetInt64(colName string, value int64) (err error) {
	return r.SetUint64(colName, uint64(value))
}

// SetFloat32 modifies a single float32 from the column with the given name.
func (r *Row) SetFloat32(colName string, value float32) (err error) {
	return r.SetUint32(colName, math.Float32bits(value))
}

// SetFloat64 modifies a single float64 from the column with the given name.
func (r *Row) SetFloat64(colName string, value float64) (err error) {
	return r.SetUint64(colName, math.Float64bits(value))
}

// SetTime modifies a single Time from the column with the given name.
func (r *Row) SetTime(colName string, value time.Time) (err error) {
	bits, err := value.MarshalBinary()
	if err != nil {
		return
	}
	return r.SetBytes(colName, bits)
}

// SetString modifies a single fixed-length string from the column with the given name.
func (r *Row) SetString(colName string, value string) (err error) {
	bits := []byte(value)
	return r.SetBytes(colName, bits)
}

// SetVarCustomData stores variable-length data of a custom type in this Row
func (r *Row) SetVarCustomData(colName string, value interface{}) (err error) {
	offset, err := r.schema.getOffset(colName)
	if err != nil {
		return
	}
	r.setNotNil(offset)
	delete(r.serializedVarData, colName)
	r.varData[colName] = value
	return nil
}

// SetVarBytes modifies a single variable-length byte array from the column with the given name.
func (r *Row) SetVarBytes(colName string, value []byte) (err error) {
	return r.SetVarCustomData(colName, value)
}

// SetVarString modifies a single string from the column with the given name.
func (r *Row) SetVarString(colName string, value string) (err error) {
	return r.SetVarCustomData(colName, value)
}

// Repack resizes a row to a new Schema
func (r *Row) repack(newSchema *Schema) (*Row, error) {
	meta := make([]byte, newSchema.NumColumns())
	buff := make([]byte, newSchema.size)
	varData := make(map[string]interface{})
	serializedVarData := make(map[string][]byte)
	// transfer values
	offset := 0
	err := newSchema.ForEachColumn(func(name string, col *column) error {
		oldCol, err := r.schema.getOffset(name)
		if err != nil {
			return err
		}
		if !isVariableLength(oldCol.colType) {
			// copy data
			copy(buff[col.start:col.start+col.colType.Size()], r.data[oldCol.start:oldCol.start+oldCol.colType.Size()])
			offset += col.start
		} else {
			varData[name] = r.varData[name]
		}
		// copy meta
		meta[col.idx] = r.meta[oldCol.idx]
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Row{meta, buff, varData, serializedVarData, newSchema}, nil
}
