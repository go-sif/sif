package partition

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	time "time"

	"github.com/go-sif/sif"
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
type rowImpl struct {
	partID            string
	meta              []byte
	data              []byte                 // likely a slice of a partition array
	varData           map[string]interface{} // variable-length data
	serializedVarData map[string][]byte      // variable-length data
	schema            sif.Schema             // schema lets us pick the values we need out of the row
}

// CreateRow builds a new row from individual internal components
func CreateRow(partID string, meta []byte, data []byte, varData map[string]interface{}, serializedVarData map[string][]byte, schema sif.Schema) sif.Row {
	return &rowImpl{partID: partID, meta: meta, data: data, varData: varData, serializedVarData: serializedVarData, schema: schema}
}

// CreateTempRow builds an empty row struct which cannot be used until passed to a function which populates it with data
func CreateTempRow() sif.Row {
	return &rowImpl{}
}

// Schema returns a read-only copy of the schema for a row
func (r *rowImpl) Schema() sif.Schema {
	return r.schema.Clone() // TODO expensive but safe?
}

// ToString returns a string representation of this row
func (r *rowImpl) ToString() string {
	var res strings.Builder
	fmt.Fprint(&res, "{")
	r.schema.ForEachColumn(func(name string, col sif.Column) error {
		var val string
		if r.IsNil(name) {
			val = "nil"
		} else {
			v, err := r.Get(name)
			if err != nil {
				return err
			}
			val = col.Type().ToString(v)
		}
		fmt.Fprintf(&res, "\"%s\": %s,", name, val)
		return nil
	})
	fmt.Fprint(&res, "}")
	return res.String()
}

// IsNil returns true iff the given column value is nil in this row. If an error occurs, this function will return false.
func (r *rowImpl) IsNil(colName string) bool {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		return false
	}
	if !sif.IsVariableLength(offset.Type()) {
		return r.meta[offset.Index()]&colValueIsNilFlag > 0
	}
	_, ok := r.varData[colName]
	return !ok
}

// SetNil sets the given column value to nil within this row
func (r *rowImpl) SetNil(colName string) error {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return err
	}
	if !sif.IsVariableLength(offset.Type()) {
		r.meta[offset.Index()] = r.meta[offset.Index()] & colValueIsNilFlag
	} else {
		r.varData[colName] = nil
	}
	return nil
}

// CheckIsNil is for internal use only
func (r *rowImpl) CheckIsNil(colName string, offset sif.Column) error {
	if !sif.IsVariableLength(offset.Type()) && r.meta[offset.Index()]&colValueIsNilFlag > 0 {
		return errors.NilValueError{Name: colName}
	} else if sif.IsVariableLength(offset.Type()) {
		if _, ok := r.varData[colName]; !ok {
			return errors.NilValueError{Name: colName}
		}
	}
	return nil
}

// SetNotNil is for internal use only
func (r *rowImpl) SetNotNil(offset sif.Column) {
	if !sif.IsVariableLength(offset.Type()) {
		r.meta[offset.Index()] = r.meta[offset.Index()] &^ colValueIsNilFlag
	}
}

// Get returns the value of any column as an interface{}, if it exists
func (r *rowImpl) Get(colName string) (col interface{}, err error) {
	offset, err := r.Schema().GetOffset(colName)
	if err != nil {
		return nil, err
	} else if sif.IsVariableLength(offset.Type()) {
		switch offset.Type().(type) {
		case *sif.VarStringColumnType:
			return r.GetVarString(colName)
		case *sif.VarBytesColumnType:
			return r.GetVarBytes(colName)
		default:
			return r.GetVarCustomData(colName)
		}
	} else {
		switch offset.Type().(type) {
		case *sif.ByteColumnType:
			return r.GetByte(colName)
		case *sif.BytesColumnType:
			return r.GetBytes(colName)
		case *sif.BoolColumnType:
			return r.GetBool(colName)
		case *sif.Uint8ColumnType:
			return r.GetUint8(colName)
		case *sif.Uint16ColumnType:
			return r.GetUint16(colName)
		case *sif.Uint32ColumnType:
			return r.GetUint32(colName)
		case *sif.Uint64ColumnType:
			return r.GetUint64(colName)
		case *sif.Int8ColumnType:
			return r.GetInt8(colName)
		case *sif.Int16ColumnType:
			return r.GetInt16(colName)
		case *sif.Int32ColumnType:
			return r.GetInt32(colName)
		case *sif.Int64ColumnType:
			return r.GetInt64(colName)
		case *sif.Float32ColumnType:
			return r.GetFloat32(colName)
		case *sif.Float64ColumnType:
			return r.GetFloat64(colName)
		case *sif.TimeColumnType:
			return r.GetTime(colName)
		case *sif.StringColumnType:
			return r.GetString(colName)
		default:
			return nil, fmt.Errorf("Cannot fetch value for unknown column type")
		}
	}
}

// GetByte retrieves a single byte from the column with the given name.
func (r *rowImpl) GetByte(colName string) (col byte, err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return
	}
	col = r.data[offset.Start()]
	return
}

// GetBytes retrieves a multiple byte from the column with the given name.
func (r *rowImpl) GetBytes(colName string) (col []byte, err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return
	}
	col = r.data[offset.Start() : offset.Start()+offset.Type().Size()]
	return
}

// GetBool retrieves a single bool from the column with the given name.
func (r *rowImpl) GetBool(colName string) (col bool, err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return
	}
	col = r.data[offset.Start()] > 0
	return
}

// GetUint8 retrieves a single uint8 from the column with the given name.
func (r *rowImpl) GetUint8(colName string) (col uint8, err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return
	}
	col = uint8(r.data[offset.Start()])
	return
}

// GetUint16 retrieves a single uint16 from the column with the given name
func (r *rowImpl) GetUint16(colName string) (col uint16, err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return
	}
	col = binary.LittleEndian.Uint16(r.data[offset.Start():])
	return
}

// GetUint32 retrieves a single uint32 from the column with the given name
func (r *rowImpl) GetUint32(colName string) (col uint32, err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	e = r.CheckIsNil(colName, offset)
	if e != nil {
		return
	}
	col = binary.LittleEndian.Uint32(r.data[offset.Start():])
	return
}

// GetUint64 retrieves a single uint64 from the column with the given name
func (r *rowImpl) GetUint64(colName string) (col uint64, err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return
	}
	col = binary.LittleEndian.Uint64(r.data[offset.Start():])
	return
}

// GetInt8 retrieves a single int8 from the column with the given name
func (r *rowImpl) GetInt8(colName string) (col int8, err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return
	}
	col = int8(r.data[offset.Start()])
	return
}

// GetInt16 retrieves a single int16 from the column with the given name
func (r *rowImpl) GetInt16(colName string) (col int16, err error) {
	result, err := r.GetUint16(colName)
	if err != nil {
		return
	}
	col = int16(result)
	return
}

// GetInt32 retrieves a single int32 from the column with the given name
func (r *rowImpl) GetInt32(colName string) (col int32, err error) {
	result, err := r.GetUint32(colName)
	if err != nil {
		return
	}
	col = int32(result)
	return
}

// GetInt64 retrieves a single int64 from the column with the given name
func (r *rowImpl) GetInt64(colName string) (col int64, err error) {
	result, err := r.GetUint64(colName)
	if err != nil {
		return
	}
	col = int64(result)
	return
}

// GetFloat32 retrieves a single float32 from the column with the given name
func (r *rowImpl) GetFloat32(colName string) (col float32, err error) {
	bits, err := r.GetUint32(colName)
	if err != nil {
		return
	}
	col = math.Float32frombits(bits)
	return
}

// GetFloat64 retrieves a single float64 from the column with the given name
func (r *rowImpl) GetFloat64(colName string) (col float64, err error) {
	bits, err := r.GetUint64(colName)
	if err != nil {
		return
	}
	col = math.Float64frombits(bits)
	return
}

// GetTime retrieves a single Time from the column with the given name
func (r *rowImpl) GetTime(colName string) (col time.Time, err error) {
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
func (r *rowImpl) GetString(colName string) (string, error) {
	bits, err := r.GetBytes(colName)
	if err != nil {
		return "", err
	}
	return string(bits), nil
}

// GetVarCustomData retrieves variable-length data of a custom type from the column with the given name
func (r *rowImpl) GetVarCustomData(colName string) (interface{}, error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return nil, err
	}
	vcol, ok := offset.Type().(sif.VarColumnType)
	if !ok {
		return nil, fmt.Errorf("Column %s is not a VarColumnType", colName)
	}
	// deserialize serialized data if present
	if ser, ok := r.serializedVarData[colName]; ok {
		if ser == nil {
			// if the serialized data is nil, then it represents a nil value
			r.varData[colName] = nil
			delete(r.serializedVarData, colName)
			return nil, errors.NilValueError{Name: colName}
		}
		// serialized data should never be empty
		if len(ser) == 0 {
			return nil, fmt.Errorf("Serialized column data for column %s in partition %s should not be zero-length", colName, r.partID)
		}
		deser, err := vcol.Deserialize(ser)
		if err != nil {
			return nil, fmt.Errorf("Error deserializing variable-length column data for column %s in partition %s: %w", colName, r.partID, err)
		}
		r.varData[colName] = deser
		// log.Printf("Deserializing column %s in partition %s", colName, r.partID)
		delete(r.serializedVarData, colName)
		return r.varData[colName], nil
	}
	err = r.CheckIsNil(colName, offset)
	if err != nil {
		return nil, err
	}
	return r.varData[colName], nil
}

// GetVarBytes retrieves a variable-length byte array from the column with the given name
func (r *rowImpl) GetVarBytes(colName string) (col []byte, err error) {
	val, err := r.GetVarCustomData(colName)
	if err != nil {
		return nil, err
	}
	return val.([]byte), nil
}

// GetVarString retrieves a single string from the column with the given name
func (r *rowImpl) GetVarString(colName string) (col string, err error) {
	val, err := r.GetVarCustomData(colName)
	if err != nil {
		return "", err
	}
	return val.(string), nil
}

// SetByte modifies a single byte from the column with the given name.
func (r *rowImpl) SetByte(colName string, value byte) (err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.SetNotNil(offset)
	r.data[offset.Start()] = value
	return
}

// SetBytes overwrites multiple byte from the column with the given name.
func (r *rowImpl) SetBytes(colName string, value []byte) (err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	if len(value) > offset.Type().Size() {
		err = fmt.Errorf("Value is wider than column: %d/%d", offset.Type().Size(), len(value))
	}
	r.SetNotNil(offset)
	copy(r.data[offset.Start():offset.Start()+offset.Type().Size()], value)
	return
}

// SetBool modifies a single bool from the column with the given name.
func (r *rowImpl) SetBool(colName string, value bool) (err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	var newVal byte
	if value {
		newVal = 1
	}
	r.SetNotNil(offset)
	r.data[offset.Start()] = newVal
	return
}

// SetUint8 modifies a single uint8 from the column with the given name.
func (r *rowImpl) SetUint8(colName string, value uint8) (err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.SetNotNil(offset)
	r.data[offset.Start()] = value
	return
}

// SetUint16 modifies a single uint16 from the column with the given name.
func (r *rowImpl) SetUint16(colName string, value uint16) (err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.SetNotNil(offset)
	buff := make([]byte, offset.Type().Size())
	binary.LittleEndian.PutUint16(buff, value)
	copy(r.data[offset.Start():offset.Start()+offset.Type().Size()], buff)
	return
}

// SetUint32 modifies a single uint32 from the column with the given name.
func (r *rowImpl) SetUint32(colName string, value uint32) (err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.SetNotNil(offset)
	buff := make([]byte, offset.Type().Size())
	binary.LittleEndian.PutUint32(buff, value)
	copy(r.data[offset.Start():offset.Start()+offset.Type().Size()], buff)
	return
}

// SetUint64 modifies a single uint64 from the column with the given name.
func (r *rowImpl) SetUint64(colName string, value uint64) (err error) {
	offset, e := r.schema.GetOffset(colName)
	if e != nil {
		err = e
		return
	}
	r.SetNotNil(offset)
	buff := make([]byte, offset.Type().Size())
	binary.LittleEndian.PutUint64(buff, value)
	copy(r.data[offset.Start():offset.Start()+offset.Type().Size()], buff)
	return
}

// SetInt8 modifies a single int8 from the column with the given name.
func (r *rowImpl) SetInt8(colName string, value int8) (err error) {
	return r.SetUint8(colName, uint8(value))
}

// SetInt16 modifies a single int16 from the column with the given name.
func (r *rowImpl) SetInt16(colName string, value int16) (err error) {
	return r.SetUint16(colName, uint16(value))
}

// SetInt32 modifies a single int32 from the column with the given name.
func (r *rowImpl) SetInt32(colName string, value int32) (err error) {
	return r.SetUint32(colName, uint32(value))
}

// SetInt64 modifies a single int64 from the column with the given name.
func (r *rowImpl) SetInt64(colName string, value int64) (err error) {
	return r.SetUint64(colName, uint64(value))
}

// SetFloat32 modifies a single float32 from the column with the given name.
func (r *rowImpl) SetFloat32(colName string, value float32) (err error) {
	return r.SetUint32(colName, math.Float32bits(value))
}

// SetFloat64 modifies a single float64 from the column with the given name.
func (r *rowImpl) SetFloat64(colName string, value float64) (err error) {
	return r.SetUint64(colName, math.Float64bits(value))
}

// SetTime modifies a single Time from the column with the given name.
func (r *rowImpl) SetTime(colName string, value time.Time) (err error) {
	bits, err := value.MarshalBinary()
	if err != nil {
		return
	}
	return r.SetBytes(colName, bits)
}

// SetString modifies a single fixed-length string from the column with the given name.
func (r *rowImpl) SetString(colName string, value string) (err error) {
	bits := []byte(value)
	return r.SetBytes(colName, bits)
}

// SetVarCustomData stores variable-length data of a custom type in this Row
func (r *rowImpl) SetVarCustomData(colName string, value interface{}) (err error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return
	}
	r.SetNotNil(offset)
	delete(r.serializedVarData, colName)
	r.varData[colName] = value
	return nil
}

// SetVarBytes modifies a single variable-length byte array from the column with the given name.
func (r *rowImpl) SetVarBytes(colName string, value []byte) (err error) {
	return r.SetVarCustomData(colName, value)
}

// SetVarString modifies a single string from the column with the given name.
func (r *rowImpl) SetVarString(colName string, value string) (err error) {
	return r.SetVarCustomData(colName, value)
}

// Repack resizes a row to a new Schema
func (r *rowImpl) Repack(newSchema sif.Schema) (sif.Row, error) {
	meta := make([]byte, newSchema.NumColumns())
	buff := make([]byte, newSchema.Size())
	varData := make(map[string]interface{})
	serializedVarData := make(map[string][]byte)
	// transfer values
	offset := 0
	err := newSchema.ForEachColumn(func(name string, col sif.Column) error {
		// if we're widening instead of shrinking, there might be new columns
		if !r.schema.HasColumn(name) {
			return nil
		}
		// otherwise, copy old values
		oldCol, err := r.schema.GetOffset(name)
		if err != nil {
			return err
		}
		if !sif.IsVariableLength(oldCol.Type()) {
			// copy data
			copy(buff[col.Start():col.Start()+col.Type().Size()], r.data[oldCol.Start():oldCol.Start()+oldCol.Type().Size()])
			offset += col.Start()
		} else {
			varData[name] = r.varData[name]
		}
		// copy meta
		meta[col.Index()] = r.meta[oldCol.Index()]
		return nil
	})
	if err != nil {
		return nil, err
	}
	// no partID or lock, because this new row belongs to no partition
	return &rowImpl{"", meta, buff, varData, serializedVarData, newSchema}, nil
}
