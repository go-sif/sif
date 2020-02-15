package types

import (
	"fmt"
	"strings"
	"time"
)

// IsVariableLength returns true iff colType is a VarColumnType
func IsVariableLength(colType ColumnType) (isVariableLength bool) {
	_, isVariableLength = colType.(VarColumnType)
	return
}

// ColumnType is an interface which is implemented to define a supported fixed-width column types.
// Sif provides a variety of built-in types in the columntype package.
type ColumnType interface {
	Size() int                     // returns size in bytes of a column type
	ToString(v interface{}) string // produces a string representation of a value of this type
}

// VarColumnType is an interface which is implemented to define supported variable-length column types. Size() for VarColumnTypes should always return 0.
// Sif provides a variety of built-in types in the columntype package.
type VarColumnType interface {
	ColumnType
	Serialize(v interface{}) ([]byte, error) // Defines how this type is serialized
	Deserialize([]byte) (interface{}, error) // Defines how this type is deserialized
}

// ByteColumnType is a column type which stores a byte
type ByteColumnType struct{}

// Size in bytes of a ByteColumn
func (b *ByteColumnType) Size() int {
	return 1
}

// ToString produces a string representation of a value of a ByteColumnType value
func (b *ByteColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%x", v.(byte))
}

// BytesColumnType is a column type which stores multiple bytes
type BytesColumnType struct {
	Length int
}

// Size in bytes of a ByteColumn
func (b *BytesColumnType) Size() int {
	return b.Length
}

// ToString produces a string representation of a value of a BytesColumnType value
func (b *BytesColumnType) ToString(v interface{}) string {
	var res strings.Builder
	fmt.Fprint(&res, "[")
	i := 0
	for _, v := range v.([]byte) {
		// don't print more than 5 entries
		if i > 5 {
			fmt.Fprintf(&res, "... %d more", b.Length-5)
			break
		}
		fmt.Fprintf(&res, "%x", v)
		i++
	}
	fmt.Fprint(&res, "]")
	return res.String()
}

// BoolColumnType is a column type which stores a boolean value
type BoolColumnType struct{}

// Size in bytes of a BoolColumn
func (b *BoolColumnType) Size() int {
	return 1
}

// ToString produces a string representation of a value of a BoolColumnType value
func (b *BoolColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%t", v.(bool))
}

// Uint8ColumnType is a column type which stores a uint8 value
type Uint8ColumnType struct{}

// Size in bytes of a Uint8Column
func (b *Uint8ColumnType) Size() int {
	return 1
}

// ToString produces a string representation of a value of a Uint8ColumnType value
func (b *Uint8ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(uint8))
}

// Uint16ColumnType is a column type which stores a uint16 value
type Uint16ColumnType struct{}

// Size in bytes of a Uint16Column
func (b *Uint16ColumnType) Size() int {
	return 2
}

// ToString produces a string representation of a value of a Uint16ColumnType value
func (b *Uint16ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(uint16))
}

// Uint32ColumnType is a column type which stores a uint32 value
type Uint32ColumnType struct{}

// Size in bytes of a Uint32Column
func (b *Uint32ColumnType) Size() int {
	return 4
}

// ToString produces a string representation of a value of a Uint32ColumnType value
func (b *Uint32ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(uint32))
}

// Uint64ColumnType is a column type which stores a uint64 value
type Uint64ColumnType struct{}

// Size in bytes of a Uint64Column
func (b *Uint64ColumnType) Size() int {
	return 8
}

// ToString produces a string representation of a value of a Uint64ColumnType value
func (b *Uint64ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(uint64))
}

// Int8ColumnType is a column type which stores a int8 value
type Int8ColumnType struct{}

// Size in bytes of a Int8Column
func (b *Int8ColumnType) Size() int {
	return 1
}

// ToString produces a string representation of a value of a Int8ColumnType value
func (b *Int8ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(int8))
}

// Int16ColumnType is a column type which stores a int16 value
type Int16ColumnType struct{}

// Size in bytes of a Int16Column
func (b *Int16ColumnType) Size() int {
	return 2
}

// ToString produces a string representation of a value of a Int16ColumnType value
func (b *Int16ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(int16))
}

// Int32ColumnType is a column type which stores a int32 value
type Int32ColumnType struct{}

// Size in bytes of a Int32Column
func (b *Int32ColumnType) Size() int {
	return 4
}

// ToString produces a string representation of a value of a Int32ColumnType value
func (b *Int32ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(int32))
}

// Int64ColumnType is a column type which stores a int64 value
type Int64ColumnType struct{}

// Size in bytes of a Int64Column
func (b *Int64ColumnType) Size() int {
	return 8
}

// ToString produces a string representation of a value of a Int64ColumnType value
func (b *Int64ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%d", v.(int64))
}

// Float32ColumnType is a column type which stores a float32 value
type Float32ColumnType struct{}

// Size in bytes of a Float32Column
func (b *Float32ColumnType) Size() int {
	return 4
}

// ToString produces a string representation of a value of a Float32ColumnType value
func (b *Float32ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%f", v.(float32))
}

// Float64ColumnType is a column type which stores a float64 value
type Float64ColumnType struct{}

// Size in bytes of a Float64Column
func (b *Float64ColumnType) Size() int {
	return 8
}

// ToString produces a string representation of a value of a Float64ColumnType value
func (b *Float64ColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("%f", v.(float64))
}

// TimeColumnType is a column type which stores a time.Time value. Because of https://github.com/golang/go/issues/15716, Times stored and retrieved may fail equality tests, despite passing UnixNano() equality tests.
type TimeColumnType struct {
	Format string
}

// Size in bytes of a TimeColumn
func (b *TimeColumnType) Size() int {
	return 15
}

// ToString produces a string representation of a value of a TimeColumnType value
func (b *TimeColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("\"%s\"", v.(time.Time).String())
}

// StringColumnType is a column type which stores fixed-length strings. Useful for hashes, etc.
type StringColumnType struct {
	Length int
}

// Size in bytes of a StringColumn
func (b *StringColumnType) Size() int {
	return b.Length
}

// ToString produces a string representation of a value of a StringColumnType value
func (b *StringColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("\"%s\"", v.(string))
}
