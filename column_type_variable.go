package sif

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
)

// VarStringColumnType is a column type which stores a variable-length string value
type VarStringColumnType struct {
	// TODO store encoding type via https://godoc.org/golang.org/x/text/encoding, for inter-language stringing
}

// Size in bytes of the fixed-length StringColumn
func (b *VarStringColumnType) Size() int {
	return 0
}

// ToString produces a string representation of a value of a VarStringColumnType value
func (b *VarStringColumnType) ToString(v interface{}) string {
	return fmt.Sprintf("\"%s\"", v.(string))
}

// Serialize serializes this VarStringColumnType to binary data
func (b *VarStringColumnType) Serialize(v interface{}) ([]byte, error) {
	buff := new(bytes.Buffer)
	e := gob.NewEncoder(buff)
	err := e.Encode(v)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// Deserialize deserializes a VarStringColumnType from binary data
func (b *VarStringColumnType) Deserialize(ser []byte) (interface{}, error) {
	var deser string
	buff := bytes.NewBuffer(ser)
	d := gob.NewDecoder(buff)
	err := d.Decode(&deser)
	if err != nil {
		return nil, err
	}
	return deser, nil
}

// VarBytesColumnType is a column type which stores variable-length byte arrays
type VarBytesColumnType struct {
}

// Size in bytes of a VarBytesColumn
func (b *VarBytesColumnType) Size() int {
	return 0
}

// ToString produces a string representation of a value of a VarBytesColumnType value
func (b *VarBytesColumnType) ToString(v interface{}) string {
	bytes := v.([]byte)
	var res strings.Builder
	fmt.Fprint(&res, "[")
	i := 0
	for _, v := range bytes {
		// don't print more than 5 entries
		if i > 5 {
			fmt.Fprintf(&res, "... %d more", len(bytes)-5)
			break
		}
		fmt.Fprintf(&res, "%x", v)
		i++
	}
	fmt.Fprint(&res, "]")
	return res.String()
}

// Serialize serializes this VarBytesColumnType to binary data
func (b *VarBytesColumnType) Serialize(v interface{}) ([]byte, error) {
	buff := new(bytes.Buffer)
	e := gob.NewEncoder(buff)
	err := e.Encode(v)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// Deserialize deserializes a VarBytesColumnType from binary data
func (b *VarBytesColumnType) Deserialize(ser []byte) (interface{}, error) {
	var deser []byte
	buff := bytes.NewBuffer(ser)
	d := gob.NewDecoder(buff)
	err := d.Decode(&deser)
	if err != nil {
		return nil, err
	}
	return deser, nil
}
