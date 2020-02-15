package partition

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math"
	"testing"
	"time"

	"github.com/go-sif/sif/internal/schema"
	types "github.com/go-sif/sif/types"
	"github.com/stretchr/testify/require"
)

func TestGetUint64(t *testing.T) {
	schema := schema.CreateSchema()
	_, err := schema.CreateColumn("col1", &types.Uint64ColumnType{})
	require.Nil(t, err)
	row := rowImpl{
		schema: schema,
		data:   make([]byte, 16),
		meta:   make([]byte, 1),
	}
	binary.LittleEndian.PutUint64(row.data, math.MaxUint64)
	data, err := row.GetUint64("col1")
	require.Nil(t, err)
	if data != math.MaxUint64 {
		t.FailNow()
	}
}

func TestTime(t *testing.T) {
	schema := schema.CreateSchema()
	_, err := schema.CreateColumn("col1", &types.TimeColumnType{})
	require.Nil(t, err)
	row := rowImpl{
		schema: schema,
		data:   make([]byte, 15),
		meta:   make([]byte, 1),
	}
	v := time.Now()
	err = row.SetTime("col1", v)
	require.Nil(t, err)
	v2, err := row.GetTime("col1")
	require.Nil(t, err)
	require.EqualValues(t, v.UnixNano(), v2.UnixNano())
}

func TestDeserialization(t *testing.T) {
	// When partition is transferred over a network, all variable-length data is Gob-encoded and deserialized on-demand on the other side.
	serialized := make(map[string][]byte)
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	err := e.Encode("world")
	serialized["hello"] = b.Bytes()
	require.Nil(t, err)
	schema := schema.CreateSchema()
	_, err = schema.CreateColumn("hello", &types.VarStringColumnType{})
	require.Nil(t, err)
	row := rowImpl{
		schema:            schema,
		data:              make([]byte, 16),
		varData:           make(map[string]interface{}),
		serializedVarData: serialized,
		meta:              make([]byte, 1),
	}
	val, err := row.GetVarString("hello")
	require.Nil(t, err)
	require.Equal(t, "world", val)
}
