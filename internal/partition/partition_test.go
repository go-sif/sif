package partition

import (
	"testing"

	"github.com/go-sif/sif"
	errors "github.com/go-sif/sif/errors"
	"github.com/go-sif/sif/operations/transform"
	"github.com/go-sif/sif/schema"
	"github.com/stretchr/testify/require"
)

func createPartitionTestSchema() sif.Schema {
	schema := schema.CreateSchema()
	schema.CreateColumn("col1", &sif.Uint8ColumnType{})
	return schema
}

func TestCreatePartitionImpl(t *testing.T) {
	schema := createPartitionTestSchema()
	part := createPartitionImpl(4, 4, schema)
	require.Equal(t, part.GetMaxRows(), 4)
	require.Equal(t, part.GetNumRows(), 0)
	require.Nil(t, part.CanInsertRowData(make([]byte, 1)))
	require.NotNil(t, part.CanInsertRowData(make([]byte, 18))) // rows are padded to at least 16bytes
	require.False(t, part.GetIsKeyed())
}

func TestAppendRowData(t *testing.T) {
	// make partition
	schema := createPartitionTestSchema()
	part := createPartitionImpl(4, 4, schema)
	require.Equal(t, part.GetNumRows(), 0)
	r := []byte{byte(uint8(1))}
	// append and validate row
	err := part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), 1)
	val, err := part.GetRow(0).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(1))
	// append and validate another row
	r = []byte{byte(uint8(2))}
	err = part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), 2)
	val, err = part.GetRow(1).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(2))
}

func TestInsertRowData(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	part := createPartitionImpl(4, 4, schema)
	require.Equal(t, part.GetNumRows(), 0)
	// append and validate row
	r := []byte{byte(uint8(1))}
	err := part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), 1)
	val, err := part.GetRow(0).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(1))
	// insert and validate row
	r = []byte{byte(uint8(2))}
	err = part.InsertRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte), 0)
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), 2)
	val, err = part.GetRow(0).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(2))
}

func TestPartitionFullError(t *testing.T) {
	// create partition with max 1 row
	schema := createPartitionTestSchema()
	part := createPartitionImpl(1, 1, schema)
	require.Equal(t, part.GetNumRows(), 0)
	// append and validate row
	r := []byte{byte(uint8(1))}
	err := part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), 1)
	val, err := part.GetRow(0).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(1))
	// attempt to append row again
	err = part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	require.NotNil(t, err)
	_, ok := err.(errors.PartitionFullError)
	require.True(t, ok)
}

func TestIncompatibleRowError(t *testing.T) {
	// create partition with max 1 row
	schema := createPartitionTestSchema()
	part := createPartitionImpl(1, 1, schema)
	require.Equal(t, part.GetNumRows(), 0)
	// append and validate row
	r := []byte{byte(uint8(1))}
	err := part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), 1)
	val, err := part.GetRow(0).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(1))
	// attempt to append incompatible row (because of padding, we have to actually append up to 16 bytes)
	r = make([]byte, 17)
	r[0] = 1
	r[1] = 2
	err = part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	require.NotNil(t, err)
	_, ok := err.(errors.IncompatibleRowError)
	require.True(t, ok)
}

func TestMapRows(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	part := createPartitionImpl(4, 4, schema)
	require.Equal(t, part.GetNumRows(), 0)
	// append rows
	for i := 0; i < 4; i++ {
		r := []byte{byte(uint8(i))}
		err := part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
		require.Nil(t, err)
	}
	sum := 0
	_, err := part.MapRows(func(row sif.Row) error {
		val, err := row.GetUint8("col1")
		sum += int(val)
		return err
	})
	require.Nil(t, err)
	require.Equal(t, sum, 6)
}

func TestKeyRows(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	part := createPartitionImpl(8, 8, schema)
	// append rows
	for i := 0; i < 7; i++ {
		r := []byte{uint8(i)}
		meta := []byte{0}
		err := part.AppendRowData(r, meta, make(map[string]interface{}), make(map[string][]byte))
		require.Nil(t, err)
	}
	// add in a single duplicate row for good measure.
	err := part.AppendRowData([]byte{6}, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	// shouldn't be able to get keys before we key a partition
	_, err = part.GetKey(0)
	require.NotNil(t, err)
	// key rows
	_, err = part.KeyRows(func(row sif.Row) ([]byte, error) {
		val, err := row.GetUint8("col1")
		if err != nil {
			return nil, err
		}
		return []byte{byte(val)}, nil
	})
	require.Nil(t, err)
	require.True(t, part.GetIsKeyed())
	// compare keys for identical rows
	key1, err := part.GetKey(6)
	require.Nil(t, err)
	key2, err := part.GetKey(7)
	require.Nil(t, err)
	require.EqualValues(t, key1, key2)
	// even though the key appears twice, FindFirstKey should always return the first occurrence
	idx, err := part.FindFirstKey(key2)
	require.Nil(t, err)
	require.Equal(t, 6, idx)
	// keys that don't exist
	_, err = part.FindFirstKey(uint64(1234))
	require.NotNil(t, err)
}

func TestSplit(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	part := createPartitionImpl(8, 8, schema)
	// append rows
	for i := 0; i < 8; i++ {
		r := []byte{byte(uint8(i))}
		err := part.AppendRowData(r, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
		require.Nil(t, err)
	}
	left, right, err := part.Split(4)
	require.Nil(t, err)
	// verify values
	val, err := left.GetRow(0).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(0))
	val, err = right.GetRow(0).GetUint8("col1")
	require.Nil(t, err)
	require.Equal(t, val, uint8(4))
	// key rows
	_, err = part.KeyRows(func(row sif.Row) ([]byte, error) {
		val, err := row.GetUint8("col1")
		if err != nil {
			return nil, err
		}
		return []byte{byte(val)}, nil
	})
	// split again and verify keys
	left, right, err = part.Split(4)
	key, err := part.GetKey(0)
	require.Nil(t, err)
	lkey, err := left.GetKey(0)
	require.Nil(t, err)
	require.Equal(t, key, lkey)
	key, err = part.GetKey(4)
	require.Nil(t, err)
	rkey, err := right.GetKey(0)
	require.Nil(t, err)
	require.Equal(t, rkey, key)
}

func TestSerialization(t *testing.T) {
	// TODO test pre-serialized var row data
	// create partition
	schema := createPartitionTestSchema()
	schema.CreateColumn("col2", &sif.VarStringColumnType{})
	var part sif.ReduceablePartition
	part = createPartitionImpl(8, 2, schema)
	// append rows
	tempRow := &rowImpl{}
	for i := 0; i < 8; i++ {
		// serialize and deserialize
		buff, err := part.ToBytes()
		require.Nil(t, err)
		part, err = FromBytes(buff, schema)
		require.Nil(t, err)
		// add values
		row, err := part.AppendEmptyRowData(tempRow)
		require.Nil(t, err)
		err = row.SetUint8("col1", uint8(i))
		require.Nil(t, err)
		err = row.SetVarString("col2", "Hello World")
		require.Nil(t, err)
	}
	// verify values
	require.Equal(t, 8, part.GetNumRows())
	for i := 0; i < 8; i++ {
		val1, err := part.GetRow(i).GetUint8("col1")
		require.Nil(t, err)
		require.Equal(t, val1, uint8(i))
		val2, err := part.GetRow(i).GetVarString("col2")
		require.Nil(t, err)
		require.Equal(t, val2, "Hello World")
	}
	// serialize and deserialize
	buff, err := part.ToBytes()
	require.Nil(t, err)
	part, err = FromBytes(buff, schema)
	require.Nil(t, err)
	// verify values again
	require.Equal(t, 8, part.GetNumRows())
	for i := 0; i < 8; i++ {
		val1, err := part.GetRow(i).GetUint8("col1")
		require.Nil(t, err)
		require.Equal(t, val1, uint8(i))
		val2, err := part.GetRow(i).GetVarString("col2")
		require.Nil(t, err)
		require.Equal(t, val2, "Hello World")
	}
}

func TestRepackShrink(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	schema.CreateColumn("col2", &sif.Float64ColumnType{})
	schema.CreateColumn("col3", &sif.VarStringColumnType{})
	part := createPartitionImpl(8, 8, schema)
	// append rows
	tempRow := CreateTempRow()
	for i := 0; i < 8; i++ {
		row, err := part.AppendEmptyRowData(tempRow)
		require.Nil(t, err)
		row.SetInt8("col1", int8(i))
		row.SetFloat64("col2", float64(i+1))
		row.SetVarString("col3", "Hello World")
	}
	// verify values
	require.Equal(t, 8, part.GetNumRows())
	for i := 0; i < 8; i++ {
		val1, err := part.GetRow(i).GetUint8("col1")
		require.Nil(t, err)
		require.Equal(t, val1, uint8(i))
		val2, err := part.GetRow(i).GetFloat64("col2")
		require.Nil(t, err)
		require.Equal(t, val2, float64(i+1))
		val3, err := part.GetRow(i).GetVarString("col3")
		require.Nil(t, err)
		require.Equal(t, val3, "Hello World")
	}
	// test repack
	newSchema := schema.Clone()
	newSchema.RemoveColumn("col1")
	newPart, err := part.Repack(newSchema)
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), newPart.GetNumRows())
	require.Equal(t, part.GetMaxRows(), newPart.GetMaxRows())
	for i := 0; i < 8; i++ {
		origRow := part.GetRow(i)
		newRow := newPart.GetRow(i)
		val2, err := origRow.GetFloat64("col2")
		require.Nil(t, err)
		newVal2, err := newRow.GetFloat64("col2")
		require.Nil(t, err)
		require.Equal(t, val2, newVal2)
		val3, err := origRow.GetVarString("col3")
		require.Nil(t, err)
		newVal3, err := newRow.GetVarString("col3")
		require.Nil(t, err)
		require.Equal(t, val3, newVal3)
	}
}

func TestRepackGrow(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	schema.CreateColumn("col2", &sif.Float64ColumnType{})
	schema.CreateColumn("col3", &sif.VarStringColumnType{})
	part := createPartitionImpl(8, 8, schema)
	// append rows
	tempRow := CreateTempRow()
	for i := 0; i < 8; i++ {
		row, err := part.AppendEmptyRowData(tempRow)
		require.Nil(t, err)
		row.SetInt8("col1", int8(i))
		row.SetFloat64("col2", float64(i+1))
		row.SetVarString("col3", "Hello World")
	}
	// verify values
	require.Equal(t, 8, part.GetNumRows())
	for i := 0; i < 8; i++ {
		val1, err := part.GetRow(i).GetUint8("col1")
		require.Nil(t, err)
		require.Equal(t, val1, uint8(i))
		val2, err := part.GetRow(i).GetFloat64("col2")
		require.Nil(t, err)
		require.Equal(t, val2, float64(i+1))
		val3, err := part.GetRow(i).GetVarString("col3")
		require.Nil(t, err)
		require.Equal(t, val3, "Hello World")
	}
	// test repack
	newSchema := schema.Clone()
	newSchema.RemoveColumn("col1")
	newSchema.CreateColumn("col4", &sif.Int32ColumnType{})
	newSchema.CreateColumn("col5", &sif.Int32ColumnType{})
	newPart, err := part.Repack(newSchema)
	require.Nil(t, err)
	require.Equal(t, part.GetNumRows(), newPart.GetNumRows())
	require.Equal(t, part.GetMaxRows(), newPart.GetMaxRows())
	for i := 0; i < 8; i++ {
		origRow := part.GetRow(i)
		newRow := newPart.GetRow(i)
		val2, err := origRow.GetFloat64("col2")
		require.Nil(t, err)
		newVal2, err := newRow.GetFloat64("col2")
		require.Nil(t, err)
		require.Equal(t, val2, newVal2)
		val3, err := origRow.GetVarString("col3")
		require.Nil(t, err)
		newVal3, err := newRow.GetVarString("col3")
		require.Nil(t, err)
		require.Equal(t, val3, newVal3)
	}
}

func TestKeyColumns(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	schema.CreateColumn("col2", &sif.Float64ColumnType{})
	schema.CreateColumn("col3", &sif.VarStringColumnType{})
	part1 := createPartitionImpl(8, 8, schema)
	part2 := createPartitionImpl(8, 8, schema)
	// append rows
	tempRow := CreateTempRow()
	for i := 0; i < 4; i++ {
		row, err := part1.AppendEmptyRowData(tempRow)
		require.Nil(t, err)
		row.SetInt8("col1", int8(i))
		row.SetFloat64("col2", float64(i+1))
		row.SetVarString("col3", "Hello World")
		row, err = part2.AppendEmptyRowData(tempRow)
		require.Nil(t, err)
		row.SetInt8("col1", int8(i))
		row.SetFloat64("col2", float64(i+1))
		row.SetVarString("col3", "Hello World")
	}

	for i := 0; i < 4; i++ {
		key1, err := transform.KeyColumns("col1", "col2", "col3")(part1.GetRow(i))
		require.Nil(t, err)
		key2, err := transform.KeyColumns("col1", "col2", "col3")(part2.GetRow(i))
		require.Nil(t, err)
		require.Equal(t, key1, key2)
	}

	// Keys for different values shouldn't be equal
	for i := 0; i < 4; i++ {
		key1, err := transform.KeyColumns("col1", "col2", "col3")(part1.GetRow(i))
		require.Nil(t, err)
		key2, err := transform.KeyColumns("col1", "col2", "col3")(part2.GetRow(3 - i))
		require.Nil(t, err)
		require.NotEqual(t, key1, key2)
	}

	// Order matters
	for i := 0; i < 4; i++ {
		key1, err := transform.KeyColumns("col2", "col1", "col3")(part1.GetRow(i))
		require.Nil(t, err)
		key2, err := transform.KeyColumns("col1", "col2", "col3")(part2.GetRow(i))
		require.Nil(t, err)
		require.NotEqual(t, key1, key2)
	}
}

func TestGrow(t *testing.T) {
	// create partition
	schema := createPartitionTestSchema()
	schema.CreateColumn("col2", &sif.Float64ColumnType{})
	schema.CreateColumn("col3", &sif.VarStringColumnType{})
	part := createPartitionImpl(8, defaultCapacity, schema)
	require.Equal(t, defaultCapacity, part.capacity)
	require.Equal(t, part.capacity, len(part.varRowData))
	require.Equal(t, part.capacity, len(part.serializedVarRowData))
	require.Equal(t, part.capacity, len(part.rowMeta)/schema.NumColumns())
	require.Equal(t, part.capacity, len(part.rows)/schema.Size())
	// append rows
	tempRow := CreateTempRow()
	for i := 0; i < defaultCapacity+1; i++ {
		row, err := part.AppendEmptyRowData(tempRow)
		require.Nil(t, err)
		row.SetInt8("col1", int8(i))
		row.SetFloat64("col2", float64(i+1))
		row.SetVarString("col3", "Hello World")
	}
	// check sizes
	require.Equal(t, defaultCapacity+1, part.numRows)
	require.Equal(t, defaultCapacity*2, part.capacity)
	require.Equal(t, part.capacity, len(part.varRowData))
	require.Equal(t, part.capacity, len(part.serializedVarRowData))
	require.Equal(t, part.capacity, len(part.rowMeta)/schema.NumColumns())
	require.Equal(t, part.capacity, len(part.rows)/schema.Size())
	// check values
	for i := 0; i < defaultCapacity+1; i++ {
		row := part.GetRow(i)
		col1, err := row.GetInt8("col1")
		require.Nil(t, err)
		require.Equal(t, int8(i), col1)
		col2, err := row.GetFloat64("col2")
		require.Nil(t, err)
		require.Equal(t, float64(i+1), col2)
		col3, err := row.GetVarString("col3")
		require.Nil(t, err)
		require.Equal(t, "Hello World", col3)
	}
}
