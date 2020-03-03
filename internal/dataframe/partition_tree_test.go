package dataframe

import (
	"testing"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/go-sif/sif/schema"
	"github.com/stretchr/testify/require"
)

func createPTreeTestSchema() sif.Schema {
	schema := schema.CreateSchema()
	schema.CreateColumn("key", &sif.ByteColumnType{})
	schema.CreateColumn("val", &sif.ByteColumnType{})
	return schema
}

func pTreeTestReducer(lrow sif.Row, rrow sif.Row) error {
	lval, err := lrow.GetByte("val")
	if err != nil {
		return err
	}
	rval, err := rrow.GetByte("val")
	if err != nil {
		return err
	}
	lrow.SetByte("val", lval+rval)
	return nil
}

func pTreeTestKeyer(row sif.Row) ([]byte, error) {
	val, err := row.GetByte("key")
	if err != nil {
		return nil, err
	}
	return []byte{val}, nil
}

func TestCreatePartitionTree(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: "./", InMemoryPartitions: 20}
	root := createPTreeNode(conf, 3, schema, schema)

	require.NotNil(t, root.part)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	require.Equal(t, 0, root.part.GetNumRows())
}

func TestMergeRow(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: "./", InMemoryPartitions: 20}
	root := createPTreeNode(conf, 3, schema, schema)

	// add the first row
	row := partition.CreateRow([]byte{0, 0}, []byte{1, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
	err := root.mergeRow(row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.NotNil(t, root.part)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	require.Equal(t, 1, root.part.GetNumRows())

	// add another distinct row
	row = partition.CreateRow([]byte{0, 0}, []byte{2, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
	err = root.mergeRow(row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.NotNil(t, root.part)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	require.Equal(t, 2, root.part.GetNumRows())

	// add a merge row
	row = partition.CreateRow([]byte{0, 0}, []byte{1, 2}, make(map[string]interface{}), make(map[string][]byte), schema)
	err = root.mergeRow(row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.NotNil(t, root.part)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	require.Equal(t, 2, root.part.GetNumRows(), "We should have merged, not appended")
	// Test keys are sorted
	lastKey := uint64(0)
	for i := 0; i < root.part.GetNumRows(); i++ {
		k, err := root.part.GetKey(i)
		require.Nil(t, err)
		require.True(t, k > lastKey)
		lastKey = k
	}
	// find row number for key 1
	keyBuf, err := pTreeTestKeyer(row)
	require.Nil(t, err)
	hasher := xxhash.New()
	hasher.Write(keyBuf)
	hashedKey := hasher.Sum64()
	idx, err := root.part.FindFirstRowKey(keyBuf, hashedKey, pTreeTestKeyer)
	require.Nil(t, err)
	// Test value is correct
	val, err := root.part.GetRow(idx).GetByte("val")
	require.Nil(t, err)
	require.EqualValues(t, 3, val)
}

func TestMergeRowWithSplit(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: "./", InMemoryPartitions: 20}
	root := createPTreeNode(conf, 3, schema, schema)

	for i := byte(0); i < byte(6); i++ {
		row := partition.CreateRow([]byte{0, 0}, []byte{i, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
		err := root.mergeRow(row, pTreeTestKeyer, pTreeTestReducer)
		require.Nil(t, err)
	}
	require.Nil(t, root.part)
	require.NotNil(t, root.left)
	require.NotNil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)

	numTreeRows := 0
	for start := root.firstNode(); start != nil; start = start.next {
		require.NotNil(t, start.part)
		numTreeRows += start.part.GetNumRows()
	}
	require.Equal(t, 6, numTreeRows)
}
