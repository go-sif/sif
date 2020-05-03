package dataframe

import (
	"io/ioutil"
	"os"
	"testing"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/go-sif/sif/operations/transform"
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

	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, err := root.loadPartition()
	require.Nil(t, err)
	require.Equal(t, 0, part.GetNumRows())
}

func TestMergeRow(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: "./", InMemoryPartitions: 20}
	root := createPTreeNode(conf, 3, schema, schema)

	// add the first row
	row := partition.CreateRow([]byte{0, 0}, []byte{1, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
	err := root.mergeRow(partition.CreateTempRow(), row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, err := root.loadPartition()
	require.Nil(t, err)
	require.Equal(t, 1, part.GetNumRows())

	// add another distinct row
	row = partition.CreateRow([]byte{0, 0}, []byte{2, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
	err = root.mergeRow(partition.CreateTempRow(), row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, err = root.loadPartition()
	require.Nil(t, err)
	require.Equal(t, 2, part.GetNumRows())

	// add a merge row
	row = partition.CreateRow([]byte{0, 0}, []byte{1, 2}, make(map[string]interface{}), make(map[string][]byte), schema)
	err = root.mergeRow(partition.CreateTempRow(), row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, err = root.loadPartition()
	require.Nil(t, err)
	require.Equal(t, 2, part.GetNumRows(), "We should have merged, not appended")
	// Test keys are sorted
	lastKey := uint64(0)
	for i := 0; i < part.GetNumRows(); i++ {
		k, err := part.GetKey(i)
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
	// uncomment if true key matching ends up being important
	// idx, err := root.part.FindFirstRowKey(keyBuf, hashedKey, pTreeTestKeyer)
	idx, err := part.FindFirstKey(hashedKey)
	require.Nil(t, err)
	// Test value is correct
	val, err := part.GetRow(idx).GetByte("val")
	require.Nil(t, err)
	require.EqualValues(t, 3, val)
}

func TestMergeRowWithSplit(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: "./", InMemoryPartitions: 20}
	root := createPTreeNode(conf, 3, schema, schema)

	tempRow := partition.CreateTempRow()
	for i := byte(0); i < byte(6); i++ {
		row := partition.CreateRow([]byte{0, 0}, []byte{i, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
		err := root.mergeRow(tempRow, row, pTreeTestKeyer, pTreeTestReducer)
		require.Nil(t, err)
	}
	require.Equal(t, len(root.partID), 0)
	require.NotNil(t, root.left)
	require.NotNil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)

	numTreeRows := 0
	for start := root.firstNode(); start != nil; start = start.next {
		require.NotNil(t, start.partID)
		part, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
	}
	require.Equal(t, 6, numTreeRows)
}

func TestMergeRowWithRotate(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: "./", InMemoryPartitions: 20}
	root := createPTreeNode(conf, 3, schema, schema)
	tempRow := partition.CreateTempRow()
	for i := 0; i < 8; i++ {
		row := partition.CreateRow([]byte{0, 0}, []byte{1, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
		err := root.mergeRow(tempRow, row, pTreeTestKeyer, nil)
		require.Nil(t, err)
	}
	require.Equal(t, len(root.partID), 0)
	require.NotNil(t, root.center)
	require.NotNil(t, root.left)
	require.NotNil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	numTreeRows := 0
	for start := root.firstNode(); start != nil; start = start.next {
		require.NotNil(t, start.partID)
		part, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
	}
	require.Equal(t, 8, numTreeRows)
	// add more rows with a different key, and check that they're sorted properly
	for i := 0; i < 8; i++ {
		row := partition.CreateRow([]byte{0, 0}, []byte{2, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
		err := root.mergeRow(tempRow, row, pTreeTestKeyer, nil)
		require.Nil(t, err)
	}
	lastKey := uint64(0)
	numTreeRows = 0
	for start := root.firstNode(); start != nil; start = start.next {
		require.NotNil(t, start.partID)
		part, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
		for i := 0; i < part.GetNumRows(); i++ {
			k, err := part.GetKey(i)
			require.Nil(t, err)
			require.True(t, k >= lastKey)
			lastKey = k
		}
	}
	require.Equal(t, 16, numTreeRows)
}

func TestDiskSwap(t *testing.T) {
	schema := schema.CreateSchema()
	schema.CreateColumn("key", &sif.Uint32ColumnType{})
	schema.CreateColumn("val", &sif.Uint32ColumnType{})

	reduceFn := func(lrow sif.Row, rrow sif.Row) error {
		// validate that both rows have the same values
		lval, err := lrow.GetUint32("val")
		require.Nil(t, err)
		rval, err := rrow.GetUint32("val")
		require.Nil(t, err)
		require.Equal(t, lval, rval)
		return nil
	}

	tempDir, err := ioutil.TempDir("./", "sif-worker-8080")
	require.Nil(t, err)
	defer os.RemoveAll(tempDir)
	conf := &itypes.PlanExecutorConfig{TempFilePath: tempDir, InMemoryPartitions: 5}
	// each partition can store 2 rows
	root := createPTreeNode(conf, 2, schema, schema)
	tempRow := partition.CreateTempRow()
	// store enough rows that we have 20 partitions, so some get swapped to disk
	for i := uint32(0); i < 40; i++ {
		row := partition.CreateRow([]byte{0, 0}, make([]byte, 8), make(map[string]interface{}), make(map[string][]byte), schema)
		require.Nil(t, row.SetUint32("key", i))
		require.Nil(t, row.SetUint32("val", i))
		err := root.mergeRow(tempRow, row, transform.KeyColumns("key"), reduceFn)
		require.Nil(t, err)
	}
	// Now do it again, forcing those partitions to be reloaded
	for i := uint32(0); i < 40; i++ {
		row := partition.CreateRow([]byte{0, 0}, make([]byte, 8), make(map[string]interface{}), make(map[string][]byte), schema)
		require.Nil(t, row.SetUint32("key", i))
		require.Nil(t, row.SetUint32("val", i))
		err := root.mergeRow(tempRow, row, transform.KeyColumns("key"), reduceFn)
		require.Nil(t, err)
	}
}
