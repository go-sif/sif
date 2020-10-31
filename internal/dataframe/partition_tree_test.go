package dataframe

import (
	"math/rand"
	"os"
	"testing"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/go-sif/sif/operations/transform"
	"github.com/go-sif/sif/schema"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
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
	conf := &itypes.PlanExecutorConfig{TempFilePath: os.TempDir(), CacheMemoryInitialSize: 20}
	root := createPTreeNode(conf, 3, schema)
	defer root.clearCaches()

	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, unlockPartition, err := root.loadPartition()
	defer unlockPartition()
	require.Nil(t, err)
	require.Equal(t, 0, part.GetNumRows())
}

func TestMergeRow(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: os.TempDir(), CacheMemoryInitialSize: 20}
	root := createPTreeNode(conf, 3, schema)
	defer root.clearCaches()

	// add the first row
	row := partition.CreateRow("part-0", []byte{0, 0}, []byte{1, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
	err := root.mergeRow(partition.CreateTempRow(), row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, unlockPartition, err := root.loadPartition()
	require.Nil(t, err)
	require.Equal(t, 1, part.GetNumRows())
	unlockPartition()

	// add another distinct row
	row = partition.CreateRow("part-0", []byte{0, 0}, []byte{2, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
	err = root.mergeRow(partition.CreateTempRow(), row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, unlockPartition, err = root.loadPartition()
	require.Nil(t, err)
	require.Equal(t, 2, part.GetNumRows())
	unlockPartition()

	// add a merge row
	row = partition.CreateRow("part-0", []byte{0, 0}, []byte{1, 2}, make(map[string]interface{}), make(map[string][]byte), schema)
	err = root.mergeRow(partition.CreateTempRow(), row, pTreeTestKeyer, pTreeTestReducer)
	require.Nil(t, err)
	require.Greater(t, len(root.partID), 0)
	require.Nil(t, root.parent)
	require.Nil(t, root.left)
	require.Nil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)
	require.Equal(t, root, root.firstNode())
	part, unlockPartition, err = root.loadPartition()
	defer unlockPartition()
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
	conf := &itypes.PlanExecutorConfig{TempFilePath: os.TempDir(), CacheMemoryInitialSize: 20}
	root := createPTreeNode(conf, 3, schema)
	defer root.clearCaches()

	tempRow := partition.CreateTempRow()
	for i := byte(0); i < byte(6); i++ {
		row := partition.CreateRow("part-0", []byte{0, 0}, []byte{i, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
		err := root.mergeRow(tempRow, row, pTreeTestKeyer, pTreeTestReducer)
		require.Nil(t, err)
	}
	require.Equal(t, len(root.partID), 0)
	require.NotNil(t, root.left)
	require.NotNil(t, root.right)
	require.Nil(t, root.prev)
	require.Nil(t, root.next)

	numTreeRows := 0
	numParts := 0
	for start := root.firstNode(); start != nil; start = start.next {
		numParts++
		require.NotNil(t, start.partID)
		part, unlockPartition, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
		unlockPartition()
	}
	require.Equal(t, 6, numTreeRows)
	require.EqualValues(t, numParts, root.numParts())
}

func TestMergeRowWithRotate(t *testing.T) {
	schema := createPTreeTestSchema()
	conf := &itypes.PlanExecutorConfig{TempFilePath: os.TempDir(), CacheMemoryInitialSize: 20}
	root := createPTreeNode(conf, 3, schema)
	defer root.clearCaches()
	tempRow := partition.CreateTempRow()
	for i := 0; i < 8; i++ {
		row := partition.CreateRow("part-0", []byte{0, 0}, []byte{1, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
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
	numParts := 0
	for start := root.firstNode(); start != nil; start = start.next {
		require.NotNil(t, start.partID)
		part, unlockPartition, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
		numParts++
		unlockPartition()
	}
	require.Equal(t, 8, numTreeRows)
	require.EqualValues(t, numParts, root.numParts())
	// add more rows with a different key, and check that they're sorted properly
	for i := 0; i < 8; i++ {
		row := partition.CreateRow("part-0", []byte{0, 0}, []byte{2, 1}, make(map[string]interface{}), make(map[string][]byte), schema)
		err := root.mergeRow(tempRow, row, pTreeTestKeyer, nil)
		require.Nil(t, err)
	}
	lastKey := uint64(0)
	numTreeRows = 0
	numParts = 0
	for start := root.firstNode(); start != nil; start = start.next {
		require.NotNil(t, start.partID)
		part, unlockPartition, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
		numParts++
		for i := 0; i < part.GetNumRows(); i++ {
			k, err := part.GetKey(i)
			require.Nil(t, err)
			require.True(t, k >= lastKey)
			lastKey = k
		}
		unlockPartition()
	}
	require.Equal(t, 16, numTreeRows)
	require.EqualValues(t, numParts, root.numParts())
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

	conf := &itypes.PlanExecutorConfig{TempFilePath: os.TempDir(), CacheMemoryInitialSize: 5}
	// each partition can store 2 rows
	root := createPTreeNode(conf, 2, schema)
	defer root.clearCaches()
	tempRow := partition.CreateTempRow()
	// store enough rows that we have 20 partitions, so some get swapped to disk
	for i := uint32(0); i < 40; i++ {
		row := partition.CreateRow("part-0", []byte{0, 0}, make([]byte, 8), make(map[string]interface{}), make(map[string][]byte), schema)
		require.Nil(t, row.SetUint32("key", i))
		require.Nil(t, row.SetUint32("val", i))
		err := root.mergeRow(tempRow, row, transform.KeyColumns("key"), reduceFn)
		require.Nil(t, err)
	}
	// Now do it again, forcing those partitions to be reloaded
	for i := uint32(0); i < 40; i++ {
		row := partition.CreateRow("part-0", []byte{0, 0}, make([]byte, 8), make(map[string]interface{}), make(map[string][]byte), schema)
		require.Nil(t, row.SetUint32("key", i))
		require.Nil(t, row.SetUint32("val", i))
		err := root.mergeRow(tempRow, row, transform.KeyColumns("key"), reduceFn)
		require.Nil(t, err)
	}
}

func TestPartitionIterationDuringReduction(t *testing.T) {
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

	conf := &itypes.PlanExecutorConfig{TempFilePath: os.TempDir(), CacheMemoryInitialSize: 5}
	// each partition can store 2 rows
	root := createPTreeNode(conf, 2, schema)
	defer root.clearCaches()
	tempRow := partition.CreateTempRow()
	rowCount := 25
	// store a bunch of random rows, so some partitions get swapped to disk
	for i := 0; i < rowCount; i++ {
		row := partition.CreateRow("part-0", []byte{0, 0}, make([]byte, 8), make(map[string]interface{}), make(map[string][]byte), schema)
		require.Nil(t, row.SetUint32("key", uint32(i)))
		require.Nil(t, row.SetUint32("val", rand.Uint32()))
		err := root.mergeRow(tempRow, row, transform.KeyColumns("key"), reduceFn)
		require.Nil(t, err)
	}
	// make sure all rows are present, and sorted by hashed key
	lastKey := uint64(0)
	numTreeRows := 0
	numParts := 0
	firstNode := root.firstNode()
	for start := firstNode; start != nil; start = start.next {
		require.NotNil(t, start.partID)
		part, unlockPartition, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
		numParts++
		for i := 0; i < part.GetNumRows(); i++ {
			k, err := part.GetKey(i)
			require.Nil(t, err)
			require.True(t, k >= lastKey)
			lastKey = k
		}
		unlockPartition()
	}
	require.Equal(t, rowCount, numTreeRows)
	require.EqualValues(t, numParts, root.numParts())
}

func TestPartitionIterationDuringRepartition(t *testing.T) {
	defer goleak.VerifyNone(t)

	schema := schema.CreateSchema()
	schema.CreateColumn("key", &sif.Uint32ColumnType{})
	schema.CreateColumn("val", &sif.Uint32ColumnType{})

	conf := &itypes.PlanExecutorConfig{TempFilePath: os.TempDir(), CacheMemoryInitialSize: 10}
	// each partition can store 2 rows
	root := createPTreeNode(conf, 2, schema)
	defer root.clearCaches()
	tempRow := partition.CreateTempRow()
	rowCount := 200
	// store a bunch of random rows, so some partitions get swapped to disk
	for i := 0; i < rowCount; i++ {
		row := partition.CreateRow("part-0", []byte{0, 0}, make([]byte, 8), make(map[string]interface{}), make(map[string][]byte), schema)
		require.Nil(t, row.SetUint32("key", uint32(i/5))) // make sure we have duplicate keys
		require.Nil(t, row.SetUint32("val", rand.Uint32()))
		err := root.mergeRow(tempRow, row, transform.KeyColumns("key"), nil)
		require.Nil(t, err)
	}
	// make sure all rows are present, and sorted by hashed key
	lastKey := uint64(0)
	numTreeRows := 0
	numTreeParts := 0
	firstNode := root.firstNode()
	for start := firstNode; start != nil; start = start.next {
		require.NotNil(t, start.partID)
		numTreeParts++
		part, unlockPartition, err := start.loadPartition()
		require.Nil(t, err)
		numTreeRows += part.GetNumRows()
		for i := 0; i < part.GetNumRows(); i++ {
			k, err := part.GetKey(i)
			require.Nil(t, err)
			require.True(t, k >= lastKey)
			lastKey = k
		}
		unlockPartition()
	}
	require.Equal(t, rowCount, numTreeRows)
	require.EqualValues(t, numTreeParts, root.numParts())
}
