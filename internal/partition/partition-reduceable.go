package partition

import (
	"fmt"
	"log"
	"reflect"

	"github.com/go-sif/sif"
	errors "github.com/go-sif/sif/errors"
	pb "github.com/go-sif/sif/internal/rpc"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/golang/protobuf/proto"
)

// CreateReduceablePartition creates a new Partition containing an empty byte array and a schema
func CreateReduceablePartition(maxRows int, schema sif.Schema) itypes.ReduceablePartition {
	return createPartitionImpl(maxRows, schema)
}

// CreateKeyedReduceablePartition creates a new Partition containing an empty byte array and a schema
func CreateKeyedReduceablePartition(maxRows int, schema sif.Schema) itypes.ReduceablePartition {
	part := createPartitionImpl(maxRows, schema)
	part.isKeyed = true
	part.keys = make([]uint64, maxRows)
	return part
}

// FindFirstKey locates the first instance of a key within a sorted Partition,
// returning the FIRST index of the key in the Partition, or an error
// if it isn't found along with the location the key should be
// inserted at.
// PRECONDITION: Partition must already be sorted by key
func (p *partitionImpl) FindFirstKey(key uint64) (int, error) {
	l := 0
	r := p.GetNumRows() - 1
	for l <= r {
		m := (l + r) >> 1
		if key > p.keys[m] {
			l = m + 1
		} else if key < p.keys[m] {
			r = m - 1
		} else if l != m {
			r = m
		} else {
			break
		}
	}
	if l < len(p.keys) && key == p.keys[l] {
		return l, nil
	}
	return l, errors.MissingKeyError{}
}

// FindLastKey locates the last instance of a uint64 key within a sorted Partition,
// returning the LAST index of the key in the Partition, or an error
// if it isn't found along with the location the key should be
// inserted at.
// PRECONDITION: Partition must already be sorted by key
func (p *partitionImpl) FindLastKey(key uint64) (int, error) {
	firstKey, err := p.FindFirstKey(key) // this will error with missing key if it doesn't exist
	if err != nil {
		return firstKey, err
	}
	lastKey := firstKey
	// iterate over each row with a matching key to find the last one with identical key bytes
	for i := firstKey + 1; i < p.GetNumRows(); i++ {
		if k, err := p.GetKey(i); err != nil {
			return -1, err
		} else if k != key {
			break // current key isn't the same, break out
		}
		lastKey = i
	}
	return lastKey, nil
}

// FindFirstRowKey locates the first instance of a uint64 key within a sorted Partition,
// then uses a KeyingOperation to find the actual row whose key bytes match
// a specific set of key bytes used to produce the uint64 key. If the key does not
// exist within the Partition, an error is returned along with the position it should
// be located at.
// PRECONDITION: Partition must already be sorted by key
func (p *partitionImpl) FindFirstRowKey(keyBuf []byte, key uint64, keyfn sif.KeyingOperation) (int, error) {
	// find the first matching uint64 key
	firstKey, err := p.FindFirstKey(key)
	if err != nil {
		return firstKey, err
	}
	// iterate over each row with a matching key to find the first one with identical key bytes
	tempRow := &rowImpl{}
	for i := firstKey; i < p.GetNumRows(); i++ {
		if k, err := p.GetKey(i); err != nil || k != key {
			return -1, err
		}

		rowKey, err := keyfn(p.getRow(tempRow, i))
		if err != nil {
			return -1, err
		} else if reflect.DeepEqual(keyBuf, rowKey) {
			return i, nil
		}
	}
	return firstKey, nil
}

// FindLastRowKey locates the last instance of a uint64 key within a sorted Partition,
// then uses a KeyingOperation to find the actual row whose key bytes match
// a specific set of key bytes used to produce the uint64 key. If the key does not
// exist within the Partition, an error is returned along with the position it should
// be located at.
// PRECONDITION: Partition must already be sorted by key
func (p *partitionImpl) FindLastRowKey(keyBuf []byte, key uint64, keyfn sif.KeyingOperation) (int, error) {
	// find the first matching uint64 key
	firstKey, err := p.FindFirstRowKey(keyBuf, key, keyfn) // this will error with missing key if it doesn't exist
	if err != nil {
		return firstKey, err
	}
	lastKey := firstKey
	// iterate over each row with a matching key to find the last one with identical key bytes
	tempRow := &rowImpl{}
	for i := firstKey + 1; i < p.GetNumRows(); i++ {
		if k, err := p.GetKey(i); err != nil {
			return -1, err
		} else if k != key {
			break // current key isn't the same, break out
		}
		rowKey, err := keyfn(p.getRow(tempRow, i))
		if err != nil {
			return -1, err
		} else if reflect.DeepEqual(keyBuf, rowKey) {
			lastKey = i
		} else {
			break // current key isn't the same, break out
		}
	}
	return lastKey, nil
}

// AverageKeyValue computes the floored average
// value of key within this sorted, keyed Partition
func (p *partitionImpl) AverageKeyValue() (uint64, error) {
	if p.GetNumRows() == 0 {
		return 0, nil
	}
	sum := uint64(0)
	firstKey, err := p.GetKey(0)
	if err != nil {
		return 0, err
	}
	for _, k := range p.keys {
		sum += k - firstKey
	}
	return sum/uint64(len(p.keys)) + firstKey, nil
}

// Split splits a Partition into two Partitions. Split position ends up in right Partition.
func (p *partitionImpl) Split(pos int) (itypes.ReduceablePartition, itypes.ReduceablePartition, error) {
	if pos >= p.numRows {
		return nil, nil, fmt.Errorf("Split position is outside of Partition bounds")
	}
	left := createPartitionImpl(p.maxRows, p.schema)
	right := createPartitionImpl(p.maxRows, p.schema)
	if p.isKeyed {
		left.isKeyed = true
		left.keys = make([]uint64, p.maxRows)
		right.isKeyed = true
		right.keys = make([]uint64, p.maxRows)
		for i := 0; i < p.GetNumRows(); i++ {
			row := p.GetRowData(i)
			meta := p.GetRowMeta(i)
			varData := p.GetVarRowData(i)
			serializedVarData := p.GetSerializedVarRowData(i)
			key, err := p.GetKey(i)
			if err != nil {
				return nil, nil, err
			}
			if i < pos {
				left.AppendKeyedRowData(row, meta, varData, serializedVarData, key)
			} else {
				right.AppendKeyedRowData(row, meta, varData, serializedVarData, key)
			}
		}
	} else {
		for i := 0; i < p.GetNumRows(); i++ {
			row := p.GetRowData(i)
			meta := p.GetRowMeta(i)
			varData := p.GetVarRowData(i)
			serializedVarData := p.GetSerializedVarRowData(i)
			if i < pos {
				left.AppendRowData(row, meta, varData, serializedVarData)
			} else {
				right.AppendRowData(row, meta, varData, serializedVarData)
			}
		}
	}
	return left, right, nil
}

// BalancedSplit attempts to split a sorted Partition based on the
// average key value in the Partition, assuring
// that identical keys end up in the same Partition.
// Identical keys can occur due to hash collisions, or if the containing
// tree is not reducing. Split position ends up in right Partition.
func (p *partitionImpl) BalancedSplit() (uint64, itypes.ReduceablePartition, itypes.ReduceablePartition, error) {
	if !p.isKeyed {
		return 0, nil, nil, fmt.Errorf("Partition is not keyed")
	}
	avgKey, err := p.AverageKeyValue()
	if err != nil {
		return 0, nil, nil, err
	}
	splitPos, _ := p.FindFirstKey(avgKey) // doesn't matter if key doesn't exist in p
	// find the first instance of the actual split position key
	// within the Partition so as not to split up identical keys
	key := p.keys[splitPos]
	for splitPos > 0 {
		if p.keys[splitPos-1] != key {
			break
		}
		splitPos--
	}
	// if the split position is the first row, then we don't need to do any work
	if splitPos == 0 {
		return avgKey, nil, nil, errors.FullOfIdenticalKeysError{}
	}
	lp, rp, err := p.Split(splitPos)
	return avgKey, lp, rp, err
}

// ToBytes serializes a Partition to a byte array suitable for persistence to disk
func (p *partitionImpl) ToBytes() ([]byte, error) {
	numRows := p.GetNumRows()
	svrd := make(map[string]*pb.DPartition_DVarCol)
	// set up column data holders
	p.schema.ForEachColumn(func(name string, col sif.Column) error {
		if p.schema.IsMarkedForRemoval(name) {
			return nil
		} else if !sif.IsVariableLength(col.Type()) {
			return nil
		}
		svrd[name] = &pb.DPartition_DVarCol{
			RowData: make(map[uint32][]byte),
		}
		return nil
	})
	for i := 0; i < numRows; i++ {
		// transfer un-deserialized variable-length data (possible if never accessed since last reduction)
		svarData := p.GetSerializedVarRowData(i)
		for k, v := range svarData {
			svrd[k].RowData[uint32(i)] = v
			if len(v) == 0 {
				log.Panicf("Serialized column data for Column %s should not be zero-length", k)
			}
		}
		// then, serialize any data which was deserialized during this stage
		varData := p.GetVarRowData(i)
		for k, v := range varData {
			if v == nil {
				svrd[k].RowData[uint32(i)] = nil
				continue
			}
			if col, err := p.schema.GetOffset(k); err == nil {
				if vcol, ok := col.Type().(sif.VarColumnType); ok {
					sdata, err := vcol.Serialize(v)
					if err != nil {
						return nil, err
					}
					svrd[k].RowData[uint32(i)] = sdata
					if len(sdata) == 0 {
						log.Panicf("Serialized column data for Column %s should not be zero-length", k)
					}
				} else {
					log.Panicf("Column %s is not a variable-length type", k)
				}
			}
		}
	}
	dm := &pb.DPartition{
		Id:                   p.id,
		NumRows:              uint32(p.numRows),
		MaxRows:              uint32(p.maxRows),
		IsKeyed:              p.isKeyed,
		RowData:              p.rows,
		RowMeta:              p.rowMeta,
		Keys:                 p.keys,
		SerializedVarRowData: svrd,
	}
	data, err := proto.Marshal(dm)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// FromBytes converts disk-serialized bytes into a Partition
func FromBytes(data []byte, schema sif.Schema) (itypes.ReduceablePartition, error) {
	m := &pb.DPartition{}
	err := proto.Unmarshal(data, m)
	if err != nil {
		return nil, err
	}
	part := &partitionImpl{
		id:                   m.Id,
		maxRows:              int(m.MaxRows),
		numRows:              int(m.NumRows),
		rows:                 m.RowData,
		varRowData:           make([]map[string]interface{}, int(m.MaxRows)*schema.Size()),
		serializedVarRowData: make([]map[string][]byte, int(m.MaxRows)*schema.Size()),
		rowMeta:              m.RowMeta,
		schema:               schema,
		keys:                 nil,
		isKeyed:              m.IsKeyed,
	}
	for colName, colData := range m.SerializedVarRowData {
		for j, sv := range colData.RowData {
			if part.serializedVarRowData[j] == nil {
				part.serializedVarRowData[j] = make(map[string][]byte)
			}
			part.serializedVarRowData[j][colName] = sv
		}
	}
	if m.IsKeyed {
		part.keys = m.Keys
	}
	return part, nil
}
