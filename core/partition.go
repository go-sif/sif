package core

import (
	"fmt"
	"io"
	"log"
	"reflect"

	xxhash "github.com/cespare/xxhash"
	pb "github.com/go-sif/sif/core/internal/rpc"
	errors "github.com/go-sif/sif/errors"
	uuid "github.com/gofrs/uuid"
	proto "github.com/golang/protobuf/proto"
	"github.com/hashicorp/go-multierror"
)

const (
	// rowDataType indicates that Row data is being transferred
	rowDataType = 1 << iota
	// rowVarDataType indicates that variable-length Row data is being transferred
	rowVarDataType
	// metaDataType indicates that Meta data is being transferred
	metaDataType
	// keyDataType indicates that Key data is being transferred
	keyDataType
)

// Partition is a portion of a columnar dataset, consisting of multiple Rows.
// Partitions are not generally interacted with directly, instead being
// manipulated in parallel by DataFrame Tasks.
type Partition struct {
	id                   string
	maxRows              int
	numRows              int
	rows                 []byte
	varRowData           []map[string]interface{}
	serializedVarRowData []map[string][]byte // for receiving serialized data from a shuffle (temporary)
	rowMeta              []byte
	widestSchema         *Schema
	currentSchema        *Schema
	keys                 []uint64
	isKeyed              bool
}

// CreatePartition creates a new Partition containing an empty byte array and a schema
func CreatePartition(maxRows int, widestSchema *Schema, currentSchema *Schema) *Partition {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID for Partition: %v", err)
	}
	return &Partition{
		id:                   id.String(),
		maxRows:              maxRows,
		numRows:              0,
		rows:                 make([]byte, maxRows*widestSchema.Size(), maxRows*widestSchema.Size()),
		varRowData:           make([]map[string]interface{}, maxRows),
		serializedVarRowData: make([]map[string][]byte, maxRows),
		rowMeta:              make([]byte, maxRows*widestSchema.NumColumns()),
		widestSchema:         widestSchema,
		currentSchema:        currentSchema,
		keys:                 make([]uint64, 0),
		isKeyed:              false,
	}
}

// ID retrieves the ID of this Partition
func (p *Partition) ID() string {
	return p.id
}

// GetMaxRows retrieves the maximum number of rows in this Partition
func (p *Partition) GetMaxRows() int {
	return p.maxRows
}

// GetNumRows retrieves the number of rows in this Partition
func (p *Partition) GetNumRows() int {
	return p.numRows
}

// GetRow retrieves a specific row from this Partition
func (p *Partition) GetRow(rowNum int) *Row {
	return &Row{
		meta:              p.getRowMeta(rowNum),
		data:              p.getRowData(rowNum),
		varData:           p.getVarRowData(rowNum),
		serializedVarData: p.getSerializedVarRowData(rowNum),
		schema:            p.currentSchema,
	}
}

// getRowMeta retrieves specific row metadata from this Partition
func (p *Partition) getRowMeta(rowNum int) []byte {
	start := rowNum * p.widestSchema.NumColumns()
	end := start + p.widestSchema.NumColumns()
	return p.rowMeta[start:end]
}

// getRowMetaRange retrieves an arbitrary range of bytes from the row meta
func (p *Partition) getRowMetaRange(start int, end int) []byte {
	maxByte := p.numRows * p.widestSchema.NumColumns()
	if end > maxByte {
		end = maxByte
	}
	return p.rowMeta[start:end]
}

// getRowData retrieves a specific row from this Partition
func (p *Partition) getRowData(rowNum int) []byte {
	start := rowNum * p.widestSchema.Size()
	end := start + p.widestSchema.Size()
	return p.rows[start:end]
}

// getRowDataRange retrieves an arbitrary range of bytes from the row data
func (p *Partition) getRowDataRange(start int, end int) []byte {
	maxByte := p.numRows * p.widestSchema.Size()
	if end > maxByte {
		end = maxByte
	}
	return p.rows[start:end]
}

// getVarRowData retrieves the variable-length data for a given row from this Partition
func (p *Partition) getVarRowData(rowNum int) map[string]interface{} {
	if p.varRowData[rowNum] == nil {
		p.varRowData[rowNum] = make(map[string]interface{})
	}
	return p.varRowData[rowNum]
}

// getSerializedVarRowData retrieves the serialized variable-length data for a given row from this Partition
func (p *Partition) getSerializedVarRowData(rowNum int) map[string][]byte {
	if p.serializedVarRowData[rowNum] == nil {
		p.serializedVarRowData[rowNum] = make(map[string][]byte)
	}
	return p.serializedVarRowData[rowNum]
}

// canInsertRowData checks if a Row can be inserted into this Partition
func (p *Partition) canInsertRowData(row []byte) error {
	// TODO accept and check variable length map for unknown keys
	if len(row) > p.widestSchema.size {
		return errors.IncompatibleRowError{}
	} else if p.numRows >= p.maxRows {
		return errors.PartitionFullError{}
	} else {
		return nil
	}
}

// AppendEmptyRowData is a convenient way to add an empty Row to the end of this Partition, returning the Row so that Row methods can be used to populate it
func (p *Partition) AppendEmptyRowData() (*Row, error) {
	newRowNum := p.numRows
	err := p.AppendRowData([]byte{0}, []byte{0}, make(map[string]interface{}), make(map[string][]byte))
	if err != nil {
		return nil, err
	}
	return p.GetRow(newRowNum), nil
}

// AppendRowData adds a Row to the end of this Partition, if it isn't full and if the Row fits within the schema
func (p *Partition) AppendRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte) error {
	if err := p.canInsertRowData(row); err != nil {
		return err
	}
	copy(p.rows[p.numRows*p.widestSchema.Size():(p.numRows+1)*p.widestSchema.Size()], row)
	copy(p.rowMeta[p.numRows*p.widestSchema.NumColumns():(p.numRows+1)*p.widestSchema.NumColumns()], meta)
	p.varRowData[p.numRows] = varData
	p.serializedVarRowData[p.numRows] = serializedVarRowData
	p.numRows++
	return nil
}

// appendKeyedRowData appends a keyed Row to the end of this Partition
func (p *Partition) appendKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64) error {
	if !p.isKeyed {
		return fmt.Errorf("Partition is not keyed")
	}
	err := p.AppendRowData(row, meta, varData, serializedVarRowData)
	if err != nil {
		return err
	}
	if p.isKeyed {
		p.keys[p.numRows-1] = key
	}
	return nil
}

// InsertRowData inserts a Row at a specific position within this Partition, if it isn't full and if the Row fits within the schema. Other Rows are shifted as necessary.
func (p *Partition) InsertRowData(row []byte, meta []byte, varRowData map[string]interface{}, serializedVarRowData map[string][]byte, pos int) error {
	if err := p.canInsertRowData(row); err != nil {
		return err
	}
	rowWidth := p.widestSchema.Size()
	numCols := p.widestSchema.NumColumns()
	// shift row data
	copy(p.rows[(pos+1)*rowWidth:], p.rows[pos*rowWidth:p.numRows*rowWidth])
	// insert row data
	copy(p.rows[pos*rowWidth:(pos+1)*rowWidth], row)
	// shift meta data
	copy(p.rowMeta[(pos+1)*numCols:], p.rowMeta[pos*numCols:p.numRows*numCols])
	// insert meta data
	copy(p.rowMeta[pos*numCols:(pos+1)*numCols], meta)
	// shift variable length row data
	copy(p.varRowData[pos+1:], p.varRowData[pos:p.numRows])
	// insert variable length row data
	p.varRowData[pos] = varRowData
	// shift serialized variable length row data
	copy(p.serializedVarRowData[pos+1:], p.serializedVarRowData[pos:p.numRows])
	// insert serialized variable length row data
	p.serializedVarRowData[pos] = serializedVarRowData
	p.numRows++
	return nil
}

// insertKeyedRowData inserts a keyed Row into this Partition
func (p *Partition) insertKeyedRowData(row []byte, meta []byte, varData map[string]interface{}, serializedVarRowData map[string][]byte, key uint64, pos int) error {
	if !p.isKeyed {
		return fmt.Errorf("Partition is not keyed")
	}
	err := p.InsertRowData(row, meta, varData, serializedVarRowData, pos)
	if err != nil {
		return err
	}
	// shift
	copy(p.keys[pos+1:], p.keys[pos:p.numRows-1]) // we updated numRows already, so subtract
	// insert
	p.keys[pos] = key
	return nil
}

func (p *Partition) buildRow(idx int) *Row {
	return &Row{
		meta:              p.getRowMeta(idx),
		data:              p.getRowData(idx),
		varData:           p.getVarRowData(idx),
		serializedVarData: p.getSerializedVarRowData(idx),
		schema:            p.currentSchema,
	}
}

// MapRows runs a MapOperation on each row in this Partition, manipulating them in-place. Will fall back to creating a fresh partition if PartitionRowErrors occur.
func (p *Partition) MapRows(fn MapOperation) (*Partition, error) {
	inPlace := true // start by attempting to manipulate rows in-place
	result := p
	var multierr *multierror.Error
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.buildRow(i)
		err := fn(row)
		if err != nil {
			multierr = multierror.Append(multierr, err)
			// create a new partition and switch to non-in-place mode
			if inPlace {
				inPlace = false
				// immediately switch into creating a new Partition if we haven't already
				result := CreatePartition(p.maxRows, p.widestSchema, p.currentSchema)
				// append all rows we've successfully processed so far (up to this one)
				for j := 0; j < i; j++ {
					err := result.AppendRowData(p.getRowData(j), p.getRowMeta(j), p.getVarRowData(j), p.getSerializedVarRowData(j))
					if err != nil {
						return nil, err
					}
				}
			}
		} else if !inPlace { // if we're not in in-place mode, append successful rows to new Partition
			result.AppendRowData(p.getRowData(i), p.getRowMeta(i), p.getVarRowData(i), p.getSerializedVarRowData(i))
		}
	}
	return result, multierr.ErrorOrNil()
}

// FlatMapRows runs a FlatMapOperation on each row in this Partition, creating new Partitions
func (p *Partition) FlatMapRows(fn FlatMapOperation) ([]*Partition, error) {
	var multierr *multierror.Error
	// factory for producing new rows compatible with this Partition
	factory := func() *Row {
		return &Row{
			meta:              make([]byte, p.widestSchema.NumColumns()),
			data:              make([]byte, p.widestSchema.Size()),
			varData:           make(map[string]interface{}),
			serializedVarData: make(map[string][]byte),
			schema:            p.currentSchema,
		}
	}
	parts := make([]*Partition, 1)
	parts = append(parts, CreatePartition(p.maxRows, p.widestSchema, p.currentSchema))
	for i := 0; i < p.GetNumRows(); i++ {
		newRows, err := fn(p.buildRow(i), factory)
		if err != nil {
			multierr = multierror.Append(multierr, err)
		} else {
			for _, row := range newRows {
				appendTarget := parts[len(parts)-1]
				if appendTarget.numRows >= appendTarget.maxRows {
					parts = append(parts, CreatePartition(p.maxRows, p.widestSchema, p.currentSchema))
					appendTarget = parts[len(parts)-1]
				}
				appendTarget.AppendRowData(row.data, row.meta, row.varData, row.serializedVarData)
			}
		}
	}
	return parts, multierr.ErrorOrNil()
}

// FilterRows filters the Rows in the current Partition, creating a new one
func (p *Partition) FilterRows(fn FilterOperation) (*Partition, error) {
	var multierr *multierror.Error
	result := CreatePartition(p.maxRows, p.widestSchema, p.currentSchema)
	for i := 0; i < p.GetNumRows(); i++ {
		shouldKeep, err := fn(p.buildRow(i))
		if err != nil {
			multierr = multierror.Append(multierr, err)
		}
		if shouldKeep {
			err := result.AppendRowData(p.getRowData(i), p.getRowMeta(i), p.getVarRowData(i), p.getSerializedVarRowData(i))
			// there's no way we can fill up this Partition, since we have to have fewer rows that
			// the current one, so this error should never happen
			if err != nil {
				return nil, err
			}
		}
	}
	return result, multierr.ErrorOrNil()
}

// GetCurrentSchema retrieves the Schema from the most recent task that manipulated this Partition
func (p *Partition) GetCurrentSchema() *Schema {
	return p.currentSchema
}

// getWidestSchema retrieves the widest Schema from the stage that produced this Partition, which is equal to the size of a row
func (p *Partition) getWidestSchema() *Schema {
	return p.widestSchema
}

// Repack repacks a Partition according to a new Schema
func (p *Partition) Repack(newSchema *Schema) (*Partition, error) {
	// create a new Partition
	part := CreatePartition(p.maxRows, newSchema, newSchema)
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.buildRow(i)
		newRow, err := row.repack(newSchema)
		if err != nil {
			return nil, err
		}
		err = part.AppendRowData(newRow.data, newRow.meta, newRow.varData, newRow.serializedVarData)
		if err != nil {
			return nil, err
		}
	}
	return part, nil
}

// KeyRows generates hash keys for a row from a key column. Attempts to manipulate partition in-place, falling back to creating a fresh partition if there are row errors
func (p *Partition) KeyRows(kfn KeyingOperation) (*Partition, error) {
	var multierr *multierror.Error
	inPlace := true // start by attempting to manipulate rows in-place
	result := p
	result.isKeyed = false // clear keyed status if there was one
	result.keys = make([]uint64, p.maxRows)
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.buildRow(i)
		hasher := xxhash.New()
		keyBuf, err := kfn(row)
		if err != nil {
			multierr = multierror.Append(multierr, err)
			// create a new partition and switch to non-in-place mode
			if inPlace {
				inPlace = false
				// immediately switch into creating a new Partition if we haven't already
				result := CreatePartition(p.maxRows, p.widestSchema, p.currentSchema)
				result.isKeyed = true
				result.keys = make([]uint64, p.maxRows)
				// append all rows we've successfully processed so far (up to this one)
				for j := 0; j < i; j++ {
					err := result.appendKeyedRowData(p.getRowData(j), p.getRowMeta(j), p.getVarRowData(j), p.getSerializedVarRowData(j), p.keys[i])
					if err != nil {
						return nil, err
					}
				}
			}
		} else if inPlace {
			hasher.Write(keyBuf)
			result.keys[i] = hasher.Sum64()
		} else if !inPlace {
			hasher.Write(keyBuf)
			err := result.appendKeyedRowData(p.getRowData(i), p.getRowMeta(i), p.getVarRowData(i), p.getSerializedVarRowData(i), hasher.Sum64())
			if err != nil {
				return nil, err
			}
		}
	}
	result.isKeyed = true
	return p, multierr.ErrorOrNil()
}

// IsKeyed returns true iff this Partition has been keyed with KeyRows
func (p *Partition) IsKeyed() bool {
	return p.isKeyed
}

// findFirstKey locates the first instance of a key within a sorted Partition,
// returning the FIRST index of the key in the Partition, or an error
// if it isn't found along with the location the key should be
// inserted at.
// PRECONDITION: Partition must already be sorted by key
func (p *Partition) findFirstKey(key uint64) (int, error) {
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

// findFirstRowKey locates the first instance of a uint64 key within a sorted Partition,
// then uses a KeyingOperation to find the actual row whose key bytes match
// a specific set of key bytes used to produce the uint64 key. If the key does not
// exist within the Partition, an error is returned along with the position it should
// be located at.
// PRECONDITION: Partition must already be sorted by key
func (p *Partition) findFirstRowKey(keyBuf []byte, key uint64, keyfn KeyingOperation) (int, error) {
	// find the first matching uint64 key
	firstKey, err := p.findFirstKey(key)
	if err != nil {
		return firstKey, err
	}
	// iterate over each row with a matching key to find the first one with identical key bytes
	for i := firstKey; i < p.GetNumRows(); i++ {
		if k, err := p.GetKey(i); err != nil || k != key {
			return -1, err
		}
		rowKey, err := keyfn(&Row{
			meta:              p.getRowMeta(i),
			data:              p.getRowData(i),
			varData:           p.getVarRowData(i),
			serializedVarData: p.getSerializedVarRowData(i),
			schema:            p.GetCurrentSchema(),
		})
		if err != nil {
			return -1, err
		} else if reflect.DeepEqual(keyBuf, rowKey) {
			return i, nil
		}
	}
	return firstKey, errors.MissingKeyError{}
}

// GetKey returns the shuffle key for a row, as generated by KeyRows
func (p *Partition) GetKey(rowNum int) (uint64, error) {
	if !p.isKeyed {
		return 0, fmt.Errorf("Partition is not keyed")
	}
	return p.keys[rowNum], nil
}

// getKeyRange returns a range of shuffle keys for a row, as generated by KeyRows, starting at rowNum
func (p *Partition) getKeyRange(rowNum int, numRows int) []uint64 {
	end := rowNum + numRows
	if end > p.numRows {
		end = p.numRows
	}
	return p.keys[rowNum:end]
}

// split splits a Partition into two Partitions. Split position ends up in right Partition.
func (p *Partition) split(pos int) (*Partition, *Partition, error) {
	if pos >= p.numRows {
		return nil, nil, fmt.Errorf("Split position is outside of Partition bounds")
	}
	left := CreatePartition(p.maxRows, p.widestSchema, p.currentSchema)
	right := CreatePartition(p.maxRows, p.widestSchema, p.currentSchema)
	if p.isKeyed {
		left.isKeyed = true
		left.keys = make([]uint64, p.maxRows)
		right.isKeyed = true
		right.keys = make([]uint64, p.maxRows)
		for i := 0; i < p.GetNumRows(); i++ {
			row := p.getRowData(i)
			meta := p.getRowMeta(i)
			varData := p.getVarRowData(i)
			serializedVarData := p.getSerializedVarRowData(i)
			key, err := p.GetKey(i)
			if err != nil {
				return nil, nil, err
			}
			if i < pos {
				left.appendKeyedRowData(row, meta, varData, serializedVarData, key)
			} else {
				right.appendKeyedRowData(row, meta, varData, serializedVarData, key)
			}
		}
	} else {
		for i := 0; i < p.GetNumRows(); i++ {
			row := p.getRowData(i)
			meta := p.getRowMeta(i)
			varData := p.getVarRowData(i)
			serializedVarData := p.getSerializedVarRowData(i)
			if i < pos {
				left.AppendRowData(row, meta, varData, serializedVarData)
			} else {
				right.AppendRowData(row, meta, varData, serializedVarData)
			}
		}
	}
	return left, right, nil
}

// AverageKeyValue computes the floored average
// value of key within this sorted, keyed Partition
func (p *Partition) averageKeyValue() (uint64, error) {
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

// balancedSplit attempts to split a sorted Partition based on the
// average key value in the Partition, assuring
// that identical keys (if the Partition is keyed) end up in the same
// Partition. Identical keys occur due to hash collisions.
// Split position ends up in right Partition.
func (p *Partition) balancedSplit() (uint64, *Partition, *Partition, error) {
	if !p.isKeyed {
		splitPos := p.GetNumRows() / 2
		lp, rp, err := p.split(splitPos)
		return 0, lp, rp, err
	}
	avgKey, err := p.averageKeyValue()
	if err != nil {
		return 0, nil, nil, err
	}
	splitPos, _ := p.findFirstKey(avgKey) // doesn't matter if key doesn't exist in p
	// find the first instance of the actual split position key
	// within the Partition so as not to split up identical keys
	key := p.keys[splitPos]
	for splitPos > 0 {
		if p.keys[splitPos-1] != key {
			break
		}
		splitPos--
	}
	lp, rp, err := p.split(splitPos)
	return avgKey, lp, rp, err
}

// toMetaMessage serializes metadata about this Partition to a protobuf message
func (p *Partition) toMetaMessage() *pb.MPartitionMeta {
	return &pb.MPartitionMeta{
		Id:      p.id,
		NumRows: uint32(p.numRows),
		MaxRows: uint32(p.maxRows),
		IsKeyed: p.isKeyed,
	}
}

// receiveStreamedData loads data from a protobuf stream into this Partition
func (p *Partition) receiveStreamedData(stream pb.PartitionsService_TransferPartitionDataClient, incomingSchema *Schema) error {
	// stream data for Partition
	rowOffset := 0
	metaOffset := 0
	keyOffset := 0
	for chunk, err := stream.Recv(); err != io.EOF; chunk, err = stream.Recv() {
		if err != nil {
			// not an EOF, but something else
			return err
		}
		switch chunk.DataType {
		case rowDataType:
			copy(p.rows[rowOffset:rowOffset+len(chunk.Data)], chunk.Data)
			rowOffset += len(chunk.Data)
		case rowVarDataType:
			// Stream one key at a time, basically. Not efficient if people are
			// using var data to store small data, but better if they're storing
			// large data there. Data is streamed in chunks, especially if it's
			// bigger than the grpc max message size
			// When Partition is transferred over a network, all variable-length data is Gob-encoded.
			// We deserialize later, the first time they ask for a value from a Row, since that's when
			// we know the type they're looking for
			m := p.getSerializedVarRowData(int(chunk.VarDataRowNum))
			if chunk.Append > 0 {
				copy(m[chunk.VarDataColName][chunk.Append:], chunk.Data)
			} else {
				m[chunk.VarDataColName] = make([]byte, chunk.TotalSizeBytes)
				copy(m[chunk.VarDataColName], chunk.Data)
			}
		case metaDataType:
			copy(p.rowMeta[metaOffset:metaOffset+len(chunk.Data)], chunk.Data)
			metaOffset += len(chunk.Data)
		case keyDataType:
			copy(p.keys[keyOffset:keyOffset+len(chunk.KeyData)], chunk.KeyData)
			keyOffset += len(chunk.KeyData)
		}
	}
	// confirm we received the correct number of rows
	if p.numRows != rowOffset/incomingSchema.size {
		return fmt.Errorf("Streamed %d rows for Partition %s. Expected %d", rowOffset/incomingSchema.size, p.id, p.numRows)
	} else if incomingSchema.NumFixedLengthColumns() > 0 && p.numRows != metaOffset/incomingSchema.NumColumns() {
		return fmt.Errorf("Streamed %d rows' metadata for Partition %s. Expected %d", metaOffset/incomingSchema.NumColumns(), p.id, p.numRows)
	} else if p.isKeyed && p.numRows != keyOffset {
		return fmt.Errorf("Streamed %d keys for Partition %s. Expected %d", keyOffset, p.id, p.numRows)
	}
	return nil
}

// PartitionFromMetaMessage deserializes a Partition from a protobuf message
func partitionFromMetaMessage(m *pb.MPartitionMeta, widestSchema *Schema, currentSchema *Schema) *Partition {
	part := &Partition{
		m.Id,
		int(m.MaxRows),
		int(m.NumRows),
		make([]byte, int(m.MaxRows)*widestSchema.Size()),
		make([]map[string]interface{}, int(m.MaxRows)*widestSchema.Size()),
		make([]map[string][]byte, int(m.MaxRows)*widestSchema.Size()),
		make([]byte, int(m.MaxRows)*widestSchema.NumColumns()),
		widestSchema,
		currentSchema,
		nil,
		m.IsKeyed,
	}
	if m.IsKeyed {
		part.keys = make([]uint64, int(m.MaxRows))
	}
	return part
}

// toBytes serializes a Partition to a byte array suitable for persistance to disk
func (p *Partition) toBytes() ([]byte, error) {
	numRows := p.GetNumRows()
	svrd := make([]*pb.DPartition_DVarRow, numRows)
	// include deserialized var row data
	for i := 0; i < numRows; i++ {
		svrd[i] = &pb.DPartition_DVarRow{
			RowData: make(map[string][]byte),
		}
		varData := p.getVarRowData(i)
		for k, v := range varData {
			if v == nil {
				svrd[i].RowData[k] = nil
				continue
			}
			// no need to serialize values for columns we've dropped
			if col, ok := p.currentSchema.schema[k]; ok {
				if vcol, ok := col.colType.(VarColumnType); ok {
					sdata, err := vcol.Serialize(v)
					if err != nil {
						return nil, err
					}
					svrd[i].RowData[k] = sdata
				} else {
					log.Panicf("Column %s is not a variable-length type", k)
				}
			}
		}
		// transfer un-deserialized variable-length data (possible if never accessed after a reduction)// transfer un-deserialized variable-length data (possible if never accessed after a reduction)
		svarData := p.getSerializedVarRowData(i)
		for k, v := range svarData {
			svrd[i].RowData[k] = v
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

// partitionFromBytes converts disk-serialized bytes into a Partition
func partitionFromBytes(data []byte, widestSchema *Schema, currentSchema *Schema) (*Partition, error) {
	m := &pb.DPartition{}
	err := proto.Unmarshal(data, m)
	if err != nil {
		return nil, err
	}
	part := &Partition{
		id:                   m.Id,
		maxRows:              int(m.MaxRows),
		numRows:              int(m.NumRows),
		rows:                 m.RowData,
		varRowData:           make([]map[string]interface{}, int(m.MaxRows)*widestSchema.Size()),
		serializedVarRowData: make([]map[string][]byte, int(m.MaxRows)*widestSchema.Size()),
		rowMeta:              m.RowMeta,
		widestSchema:         widestSchema,
		currentSchema:        currentSchema,
		keys:                 nil,
		isKeyed:              m.IsKeyed,
	}
	for i, row := range m.SerializedVarRowData {
		part.serializedVarRowData[i] = row.RowData
	}
	if m.IsKeyed {
		part.keys = m.Keys
	}
	return part, nil
}
