package partition

import (
	xxhash "github.com/cespare/xxhash/v2"
	"github.com/go-sif/sif"
	"github.com/hashicorp/go-multierror"
)

// createKeyablePartition creates a new Partition containing an empty byte array and a schema
func createKeyablePartition(maxRows int, privateSchema sif.Schema, publicSchema sif.Schema) sif.KeyablePartition {
	return createPartitionImpl(maxRows, privateSchema, publicSchema)
}

// KeyRows generates hash keys for a row from a key column. Attempts to manipulate partition in-place, falling back to creating a fresh partition if there are row errors
func (p *partitionImpl) KeyRows(kfn sif.KeyingOperation) (sif.OperablePartition, error) {
	var multierr *multierror.Error
	inPlace := true // start by attempting to manipulate rows in-place
	result := p
	result.isKeyed = false // clear keyed status if there was one
	result.keys = make([]uint64, p.maxRows)
	row := &rowImpl{}
	for i := 0; i < p.GetNumRows(); i++ {
		row := p.getRow(row, i)
		hasher := xxhash.New()
		keyBuf, err := kfn(row)
		if err != nil {
			multierr = multierror.Append(multierr, err)
			// create a new partition and switch to non-in-place mode
			if inPlace {
				inPlace = false
				// immediately switch into creating a new Partition if we haven't already
				result := createPartitionImpl(p.maxRows, p.privateSchema, p.publicSchema)
				result.isKeyed = true
				result.keys = make([]uint64, p.maxRows)
				// append all rows we've successfully processed so far (up to this one)
				for j := 0; j < i; j++ {
					err := result.AppendKeyedRowData(p.GetRowData(j), p.GetRowMeta(j), p.GetVarRowData(j), p.GetSerializedVarRowData(j), p.keys[i])
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
			err := result.AppendKeyedRowData(p.GetRowData(i), p.GetRowMeta(i), p.GetVarRowData(i), p.GetSerializedVarRowData(i), hasher.Sum64())
			if err != nil {
				return nil, err
			}
		}
	}
	result.isKeyed = true
	return p, multierr.ErrorOrNil()
}
