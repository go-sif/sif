package partition

import "github.com/go-sif/sif"

// GetMeta returns Row internal data
func (r *rowImpl) GetMeta() []byte {
	return r.meta
}

// GetData returns Row internal data
func (r *rowImpl) GetData() []byte {
	return r.data
}

// GetVarData returns Row internal data
func (r *rowImpl) GetVarData() map[string]interface{} {
	return r.varData
}

// GetSerializedVarData returns Row internal data
func (r *rowImpl) GetSerializedVarData() map[string][]byte {
	return r.serializedVarData
}

// GetSchema returns Row internal data
func (r *rowImpl) GetSchema() sif.Schema {
	return r.schema
}

// GetColData retrives raw bytes for a column
func (r *rowImpl) GetColData(colName string) ([]byte, error) {
	offset, err := r.schema.GetOffset(colName)
	if err != nil {
		return nil, err
	}
	// if it's fixed width, grab the data from the row slice
	if !sif.IsVariableLength(offset.Type()) {
		return r.data[offset.Start() : offset.Start()+offset.Type().Size()], nil
	}
	// if it's variable width, but still serialized, use that directly
	if ser, ok := r.serializedVarData[colName]; ok {
		return ser, nil
	}
	// if it's variable width, but not serialized, serialize it
	vcol, _ := offset.Type().(sif.VarColumnType)
	v, err := r.GetVarCustomData(colName)
	if err != nil {
		return nil, err
	}
	sv, err := vcol.Serialize(v)
	if err != nil {
		return nil, err
	}
	return sv, nil
}
