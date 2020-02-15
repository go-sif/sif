package partition

import "github.com/go-sif/sif/types"

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
func (r *rowImpl) GetSchema() types.Schema {
	return r.schema
}
