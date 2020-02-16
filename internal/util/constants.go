package util

const (
	// RowDataType indicates that Row data is being transferred
	RowDataType = 1 << iota
	// RowVarDataType indicates that variable-length Row data is being transferred
	RowVarDataType
	// MetaDataType indicates that Meta data is being transferred
	MetaDataType
	// KeyDataType indicates that Key data is being transferred
	KeyDataType
)
