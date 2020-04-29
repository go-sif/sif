package sif

// A DataFrame is a tool for constructing a chain of
// transformations and actions applied to columnar data
type DataFrame interface {
	GetPublicSchema() Schema                     // GetPublicSchema returns the operation-facing schema of the data at this task. Will omit columns which have been removed.
	GetPrivateSchema() Schema                    // GetPrivateSchema returns the internal schema of the data at this task. Will include columns which have been removed, until a repack.
	GetDataSource() DataSource                   // GetDataSource returns the DataSource of a DataFrame
	GetParser() DataSourceParser                 // GetParser returns the DataSourceParser of a DataFrame
	To(...DataFrameOperation) (DataFrame, error) // To is a "functional operations" factory method for DataFrames, chaining operations onto the current one(s).
}
