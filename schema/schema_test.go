package schema

import (
	"testing"

	"github.com/go-sif/sif"
	"github.com/stretchr/testify/require"
)

func TestSchemaEqualityBasic(t *testing.T) {
	schema1 := CreateSchema()
	_, err := schema1.CreateColumn("col1", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col2", &sif.VarStringColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col3", &sif.StringColumnType{Length: 12})
	require.Nil(t, err)

	schema2 := CreateSchema()
	_, err = schema2.CreateColumn("col1", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema2.CreateColumn("col2", &sif.VarStringColumnType{})
	require.Nil(t, err)
	_, err = schema2.CreateColumn("col3", &sif.StringColumnType{Length: 12})
	require.Nil(t, err)

	require.Nil(t, schema1.Equals(schema2))
}

func TestSchemaEqualityDifferentLength(t *testing.T) {
	schema1 := CreateSchema()
	_, err := schema1.CreateColumn("col1", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col2", &sif.Uint32ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col3", &sif.StringColumnType{Length: 12})
	require.Nil(t, err)

	schema2 := CreateSchema()
	_, err = schema2.CreateColumn("col1", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema2.CreateColumn("col2", &sif.Uint32ColumnType{})
	require.Nil(t, err)
	_, err = schema2.CreateColumn("col3", &sif.StringColumnType{Length: 13})
	require.Nil(t, err)

	require.NotNil(t, schema1.Equals(schema2))
}

func TestSchemaEqualityOrder(t *testing.T) {
	schema1 := CreateSchema()
	_, err := schema1.CreateColumn("col1", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col2", &sif.Uint32ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col3", &sif.VarStringColumnType{})
	require.Nil(t, err)

	schema2 := CreateSchema()
	_, err = schema2.CreateColumn("col1", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema2.CreateColumn("col3", &sif.VarStringColumnType{})
	require.Nil(t, err)
	_, err = schema2.CreateColumn("col2", &sif.Uint32ColumnType{})
	require.Nil(t, err)

	require.NotNil(t, schema1.Equals(schema2))
}

func TestSchemaRepackEquality(t *testing.T) {
	schema1 := CreateSchema()
	_, err := schema1.CreateColumn("col1", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col2", &sif.Uint32ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col3", &sif.VarStringColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col4", &sif.Uint32ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col5", &sif.Uint64ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col6", &sif.Int32ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col7", &sif.Int8ColumnType{})
	require.Nil(t, err)
	_, err = schema1.CreateColumn("col8", &sif.Uint32ColumnType{})
	require.Nil(t, err)
	schema1.RemoveColumn("col2")
	schema1.RemoveColumn("col4")
	for i := 0; i < 1000; i++ {
		schema2 := schema1.Clone().Repack()
		schema3 := schema1.Clone().Repack()
		require.Nil(t, schema2.Equals(schema3))
	}

}
