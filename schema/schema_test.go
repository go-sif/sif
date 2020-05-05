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

	require.True(t, schema1.Equals(schema2))
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

	require.False(t, schema1.Equals(schema2))
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

	require.False(t, schema1.Equals(schema2))
}
