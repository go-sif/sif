package dsv

import (
	"os"
	"path"
	"testing"

	file "github.com/go-sif/sif/datasource/file"
	"github.com/go-sif/sif/schema"
	"github.com/go-sif/sif/types"
	"github.com/stretchr/testify/require"
)

func TestDSVDatasourceParser(t *testing.T) {
	// Create a dataframe for the file, load it, and test things
	schema := schema.CreateSchema()
	schema.CreateColumn("hack", &types.BytesColumnType{Length: 32})
	schema.CreateColumn("license", &types.BytesColumnType{Length: 32})
	schema.CreateColumn("code", &types.BytesColumnType{Length: 3})
	schema.CreateColumn("flag", &types.Uint8ColumnType{})
	schema.CreateColumn("type", &types.VarStringColumnType{})
	schema.CreateColumn("pickup_time", &types.VarStringColumnType{})
	schema.CreateColumn("dropoff_time", &types.VarStringColumnType{})
	schema.CreateColumn("passengers", &types.Uint8ColumnType{})
	schema.CreateColumn("duration", &types.Uint32ColumnType{})
	schema.CreateColumn("distance", &types.Float32ColumnType{})
	schema.CreateColumn("pickup_lon", &types.Float32ColumnType{})
	schema.CreateColumn("pickup_lat", &types.Float32ColumnType{})
	schema.CreateColumn("dropoff_lon", &types.Float32ColumnType{})
	schema.CreateColumn("dropoff_lat", &types.Float32ColumnType{})

	cwd, err := os.Getwd()
	require.Nil(t, err)
	parser := CreateParser(&ParserConf{
		NilValue:      "null",
		PartitionSize: 128,
	})
	dataframe := file.CreateDataFrame(path.Join(cwd, "../../../testenv/*.csv"), parser, schema)

	pm, err := dataframe.GetDataSource().Analyze()
	require.Nil(t, err, "Analyze err should be null")
	totalRows := 0
	for pm.HasNext() {
		pl := pm.Next()
		ps, err := pl.Load(parser, schema)
		require.Nil(t, err)
		for ps.HasNextPartition() {
			part, err := ps.NextPartition()
			require.Nil(t, err)
			totalRows += part.GetNumRows()
		}
	}
	require.False(t, pm.HasNext())
	require.Equal(t, 477871, totalRows)
}
