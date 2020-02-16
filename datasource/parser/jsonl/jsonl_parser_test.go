package jsonl

import (
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/datasource/memory"
	"github.com/go-sif/sif/internal/schema"
	"github.com/stretchr/testify/require"
)

func TestJSONLDatasourceParser(t *testing.T) {
	// Create a dataframe for the file, load it, and test things
	schema := schema.CreateSchema()
	schema.CreateColumn("name", &sif.VarStringColumnType{})
	schema.CreateColumn("meta.index", &sif.Int8ColumnType{})
	schema.CreateColumn("meta.first", &sif.VarStringColumnType{})
	schema.CreateColumn("meta.last", &sif.VarStringColumnType{})

	parser := CreateParser(&ParserConf{
		PartitionSize: 128,
	})
	data := [][]byte{
		[]byte("{\"name\": \"Sean\", \"meta\": { \"index\": 1, \"first\": \"Sean\", \"last\": \"McIntyre\"}}\n{\"name\": \"Chris\", \"meta\": { \"index\": 3, \"first\": \"Chris\", \"last\": \"Dickson\"}}"),
		[]byte("{\"name\": \"Phil\", \"meta\": { \"index\": 2, \"first\": \"Phil\", \"last\": \"Laliberté\"}}\n{\"name\": \"Fahd\", \"meta\": { \"index\": 4, \"first\": \"Fahd\", \"last\": \"Husain\"}}"),
	}
	dataframe := memory.CreateDataFrame(data, parser, schema)

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
	require.Equal(t, 4, totalRows)
}
