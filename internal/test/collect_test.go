package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	types "github.com/go-sif/sif/columntype"
	core "github.com/go-sif/sif/core"
	memory "github.com/go-sif/sif/datasource/memory"
	jsonl "github.com/go-sif/sif/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/operations/transform"
	util "github.com/go-sif/sif/operations/util"
	"github.com/stretchr/testify/require"
)

func createTestCollectDataFrame(t *testing.T, numRows int) *core.DataFrame {
	row := []byte("{\"col1\": \"abc\"}")
	data := make([][]byte, numRows)
	for i := 0; i < len(data); i++ {
		data[i] = row
	}

	// Create a dataframe for the data
	schema := core.CreateSchema()
	schema.CreateColumn("col1", &types.StringColumnType{Length: 3})
	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 5,
	})
	dataframe := memory.CreateDataFrame(data, parser, schema)
	return dataframe
}

func TestCollect(t *testing.T) {
	// create dataframe
	frame, err := createTestCollectDataFrame(t, 10).To(
		ops.AddColumn("res", &types.VarStringColumnType{}),
		ops.Map(func(row *core.Row) error {
			col1, err := row.GetString("col1")
			if err != nil {
				return err
			}
			err = row.SetVarString("res", fmt.Sprintf("%s", strings.ToUpper(col1)))
			if err != nil {
				return err
			}
			return nil
		}),
		ops.RemoveColumn("col1"),
		// ops.Repack(),
		util.Collect(2), // 2 partitions because there are 10 rows and 5 per partition
	)
	require.Nil(t, err)

	// run dataframe
	copts := &core.NodeOptions{}
	wopts := &core.NodeOptions{}
	res, err := runTestFrame(context.Background(), t, frame, copts, wopts, 2)
	for _, part := range res {
		part.MapRows(func(row *core.Row) error {
			val, err := row.GetVarString("res")
			require.Nil(t, err)
			require.Equal(t, "ABC", val)
			return nil
		})
	}
	require.Nil(t, err)
}
