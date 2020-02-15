package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	core "github.com/go-sif/sif/core"
	memory "github.com/go-sif/sif/datasource/memory"
	jsonl "github.com/go-sif/sif/datasource/parser/jsonl"
	"github.com/go-sif/sif/internal/schema"
	ops "github.com/go-sif/sif/operations/transform"
	util "github.com/go-sif/sif/operations/util"
	"github.com/go-sif/sif/types"
	"github.com/stretchr/testify/require"
)

func createTestCollectDataFrame(t *testing.T, numRows int) types.DataFrame {
	row := []byte("{\"col1\": \"abc\"}")
	data := make([][]byte, numRows)
	for i := 0; i < len(data); i++ {
		data[i] = row
	}

	// Create a dataframe for the data
	schema := schema.CreateSchema()
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
		ops.Map(func(row types.Row) error {
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
		part.ForEachRow(func(row types.Row) error {
			val, err := row.GetVarString("res")
			require.Nil(t, err)
			require.Equal(t, "ABC", val)
			return nil
		})
	}
	require.Nil(t, err)
}
