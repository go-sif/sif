package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
	memory "github.com/go-sif/sif/datasource/memory"
	jsonl "github.com/go-sif/sif/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/operations/transform"
	util "github.com/go-sif/sif/operations/util"
	"github.com/go-sif/sif/schema"
	siftest "github.com/go-sif/sif/testing"
	"github.com/stretchr/testify/require"
)

func createTestCollectDataFrame(t *testing.T, numRows int) sif.DataFrame {
	row := []byte("{\"col1\": \"abc\"}")
	data := make([][]byte, numRows)
	for i := 0; i < len(data); i++ {
		data[i] = row
	}

	// Create a dataframe for the data
	schema := schema.CreateSchema()
	schema.CreateColumn("col1", &sif.StringColumnType{Length: 3})
	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 5,
	})
	dataframe := memory.CreateDataFrame(data, parser, schema)
	return dataframe
}

func TestCollect(t *testing.T) {
	// create dataframe
	frame, err := createTestCollectDataFrame(t, 10).To(
		ops.AddColumn("res", &sif.VarStringColumnType{}),
		ops.Map(func(row sif.Row) error {
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
		util.Collect(1), // 2 partitions because there are 10 rows and 5 per partition
	)
	require.Nil(t, err)

	// run dataframe
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{}, 2)
	require.Nil(t, err)
	for _, part := range res.Collected {
		part.ForEachRow(func(row sif.Row) error {
			val, err := row.GetVarString("res")
			require.Nil(t, err)
			require.Equal(t, "ABC", val)
			return nil
		})
	}
	require.Nil(t, err)
}
