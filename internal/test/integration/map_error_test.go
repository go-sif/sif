package integration

import (
	"context"
	"fmt"
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

func createTestMapErrorDataFrame(t *testing.T, numRows int) sif.DataFrame {
	data := make([][]byte, numRows)
	for i := 0; i < len(data); i++ {
		data[i] = []byte(fmt.Sprintf("{\"col1\": %d}", i))
	}

	// Create a dataframe for the data
	schema := schema.CreateSchema()
	schema.CreateColumn("col1", &sif.Int32ColumnType{})
	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 5,
	})
	dataframe := memory.CreateDataFrame(data, parser, schema)
	return dataframe
}

func TestMapErrors(t *testing.T) {
	// create dataframe, erroring on all odd numbers
	frame, err := createTestMapErrorDataFrame(t, 10).To(
		ops.Map(func(row sif.Row) error {
			col1, err := row.GetInt32("col1")
			if err != nil {
				return err
			}
			// error out for odd numbers
			if col1%2 == 1 {
				return fmt.Errorf("Odd numbers cause errors")
			}
			// leave even numbers alone
			return nil
		}),
		util.Collect(2), // 2 partitions because there are 10 rows and 5 per partition
	)
	require.Nil(t, err)

	// run dataframe
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{IgnoreRowErrors: true}, 2)
	require.Nil(t, err)
	for _, part := range res {
		part.ForEachRow(func(row sif.Row) error {
			val, err := row.GetInt32("col1")
			require.Nil(t, err)
			require.Equal(t, int32(0), val%2)
			return nil
		})
	}
	require.Nil(t, err)
}
