package integration

import (
	"context"
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

func createTestReduceDataFrame(t *testing.T, numRows int) sif.DataFrame {
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

func TestReduce(t *testing.T) {
	// create dataframe
	numRows := 100
	frame, err := createTestReduceDataFrame(t, numRows).To(
		ops.AddColumn("count", &sif.Uint32ColumnType{}),
		ops.Map(func(row sif.Row) error {
			err := row.SetInt32("count", int32(1))
			if err != nil {
				return err
			}
			return nil
		}),
		ops.RemoveColumn("col1"),
		ops.Reduce(func(row sif.Row) ([]byte, error) {
			return []byte{byte(1)}, nil
		}, func(lrow sif.Row, rrow sif.Row) error {
			lval, err := lrow.GetInt32("count")
			if err != nil {
				return err
			}
			rval, err := rrow.GetInt32("count")
			if err != nil {
				return err
			}
			return lrow.SetInt32("count", lval+rval)
		}),
		util.Collect(10), // should be 1 partitions because we are summing to a single row, but collect extra as a test
	)
	require.Nil(t, err)

	// run dataframe and verify results
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{}, 2)
	require.Nil(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res.Collected))
	for _, part := range res.Collected {
		require.Equal(t, 1, part.GetNumRows())
		row := part.GetRow(0)
		count, err := row.GetInt32("count")
		require.Nil(t, err)
		require.EqualValues(t, numRows, count)
		break
	}
}
