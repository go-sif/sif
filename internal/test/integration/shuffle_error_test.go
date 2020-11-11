package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
	"github.com/go-sif/sif/datasource/memory"
	"github.com/go-sif/sif/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/operations/transform"
	util "github.com/go-sif/sif/operations/util"
	"github.com/go-sif/sif/schema"
	siftest "github.com/go-sif/sif/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func createTestShuffleErrorDataFrame(t *testing.T, numRows int) sif.DataFrame {
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

func TestShuffleErrors(t *testing.T) {
	defer goleak.VerifyNone(t)

	// create dataframe, summing all even numbers and erroring for all odd
	frame, err := createTestShuffleErrorDataFrame(t, 10).To(
		ops.AddColumn("res", &sif.Int32ColumnType{}),
		ops.Reduce(func(row sif.Row) ([]byte, error) {
			col1, err := row.GetInt32("col1")
			if err != nil {
				return nil, err
			} else if col1 < 2 {
				return nil, fmt.Errorf("Don't key numbers smaller than 2")
			}
			return []byte{0}, nil // reduce all rows together
		}, func(lrow sif.Row, rrow sif.Row) error {
			rcol1, err := rrow.GetInt32("col1")
			if err != nil {
				return err
			}
			lcol1, err := lrow.GetInt32("col1")
			if err != nil {
				return err
			}
			if lcol1+rcol1 > 15 {
				panic(fmt.Errorf("Prevent totals larger than 15"))
			}
			return lrow.SetInt32("col1", lcol1+rcol1)
		}),
		util.Collect(1), // 1 partitions because we're reducing
	)
	require.Nil(t, err)

	// run dataframe
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{IgnoreRowErrors: true}, 2)
	require.Nil(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Collected)
	for _, part := range res.Collected {
		part.ForEachRow(func(row sif.Row) error {
			val, err := row.GetInt32("res")
			require.Nil(t, err)
			require.True(t, val < 15)
			return nil
		})
	}
}
