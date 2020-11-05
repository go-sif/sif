package repartition

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

func createTestRepartitionDataFrame(t *testing.T, numFiles int) sif.DataFrame {
	data := make([][]byte, numFiles)
	for i := 0; i < numFiles; i++ {
		if i%2 == 0 {
			data[i] = []byte("{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}")
		} else {
			data[i] = []byte("{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}\n{\"col1\": \"def\"}\n{\"col1\": \"abc\"}")
		}
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

func TestRepartition(t *testing.T) {
	// create dataframe
	numFiles := 2
	frame, err := createTestRepartitionDataFrame(t, numFiles).To(
		ops.Repartition(10, func(row sif.Row) ([]byte, error) {
			val, err := row.GetString("col1")
			if err != nil {
				return nil, err
			}
			return []byte(val), nil
		}),
		util.Collect(4), // should be 2 partitions, but we'll take more to confirm
	)
	require.Nil(t, err)

	// run dataframe and verify results
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{}, 1)
	require.Nil(t, err)
	require.NotNil(t, res)
	require.Equal(t, 2, len(res.Collected))
	for _, part := range res.Collected {
		require.Equal(t, 10, part.GetMaxRows())
		require.Equal(t, 10, part.GetNumRows())
		var lastVal string
		err := part.ForEachRow(func(row sif.Row) error {
			val, err := row.GetString("col1")
			require.Nil(t, err)
			if len(lastVal) > 0 {
				require.Equal(t, lastVal, val)
			}
			lastVal = val
			return nil
		})
		require.Nil(t, err)
		break
	}
}
