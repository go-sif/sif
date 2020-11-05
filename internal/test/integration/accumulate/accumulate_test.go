package accumulate

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/accumulators"
	"github.com/go-sif/sif/cluster"
	memory "github.com/go-sif/sif/datasource/memory"
	jsonl "github.com/go-sif/sif/datasource/parser/jsonl"
	"github.com/go-sif/sif/operations/util"
	"github.com/go-sif/sif/schema"
	siftest "github.com/go-sif/sif/testing"
	"github.com/stretchr/testify/require"
)

func createTestAccumulateDataFrame(t *testing.T, numRows int) sif.DataFrame {
	data := make([][]byte, numRows)
	for i := 0; i < len(data); i++ {
		data[i] = []byte(fmt.Sprintf("{\"col1\": \"%d\"}", i))
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

func TestAccumulate(t *testing.T) {
	// create dataframe
	numRows := 100
	sum := 0
	for i := 0; i < numRows; i++ {
		sum += i
	}
	frame, err := createTestAccumulateDataFrame(t, numRows).To(
		util.Accumulate(accumulators.Compose(accumulators.Counter, accumulators.Adder("col1"))),
	)
	require.Nil(t, err)

	// run dataframe and verify results
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{}, 2)
	require.Nil(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Accumulated)
	compa, isComposedAccumulator := res.Accumulated.(*accumulators.Composed)
	require.True(t, isComposedAccumulator)
	ca, isCountAccumulator := compa.GetResults()[0].(*accumulators.Count)
	require.True(t, isCountAccumulator)
	require.Equal(t, numRows, int(ca.GetCount()))
	sa, isSumAccumulator := compa.GetResults()[1].(*accumulators.Sum)
	require.True(t, isSumAccumulator)
	require.EqualValues(t, sum, sa.GetSum())
}
