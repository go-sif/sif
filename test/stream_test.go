package integration

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	types "github.com/go-sif/sif/v0.0.1/columntype"
	core "github.com/go-sif/sif/v0.0.1/core"
	memstream "github.com/go-sif/sif/v0.0.1/datasource/memorystream"
	jsonl "github.com/go-sif/sif/v0.0.1/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/v0.0.1/operations/transform"
	"github.com/stretchr/testify/require"
)

func createTestStreamDataFrame(t *testing.T, numGenerators int) *core.DataFrame {
	data := make([]func() []byte, numGenerators)
	generator := func() []byte {
		num := rand.Intn(10)
		return []byte(fmt.Sprintf("{\"col1\": %d}\n", num))
	}
	for i := 0; i < len(data); i++ {
		data[i] = generator
	}

	// Create a dataframe for the data
	schema := core.CreateSchema()
	schema.CreateColumn("col1", &types.Int32ColumnType{})
	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 5,
	})
	dataframe := memstream.CreateDataFrame(data, 12, parser, schema)
	return dataframe
}

func TestStream(t *testing.T) {
	var processedRowsLock sync.Mutex // multiple workers will record their processed rows via this lock
	var processedRows []string       // multiple workers will record their processed rows here
	// create dataframe
	frame, err := createTestStreamDataFrame(t, 4).To(
		ops.WithColumn("res", &types.VarStringColumnType{}),
		ops.Map(func(row *core.Row) error {
			col1, err := row.GetInt32("col1")
			if err != nil {
				return err
			}
			err = row.SetVarString("res", fmt.Sprintf("%d", col1))
			if err != nil {
				return err
			}
			return nil
		}),
		ops.Reduce(
			func(row *core.Row) ([]byte, error) {
				return []byte("key"), nil
			},
			func(lrow *core.Row, rrow *core.Row) error {
				l, err := lrow.GetVarString("res")
				if err != nil {
					return err
				}
				r, err := rrow.GetVarString("res")
				if err != nil {
					return err
				}
				err = lrow.SetVarString("res", fmt.Sprintf("%s %s", l, r))
				if err != nil {
					return err
				}
				return nil
			}),
		ops.Map(func(row *core.Row) error {
			col1, err := row.GetVarString("res")
			if err != nil {
				return err
			}
			processedRowsLock.Lock()
			processedRows = append(processedRows, col1)
			processedRowsLock.Unlock()
			return nil
		}),
	)
	require.Nil(t, err)

	// run dataframe
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	copts := &core.NodeOptions{}
	wopts := &core.NodeOptions{}
	_, err = runTestFrame(ctx, t, frame, copts, wopts, 2)
	require.True(t, len(processedRows) > 6)
	for _, r := range processedRows {
		// 12 rows per batch x 4 generators across 2 workers = 48 ints per reduction
		require.Len(t, strings.Split(r, " "), 48)
	}
	// we gave the test a timeout, so expect DeadlineExceeded as an error
	require.IsType(t, err, context.DeadlineExceeded)
}
