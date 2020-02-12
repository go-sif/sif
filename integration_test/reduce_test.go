package integration_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	types "github.com/go-sif/sif/v0.0.1/columntype"
	core "github.com/go-sif/sif/v0.0.1/core"
	memory "github.com/go-sif/sif/v0.0.1/datasource/memory"
	jsonl "github.com/go-sif/sif/v0.0.1/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/v0.0.1/operations/transform"
	util "github.com/go-sif/sif/v0.0.1/operations/util"
	"github.com/stretchr/testify/require"
)

func createTestReduceDataFrame(t *testing.T, numRows int) *core.DataFrame {
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

func TestReduce(t *testing.T) {
	// create dataframe
	numRows := 100
	frame, err := createTestReduceDataFrame(t, numRows).To(
		ops.WithColumn("count", &types.Uint32ColumnType{}),
		ops.Map(func(row *core.Row) error {
			err := row.SetInt32("count", int32(1))
			if err != nil {
				return err
			}
			return nil
		}),
		ops.Reduce(func(row *core.Row) ([]byte, error) {
			return []byte{byte(1)}, nil
		}, func(lrow *core.Row, rrow *core.Row) error {
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

	// start coordinator
	numWorkers := 2
	opts := &core.CoordinatorOptions{Port: 8080, Host: "localhost", NumWorkers: numWorkers, WorkerJoinTimeout: time.Duration(5) * time.Second, RPCTimeout: time.Duration(5) * time.Second}
	coordinator, err := core.CreateNode(core.Coordinator, opts)
	require.Nil(t, err)
	go func() {
		err := coordinator.Start(frame)
		require.Nil(t, err)
	}()
	defer coordinator.GracefulStop()
	time.Sleep(50 * time.Millisecond) // TODO worker should retry a few times

	// start workers and register with coordinator
	baseWorkerPort := 8081
	for port := baseWorkerPort; port < baseWorkerPort+numWorkers; port++ {
		cwd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		tmpDir, err := ioutil.TempDir(cwd, fmt.Sprintf("sif-worker-%d", port))
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(tmpDir)
		wopts := &core.WorkerOptions{
			Port:                  port,
			Host:                  "localhost",
			CoordinatorPort:       8080,
			CoordinatorHost:       "localhost",
			RPCTimeout:            time.Duration(5) * time.Second,
			TempDir:               tmpDir,
			NumInMemoryPartitions: 10,
		}
		worker, err := core.CreateNode(core.Worker, wopts)
		require.Nil(t, err)
		go func() {
			err := worker.Start(frame)
			require.Nil(t, err)
		}()
		defer worker.GracefulStop()
	}

	// run dataframe and verify results
	res, err := coordinator.Run(context.Background())
	require.Nil(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res))
	for _, part := range res {
		require.Equal(t, 1, part.GetNumRows())
		row := part.GetRow(0)
		count, err := row.GetInt32("count")
		require.Nil(t, err)
		require.EqualValues(t, numRows, count)
		break
	}
}
