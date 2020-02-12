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

func createTestMapErrorDataFrame(t *testing.T, numRows int) *core.DataFrame {
	data := make([][]byte, numRows)
	for i := 0; i < len(data); i++ {
		data[i] = []byte(fmt.Sprintf("{\"col1\": %d}", i))
	}

	// Create a dataframe for the data
	schema := core.CreateSchema()
	schema.CreateColumn("col1", &types.Int32ColumnType{})
	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 5,
	})
	dataframe := memory.CreateDataFrame(data, parser, schema)
	return dataframe
}

func TestMapErrors(t *testing.T) {
	// create dataframe, erroring on all odd numbers
	frame, err := createTestMapErrorDataFrame(t, 10).To(
		ops.Map(func(row *core.Row) error {
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
	// start coordinator
	numWorkers := 2
	opts := &core.CoordinatorOptions{
		Port:              8080,
		Host:              "localhost",
		NumWorkers:        numWorkers,
		WorkerJoinTimeout: time.Duration(5) * time.Second,
		RPCTimeout:        time.Duration(5) * time.Second,
	}
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
			IgnoreRowErrors:       true,
		}
		worker, err := core.CreateNode(core.Worker, wopts)
		require.Nil(t, err)
		go func() {
			err := worker.Start(frame)
			require.Nil(t, err)
		}()
		defer worker.GracefulStop()
	}

	// run dataframe
	res, err := coordinator.Run(context.Background())
	for _, part := range res {
		part.MapRows(func(row *core.Row) error {
			val, err := row.GetInt32("col1")
			require.Nil(t, err)
			require.Equal(t, int32(0), val%2)
			return nil
		})
	}
	require.Nil(t, err)
}
