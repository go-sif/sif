package integration_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
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

func createTestCollectDataFrame(t *testing.T, numRows int) *core.DataFrame {
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

func TestCollect(t *testing.T) {
	// create dataframe
	frame, err := createTestCollectDataFrame(t, 10).To(
		ops.WithColumn("res", &types.VarStringColumnType{}),
		ops.Map(func(row *core.Row) error {
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
			val, err := row.GetVarString("res")
			require.Nil(t, err)
			require.Equal(t, "ABC", val)
			return nil
		})
	}
	require.Nil(t, err)
}
