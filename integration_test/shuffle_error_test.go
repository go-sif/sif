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
	ops "github.com/go-sif/sif/v0.0.1/operations/transform"
	util "github.com/go-sif/sif/v0.0.1/operations/util"
	"github.com/stretchr/testify/require"
)

func TestShuffleErrors(t *testing.T) {
	// create dataframe, summing all even numbers and erroring for all odd
	frame, err := createTestMapErrorDataFrame(t, 10).To(
		ops.WithColumn("res", &types.Int32ColumnType{}),
		ops.Reduce(func(row *core.Row) ([]byte, error) {
			col1, err := row.GetInt32("col1")
			if err != nil {
				return nil, err
			} else if col1 < 2 {
				return nil, fmt.Errorf("Don't key numbers smaller than 2")
			}
			return []byte{0}, nil // reduce all rows together
		}, func(lrow *core.Row, rrow *core.Row) error {
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
			val, err := row.GetInt32("res")
			require.Nil(t, err)
			require.True(t, val < 15)
			return nil
		})
	}
	require.Nil(t, err)
}
