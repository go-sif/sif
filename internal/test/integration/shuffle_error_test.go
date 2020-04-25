package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
	ops "github.com/go-sif/sif/operations/transform"
	util "github.com/go-sif/sif/operations/util"
	siftest "github.com/go-sif/sif/testing"
	"github.com/stretchr/testify/require"
)

func TestShuffleErrors(t *testing.T) {
	// create dataframe, summing all even numbers and erroring for all odd
	frame, err := createTestMapErrorDataFrame(t, 10).To(
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
	for _, part := range res.Collected {
		part.ForEachRow(func(row sif.Row) error {
			val, err := row.GetInt32("res")
			require.Nil(t, err)
			require.True(t, val < 15)
			return nil
		})
	}
	require.Nil(t, err)
}
