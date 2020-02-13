package integration

import (
	"context"
	"fmt"
	"testing"

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

	// run dataframe
	copts := &core.NodeOptions{}
	wopts := &core.NodeOptions{IgnoreRowErrors: true}
	res, err := runTestFrame(context.Background(), t, frame, copts, wopts, 2)
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
