package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
	ops "github.com/go-sif/sif/operations/transform"
	util "github.com/go-sif/sif/operations/util"
	siftest "github.com/go-sif/sif/testing"
	"github.com/stretchr/testify/require"
)

func TestFlatMap(t *testing.T) {
	// create dataframe
	frame, err := createTestCollectDataFrame(t, 10).To(
		ops.AddColumn("res", &sif.StringColumnType{Length: 1}),
		ops.FlatMap(func(row sif.Row, factory sif.RowFactory) error {
			col1, err := row.GetString("col1")
			if err != nil {
				return err
			}
			for _, c := range col1 {
				r := factory()
				err = r.SetString("res", strings.ToUpper(string(c)))
				if err != nil {
					return err
				}
			}
			return nil
		}),
		ops.RemoveColumn("col1"),
		util.Collect(6),
	)
	require.Nil(t, err)

	// run dataframe
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{}, 2)
	require.Nil(t, err)
	for _, part := range res.Collected {
		part.ForEachRow(func(row sif.Row) error {
			val, err := row.GetString("res")
			require.Nil(t, err)
			require.True(t, val == "A" || val == "B" || val == "C")
			return nil
		})
	}
	require.Nil(t, err)
}
