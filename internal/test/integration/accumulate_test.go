package integration

import (
	"context"
	"testing"

	"github.com/go-sif/sif/accumulators"
	"github.com/go-sif/sif/cluster"
	"github.com/go-sif/sif/operations/util"
	siftest "github.com/go-sif/sif/testing"
	"github.com/stretchr/testify/require"
)

func TestAccumulate(t *testing.T) {
	// create dataframe
	numRows := 100
	frame, err := createTestReduceDataFrame(t, numRows).To(
		util.Accumulate(accumulators.Counter),
	)
	require.Nil(t, err)

	// run dataframe and verify results
	res, err := siftest.LocalRunFrame(context.Background(), frame, &cluster.NodeOptions{}, 2)
	require.Nil(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Accumulated)
	ca, isCountAccumulator := res.Accumulated.(*accumulators.Count)
	require.True(t, isCountAccumulator)
	require.Equal(t, 100, int(ca.GetCount()))
}
