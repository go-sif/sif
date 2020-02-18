package integration

import (
	"bufio"
	"context"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"os"
	"path"
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/cluster"
	"github.com/go-sif/sif/datasource/file"
	jsonl "github.com/go-sif/sif/datasource/parser/jsonl"
	ops "github.com/go-sif/sif/operations/transform"
	"github.com/go-sif/sif/schema"
	"github.com/stretchr/testify/require"
)

func createTestEDSMDataFrame(t *testing.T) sif.DataFrame {
	schema := schema.CreateSchema()
	schema.CreateColumn("coords.x", &sif.Float64ColumnType{})
	schema.CreateColumn("coords.z", &sif.Float64ColumnType{})
	schema.CreateColumn("date", &sif.TimeColumnType{Format: "2006-01-02 15:04:05"})

	parser := jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 256,
	})
	cwd, err := os.Getwd()
	require.Nil(t, err)
	dataframe := file.CreateDataFrame(path.Join(cwd, "../../../testenv/*.jsonl"), parser, schema)
	return dataframe
}

// Note: heatmap column type declared in nyc_taxi_test.go

func TestEDSMHeatmap(t *testing.T) {
	heatmapSize := 1024
	maxVal := uint32(150) // max threshold for numeric value at pixel (corresponds to "hottest" colour in ramp) (186 in 7-day dataset)
	// utility functions
	minLat := float64(-18000)
	maxLat := float64(68000)
	minLon := float64(-43000)
	maxLon := float64(43000)
	latRange := maxLat - minLat
	lonRange := maxLon - minLon
	outsideBounds := func(lat float64, lon float64) bool {
		return lat < minLat || lat > maxLat || lon < minLon || lon > maxLon
	}
	latToY := func(lat float64) int {
		translated := lat - minLat
		scaled := translated / latRange
		y := heatmapSize - int(scaled*float64(heatmapSize))
		return y
	}
	lonToX := func(lon float64) int {
		translated := lon - minLon
		scaled := translated / lonRange
		x := int(scaled * float64(heatmapSize))
		return x
	}
	xYToCoord := func(x int, y int) int {
		return y*heatmapSize + x
	}

	// create tiling dataframe
	frame := createTestEDSMDataFrame(t)
	frame, err := frame.To(
		// create column for heatmap reduction
		ops.AddColumn("heatmap", &VarHeatmapColumnType{}),
		// compute partial heatmaps
		ops.Map(func(row sif.Row) error {
			if row.IsNil("coords.x") || row.IsNil("coords.z") {
				return nil
			}
			lat, err := row.GetFloat64("coords.z")
			if err != nil {
				return err
			}
			lon, err := row.GetFloat64("coords.x")
			if err != nil {
				return err
			}
			if outsideBounds(lat, lon) {
				return row.SetVarCustomData("heatmap", map[int]uint32{})
			}
			y := latToY(lat)
			x := lonToX(lon)
			// add one to count at the correct location within the heatmap
			idx := xYToCoord(x, y)
			err = row.SetVarCustomData("heatmap", map[int]uint32{idx: 1})
			return err
		}),
		// perform heatmap reduction
		ops.Reduce(func(row sif.Row) ([]byte, error) {
			tval, err := row.GetTime("date")
			if err != nil {
				return nil, err
			}
			key := tval.Format("2006")
			// key := tval.Format("2006-01-02-15") // uncomment for hourly
			return []byte(key), nil
		}, func(lrow sif.Row, rrow sif.Row) error {
			lval, err := lrow.GetVarCustomData("heatmap")
			if err != nil {
				return err
			}
			rval, err := rrow.GetVarCustomData("heatmap")
			if err != nil {
				return err
			}
			lheatmap, ok := lval.(map[int]uint32)
			if !ok {
				return fmt.Errorf("Heatmap data is not a map[int]uint32")
			}
			rheatmap, ok := rval.(map[int]uint32)
			if !ok {
				return fmt.Errorf("Heatmap data is not a map[int]uint32")
			}
			for idx, count := range rheatmap {
				if lheatmap[idx] < math.MaxUint32 {
					lheatmap[idx] = lheatmap[idx] + count
				}
			}
			return lrow.SetVarCustomData("heatmap", lheatmap)
		}),
		ops.Map(func(row sif.Row) error {
			tval, err := row.GetTime("date")
			if err != nil {
				return err
			}
			dateOfImage := tval.Format("2006")
			// dateOfImage := tval.Format("2006-01-02-15") // uncomment for hourly
			heatmapData, err := row.GetVarCustomData("heatmap")
			if err != nil {
				return err
			}
			heatmap, ok := heatmapData.(map[int]uint32)
			if !ok {
				return fmt.Errorf("Unable to cast heatmap data to map[int]uint32")
			}
			// write heatmap to image
			bounds := image.Rectangle{Min: image.Point{0, 0}, Max: image.Point{heatmapSize, heatmapSize}}
			img := image.NewRGBA(bounds)
			draw.Draw(img, img.Bounds(), &image.Uniform{color.RGBA{17, 17, 17, 255}}, image.ZP, draw.Src)
			// 247,252,240 to 04,20,40
			for idx, val := range heatmap {
				x := idx % heatmapSize
				y := idx / heatmapSize
				if val > maxVal {
					val = maxVal
				}
				ratio := math.Log10(1 + 9*float64(val)/float64(maxVal))
				red := uint8(4 + ratio*(247-4))
				green := uint8(20 + ratio*(252-20))
				blue := uint8(40 + ratio*(240-40))
				// alpha := uint8(150 + ratio*(255-150))
				img.Set(x, y, color.RGBA{red, green, blue, 255})
			}
			cwd, err := os.Getwd()
			if err != nil {
				return err
			}
			f, err := os.OpenFile(path.Join(cwd, fmt.Sprintf("../../../testenv/edsm-%s.png", dateOfImage)), os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				return err
			}
			defer f.Close()
			writer := bufio.NewWriter(f)
			png.Encode(writer, img)
			writer.Flush()
			return nil
		}),
	)
	require.Nil(t, err)

	// run dataframe and verify results
	copts := &cluster.NodeOptions{}
	wopts := &cluster.NodeOptions{NumInMemoryPartitions: 20}
	_, err = runTestFrame(context.Background(), t, frame, copts, wopts, 8)
	require.Nil(t, err)
}
