package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"math"
	"os"
	"path"
	"testing"

	types "github.com/go-sif/sif/columntype"
	core "github.com/go-sif/sif/core"
	"github.com/go-sif/sif/datasource/file"
	dsv "github.com/go-sif/sif/datasource/parser/dsv"
	ops "github.com/go-sif/sif/operations/transform"
	util "github.com/go-sif/sif/operations/util"
	"github.com/stretchr/testify/require"
)

func createTestNYCTaxiDataFrame(t *testing.T) *core.DataFrame {
	schema := core.CreateSchema()
	schema.CreateColumn("hack", &types.StringColumnType{Length: 32})
	schema.CreateColumn("license", &types.StringColumnType{Length: 32})
	schema.CreateColumn("code", &types.StringColumnType{Length: 3})
	schema.CreateColumn("flag", &types.Uint8ColumnType{})
	schema.CreateColumn("type", &types.VarStringColumnType{})
	schema.CreateColumn("pickup_time", &types.VarStringColumnType{})
	schema.CreateColumn("dropoff_time", &types.VarStringColumnType{})
	schema.CreateColumn("passengers", &types.Uint8ColumnType{})
	schema.CreateColumn("duration", &types.Uint32ColumnType{})
	schema.CreateColumn("distance", &types.Float32ColumnType{})
	schema.CreateColumn("pickup_lon", &types.Float64ColumnType{})
	schema.CreateColumn("pickup_lat", &types.Float64ColumnType{})
	schema.CreateColumn("dropoff_lon", &types.Float64ColumnType{})
	schema.CreateColumn("dropoff_lat", &types.Float64ColumnType{})

	cwd, err := os.Getwd()
	require.Nil(t, err)
	parser := dsv.CreateParser(&dsv.ParserConf{
		NilValue: "null",
	})
	dataframe := file.CreateDataFrame(path.Join(cwd, "../../testenv/*.csv"), parser, schema)
	return dataframe
}

// Custom column type to store sparse heatmap data
type VarHeatmapColumnType struct {
}

// Size in bytes of a VarHeatmapColumnType
func (b *VarHeatmapColumnType) Size() int {
	return 0
}

// ToString produces a string representation of this heatmap
func (b *VarHeatmapColumnType) ToString(v interface{}) string {
	return "[HEATMAP]"
}

// Serialize serializes this VarHeatmapColumnType to binary data
func (b *VarHeatmapColumnType) Serialize(v interface{}) ([]byte, error) {
	buff := new(bytes.Buffer)
	e := gob.NewEncoder(buff)
	err := e.Encode(v)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// Deserialize deserializes a VarHeatmapColumnType from binary data
func (b *VarHeatmapColumnType) Deserialize(ser []byte) (interface{}, error) {
	var deser map[int]uint32
	buff := bytes.NewBuffer(ser)
	d := gob.NewDecoder(buff)
	err := d.Decode(&deser)
	if err != nil {
		return nil, err
	}
	return deser, nil
}

func TestNYCTaxi(t *testing.T) {
	heatmapSize := 2048
	// utility functions
	minLat := float64(40.690015)
	maxLat := float64(40.839271)
	minLon := float64(-74.080783)
	maxLon := float64(-73.863017)
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
	frame := createTestNYCTaxiDataFrame(t)
	frame, err := frame.To(
		// drop unnecessary columns
		ops.RemoveColumn("hack", "license", "code", "flag", "type", "pickup_time", "dropoff_time", "passengers", "duration", "distance", "pickup_lon", "pickup_lat"),
		ops.Repack(),
		// create column for heatmap reduction
		ops.AddColumn("heatmap", &VarHeatmapColumnType{}),
		// compute partial heatmaps
		ops.Map(func(row *core.Row) error {
			if row.IsNil("dropoff_lat") || row.IsNil("dropoff_lon") {
				return nil
			}
			lat, err := row.GetFloat64("dropoff_lat")
			if err != nil {
				return err
			}
			lon, err := row.GetFloat64("dropoff_lon")
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
		ops.Reduce(func(row *core.Row) ([]byte, error) {
			return []byte{byte(1)}, nil
		}, func(lrow *core.Row, rrow *core.Row) error {
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
				lheatmap[idx] = lheatmap[idx] + count
			}
			return lrow.SetVarCustomData("heatmap", lheatmap)
		}),
		util.Collect(1), // TODO should be 1 partitions because we are summing to a single row, but collect extra as a test
	)
	require.Nil(t, err)

	// run dataframe and verify results
	copts := &core.NodeOptions{}
	wopts := &core.NodeOptions{NumInMemoryPartitions: 20}
	res, err := runTestFrame(context.Background(), t, frame, copts, wopts, 2)
	require.Nil(t, err)
	require.NotNil(t, res)

	// rasterize result
	for _, part := range res {
		require.Equal(t, 1, part.GetNumRows())
		row := part.GetRow(0)
		heatmapData, err := row.GetVarCustomData("heatmap")
		require.Nil(t, err)
		heatmap, ok := heatmapData.(map[int]uint32)
		require.True(t, ok)
		// find max value
		maxVal := uint32(0)
		for _, val := range heatmap {
			if val > maxVal {
				maxVal = val
			}
		}
		// write heatmap to image
		bounds := image.Rectangle{Min: image.Point{0, 0}, Max: image.Point{heatmapSize, heatmapSize}}
		img := image.NewRGBA(bounds)
		draw.Draw(img, img.Bounds(), &image.Uniform{color.RGBA{17, 17, 17, 255}}, image.ZP, draw.Src)
		// 222,235,247 to 49,130,189
		for idx, val := range heatmap {
			x := idx % heatmapSize
			y := idx / heatmapSize
			ratio := math.Log10(1 + 9*float64(val)/float64(maxVal))
			red := uint8(49 + ratio*(222-49))
			green := uint8(130 + ratio*(235-130))
			blue := uint8(189 + ratio*(247-189))
			img.Set(x, y, color.RGBA{red, green, blue, 255})
		}
		cwd, err := os.Getwd()
		require.Nil(t, err)
		f, err := os.OpenFile(path.Join(cwd, "../../testenv/nyc_taxi.png"), os.O_WRONLY|os.O_CREATE, 0600)
		require.Nil(t, err)
		defer f.Close()
		writer := bufio.NewWriter(f)
		png.Encode(writer, img)
		writer.Flush()
		break
	}
}
