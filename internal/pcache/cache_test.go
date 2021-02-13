package pcache

import (
	"log"
	"os"
	"testing"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	"github.com/go-sif/sif/schema"
	"github.com/stretchr/testify/require"
)

func TestCacheResize(t *testing.T) {
	schema := schema.CreateSchema()
	schema.CreateColumn("key", &sif.Uint32ColumnType{})
	schema.CreateColumn("val", &sif.Uint32ColumnType{})

	cache := NewLRU(&LRUConfig{
		InitialSize: 10,
		DiskPath:    os.TempDir(),
		Compressor:  partition.NewLZ4PartitionCompressor(),
	})
	defer cache.Destroy()

	iCache, ok := cache.(*lru)
	require.True(t, ok)

	for i := 0; i < 20; i++ {
		part := partition.CreateReduceablePartition(1024, schema)
		cache.Add(part.ID(), part)
	}
	require.Equal(t, 10, len(iCache.pmap))
	require.Equal(t, 10, iCache.recentList.Len())
	log.Println("Resizing cache to 50%...")
	cache.Resize(0.5)
	require.Equal(t, 5, len(iCache.pmap))
	require.Equal(t, 5, iCache.recentList.Len())
}
