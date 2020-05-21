package pcache

import (
	"bytes"
	"container/list"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"

	"github.com/docker/docker/pkg/locker"
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/klauspost/compress/zstd"
)

// lru is an LRU cache for Partitions
type lru struct {
	config                     *LRUConfig
	compressor                 *zstd.Encoder
	decompressor               *zstd.Decoder
	plocks                     *locker.Locker
	pmapLock                   sync.Mutex
	pmap                       map[string]*list.Element
	compressedPmapLock         sync.Mutex
	compressedPmap             map[string]*list.Element
	recentUncompressedListLock sync.Mutex
	recentUncompressedList     *list.List // back is oldest, front is newest
	recentCompressedListLock   sync.Mutex
	recentCompressedList       *list.List // back is oldest, front is newest
	maxUncompressed            int
	maxCompressed              int
	toCompressed               chan *cachedPartition
	toDisk                     chan *cachedCompressedPartition
}

type cachedPartition struct {
	key   string
	value itypes.ReduceablePartition
}

type cachedCompressedPartition struct {
	key   string
	value []byte
}

// LRUConfig configures an LRU PartitionCache
type LRUConfig struct {
	Size               int
	CompressedFraction float32
	DiskPath           string
	Schema             sif.Schema
}

// NewLRU produces an LRU PartitionCache
func NewLRU(config *LRUConfig) PartitionCache {
	if config.Size < 5 {
		log.Panicf("LRUConfig.Size %d must be greater than 5", config.Size)
	}
	if config.CompressedFraction < 0 || config.CompressedFraction > 1 {
		log.Panicf("LRUConfig.CompressedFraction %f must be between 0 and 1", config.CompressedFraction)
	}
	if config.Schema == nil {
		log.Panicf("Next stage schema was nil")
	}
	maxUncompressed := int(float32(config.Size) * (1 - config.CompressedFraction))
	maxCompressed := config.Size - maxUncompressed
	transferChanSize := config.Size / 100
	if transferChanSize < 5 {
		transferChanSize = 5
	}
	// init compressor/decompressor
	compressor, err := zstd.NewWriter(new(bytes.Buffer), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatalf("Unable to initialize compressor: %e", err)
	}
	decompressor, err := zstd.NewReader(new(bytes.Buffer))
	if err != nil {
		log.Fatalf("Unable to initialize decompressor: %e", err)
	}
	return &lru{
		compressor:             compressor,
		decompressor:           decompressor,
		config:                 config,
		plocks:                 locker.New(),
		pmap:                   make(map[string]*list.Element),
		compressedPmap:         make(map[string]*list.Element),
		recentUncompressedList: list.New(),
		recentCompressedList:   list.New(),
		maxUncompressed:        maxUncompressed,
		maxCompressed:          maxCompressed,
		toCompressed:           make(chan *cachedPartition, transferChanSize),
		toDisk:                 make(chan *cachedCompressedPartition, transferChanSize),
	}
}

func (c *lru) Destroy() {
	close(c.toCompressed)
	close(c.toDisk)
	panic("not implemented") // TODO: Implement
}

func (c *lru) Add(key string, value itypes.ReduceablePartition) {
	c.plocks.Lock(key)
	defer c.plocks.Unlock(key)

	// update the recent list
	c.recentUncompressedListLock.Lock()
	e := c.recentUncompressedList.PushFront(&cachedPartition{
		key:   key,
		value: value,
	})
	defer c.recentUncompressedListLock.Unlock()

	// update the uncompressed cache
	c.pmapLock.Lock()
	c.pmap[key] = e
	defer c.pmapLock.Unlock()

	// if we're full, it can only be because the uncompressed
	// cache has grown, so let's just check that one
	if c.recentUncompressedList.Len() > c.maxUncompressed {
		toRemove := c.recentCompressedList.Back()
		c.recentUncompressedList.Remove(toRemove)
		c.toCompressed <- toRemove.Value.(*cachedPartition)
		go c.evictToCompressedMemory()
	}
}

// Get removes the partition from the caches and returns it, if present. Returns an error otherwise.
func (c *lru) Get(key string) (value itypes.ReduceablePartition, err error) {
	value, err = c.getFromCache(key)
	if err != nil {
		value, err = c.getFromCompressedCache(key)
		if err != nil {
			value, err = c.getFromDisk(key)
			if err != nil {
				return nil, err
			}
		}
	}
	return
}

// getFromCache removes the partition from the uncompressed cache and returns it, if present
func (c *lru) getFromCache(key string) (value itypes.ReduceablePartition, err error) {
	c.pmapLock.Lock()
	defer c.pmapLock.Unlock()
	ve, ok := c.pmap[key]
	if ok {
		delete(c.pmap, key)
		c.recentUncompressedListLock.Lock()
		c.recentUncompressedList.Remove(ve)
		c.recentUncompressedListLock.Unlock()
		return ve.Value.(*cachedPartition).value, nil
	}
	return nil, fmt.Errorf("Partition %s is not in the cache", key)
}

// getFromCache removes the partition from the compressed cache and returns it, if present
func (c *lru) getFromCompressedCache(key string) (value itypes.ReduceablePartition, err error) {
	c.compressedPmapLock.Lock()
	defer c.compressedPmapLock.Unlock()
	cve, cok := c.compressedPmap[key]
	if cok {
		delete(c.compressedPmap, key)
		c.recentCompressedListLock.Lock()
		c.recentCompressedList.Remove(cve)
		c.recentCompressedListLock.Unlock()
		bf := bytes.NewReader(cve.Value.([]byte))
		err := c.decompressor.Reset(bf)
		if err != nil {
			panic(err)
		}
		buff, err := ioutil.ReadAll(c.decompressor)
		decompressedPart, err := partition.FromBytes(buff, c.config.Schema)
		if err != nil {
			panic(err)
		}
		return decompressedPart, nil
	}
	return nil, fmt.Errorf("Partition %s is not in the cache", key)
}

// getFromCache removes the partition from the disk cache and returns it, if present
func (c *lru) getFromDisk(key string) (value itypes.ReduceablePartition, err error) {
	tempFilePath := path.Join(c.config.DiskPath, key)
	f, err := os.Open(tempFilePath)
	if err != nil {
		return nil, fmt.Errorf("Unable to load disk-swapped partition %s: %e", tempFilePath, err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Printf("Unable to close file %s", tempFilePath)
		}
		err = os.Remove(tempFilePath)
		if err != nil {
			log.Printf("Unable to remove file %s", tempFilePath)
		}
	}()
	err = c.decompressor.Reset(f)
	if err != nil {
		log.Panicf("Unable to decompress disk-swapped partition %s: %e", tempFilePath, err)
	}
	buff, err := ioutil.ReadAll(c.decompressor)
	if err != nil {
		log.Panicf("Unable to decompress disk-swapped partition %s: %e", tempFilePath, err)
	}
	part, err := partition.FromBytes(buff, c.config.Schema)
	if err != nil {
		panic(err)
	}
	return part, nil
}

func (c *lru) Resize(size int) {
	panic("not implemented") // TODO: Implement
}

func (c *lru) evictToCompressedMemory() {
	panic("not implemented") // TODO: Implement
}

func (c *lru) evictToDisk() {
	panic("not implemented") // TODO: Implement
}
