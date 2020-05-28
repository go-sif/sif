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
	globalLock                 sync.Mutex
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
	tmpDir                     string
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
	InitialSize        int
	CompressedFraction float32
	DiskPath           string
	Schema             sif.Schema
}

// NewLRU produces an LRU PartitionCache
func NewLRU(config *LRUConfig) PartitionCache {
	// validate parameters
	if config.InitialSize < 5 {
		log.Panicf("LRUConfig.InitialSize %d must be greater than 5", config.InitialSize)
	}
	if config.CompressedFraction < 0 || config.CompressedFraction > 1 {
		log.Panicf("LRUConfig.CompressedFraction %f must be between 0 and 1", config.CompressedFraction)
	}
	if config.Schema == nil {
		log.Panicf("Next stage schema was nil")
	}
	// setup limits
	maxUncompressed := int(float32(config.InitialSize) * (1 - config.CompressedFraction))
	maxCompressed := config.InitialSize - maxUncompressed
	transferChanSize := 10
	// init compressor/decompressor
	compressor, err := zstd.NewWriter(new(bytes.Buffer), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Fatalf("Unable to initialize compressor: %e", err)
	}
	decompressor, err := zstd.NewReader(new(bytes.Buffer))
	if err != nil {
		log.Fatalf("Unable to initialize decompressor: %e", err)
	}
	result := &lru{
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
		tmpDir:                 config.DiskPath,
	}
	go result.evictToCompressedMemory()
	go result.evictToDisk()
	return result
}

func (c *lru) Destroy() {
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	close(c.toCompressed) // this will trigger the disk channel to be closed
}

func (c *lru) CurrentSize() int {
	return c.maxUncompressed + c.maxCompressed
}

func (c *lru) Resize(frac float64) {
	// lock out any client interaction with the data structure
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	// compute new sizes
	newSize := int(float64(c.CurrentSize()) * frac)
	newMaxUncompressed := int(float32(newSize) * (1 - c.config.CompressedFraction))
	newMaxCompressed := newSize - newMaxUncompressed
	if c.maxUncompressed == newMaxUncompressed && c.maxCompressed == newMaxCompressed {
		return // do nothing
	}
	// evict from the compressed cache, if we've shrunk
	if newMaxCompressed < c.maxCompressed {
		c.recentCompressedListLock.Lock()
		for c.recentCompressedList.Len() > newMaxCompressed {
			c.compressedPmapLock.Lock()
			toRemove := c.recentCompressedList.Back()
			cachedCompressedPart := toRemove.Value.(*cachedCompressedPartition)
			c.plocks.Lock(cachedCompressedPart.key)
			c.recentCompressedList.Remove(toRemove)
			delete(c.compressedPmap, cachedCompressedPart.key)
			c.toDisk <- cachedCompressedPart
			c.compressedPmapLock.Unlock()
		}
		c.recentCompressedListLock.Unlock()
	}
	// evict from the uncompressed cache, if we've shrunk
	if newMaxUncompressed < c.maxUncompressed {
		c.recentUncompressedListLock.Lock()
		for c.recentUncompressedList.Len() > newMaxUncompressed {
			c.pmapLock.Lock()
			toRemove := c.recentUncompressedList.Back()
			cachedPart := toRemove.Value.(*cachedPartition)
			c.plocks.Lock(cachedPart.key)
			c.recentUncompressedList.Remove(toRemove)
			delete(c.pmap, cachedPart.key)
			c.toCompressed <- cachedPart
			c.pmapLock.Unlock()
		}
		c.recentUncompressedListLock.Unlock()
	}
	c.maxUncompressed = newMaxUncompressed
	c.maxCompressed = newMaxCompressed
}

func (c *lru) Add(key string, value itypes.ReduceablePartition) {
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	c.plocks.Lock(key)
	defer c.plocks.Unlock(key)

	// log.Printf("Adding partition %s to uncompressed memory", key)

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
		toRemove := c.recentUncompressedList.Back()
		cachedPart := toRemove.Value.(*cachedPartition)
		c.plocks.Lock(cachedPart.key)
		c.recentUncompressedList.Remove(toRemove)
		delete(c.pmap, cachedPart.key)
		c.toCompressed <- cachedPart
	}
}

// Get removes the partition from the caches and returns it, if present. Returns an error otherwise.
func (c *lru) Get(key string) (value itypes.ReduceablePartition, err error) {
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	c.plocks.Lock(key)
	defer c.plocks.Unlock(key)
	// log.Printf("Loading partition %s...", key)
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
		// log.Printf("Loaded partition %s from uncompressed memory", key)
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
		bf := bytes.NewReader(cve.Value.(*cachedCompressedPartition).value)
		err := c.decompressor.Reset(bf)
		if err != nil {
			panic(err)
		}
		buff, err := ioutil.ReadAll(c.decompressor)
		decompressedPart, err := partition.FromBytes(buff, c.config.Schema)
		if err != nil {
			panic(err)
		}
		// log.Printf("Loaded partition %s from compressed memory", key)
		return decompressedPart, nil
	}
	return nil, fmt.Errorf("Partition %s is not in the cache", key)
}

// getFromCache removes the partition from the disk cache and returns it, if present
func (c *lru) getFromDisk(key string) (value itypes.ReduceablePartition, err error) {
	tempFilePath := path.Join(c.tmpDir, key)
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
	// log.Printf("Loaded partition %s from disk", key)
	return part, nil
}

func (c *lru) evictToCompressedMemory() {
	// log.Printf("Starting uncompressed memory evictor")
	for msg := range c.toCompressed {
		// log.Printf("Swapping partition %s to compressed memory", msg.key)
		buff, err := msg.value.ToBytes()
		if err != nil {
			log.Fatalf("Unable to convert partition to buffer %s", err)
		}
		var compressed bytes.Buffer
		c.compressor.Reset(&compressed)
		_, err = c.compressor.Write(buff)
		if err != nil {
			log.Fatalf("Unable to convert partition to buffer %s", err)
		}
		err = c.compressor.Close()
		if err != nil {
			log.Fatalf("Unable to convert partition to buffer %s", err)
		}
		result := compressed.Bytes()
		// update the recent list
		c.recentCompressedListLock.Lock()
		e := c.recentCompressedList.PushFront(&cachedCompressedPartition{
			key:   msg.key,
			value: result,
		})
		c.recentCompressedListLock.Unlock()
		// update the compressed map
		c.compressedPmapLock.Lock()
		c.compressedPmap[msg.key] = e
		c.compressedPmapLock.Unlock()
		// log.Printf("Finished swapping partition %s to compressed memory", msg.key)
		// check if we have too many compressed partitions
		if c.recentCompressedList.Len() > c.maxCompressed {
			toRemove := c.recentCompressedList.Back()
			cachedCompressedPart := toRemove.Value.(*cachedCompressedPartition)
			c.plocks.Lock(cachedCompressedPart.key)
			c.recentCompressedList.Remove(toRemove)
			delete(c.compressedPmap, cachedCompressedPart.key)
			c.toDisk <- cachedCompressedPart
		}
		c.plocks.Unlock(msg.key) // this partition was locked once it was scheduled for eviction. now we unlock it
	}
	// this is our cleanup logic for uncompressed partitions:
	close(c.toDisk) // we close toDisk once we're done, since we're the producer
	// empty maps
	c.pmapLock.Lock()
	defer c.pmapLock.Unlock()
	c.pmap = make(map[string]*list.Element)
	// empty lists
	c.recentUncompressedListLock.Lock()
	defer c.recentUncompressedListLock.Unlock()
	c.recentUncompressedList = list.New()
}

func (c *lru) evictToDisk() {
	// log.Printf("Starting compressed memory evictor")
	for msg := range c.toDisk {
		// log.Printf("Swapping partition %s to disk", msg.key)
		tempFilePath := path.Join(c.tmpDir, msg.key)
		f, err := os.Create(tempFilePath)
		if err != nil {
			log.Fatalf("Unable to create temporary file for partition: %e", err)
		}
		_, err = f.Write(msg.value)
		if err != nil {
			log.Fatalf("Unable to write temporary file for partition: %e", err)
		}
		err = f.Sync()
		if err != nil {
			log.Fatalf("Unable to sync file %s: %e", tempFilePath, err)
			return
		}
		err = f.Close()
		if err != nil {
			log.Fatalf("Unable to close file %s: %e", tempFilePath, err)
			return
		}
		log.Printf("Finished swapping partition %s to disk", msg.key)
		c.plocks.Unlock(msg.key) // this partition was locked once it was scheduled for eviction. now we unlock it
	}
	// this is our cleanup logic for compressed partitions:
	// empty maps
	c.compressedPmapLock.Lock()
	defer c.compressedPmapLock.Unlock()
	c.compressedPmap = make(map[string]*list.Element)
	// empty lists
	c.recentCompressedListLock.Lock()
	defer c.recentCompressedListLock.Unlock()
	c.recentCompressedList = list.New()
}
