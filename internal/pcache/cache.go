package pcache

import (
	"bytes"
	"container/list"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/docker/docker/pkg/locker"
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/internal/partition"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/pierrec/lz4"
)

// lru is an LRU cache for Partitions
type lru struct {
	config             *LRUConfig
	compressor         *lz4.Writer
	decompressor       *lz4.Reader
	globalLock         sync.Mutex
	plocks             *locker.Locker
	pmapLock           sync.Mutex
	pmap               map[string]*list.Element
	recentListLock     sync.Mutex
	recentList         *list.List // back is oldest, front is newest
	size               int
	toDisk             chan *cachedPartition
	tmpDir             string
	reusableReadBuffer *bytes.Buffer
	hits               uint64
	misses             uint64
}

type cachedPartition struct {
	key   string
	value itypes.ReduceablePartition
}

// LRUConfig configures an LRU PartitionCache
type LRUConfig struct {
	InitialSize int
	DiskPath    string
	Schema      sif.Schema
}

// NewLRU produces an LRU PartitionCache
func NewLRU(config *LRUConfig) PartitionCache {
	// validate parameters
	if config.InitialSize < 5 {
		log.Panicf("LRUConfig.InitialSize %d must be greater than 5", config.InitialSize)
	}
	if config.Schema == nil {
		log.Panicf("Next stage schema was nil")
	}
	// setup limits
	transferChanSize := 10
	// init compressor/decompressor
	decompressor := lz4.NewReader(new(bytes.Buffer))
	compressor := lz4.NewWriter(new(bytes.Buffer))
	result := &lru{
		compressor:         compressor,
		decompressor:       decompressor,
		config:             config,
		plocks:             locker.New(),
		pmap:               make(map[string]*list.Element),
		recentList:         list.New(),
		size:               config.InitialSize,
		toDisk:             make(chan *cachedPartition, transferChanSize),
		tmpDir:             config.DiskPath,
		reusableReadBuffer: new(bytes.Buffer),
	}
	go result.evictToDisk()
	return result
}

func (c *lru) Destroy() {
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	close(c.toDisk) // this will trigger the disk channel to be closed
	// stop compressor and decompressor
	c.compressor.Close()
	c.decompressor.Reset(new(bytes.Buffer))
}

func (c *lru) CurrentSize() int {
	return c.size
}

func (c *lru) Resize(frac float64) bool {
	// lock out any client interaction with the data structure
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	// compute new sizes
	c.recentListLock.Lock()
	newSize := int(float64(c.recentList.Len()) * frac)
	c.recentListLock.Unlock()
	if newSize < 5 {
		newSize = 5 // minimum size
	}
	if c.size == newSize {
		return false // do nothing
	}
	// evict from the cache, if we've shrunk
	if newSize < c.size {
		c.recentListLock.Lock()
		for c.recentList.Len() > newSize {
			c.pmapLock.Lock()
			toRemove := c.recentList.Back()
			if toRemove == nil || toRemove.Value == nil {
				break
			}
			cachedPart := toRemove.Value.(*cachedPartition)
			c.plocks.Lock(cachedPart.key)
			c.recentList.Remove(toRemove)
			delete(c.pmap, cachedPart.key)
			// c.writePartitionToDisk(cachedPart) // synchronous version
			c.toDisk <- cachedPart // async version
			c.pmapLock.Unlock()
		}
		c.recentListLock.Unlock()
	}
	// block until toDisk channel is empty (i.e. until resize is complete)
	for range time.Tick(250 * time.Millisecond) {
		if len(c.toDisk) == 0 {
			break
		}
	}
	c.size = newSize
	return true
}

func (c *lru) Add(key string, value itypes.ReduceablePartition) {
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	c.plocks.Lock(key)
	defer c.plocks.Unlock(key)

	// log.Printf("Adding partition %s to uncompressed memory", key)

	// update the recent list
	c.recentListLock.Lock()
	e := c.recentList.PushFront(&cachedPartition{
		key:   key,
		value: value,
	})
	defer c.recentListLock.Unlock()

	// update the cache
	c.pmapLock.Lock()
	c.pmap[key] = e
	defer c.pmapLock.Unlock()

	// if we're full, trigger eviction
	if c.recentList.Len() > c.size {
		toRemove := c.recentList.Back()
		cachedPart := toRemove.Value.(*cachedPartition)
		c.plocks.Lock(cachedPart.key)
		c.recentList.Remove(toRemove)
		delete(c.pmap, cachedPart.key)
		// c.writePartitionToDisk(cachedPart) // synchronous version
		c.toDisk <- cachedPart // async version
	}
}

// Get removes the partition from the caches and returns it, if present. Returns an error otherwise.
func (c *lru) Get(key string) (itypes.ReduceablePartition, error) {
	// defer func() {
	// 	log.Printf("Hits: %d | Misses: %d", c.hits, c.misses)
	// }()
	c.globalLock.Lock()
	defer c.globalLock.Unlock()
	c.plocks.Lock(key)
	defer c.plocks.Unlock(key)
	// log.Printf("Loading partition %s...", key)
	value, err := c.getFromCache(key)
	if err == nil {
		c.hits++
		return value, nil
	}
	value, err = c.getFromDisk(key)
	if err == nil {
		c.misses++
		return value, nil
	}
	return nil, err
}

// getFromCache removes the partition from the uncompressed cache and returns it, if present
func (c *lru) getFromCache(key string) (value itypes.ReduceablePartition, err error) {
	c.pmapLock.Lock()
	defer c.pmapLock.Unlock()
	ve, ok := c.pmap[key]
	if ok {
		delete(c.pmap, key)
		c.recentListLock.Lock()
		c.recentList.Remove(ve)
		c.recentListLock.Unlock()
		// log.Printf("Loaded partition %s from uncompressed memory", key)
		return ve.Value.(*cachedPartition).value, nil
	}
	return nil, fmt.Errorf("Partition %s is not in the cache", key)
}

// getFromCache removes the partition from the disk cache and returns it, if present
func (c *lru) getFromDisk(key string) (value itypes.ReduceablePartition, err error) {
	tempFilePath := path.Join(c.tmpDir, key)
	f, err := os.Open(tempFilePath)
	if err != nil {
		return nil, fmt.Errorf("Unable to open disk-swapped partition %s: %e", tempFilePath, err)
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
	c.decompressor.Reset(f)
	c.reusableReadBuffer.Reset()
	_, err = c.reusableReadBuffer.ReadFrom(c.decompressor)
	if err != nil {
		log.Panicf("Unable to decompress disk-swapped partition %s: %e", tempFilePath, err)
	}
	part, err := partition.FromBytes(c.reusableReadBuffer.Bytes(), c.config.Schema)
	if err != nil {
		panic(err)
	}
	// log.Printf("Loaded partition %s from disk", key)
	return part, nil
}

func (c *lru) evictToDisk() {
	// log.Printf("Starting disk evictor")
	for msg := range c.toDisk {
		c.writePartitionToDisk(msg)
	}
	// this is our cleanup logic for the cache, which will only
	// run when the compression channel is closed
	// empty maps
	c.pmapLock.Lock()
	defer c.pmapLock.Unlock()
	c.pmap = make(map[string]*list.Element)
	// empty lists
	c.recentListLock.Lock()
	defer c.recentListLock.Unlock()
	c.recentList = list.New()
}

func (c *lru) writePartitionToDisk(msg *cachedPartition) {
	// log.Printf("Swapping partition %s to disk", msg.key)
	tempFilePath := path.Join(c.tmpDir, msg.key)
	f, err := os.Create(tempFilePath)
	if err != nil {
		log.Fatalf("Unable to create temporary file for partition: %e", err)
	}
	bytes, err := msg.value.ToBytes()
	if err != nil {
		log.Fatalf("Unable to convert partition to buffer %s", err)
	}
	c.compressor.Reset(f)
	n, err := c.compressor.Write(bytes)
	if err != nil || n == 0 {
		log.Fatalf("Unable to write temporary file for partition: %e", err)
	}
	err = c.compressor.Close()
	if err != nil {
		log.Fatalf("Unable to close compressor for file %s: %e", tempFilePath, err)
		return
	}
	err = f.Close()
	if err != nil {
		log.Fatalf("Unable to close file %s: %e", tempFilePath, err)
		return
	}
	// log.Printf("Finished swapping partition %s to disk", msg.key)
	c.plocks.Unlock(msg.key) // this partition was locked once it was scheduled for eviction. now we unlock it
	msg.key = ""
	msg.value = nil
}
