package bucketed

import (
	"log"
	"sync"

	"github.com/go-sif/sif"
	"github.com/hashicorp/go-multierror"
)

type bucketed struct {
	nextStageSchema sif.Schema
	buckets         []uint64
	bucketsLock     sync.Mutex
	bucketFactory   func() sif.PartitionIndex
	bucketsMap      map[uint64]sif.PartitionIndex
}

// CreateBucketedPartitionIndex creates a new bucketed PartitionIndex suitable for reduction
func CreateBucketedPartitionIndex(buckets []uint64, bucketFactory func() sif.PartitionIndex, nextStageSchema sif.Schema) sif.BucketedPartitionIndex {
	bucketsMap := make(map[uint64]sif.PartitionIndex)
	return &bucketed{
		nextStageSchema: nextStageSchema,
		buckets:         buckets,
		bucketFactory:   bucketFactory,
		bucketsMap:      bucketsMap,
	}
}

func (b *bucketed) keyToBuckets(key uint64) uint64 {
	for _, i := range b.buckets {
		if key < i {
			return i
		}
	}
	// should never reach here
	log.Panicf("Key must fall within a bucket")
	return 0
}

func (b *bucketed) SetMaxRows(maxRows int) {
	for _, b := range b.bucketsMap {
		b.SetMaxRows(maxRows)
	}
}

func (b *bucketed) GetNextStageSchema() sif.Schema {
	return b.nextStageSchema
}

func (b *bucketed) MergePartition(part sif.ReduceablePartition, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error {
	var multierr *multierror.Error
	// merge rows into our trees
	i := 0
	tempRow := part.CreateTempRow()
	err := part.ForEachRow(func(row sif.Row) error {
		key, err := part.GetKey(i)
		if err != nil {
			return err
		}
		bucket := b.keyToBuckets(key)
		b.bucketsLock.Lock()
		defer b.bucketsLock.Unlock()
		if _, ok := b.bucketsMap[bucket]; !ok {
			b.bucketsMap[bucket] = b.bucketFactory()
		}
		err = b.bucketsMap[bucket].MergeRow(tempRow, row, keyfn, reducefn)
		if err != nil {
			multierr = multierror.Append(multierr, err)
		}
		i++
		return nil
	})
	if err != nil {
		return err
	}
	return multierr.ErrorOrNil()
}

func (b *bucketed) MergeRow(tempRow sif.Row, row sif.Row, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error {
	panic("not implemented") // TODO: Implement
}

func (b *bucketed) GetPartitionIterator(destructive bool) sif.PartitionIterator {
	panic("not implemented") // TODO: Implement
}

func (b *bucketed) GetSerializedPartitionIterator(destructive bool) sif.SerializedPartitionIterator {
	panic("not implemented") // TODO: Implement
}

func (b *bucketed) NumPartitions() uint64 {
	numPartitions := uint64(0)
	for _, bucket := range b.bucketsMap {
		numPartitions += bucket.NumPartitions()
	}
	return numPartitions
}

func (b *bucketed) CacheSize() int {
	for _, bucket := range b.bucketsMap {
		return bucket.CacheSize()
	}
	return 0
}

func (b *bucketed) ResizeCache(frac float64) bool {
	resized := false
	for _, bucket := range b.bucketsMap {
		resized = resized || bucket.ResizeCache(frac)
	}
	return resized
}

func (b *bucketed) Destroy() {
	for _, bucket := range b.bucketsMap {
		bucket.Destroy()
	}
}

func (b *bucketed) GetBucket(bucket uint64) sif.PartitionIndex {
	if _, ok := b.bucketsMap[bucket]; !ok {
		b.bucketsMap[bucket] = b.bucketFactory()
	}
	return b.bucketsMap[bucket]
}
