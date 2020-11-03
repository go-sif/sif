package dataframe

import (
	"bytes"
	"fmt"
	"log"

	"github.com/go-sif/sif"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/pierrec/lz4"
)

// partitionSerializingIterator serializes Partitions from another iterator
type partitionSerializingIterator struct {
	iterator           sif.PartitionIterator
	compressor         *lz4.Writer
	reusableReadBuffer *bytes.Buffer
}

func createPartitionSerializingIterator(parts sif.PartitionIterator) itypes.SerializedPartitionIterator {
	compressor := lz4.NewWriter(new(bytes.Buffer))
	return &partitionSerializingIterator{
		iterator:           parts,
		compressor:         compressor,
		reusableReadBuffer: new(bytes.Buffer),
	}
}

func (psi *partitionSerializingIterator) HasNextSerializedPartition() bool {
	return psi.iterator.HasNextPartition()
}

func (psi *partitionSerializingIterator) NextSerializedPartition() (id string, spart []byte, err error) {
	part, done, err := psi.iterator.NextPartition()
	if err != nil {
		return "", nil, err
	}
	defer func() {
		if done != nil {
			done()
		}
	}()
	// TODO Identical compression logic to cache.go. Abstract so compressor can be swapped easily.
	rpart, ok := part.(itypes.ReduceablePartition)
	if !ok {
		return "", nil, fmt.Errorf("Cannot serialize Partition which is not a ReduceablePartition")
	}
	bytes, err := rpart.ToBytes()
	if err != nil {
		log.Fatalf("Unable to convert partition to buffer %s", err)
	}
	psi.reusableReadBuffer.Reset()
	psi.compressor.Reset(psi.reusableReadBuffer)
	n, err := psi.compressor.Write(bytes)
	if err != nil || n == 0 {
		log.Fatalf("Unable to compress data for partition: %e", err)
	}
	err = psi.compressor.Close()
	return rpart.ID(), psi.reusableReadBuffer.Bytes(), nil
}

func (psi *partitionSerializingIterator) OnEnd(onEnd func()) {
	psi.iterator.OnEnd(onEnd)
}
