package dataframe

import (
	"fmt"
	"log"

	xxhash "github.com/cespare/xxhash/v2"
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/errors"
	"github.com/go-sif/sif/internal/partition"
	"github.com/go-sif/sif/internal/pcache"
	itypes "github.com/go-sif/sif/internal/types"
	"github.com/go-sif/sif/internal/util"
)

// pTreeNode is a node of a tree that builds, sorts and organizes keyed partitions. NOT THREAD SAFE.
type pTreeNode struct {
	k               uint64
	left            *pTreeNode
	right           *pTreeNode
	center          *pTreeNode
	partID          string
	maxRows         int
	nextStageSchema sif.Schema
	prev            *pTreeNode // btree-like link between leaves
	next            *pTreeNode // btree-like link between leaves
	parent          *pTreeNode
	partitionCache  pcache.PartitionCache
}

// pTreeRoot is an alias for pTreeNode representing the root node of a pTree
type pTreeRoot = pTreeNode

// createPTreeNode creates a new pTree with a limit on Partition size and a given shared Schema
func createPTreeNode(conf *itypes.PlanExecutorConfig, maxRows int, nextStageSchema sif.Schema) *pTreeNode {
	if conf.InMemoryPartitions < 5 {
		conf.InMemoryPartitions = 5
	}
	if conf.CompressedMemoryFraction < 0 || conf.CompressedMemoryFraction > 1 {
		log.Panicf("PlanExecutorConfig.CompressedMemoryFraction must be between 0 and 1")
	}
	if conf.CompressedMemoryFraction == 0 {
		conf.CompressedMemoryFraction = 0.75
	}
	cache := pcache.NewLRU(&pcache.LRUConfig{
		Size:               conf.InMemoryPartitions,
		CompressedFraction: conf.CompressedMemoryFraction,
		DiskPath:           conf.TempFilePath,
		Schema:             nextStageSchema,
	})
	// create initial partition for root node
	part := partition.CreateReduceablePartition(maxRows, nextStageSchema)
	part.KeyRows(nil)
	partID := part.ID()
	cache.Add(partID, part)
	// return root node
	return &pTreeNode{
		k:               0,
		partID:          partID,
		maxRows:         part.GetMaxRows(),
		nextStageSchema: nextStageSchema,
		partitionCache:  cache,
	}
}

// mergePartition merges the Rows from a given Partition into matching Rows within this pTree, using a KeyingOperation and a ReductionOperation, inserting if necessary
func (t *pTreeRoot) mergePartition(part itypes.ReduceablePartition, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error {
	tempRow := partition.CreateTempRow()
	return part.ForEachRow(func(row sif.Row) error {
		if err := t.mergeRow(tempRow, row, keyfn, reducefn); err != nil {
			return err
		}
		return nil
	})
}

// mergeRow merges a single Row into the matching Row within this pTree, using a KeyingOperation
// and a ReductionOperation, inserting if necessary. if the ReductionOperation is nil,
// then the row is simply inserted
func (t *pTreeRoot) mergeRow(tempRow sif.Row, row sif.Row, keyfn sif.KeyingOperation, reducefn sif.ReductionOperation) error {
	// compute key for row
	keyBuf, err := keyfn(row)
	if err != nil {
		return err
	}

	// hash key
	hasher := xxhash.New()
	hasher.Write(keyBuf)
	hashedKey := hasher.Sum64()

	return t.doMergeRow(tempRow, row, hashedKey, reducefn)
}

// doMergeRow merges a single Row into the matching Row within this pTree, using a hashed key
// and a ReductionOperation, inserting if necessary. if the ReductionOperation is nil,
// then the row is simply inserted
func (t *pTreeRoot) doMergeRow(tempRow sif.Row, row sif.Row, hashedKey uint64, reducefn sif.ReductionOperation) error {
	// locate and load partition for the given hashed key
	partNode := t.findPartition(hashedKey)
	// make sure partition is loaded
	part, cachePartition, err := partNode.loadPartition()
	defer func() {
		if cachePartition != nil {
			cachePartition()
		}
	}()
	if err != nil {
		return err
	}
	// Once we have the correct partition, find the last row with matching key in it
	// idx, err := partNode.part.FindLastRowKey(keyBuf, hashedKey, keyfn)
	idx, err := part.FindLastKey(hashedKey)
	if idx < 0 {
		// something went wrong while locating the insert/merge position
		return err
	} else if _, ok := err.(errors.MissingKeyError); ok || reducefn == nil {
		// If the key hash doesn't exist, or we're not reducing, insert row at sorted position
		irow := row.(itypes.AccessibleRow) // access row internals
		var insertErr error
		// append if the target index is at the end of the partition, otherwise insert and shift data
		// FIXME need to copy data one column at a time...
		if (idx+1) >= part.GetNumRows() && err == nil {
			insertErr = part.(itypes.InternalBuildablePartition).AppendKeyedRowData(irow.GetData(), irow.GetMeta(), irow.GetVarData(), irow.GetSerializedVarData(), hashedKey)
		} else {
			insertErr = part.(itypes.InternalBuildablePartition).InsertKeyedRowData(irow.GetData(), irow.GetMeta(), irow.GetVarData(), irow.GetSerializedVarData(), hashedKey, idx)
		}
		// if the partition was full, try split and retry
		if _, ok = insertErr.(errors.PartitionFullError); ok {
			// first try to split the rows in a balanced way, based on the avg key
			avgKey, nextNode, err := balancedSplitNode(partNode, part, hashedKey)
			if _, ok := err.(errors.FullOfIdenticalKeysError); ok {
				// we can release the partition data back to the cache immediately, because we won't use the data during rotation
				// we have to manually cache the partition, instead of calling cachePartition,
				// because cachePartition is going to look at the wrong pTreeNode for the partID now.
				// log.Printf("Returning rotated partition %s to cache", part.ID())
				cachePartition()
				cachePartition = nil // prevent original cachePartition deferred call
				// if we fail because all the keys are the same, we'll
				// start or add to a linked list of partitions which contain
				// all the same key, and make a fresh partition to work with
				nextNode, err = partNode.rotateToCenter(avgKey)
				if err != nil {
					return err
				}
				// recurse using this correct subtree to save time
				return nextNode.doMergeRow(tempRow, row, hashedKey, reducefn)
			} else if err != nil {
				return err
			} else {
				// we don't need to return the split partition to
				// the cache, because it doesn't exist anymore
				// log.Printf("Discarding split partition %s from cache", part.ID())
				cachePartition = nil
				// recurse using this correct subtree to save time
				return nextNode.doMergeRow(tempRow, row, hashedKey, reducefn)
			}
		} else if !ok && insertErr != nil {
			return insertErr
		}
		// otherwise, insertion was successful and we're done
	} else if err != nil {
		// something else went wrong with finding the first key (currently not possible)
		return err
	} else {
		// If the actual key already exists in the partition, merge into row
		part.PopulateTempRow(tempRow, idx)
		return reducefn(tempRow, row)
	}
	return nil
}

// uses PartitionReduceable.BalancedSplit to split a single pTreeNode into
// two nodes (left & right). Fails if all the rows in the current pTreeNode
// have an identical key, in which case a split achieves nothing.
// PRECONDITION: part is already loaded and locked
func balancedSplitNode(t *pTreeNode, part itypes.ReduceablePartition, hashedKey uint64) (uint64, *pTreeNode, error) {
	avgKey, lp, rp, err := part.BalancedSplit()
	if err != nil {
		// this is where we end up if all the keys are the same
		return avgKey, nil, err
	}
	log.Printf("Splitting partition %s", t.partID)
	t.k = avgKey
	t.left = &pTreeNode{
		k:               0,
		partID:          lp.ID(),
		prev:            t.prev,
		parent:          t,
		maxRows:         t.maxRows,
		nextStageSchema: t.nextStageSchema,
		partitionCache:  t.partitionCache,
	}
	t.right = &pTreeNode{
		k:               0,
		partID:          rp.ID(),
		next:            t.next,
		parent:          t,
		maxRows:         t.maxRows,
		nextStageSchema: t.nextStageSchema,
		partitionCache:  t.partitionCache,
	}
	t.left.next = t.right
	t.right.prev = t.left
	if t.right.next != nil {
		t.right.next.prev = t.right
	}
	if t.left.prev != nil {
		t.left.prev.next = t.left
	}
	t.partID = "" // non-leaf nodes don't have partitions
	t.prev = nil  // non-leaf nodes don't have horizontal links
	t.next = nil  // non-leaf nodes don't have horizontal links
	// add left and right to front of "visited" queue
	t.partitionCache.Add(t.left.partID, lp)
	t.partitionCache.Add(t.right.partID, rp)
	// tell the caller where to go next
	if hashedKey < t.k {
		return avgKey, t.left, nil
	}
	return avgKey, t.right, nil
}

// if a balancedSplit is not possible because the rows in the
// current pTreeNode all have the same key, we instead store
// the partition in the "center" of the parent, or ourselves
func (t *pTreeNode) rotateToCenter(avgKey uint64) (*pTreeNode, error) {
	log.Printf("Rotating partition %s to center", t.partID)
	// if our parent's avgKey is identical to all of the rows in this
	// pTreeNode, then this pTreeNode belongs in the parents' center chain
	if t.parent != nil && t.parent.k == avgKey {
		if t.parent.center != nil {
			// if there's already a center chain in parent, add to it
			// TODO this doesn't account for hash collisions. True keys
			// might not be in sorted order
			t.prev = t.parent.center
			t.parent.center.next = t
		} else {
			// otherwise, start one
			t.prev = t.parent.left
			t.parent.left.next = t
		}
		// we are now the new center tail of our parent
		t.parent.center = t
		// our parent has a fresh right node to insert into
		rp := partition.CreateKeyedReduceablePartition(t.maxRows, t.nextStageSchema)
		t.parent.right = &pTreeNode{
			k:               0,
			partID:          rp.ID(),
			next:            t.next,
			prev:            t.parent.center,
			parent:          t,
			maxRows:         t.maxRows,
			nextStageSchema: t.nextStageSchema,
			partitionCache:  t.partitionCache,
		}
		t.parent.center.next = t.parent.right
		if t.parent.right.next != nil {
			t.parent.right.next.prev = t.parent.right
		}
		// add new partition to front of "visited" queue
		t.partitionCache.Add(rp.ID(), rp)
		// we know we got into this situation by adding a row with key == avgKey. These
		// rows now belong in t.parent.right, so return that as the "next" node to recurse on
		return t.parent.right, nil
	}
	// otherwise, we need to start a center chain at this node
	t.k = avgKey
	// we need to start a new center chain at this node to store data
	t.center = &pTreeNode{
		k:               0,
		partID:          t.partID,
		parent:          t,
		maxRows:         t.maxRows,
		nextStageSchema: t.nextStageSchema,
		partitionCache:  t.partitionCache,
	}
	// left and right will be fresh, empty nodes, with row keys greater than or less than avgKey
	lp := partition.CreateKeyedReduceablePartition(t.maxRows, t.nextStageSchema)
	t.left = &pTreeNode{
		k:               0,
		partID:          lp.ID(),
		prev:            t.prev,
		next:            t.center,
		parent:          t,
		maxRows:         t.maxRows,
		nextStageSchema: t.nextStageSchema,
		partitionCache:  t.partitionCache,
	}
	if t.left.prev != nil {
		t.left.prev.next = t.left
	}
	rp := partition.CreateKeyedReduceablePartition(t.maxRows, t.nextStageSchema)
	t.right = &pTreeNode{
		k:               0,
		partID:          rp.ID(),
		prev:            t.center,
		next:            t.next,
		parent:          t,
		maxRows:         t.maxRows,
		nextStageSchema: t.nextStageSchema,
		partitionCache:  t.partitionCache,
	}
	if t.right.next != nil {
		t.right.next.prev = t.right
	}
	// add new partitions to front of "visited" queue
	t.partitionCache.Add(lp.ID(), lp)
	t.partitionCache.Add(rp.ID(), rp)
	// update links for center chain
	t.center.next = t.right
	t.center.prev = t.left
	// update links for t
	t.partID = "" // non-leaf nodes don't have partitions
	t.prev = nil  // non-leaf nodes don't have horizontal links
	t.next = nil  // non-leaf nodes don't have horizontal links
	// we know we got into this situation by adding a row with key == avgKey. These
	// rows now belong in t.right, so return that as the "next" node to recurse on
	return t.right, nil
}

func (t *pTreeNode) loadPartition() (itypes.ReduceablePartition, func(), error) {
	if len(t.partID) == 0 {
		return nil, nil, fmt.Errorf("Partition tree node does not have an associated partition\n %s", util.GetTrace())
	}
	part, err := t.partitionCache.Get(t.partID)
	if err != nil {
		return nil, nil, err
	}
	return part, func() {
		log.Printf("Returning partition %s to cache", t.partID)
		// we only add the partition to the LRU cache when it's finished being
		// operated on to make sure it isn't swapped to disk while it's in use
		t.partitionCache.Add(t.partID, part)
	}, nil
}

func (t *pTreeNode) findPartition(hashedKey uint64) *pTreeNode {
	if t.left != nil && hashedKey < t.k {
		return t.left.findPartition(hashedKey)
	} else if t.right != nil && hashedKey >= t.k {
		return t.right.findPartition(hashedKey)
	} else {
		return t
	}
}

// firstNode returns the bottom-left-most node in the tree
func (t *pTreeRoot) firstNode() *pTreeNode {
	first := t
	for ; first.left != nil; first = first.left {
	}
	return first
}

func (t *pTreeNode) clearCaches() {
	t.partitionCache.Destroy()
}
