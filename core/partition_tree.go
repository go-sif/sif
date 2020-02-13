package core

import (
	"io/ioutil"
	"log"

	xxhash "github.com/cespare/xxhash"
	errors "github.com/go-sif/sif/errors"
	lru "github.com/hashicorp/golang-lru"
)

// pTreeNode is a node of a tree that builds, sorts and organizes keyed partitions
type pTreeNode struct {
	k                       uint64
	left                    *pTreeNode
	right                   *pTreeNode
	part                    *Partition
	nextStageWidestSchema   *Schema
	nextStageIncomingSchema *Schema
	diskPath                string
	prev                    *pTreeNode // btree-like link between leaves
	next                    *pTreeNode // btree-like link between leaves
	parent                  *pTreeNode
	lruCache                *lru.Cache // TODO replace with a queue that is less likely to evict frequently-used entries
}

// pTreeRoot is an alias for pTreeNode representing the root node of a pTree
type pTreeRoot = pTreeNode

// createPTreeNode creates a new pTree with a limit on Partition size and a given shared Schema
func createPTreeNode(conf *PlanExecutorConfig, maxRows int, nextStageWidestSchema *Schema, nextStageIncomingSchema *Schema) *pTreeNode {
	cache, err := lru.NewWithEvict(conf.inMemoryPartitions, func(key interface{}, value interface{}) {
		partID, ok := key.(string)
		if !ok {
			log.Fatalf("Unable to sync partition %s to disk due to key casting issue", key)
		}
		part, ok := value.(*pTreeNode)
		if !ok {
			log.Fatalf("Unable to sync partition %s to disk due to value casting issue", value)
		}
		onPartitionEvict(conf.tempFilePath, partID, part)
	})
	if err != nil {
		log.Fatalf("Unable to initialize lru cache for partitions: %e", err)
	}
	part := CreatePartition(maxRows, nextStageWidestSchema, nextStageIncomingSchema)
	part.KeyRows(nil)
	cache.Add(part.ID(), part)
	return &pTreeNode{
		k:                       0,
		part:                    part,
		lruCache:                cache,
		nextStageWidestSchema:   nextStageWidestSchema,
		nextStageIncomingSchema: nextStageIncomingSchema,
	}
}

// mergePartition merges the Rows from a given Partition into matching Rows within this pTree, using a KeyingOperation and a ReductionOperation, inserting if necessary
func (t *pTreeRoot) mergePartition(part *Partition, keyfn KeyingOperation, reducefn ReductionOperation) error {
	for i := 0; i < part.GetNumRows(); i++ {
		row := part.buildRow(i)
		if err := t.mergeRow(row, keyfn, reducefn); err != nil {
			return err
		}
	}
	return nil
}

// mergeRow merges a single Row into the matching Row within this pTree, using a KeyingOperation and a ReductionOperation, inserting if necessary
func (t *pTreeRoot) mergeRow(row *Row, keyfn KeyingOperation, reducefn ReductionOperation) error {
	// compute key for row
	keyBuf, err := keyfn(row)
	if err != nil {
		return err
	}

	// hash key
	hasher := xxhash.New()
	hasher.Write(keyBuf)
	hashedKey := hasher.Sum64()

	// locate partition for the given hashed key
	partNode := t.findPartition(hashedKey)
	// make sure partition is loaded
	_, err = partNode.loadPartition()
	if err != nil {
		return err
	}
	// Once we have the correct partition, find the first row with matching key in it
	idx, err := partNode.part.findFirstRowKey(keyBuf, hashedKey, keyfn)
	if idx < 0 {
		// something went wrong while locating the insert/merge position
		return err
	} else if _, ok := err.(errors.MissingKeyError); ok {
		// If the key hash doesn't exist, insert row at sorted position
		insertErr := partNode.part.insertKeyedRowData(row.data, row.meta, row.varData, row.serializedVarData, hashedKey, idx)
		// if the partition was full, split and retry
		if _, ok = insertErr.(errors.PartitionFullError); ok {
			avgKey, lp, rp, err := partNode.part.balancedSplit()
			if err != nil {
				return err
			}
			partNode.k = avgKey
			partNode.left = &pTreeNode{
				k:        0,
				part:     lp,
				prev:     partNode.prev,
				parent:   partNode,
				lruCache: t.lruCache,
			}
			partNode.right = &pTreeNode{
				k:        0,
				part:     rp,
				next:     partNode.next,
				parent:   partNode,
				lruCache: t.lruCache,
			}
			partNode.left.next = partNode.right
			partNode.right.prev = partNode.left
			partNode.part = nil // non-leaf nodes don't have partitions
			partNode.prev = nil // non-leaf nodes don't have horizontal links
			partNode.next = nil // non-leaf nodes don't have horizontal links
			// add left and right to front of "visited" queue
			t.lruCache.Add(partNode.left.part.ID(), partNode.left)
			t.lruCache.Add(partNode.right.part.ID(), partNode.right)
			// recurse using this new subtree to save time
			return partNode.mergeRow(row, keyfn, reducefn)
		} else if !ok && insertErr != nil {
			return insertErr
		}
		// otherwise, insertion was successful and we're done
	} else if err != nil {
		// something else went wrong with finding the first key (currently not possible)
		return err
	} else {
		// If the actual key already exists in the partition, merge into row
		target := &Row{
			meta:              partNode.part.getRowMeta(idx),
			data:              partNode.part.getRowData(idx),
			varData:           partNode.part.getVarRowData(idx),
			serializedVarData: partNode.part.getSerializedVarRowData(idx),
			schema:            partNode.part.currentSchema,
		}
		return reducefn(target, row)
	}
	return nil
}

func (t *pTreeNode) loadPartition() (*Partition, error) {
	if t.part == nil {
		buff, err := ioutil.ReadFile(t.diskPath)
		if err != nil {
			return nil, err
		}
		part, err := partitionFromBytes(buff, t.nextStageWidestSchema, t.nextStageIncomingSchema)
		if err != nil {
			return nil, err
		}
		t.part = part
	}
	// move this node to the front of the "visited" queue
	t.lruCache.Add(t.part.ID(), t)
	return t.part, nil
}

func onPartitionEvict(tempDir string, partID string, t *pTreeNode) {
	if t.part != nil {
		buff, err := t.part.toBytes()
		if err != nil {
			log.Fatalf("Unable to convert partition to buffer %s", err)
		}
		tmpfile, err := ioutil.TempFile(tempDir, t.part.id)
		defer tmpfile.Close()
		if err != nil {
			log.Fatalf("Unable to create temporary file for partition %s", err)
		}
		if _, err := tmpfile.Write(buff); err != nil {
			log.Fatalf("Unable to write partition to disk %s", err)
		}
		if err := tmpfile.Close(); err != nil {
			log.Fatalf("Unable to write partition to disk %s", err)
		}
		// everything worked, so clean up node
		t.diskPath = tmpfile.Name()
		t.part = nil
	}
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
