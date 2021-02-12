package dataframe

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/errors"
	"github.com/go-sif/sif/internal/partition"
	"github.com/go-sif/sif/internal/pcache"
	pb "github.com/go-sif/sif/internal/rpc"
	"github.com/go-sif/sif/internal/stats"
	itypes "github.com/go-sif/sif/internal/types"
	uuid "github.com/gofrs/uuid"
	"github.com/hashicorp/go-multierror"
)

// planExecutorImpl executes a plan, on a master or on workers
type planExecutorImpl struct {
	id   string
	plan itypes.Plan
	conf *itypes.PlanExecutorConfig
	done context.CancelFunc

	nextStage            int
	partitionLoaders     []sif.PartitionLoader
	partitionLoadersLock sync.Mutex
	assignedBucket       uint64
	shuffleIterators     map[uint64]sif.SerializedPartitionIterator // used to serve partitions from the shuffleTree
	shuffleIteratorsLock sync.Mutex
	shuffleLock          sync.Mutex
	shuffleReady         sif.PartitionIndex
	accumulateReady      sif.Accumulator
	statsTracker         *stats.RunStatistics
	pCache               sif.PartitionCache
}

// CreatePlanExecutor is a factory for planExecutors
func CreatePlanExecutor(ctx context.Context, plan itypes.Plan, conf *itypes.PlanExecutorConfig, statsTracker *stats.RunStatistics, isCoordinator bool) itypes.PlanExecutor {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("failed to generate UUID: %v", err)
	}
	executor := &planExecutorImpl{
		id:               id.String(),
		plan:             plan,
		conf:             conf,
		done:             func() {},
		partitionLoaders: make([]sif.PartitionLoader, 0),
		shuffleIterators: make(map[uint64]sif.SerializedPartitionIterator),
		statsTracker:     statsTracker,
	}
	// create partition cache
	if conf.CacheMemoryHighWatermark == 0 {
		conf.CacheMemoryHighWatermark = 512 * 1024 * 1024 // 512MiB
	}
	if conf.CacheMemoryInitialSize == 0 {
		conf.CacheMemoryInitialSize = 32 * 1024 // pick a meaninglessly large number, as we'll use the memory high watermark to scale down
	}
	executor.pCache = pcache.NewLRU(&pcache.LRUConfig{
		InitialSize: conf.CacheMemoryInitialSize,
		DiskPath:    conf.TempFilePath,
		Compressor:  conf.PartitionCompressor,
	})
	if !isCoordinator {
		monitorCtx, canceller := context.WithCancel(ctx)
		executor.done = canceller
		go executor.monitorMemoryUsage(monitorCtx)
	}
	return executor
}

// ID returns the configuration for this PlanExecutor
func (pe *planExecutorImpl) ID() string {
	return pe.id
}

// GetConf returns the configuration for this PlanExecutor
func (pe *planExecutorImpl) GetConf() *itypes.PlanExecutorConfig {
	return pe.conf
}

func (pe *planExecutorImpl) Stop() {
	pe.done()
	pe.shuffleLock.Lock()
	defer pe.shuffleLock.Unlock()
	if pe.pCache != nil {
		pe.pCache.Destroy()
	}
}

// HasNextStage forms an iterator for planExecutor Stages
func (pe *planExecutorImpl) HasNextStage() bool {
	return pe.nextStage < (pe.plan.Size())
}

// NextStage forms an iterator for planExecutor Stages
func (pe *planExecutorImpl) GetNextStage() itypes.Stage {
	if pe.nextStage >= pe.plan.Size() {
		return nil
	}
	s := pe.plan.GetStage(pe.nextStage)
	pe.nextStage++
	if pe.conf.Streaming {
		pe.nextStage = pe.nextStage % pe.plan.Size()
	}
	return s
}

func (pe *planExecutorImpl) GetNumStages() int {
	return pe.plan.Size()
}

// peekNextStage returns the next stage without advancing the iterator, or nil if there isn't one
func (pe *planExecutorImpl) peekNextStage() itypes.Stage {
	if pe.HasNextStage() {
		return pe.plan.GetStage(pe.nextStage)
	}
	return nil
}

// GetCurrentStage returns the current stage without advancing the iterator, or nil if the iterator has never been advanced
func (pe *planExecutorImpl) GetCurrentStage() itypes.Stage {
	if pe.nextStage == 0 {
		return pe.plan.GetStage(pe.nextStage)
	}
	return pe.plan.GetStage(pe.nextStage - 1)
}

// onFirstStage returns true iff we're past the first stage
func (pe *planExecutorImpl) onFirstStage() bool {
	return pe.nextStage == 1
}

// hasPartitionLoaders returns true iff we have assigned PartitionLoaders
func (pe *planExecutorImpl) hasPartitionLoaders() bool {
	pe.partitionLoadersLock.Lock()
	defer pe.partitionLoadersLock.Unlock()
	return len(pe.partitionLoaders) > 0
}

// IsShuffleReady returns true iff a shuffle has been prepared in this planExecutor's shuffle trees
func (pe *planExecutorImpl) IsShuffleReady() bool {
	return pe.shuffleReady != nil
}

// IsAccumulatorReady returns true iff an Accumulator has been prepared in this planExecutor
func (pe *planExecutorImpl) IsAccumulatorReady() bool {
	return pe.accumulateReady != nil
}

// AssignPartitionLoader assigns a serialized PartitionLoader to this executor
func (pe *planExecutorImpl) AssignPartitionLoader(sLoader []byte) error {
	loader, err := pe.plan.Source().DeserializeLoader(sLoader[0:])
	if err != nil {
		return err
	}
	pe.partitionLoadersLock.Lock()
	defer pe.partitionLoadersLock.Unlock()
	pe.partitionLoaders = append(pe.partitionLoaders, loader)
	return nil
}

func (pe *planExecutorImpl) InitStageContext(sctx sif.StageContext, stage itypes.Stage) error {
	if pe.plan.Size() == 0 {
		return fmt.Errorf("Plan has no stages")
	}
	// populate sctx with useful variables for next stage (or the current stage if there isn't a next stage)
	nextStage := pe.peekNextStage()
	if nextStage == nil {
		nextStage = pe.GetCurrentStage()
	}
	nextStageWidestInitialSchema := nextStage.WidestInitialSchema()
	sctx.SetNextStageWidestInitialSchema(nextStageWidestInitialSchema)
	sctx.SetPartitionCache(pe.pCache)
	// set up partition loaders for this stage, if it's the first stage
	if pe.onFirstStage() && pe.hasPartitionLoaders() {
		it := createPartitionLoaderIterator(pe.partitionLoaders, pe.plan.Parser(), pe.plan.GetStage(0).WidestInitialSchema())
		// in the non-streaming context, the partition loader won't offer more partitions
		// after we're done iterating through it, so we can safely get rid of it.
		if !pe.conf.Streaming {
			pe.partitionLoadersLock.Lock()
			pe.partitionLoaders = make([]sif.PartitionLoader, 0) // clear partition loaders
			pe.partitionLoadersLock.Unlock()
		}
		if err := sctx.SetIncomingPartitionIterator(it); err != nil {
			return err
		}
	} else if pe.shuffleReady != nil {
		pe.shuffleLock.Lock()
		defer pe.shuffleLock.Unlock()
		bpi, ok := pe.shuffleReady.(sif.BucketedPartitionIndex)
		if !ok {
			return fmt.Errorf("PartitionIndex available as source for next Stage is not a BucketedPartitionIndex")
		}
		// we read from our assigned bucket, because that's the bucket which received partitions during
		// the previous shuffle, and should be the only one with data in it
		idx := bpi.GetBucket(pe.assignedBucket)
		// it := createPreloadingPartitionIterator(idx.GetPartitionIterator(true), 3)
		it := idx.GetPartitionIterator(true)
		if err := sctx.SetIncomingPartitionIterator(it); err != nil {
			return err
		}
		pe.shuffleReady = nil
		pe.accumulateReady = nil
	}
	// run init for all tasks in this stage
	err := stage.WorkerInitialize(sctx)
	if err != nil {
		return err
	}
	return nil
}

// TransformPartitions applies a PartitionTransform to all partitions from this Plan's current source.
// Partition source can either be partition loaders or the PartitionIndex from a previous Stage.
func (pe *planExecutorImpl) TransformPartitions(sctx sif.StageContext, fn itypes.PartitionTransform, onRowError func(error) error) error {
	parts := sctx.IncomingPartitionIterator()
	currentStageID := pe.GetCurrentStage().ID()
	for parts.HasNextPartition() {
		part, unlockPartition, err := parts.NextPartition()
		if _, ok := err.(errors.NoMorePartitionsError); ok {
			if unlockPartition != nil {
				unlockPartition()
			}
			// It's ok for a data source to throw this once, as HasNextPartition is just a hint
			break
		} else if err != nil {
			if unlockPartition != nil {
				unlockPartition()
			}
			return err
		}
		pe.statsTracker.StartPartition()
		opart := part.(sif.OperablePartition)
		_, err = fn(sctx, opart) // apply transformation
		if err := onRowError(err); err != nil {
			if unlockPartition != nil {
				unlockPartition()
			}
			return err
		}
		// we're done with the source partition now
		if unlockPartition != nil {
			unlockPartition()
		}
		pe.statsTracker.EndPartition(currentStageID, part.GetNumRows())
	}
	// Are we ready to shuffle or accumulate?
	pe.shuffleLock.Lock()
	defer pe.shuffleLock.Unlock()
	if sctx.PartitionIndex() != nil && sctx.PartitionIndex().NumPartitions() > 0 {
		pe.shuffleReady = sctx.PartitionIndex()
	} else if sctx.Accumulator() != nil {
		pe.accumulateReady = sctx.Accumulator()
	}
	return nil
}

// AssignShuffleBucket assigns a ShuffleBucket to this executor
func (pe *planExecutorImpl) AssignShuffleBucket(assignedBucket uint64) {
	pe.assignedBucket = assignedBucket
}

// GetShufflePartitionIterator serves up an iterator for partitions to shuffle
func (pe *planExecutorImpl) GetShufflePartitionIterator(bucket uint64) (sif.SerializedPartitionIterator, error) {
	var idx sif.PartitionIndex
	pe.shuffleLock.Lock()
	defer pe.shuffleLock.Unlock()
	if pe.shuffleReady == nil {
		return nil, fmt.Errorf("No PartitionIndex available for shuffle")
	}
	// check if our index is a BucketedPartitionIndex
	bpi, ok := pe.shuffleReady.(sif.BucketedPartitionIndex)
	if ok {
		// PartitionIndex available for shuffle is a BucketedPartitionIndex, so we must be reducing
		idx = bpi.GetBucket(bucket)
		if idx == nil {
			return nil, fmt.Errorf("No PartitionIterator available for bucket %d", bucket)
		}
	} else {
		// otherwise, we're collecting (there's just a single index with no buckets)
		idx = pe.shuffleReady
	}
	return idx.GetSerializedPartitionIterator(true), nil
}

// GetAccumulator returns this plan executor's Accumulator, if any
func (pe *planExecutorImpl) GetAccumulator() (sif.Accumulator, error) {
	pe.shuffleLock.Lock()
	defer pe.shuffleLock.Unlock()
	if pe.accumulateReady == nil {
		return nil, fmt.Errorf("No Accumulator available")
	}
	return pe.accumulateReady, nil
}

// ShufflePartitionData receives a Partition that belongs on this worker and merges it into the local shuffle tree
func (pe *planExecutorImpl) ShufflePartitionData(wg *sync.WaitGroup, partMerger chan<- sif.ReduceablePartition, asyncErrors chan<- error, mpart *pb.MPartitionMeta, dataStream pb.PartitionsService_TransferPartitionDataClient) {
	defer wg.Done()
	// if we're the last stage, then the incoming data should match our outgoing schema
	// but if there is a following stage, the data should match that stage.
	var incomingDataSchema sif.Schema
	if pe.HasNextStage() {
		incomingDataSchema = pe.peekNextStage().WidestInitialSchema()
	} else {
		incomingDataSchema = pe.GetCurrentStage().OutgoingSchema()
	}
	part, err := partition.FromStreamedData(dataStream, mpart, incomingDataSchema, pe.conf.PartitionCompressor)
	if err != nil {
		asyncErrors <- err
		return
	}
	rpart, ok := part.(sif.ReduceablePartition)
	if !ok {
		asyncErrors <- fmt.Errorf("Deserialized partition is not Reduceable")
		return
	}
	partMerger <- rpart
}

func (pe *planExecutorImpl) MergeShuffledPartitions(sctx sif.StageContext, wg *sync.WaitGroup, partMerger <-chan sif.ReduceablePartition, asyncErrors chan<- error) {
	defer wg.Done()
	// get our destination PartitionIndex
	if pe.shuffleReady == nil {
		asyncErrors <- fmt.Errorf("No PartitionIndex available for receiving shuffled Partitions")
	}
	bpi, ok := pe.shuffleReady.(sif.BucketedPartitionIndex)
	if !ok {
		asyncErrors <- fmt.Errorf("PartitionIndex available for receiving shuffled Partitions is not a BucketedPartitionIndex")
	}
	destinationIndex := bpi.GetBucket(pe.assignedBucket)
	// get our key and reduction functions from stage context
	rfn := sctx.ReductionOperation()
	kfn := sctx.KeyingOperation()
	// loop on the incoming partition channel
	for part := range partMerger {
		pe.shuffleLock.Lock()
		// merge partition into appropriate shuffle tree
		var multierr *multierror.Error
		tempRow := partition.CreateTempRow()
		areEqualSchemas := destinationIndex.GetNextStageSchema().Equals(part.GetSchema())
		if areEqualSchemas != nil {
			asyncErrors <- fmt.Errorf("Incoming shuffled partition schema does not match expected schema")
			pe.shuffleLock.Unlock()
			break
		}
		part.ForEachRow(func(row sif.Row) error {
			err := destinationIndex.MergeRow(tempRow, row, kfn, rfn)
			if err != nil {
				multierr = multierror.Append(multierr, err)
			}
			return nil
		})
		errors := multierr.ErrorOrNil()
		if errors != nil {
			asyncErrors <- errors
			pe.shuffleLock.Unlock()
			break
		}
		pe.shuffleLock.Unlock()
	}
}

func (pe *planExecutorImpl) monitorMemoryUsage(ctx context.Context) {
	var m runtime.MemStats
	usage := make([]uint64, 4)
	usageHead := 0
	var avg uint64
	for range time.Tick(250 * time.Millisecond) {
		select {
		case <-ctx.Done():
			log.Printf("Finished monitoring memory usage")
			return
		default:
			// record current memory usage
			runtime.ReadMemStats(&m)
			usage[usageHead] = m.Alloc / uint64(len(usage))
			usageHead = (usageHead + 1) % len(usage)
			// compute rolling average
			avg = 0
			for _, v := range usage {
				if v == 0 {
					// we haven't captured enough data yet
					continue
				}
				avg += v
			}
			// check watermarks
			if avg > pe.conf.CacheMemoryHighWatermark && pe.shuffleReady != nil {
				shrinkFac := (float64(pe.conf.CacheMemoryHighWatermark) / float64(avg)) - 0.05
				// pe.shuffleIndexesLock.Lock() // we won't be modifying the map at this point
				resized := true
				pe.shuffleReady.ResizeCache(shrinkFac)
				if resized {
					log.Printf("Memory usage %d is greater than high watermark %d. Shrunk partition caches by %f", avg, pe.conf.CacheMemoryHighWatermark, shrinkFac)
				}
				// pe.shuffleIndexesLock.Unlock()
			}
		}
	}
}
