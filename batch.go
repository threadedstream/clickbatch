package batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

const (
	defaultShutdownTimeout = 10 * time.Second
	backoffInterval        = 2 * time.Second

	reInsertAttempts = 1
	bucketCount      = 10
)

// StorageInsertCallback is a callback that's called upon flush into target storage
type StorageInsertCallback func(ctx context.Context, buf []any) error

// BatchInserter is a default batch inserter implementation
type BatchInserter struct {
	buf                   [bucketCount][]any
	locks                 [bucketCount]sync.Mutex
	bucketPtr             atomic.Int32
	once                  sync.Once
	storageInsertCallback StorageInsertCallback
	flushTicker           *time.Ticker
	reInsertAttempts      int
	minAcceptableRowsNum  int
	bufCap                int
	flushInterval         time.Duration
	logger                *zap.Logger
}

// NewBatchInserter returns a batch manager instance
func NewBatchInserter(
	ctx context.Context,
	flushInterval time.Duration,
	storageInsertCallback StorageInsertCallback,
	bufCap int,
	minAcceptableRowsNum int,
	logger *zap.Logger,
) *BatchInserter {

	// round up to the nearest multiple of 10
	roundedCap := bufCap
	if bufCap%bucketCount != 0 {
		roundedCap = (10 - bufCap%10) + bufCap
	}

	cbm := &BatchInserter{
		flushInterval:         flushInterval,
		bufCap:                roundedCap,
		reInsertAttempts:      reInsertAttempts,
		minAcceptableRowsNum:  minAcceptableRowsNum,
		storageInsertCallback: storageInsertCallback,
		logger:                logger,
	}

	// to start from zero
	cbm.bucketPtr.Store(-1)

	cbm.allocBuf()

	go cbm.launchFlusher(ctx)

	return cbm
}

// Append performs append to internal buffer
func (bm *BatchInserter) Append(items ...any) {
	idx := bm.bucketPtr.Add(1) % bucketCount

	bm.locks[idx].Lock()
	bm.buf[idx] = append(bm.buf[idx], items...)
	bm.locks[idx].Unlock()
}

func (bm *BatchInserter) allocBuf() {
	bufCapPart := bm.bufCap / bucketCount
	for i := 0; i < bucketCount; i++ {
		bm.locks[i].Lock()
		bm.buf[i] = make([]any, 0, bufCapPart)
		bm.locks[i].Unlock()
	}
}

func (bm *BatchInserter) bufLen() int {
	count := 0
	for i := 0; i < bucketCount; i++ {
		bm.locks[i].Lock()
		count += len(bm.buf[i])
		bm.locks[i].Unlock()
	}
	return count
}

func (bm *BatchInserter) loadBuf() []any {
	resultBuf := make([]any, 0, bm.bufCap)
	for i := 0; i < bucketCount; i++ {
		bm.locks[i].Lock()
		resultBuf = append(resultBuf, bm.buf[i]...)
		bm.locks[i].Unlock()
	}
	return resultBuf
}

func (bm *BatchInserter) launchFlusher(ctx context.Context) {
	bm.flushTicker = time.NewTicker(bm.flushInterval)

	for {
		select {
		case <-ctx.Done():
			bm.close()
			return
		case <-bm.flushTicker.C:
			err := bm.flush(ctx, false)
			bm.handleReset(err)
		}
	}
}

func (bm *BatchInserter) handleReset(err error) {
	// default value is bm.flushTimeout
	newTickerInterval := bm.flushInterval

	if err != nil {
		if bm.reInsertAttempts > 0 {
			newTickerInterval = backoffInterval
		}
		bm.reInsertAttempts--
	}

	bm.flushTicker.Reset(newTickerInterval)
	bm.logAndFireMetric(err, bm.reInsertAttempts <= 0)
}

func (bm *BatchInserter) logAndFireMetric(err error, attemptsDrained bool) {
	if err != nil {
		bm.logger.Error("failed to insert into storage", zap.Error(err))
	}

	status := statusOk
	if attemptsDrained {
		status = statusFailure
	}

	batchInsertSuccess.WithLabelValues(status).Set(float64(time.Now().UnixNano()))
}

func (bm *BatchInserter) flush(ctx context.Context, urgent bool) (err error) {
	if bm.bufLen() < bm.minAcceptableRowsNum && !urgent {
		// insufficient number of entries to batch insert into storage, try next time
		return nil
	}

	sizeToInsert := float64(len(bm.buf))

	// invalidate previous buffer and allocate a new one in case of no failure or drained attempts
	defer func() {
		if err == nil {
			batchFlushSize.Set(sizeToInsert)
		}

		if err == nil || bm.reInsertAttempts <= 0 {
			bm.reInsertAttempts = reInsertAttempts
			bm.allocBuf()
		}
	}()

	return bm.storageInsertCallback(ctx, bm.loadBuf())
}

func (bm *BatchInserter) close() {
	// prevent calling close multiple times
	bm.once.Do(func() {
		bm.logger.Info("closing batch manager...")

		withTimeoutCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()

		if err := bm.flush(withTimeoutCtx, true); err != nil {
			bm.logger.Error("failed to perform final flush", zap.Error(err))
		}
	})
}
