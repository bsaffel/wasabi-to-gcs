package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/aws/smithy-go"
	"golang.org/x/sync/errgroup"
)

// WorkerPool processes transfer jobs concurrently using a buffered channel semaphore.
type WorkerPool struct {
	sem        chan struct{}
	maxWorkers int

	jobs     <-chan ObjectJob
	state    *StateManager
	progress *ProgressReporter
	engine   *TransferEngine
	logger   *slog.Logger

	activeWorkers atomic.Int64
	completedJobs atomic.Int64
	failedJobs    atomic.Int64
	totalBytes    atomic.Int64
	nextWorkerID  atomic.Int64
}

// NewWorkerPool creates a WorkerPool with the given configuration.
func NewWorkerPool(workers int, jobs <-chan ObjectJob, engine *TransferEngine, state *StateManager, progress *ProgressReporter, logger *slog.Logger) *WorkerPool {
	return &WorkerPool{
		sem:        make(chan struct{}, workers),
		maxWorkers: workers,
		jobs:       jobs,
		engine:     engine,
		state:      state,
		progress:   progress,
		logger:     logger,
	}
}

// Run processes jobs until the channel is closed or a fatal error occurs.
func (wp *WorkerPool) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	for job := range wp.jobs {
		job := job // Capture for goroutine

		// Acquire semaphore slot (blocks if at capacity)
		select {
		case wp.sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		wp.activeWorkers.Add(1)
		workerID := int(wp.nextWorkerID.Add(1))

		g.Go(func() error {
			defer func() {
				<-wp.sem // Release semaphore slot
				wp.activeWorkers.Add(-1)
			}()

			return wp.processJob(ctx, workerID, job)
		})
	}

	return g.Wait()
}

func (wp *WorkerPool) processJob(ctx context.Context, workerID int, job ObjectJob) error {
	logger := wp.logger.With(
		slog.String("key", job.Key),
		slog.Int64("size", job.Size),
		slog.Int("worker_id", workerID),
	)

	wp.progress.StartTransfer(workerID, job)

	start := time.Now()
	err := wp.engine.Transfer(ctx, workerID, job)
	elapsed := time.Since(start)

	if err != nil {
		wp.failedJobs.Add(1)
		wp.progress.FailTransfer(workerID, job, err)

		if isFatalError(err) {
			return err // Abort entire pool
		}

		logger.Warn("transfer failed",
			slog.Any("error", err),
			slog.Duration("elapsed", elapsed),
		)
		wp.progress.ReportError(job.Key, err)
		return nil // Continue processing other jobs
	}

	wp.completedJobs.Add(1)
	wp.totalBytes.Add(job.Size)
	wp.progress.CompleteTransfer(workerID, job)

	logger.Info("transfer complete", slog.Duration("elapsed", elapsed))

	return nil
}

// isFatalError determines if an error should abort the entire migration.
func isFatalError(err error) bool {
	if errors.Is(err, ErrBucketNotFound) {
		return true
	}
	if errors.Is(err, ErrInvalidCredentials) {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return true
		case "NoSuchBucket":
			return true
		}
	}

	return false
}
