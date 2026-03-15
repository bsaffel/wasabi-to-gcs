package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ObjectJob represents a single object to be transferred.
type ObjectJob struct {
	Key  string
	Size int64
	ETag string
}

// Lister scans the source bucket and streams object jobs to workers.
type Lister struct {
	client   *s3.Client
	bucket   string
	prefix   string
	state    *StateManager
	progress *ProgressReporter
	logger   *slog.Logger

	// Scan totals — updated atomically per page
	totalObjects   atomic.Int64
	totalBytes     atomic.Int64
	skippedObjects atomic.Int64
	skippedBytes   atomic.Int64
}

// ScanAndStream performs a single listing pass that both counts totals AND
// feeds the job channel. Workers start receiving jobs immediately from page 1
// while the scan continues in the background.
func (l *Lister) ScanAndStream(ctx context.Context, jobs chan<- ObjectJob) error {
	paginator := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket: &l.bucket,
		Prefix: &l.prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		// First pass: count totals and collect jobs to send.
		// This lets us update the overall bar's denominator BEFORE
		// sending jobs to the bounded channel (which blocks when full).
		var pageObjects, pageBytes int64
		var pageSkippedObjects, pageSkippedBytes int64
		var pageJobs []ObjectJob

		for _, obj := range page.Contents {
			// Skip zero-size objects (folders)
			if obj.Size == nil || *obj.Size == 0 {
				continue
			}

			pageObjects++
			pageBytes += *obj.Size

			// Skip already completed (fast in-memory check)
			if l.state.IsCompleted(*obj.Key) {
				pageSkippedObjects++
				pageSkippedBytes += *obj.Size
				continue
			}

			pageJobs = append(pageJobs, ObjectJob{Key: *obj.Key, Size: *obj.Size, ETag: *obj.ETag})
		}

		// Update running totals before sending jobs
		l.totalObjects.Add(pageObjects)
		l.totalBytes.Add(pageBytes)
		l.skippedObjects.Add(pageSkippedObjects)
		l.skippedBytes.Add(pageSkippedBytes)

		// Notify progress reporter so overall bar updates its denominator
		l.progress.UpdateScanTotals(
			l.totalObjects.Load(),
			l.totalBytes.Load(),
			l.skippedObjects.Load(),
			l.skippedBytes.Load(),
		)

		// Second pass: send jobs (may block on bounded channel)
		for _, job := range pageJobs {
			select {
			case jobs <- job:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Scan complete — save manifest for future resume runs (only if objects exist)
	if l.totalObjects.Load() > 0 {
		manifest := &MigrationManifest{
			TotalObjects: l.totalObjects.Load(),
			TotalBytes:   l.totalBytes.Load(),
			ScannedAt:    time.Now(),
			SourceBucket: l.bucket,
			Prefix:       l.prefix,
		}
		if err := l.state.SaveManifest(manifest); err != nil {
			l.logger.Warn("failed to save manifest", slog.Any("error", err))
		}
	}

	l.progress.ScanComplete()

	l.logger.Info("scan complete",
		slog.Int64("total_objects", l.totalObjects.Load()),
		slog.Int64("total_bytes", l.totalBytes.Load()),
		slog.Int64("skipped_objects", l.skippedObjects.Load()),
	)

	return nil
}
