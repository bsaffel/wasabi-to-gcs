package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// ProgressReporter manages overall and per-worker progress bars via mpb.
type ProgressReporter struct {
	container    *mpb.Progress
	overallBar   *mpb.Bar
	workerBars   map[int]*mpb.Bar
	mu           sync.Mutex
	scanComplete atomic.Bool

	completedBytes atomic.Int64
	completedFiles atomic.Int64
	failedFiles    atomic.Int64
	logger         *slog.Logger
}

// ProgressStats holds current progress statistics.
type ProgressStats struct {
	CompletedFiles int64
	CompletedBytes int64
	FailedFiles    int64
}

// NewProgressReporter creates a progress reporter with an overall bar.
func NewProgressReporter(logger *slog.Logger) *ProgressReporter {
	container := mpb.New(
		mpb.WithOutput(os.Stderr),
		mpb.WithRefreshRate(150*time.Millisecond),
	)

	pr := &ProgressReporter{
		container:  container,
		workerBars: make(map[int]*mpb.Bar),
		logger:     logger,
	}

	// Create overall bar in indeterminate mode (total=0 until scan provides data)
	pr.overallBar = container.New(0,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		mpb.PrependDecorators(
			decor.Name("Overall: "),
			decor.CountersKibiByte("% .2f / % .2f"),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
			decor.AverageSpeed(decor.SizeB1024(0), "% .1f", decor.WCSyncSpace),
			decor.AverageETA(decor.ET_STYLE_MMSS, decor.WCSyncSpace),
		),
	)

	return pr
}

// UpdateScanTotals is called by the lister after each page to update the
// overall bar's denominator.
func (pr *ProgressReporter) UpdateScanTotals(totalObjects, totalBytes, skippedObjects, skippedBytes int64) {
	pr.overallBar.SetTotal(totalBytes, false)

	// Pre-load completed bytes from previously skipped objects so the bar
	// starts at the correct position on resume runs.
	alreadyCompleted := skippedBytes
	currentCompleted := pr.completedBytes.Load()
	if alreadyCompleted > currentCompleted {
		diff := alreadyCompleted - currentCompleted
		pr.overallBar.IncrBy(int(diff))
		pr.completedBytes.Store(alreadyCompleted)
	}
}

// ScanComplete marks the scan as finished. The overall bar's total is now final.
func (pr *ProgressReporter) ScanComplete() {
	pr.scanComplete.Store(true)
}

// StartTransfer creates a per-worker progress bar for the given transfer.
func (pr *ProgressReporter) StartTransfer(workerID int, job ObjectJob) {
	displayName := job.Key
	if len(displayName) > 40 {
		displayName = "..." + displayName[len(displayName)-37:]
	}

	bar := pr.container.New(job.Size,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("  W%02d ", workerID)),
			decor.Name(displayName, decor.WCSyncSpaceR),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
			decor.CountersKibiByte("% .1f / % .1f"),
			decor.AverageSpeed(decor.SizeB1024(0), " % .1f", decor.WCSyncSpace),
		),
	)

	pr.mu.Lock()
	pr.workerBars[workerID] = bar
	pr.mu.Unlock()
}

// CompleteTransfer marks a per-worker bar as complete and increments the overall bar.
func (pr *ProgressReporter) CompleteTransfer(workerID int, job ObjectJob) {
	pr.mu.Lock()
	bar, exists := pr.workerBars[workerID]
	if exists {
		delete(pr.workerBars, workerID)
	}
	pr.mu.Unlock()

	if exists {
		bar.SetTotal(job.Size, true)
	}

	pr.overallBar.IncrBy(int(job.Size))
	pr.completedBytes.Add(job.Size)
	pr.completedFiles.Add(1)
}

// FailTransfer aborts a per-worker bar and increments the failure counter.
func (pr *ProgressReporter) FailTransfer(workerID int, job ObjectJob, err error) {
	pr.mu.Lock()
	bar, exists := pr.workerBars[workerID]
	if exists {
		delete(pr.workerBars, workerID)
	}
	pr.mu.Unlock()

	if exists {
		bar.Abort(true)
	}

	pr.failedFiles.Add(1)
}

// WrapReader returns a reader that updates the per-worker progress bar.
func (pr *ProgressReporter) WrapReader(r io.Reader, workerID int) io.Reader {
	pr.mu.Lock()
	bar, exists := pr.workerBars[workerID]
	pr.mu.Unlock()

	if !exists {
		return r
	}

	return bar.ProxyReader(r)
}

// ReportError logs an error safely above the progress bars.
func (pr *ProgressReporter) ReportError(key string, err error) {
	fmt.Fprintf(os.Stderr, "\r  ERROR %s: %v\n", key, err)
	pr.logger.Warn("transfer error",
		slog.String("key", key),
		slog.Any("error", err),
	)
}

// Wait blocks until all progress bars complete rendering.
func (pr *ProgressReporter) Wait() {
	pr.container.Wait()
}

// Stats returns current progress statistics.
func (pr *ProgressReporter) Stats() ProgressStats {
	return ProgressStats{
		CompletedFiles: pr.completedFiles.Load(),
		CompletedBytes: pr.completedBytes.Load(),
		FailedFiles:    pr.failedFiles.Load(),
	}
}
