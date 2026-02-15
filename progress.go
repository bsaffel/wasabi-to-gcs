package main

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// ProgressReporter manages per-file and overall progress bars via mpb.
// Each file transfer gets its own bar that persists at 100% after completion,
// providing a visual log of all transferred files with their transfer speeds.
type ProgressReporter struct {
	container  *mpb.Progress
	overallBar *mpb.Bar

	currentBars []*mpb.Bar  // indexed by workerID, holds active transfer bars
	numWorkers  int
	barCounter  atomic.Int64 // incrementing priority for new file bars

	scanComplete   atomic.Bool
	totalObjects   atomic.Int64
	skippedObjects atomic.Int64

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
// Per-file bars are created dynamically as transfers start and persist
// at 100% after completion. When verbose is true, progress bar output
// is suppressed so that slog output on stderr is not corrupted.
func NewProgressReporter(logger *slog.Logger, verbose bool, numWorkers int, output io.Writer) *ProgressReporter {
	if verbose {
		output = io.Discard
	}

	container := mpb.New(
		mpb.WithOutput(output),
		mpb.WithRefreshRate(150*time.Millisecond),
	)

	pr := &ProgressReporter{
		container:   container,
		currentBars: make([]*mpb.Bar, numWorkers),
		numWorkers:  numWorkers,
		logger:      logger,
	}

	// Overall bar stays at the very bottom (highest priority value).
	// mpb sorts ascending by priority: lower = top, higher = bottom.
	pr.overallBar = container.New(0,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		mpb.BarPriority(math.MaxInt),
		mpb.PrependDecorators(
			decor.Any(func(s decor.Statistics) string {
				completed := pr.completedFiles.Load() + pr.skippedObjects.Load()
				total := pr.totalObjects.Load()
				if total == 0 {
					return "Overall: "
				}
				return fmt.Sprintf("Overall (%d/%d): ", completed, total)
			}),
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
	pr.totalObjects.Store(totalObjects)
	pr.skippedObjects.Store(skippedObjects)
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

// StartTransfer creates a new per-file progress bar for the transfer.
// The bar persists at 100% after completion via mpb's auto-complete
// (triggerComplete=true when total > 0).
func (pr *ProgressReporter) StartTransfer(workerID int, job ObjectJob) {
	if workerID < 0 || workerID >= pr.numWorkers {
		return
	}

	displayName := job.Key
	if len(displayName) > 50 {
		displayName = "..." + displayName[len(displayName)-47:]
	}

	priority := int(pr.barCounter.Add(1))

	bar := pr.container.New(job.Size,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		mpb.BarPriority(priority),
		mpb.PrependDecorators(
			decor.Name("  "+displayName+" ", decor.WCSyncSpaceR),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WCSyncSpace),
			decor.CountersKibiByte("% .1f / % .1f", decor.WCSyncSpace),
			decor.AverageSpeed(decor.SizeB1024(0), "% .1f", decor.WCSyncSpace),
		),
	)

	pr.currentBars[workerID] = bar
}

// CompleteTransfer marks a per-file bar as complete and increments the overall bar.
func (pr *ProgressReporter) CompleteTransfer(workerID int, job ObjectJob) {
	if workerID >= 0 && workerID < pr.numWorkers {
		bar := pr.currentBars[workerID]
		if bar != nil {
			// Ensure bar shows exactly 100%. If the bar already auto-completed
			// (current >= total with triggerComplete=true), these are safe no-ops.
			bar.SetCurrent(job.Size)
			bar.SetTotal(job.Size, true)
			pr.currentBars[workerID] = nil
		}
	}

	pr.overallBar.IncrBy(int(job.Size))
	pr.completedBytes.Add(job.Size)
	pr.completedFiles.Add(1)
}

// FailTransfer removes the per-file bar and increments the failure counter.
func (pr *ProgressReporter) FailTransfer(workerID int, job ObjectJob, err error) {
	if workerID >= 0 && workerID < pr.numWorkers {
		bar := pr.currentBars[workerID]
		if bar != nil {
			bar.Abort(true)
			pr.currentBars[workerID] = nil
		}
	}

	pr.failedFiles.Add(1)
}

// barCountingReader wraps an io.Reader and increments an mpb bar on each read.
// After the bar auto-completes, IncrBy calls become safe no-ops via the
// bar's done channel.
type barCountingReader struct {
	r   io.Reader
	bar *mpb.Bar
}

func (cr *barCountingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.bar.IncrBy(n)
	}
	return n, err
}

// WrapReader returns a reader that updates the per-file progress bar.
func (pr *ProgressReporter) WrapReader(r io.Reader, workerID int) io.Reader {
	if workerID < 0 || workerID >= pr.numWorkers {
		return r
	}

	bar := pr.currentBars[workerID]
	if bar == nil {
		return r
	}

	return &barCountingReader{r: r, bar: bar}
}

// ReportError logs an error to the structured logger.
// Direct stderr writes are avoided to prevent corrupting mpb's display.
func (pr *ProgressReporter) ReportError(key string, err error) {
	pr.logger.Warn("transfer error",
		slog.String("key", key),
		slog.Any("error", err),
	)
}

// Complete marks the overall bar as finished so that container.Wait() can return.
// Any still-active file bars are aborted to clean them up.
func (pr *ProgressReporter) Complete() {
	for i, bar := range pr.currentBars {
		if bar != nil {
			bar.Abort(true)
			pr.currentBars[i] = nil
		}
	}

	pr.overallBar.SetTotal(pr.overallBar.Current(), true)
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
