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

// barState holds per-bar timing state captured in closures so that
// reuse of the same workerID slot does not corrupt a previous bar's display.
type barState struct {
	startTime time.Time
	elapsedNs atomic.Int64 // frozen on completion
}

// ProgressReporter manages per-file and overall progress bars via mpb.
// Each file transfer gets its own bar that persists at 100% after completion,
// providing a visual log of all transferred files with their transfer speeds.
type ProgressReporter struct {
	container  *mpb.Progress
	overallBar *mpb.Bar

	currentBars  []*mpb.Bar    // indexed by workerID, holds active transfer bars
	currentState []*barState   // indexed by workerID, timing state for active bar
	numWorkers   int
	barCounter   atomic.Int64 // incrementing priority for new file bars

	scanComplete   atomic.Bool
	totalObjects   atomic.Int64
	skippedObjects atomic.Int64

	completedBytes   atomic.Int64
	completedFiles   atomic.Int64
	transferredBytes atomic.Int64 // bytes from completed transfers this session (excludes resumed)
	streamedBytes    atomic.Int64 // bytes read through barCountingReader (real-time, includes in-flight)
	failedFiles      atomic.Int64
	startTime        time.Time
	logger           *slog.Logger
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
		container:    container,
		currentBars:  make([]*mpb.Bar, numWorkers),
		currentState: make([]*barState, numWorkers),
		numWorkers:   numWorkers,
		startTime:    time.Now(),
		logger:       logger,
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
			decor.Any(func(s decor.Statistics) string {
				streamed := pr.streamedBytes.Load()
				elapsed := time.Since(pr.startTime).Seconds()
				if streamed == 0 || elapsed < 1 {
					return "-- MiB/s"
				}
				speed := float64(streamed) / elapsed / (1024 * 1024)
				return fmt.Sprintf("%.1f MiB/s", speed)
			}, decor.WCSyncSpace),
			decor.Any(func(s decor.Statistics) string {
				elapsed := time.Since(pr.startTime)
				elapsedStr := formatDuration(elapsed)

				streamed := pr.streamedBytes.Load()
				elapsedSec := elapsed.Seconds()
				if streamed == 0 || elapsedSec < 1 {
					return elapsedStr
				}

				// Remaining = total - completed files - in-flight bytes
				completed := pr.completedBytes.Load()
				transferred := pr.transferredBytes.Load()
				inFlight := streamed - transferred
				remaining := s.Total - completed - inFlight
				if remaining <= 0 {
					return elapsedStr
				}

				speed := float64(streamed) / elapsedSec
				etaSec := float64(remaining) / speed
				eta := elapsed + time.Duration(etaSec*float64(time.Second))
				return elapsedStr + " / ~" + formatDuration(eta)
			}, decor.WCSyncSpace),
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
	bs := &barState{startTime: time.Now()}
	pr.currentState[workerID] = bs

	bar := pr.container.New(job.Size,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		mpb.BarPriority(priority),
		mpb.PrependDecorators(
			decor.Name("  "+displayName+" ", decor.WCSyncSpaceR),
		),
		mpb.AppendDecorators(
			decor.Any(func(s decor.Statistics) string {
				// Check for frozen elapsed time, or freeze it now if bar
				// auto-completed (via barCountingReader) before CompleteTransfer ran.
				frozenNs := bs.elapsedNs.Load()
				if frozenNs == 0 && s.Completed {
					frozenNs = int64(time.Since(bs.startTime))
					bs.elapsedNs.CompareAndSwap(0, frozenNs)
				}
				if frozenNs > 0 {
					// Completed: show percentage, total size, elapsed time, effective speed
					pct := float64(0)
					if s.Total > 0 {
						pct = float64(s.Current) / float64(s.Total) * 100
					}
					elapsed := time.Duration(frozenNs)
					size := float64(s.Total)
					speed := float64(0)
					if elapsed > 0 {
						speed = size / elapsed.Seconds()
					}
					return fmt.Sprintf("%.0f%%  %s  %s  %s/s",
						pct,
						formatSize(size),
						formatDuration(elapsed),
						formatSize(speed),
					)
				}
				// In-progress: show percentage, current/total, and speed
				pct := float64(0)
				if s.Total > 0 {
					pct = float64(s.Current) / float64(s.Total) * 100
				}
				elapsed := time.Since(bs.startTime).Seconds()
				if elapsed >= 1 && s.Current > 0 {
					speed := float64(s.Current) / elapsed
					return fmt.Sprintf("%.0f%%  %s / %s  %s/s",
						pct,
						formatSize(float64(s.Current)),
						formatSize(float64(s.Total)),
						formatSize(speed),
					)
				}
				return fmt.Sprintf("%.0f%%  %s / %s",
					pct,
					formatSize(float64(s.Current)),
					formatSize(float64(s.Total)),
				)
			}, decor.WCSyncSpace),
		),
	)

	pr.currentBars[workerID] = bar
}

// CompleteTransfer marks a per-file bar as complete and increments the overall bar.
func (pr *ProgressReporter) CompleteTransfer(workerID int, job ObjectJob) {
	if workerID >= 0 && workerID < pr.numWorkers {
		bar := pr.currentBars[workerID]
		if bar != nil {
			// Freeze elapsed time before completing so the decorator shows a stable value.
			bs := pr.currentState[workerID]
			if bs != nil {
				bs.elapsedNs.CompareAndSwap(0, int64(time.Since(bs.startTime)))
			}
			// Ensure bar shows exactly 100%.
			bar.SetCurrent(job.Size)
			bar.SetTotal(job.Size, true)
			pr.currentBars[workerID] = nil
			pr.currentState[workerID] = nil
		}
	}

	pr.overallBar.IncrBy(int(job.Size))
	pr.completedBytes.Add(job.Size)
	pr.transferredBytes.Add(job.Size)
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
// It also updates the reporter's streamedBytes counter so the overall speed
// and ETA reflect in-flight data, not just completed files.
type barCountingReader struct {
	r  io.Reader
	bar *mpb.Bar
	pr  *ProgressReporter
}

func (cr *barCountingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.bar.IncrBy(n)
		cr.pr.streamedBytes.Add(int64(n))
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

	return &barCountingReader{r: r, bar: bar, pr: pr}
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

// formatSize returns a human-readable size string using binary units (KiB, MiB, GiB).
func formatSize(bytes float64) string {
	switch {
	case bytes >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GiB", bytes/(1024*1024*1024))
	case bytes >= 1024*1024:
		return fmt.Sprintf("%.1f MiB", bytes/(1024*1024))
	case bytes >= 1024:
		return fmt.Sprintf("%.1f KiB", bytes/1024)
	default:
		return fmt.Sprintf("%.0f B", bytes)
	}
}

// formatDuration returns a compact duration string using colon notation (e.g. "1:23", "0:45", "2:05:00").
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return "0:00"
	}
	d = d.Round(time.Second)
	totalSec := int(d.Seconds())
	if totalSec < 3600 {
		m := totalSec / 60
		s := totalSec % 60
		return fmt.Sprintf("%d:%02d", m, s)
	}
	h := totalSec / 3600
	m := (totalSec % 3600) / 60
	s := totalSec % 60
	return fmt.Sprintf("%d:%02d:%02d", h, m, s)
}

// Stats returns current progress statistics.
func (pr *ProgressReporter) Stats() ProgressStats {
	return ProgressStats{
		CompletedFiles: pr.completedFiles.Load(),
		CompletedBytes: pr.completedBytes.Load(),
		FailedFiles:    pr.failedFiles.Load(),
	}
}
