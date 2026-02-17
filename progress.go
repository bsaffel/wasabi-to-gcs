package main

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// barState holds per-bar timing state captured in closures so that
// reuse of the same workerID slot does not corrupt a previous bar's display.
type barState struct {
	startTime  time.Time
	elapsedNs  atomic.Int64          // frozen on completion
	seqNum     atomic.Int64          // assigned on completion, displayed as line number
	retryCount atomic.Int32          // current retry attempt (0 = first try)
	failed     atomic.Bool           // true after all retries exhausted
	errMsg     atomic.Pointer[string] // set once on failure, read by decorator
	streamed   atomic.Int64          // bytes read through this bar's counting reader
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
	completionSeq atomic.Int64 // incrementing sequence assigned at completion time

	failedBars   []*mpb.Bar  // bars for failed transfers, completed at end
	failedStates []*barState // corresponding states
	failedMu     sync.Mutex  // protects failedBars/failedStates

	failedDetails []FailedFile // full error details for summary
	failedDetMu   sync.Mutex   // protects failedDetails

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

// FailedFile records a file that failed to transfer, with its full error.
type FailedFile struct {
	Key string
	Err string
}

// ProgressStats holds current progress statistics.
type ProgressStats struct {
	CompletedFiles int64
	CompletedBytes int64
	FailedFiles    int64
	Failed         []FailedFile
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
		mpb.PopCompletedMode(),
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
				return fmt.Sprintf("%6.1f MiB/s", speed)
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
// The bar is created with total=0 (triggerComplete=false) so that the
// barCountingReader filling it to 100% does NOT auto-complete/pop it.
// Only CompleteTransfer triggers completion after assigning the sequence number.
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

	// Build the normal bar filler for non-failed state.
	normalFiller := mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]").Build()

	// Create with total=0 so triggerComplete defaults to false, preventing
	// auto-completion when barCountingReader fills the bar to 100%.
	// Use BarFillerFunc so failed bars replace the visual bar with an error message.
	bar, _ := pr.container.Add(0,
		mpb.BarFillerFunc(func(w io.Writer, st decor.Statistics) error {
			if bs.failed.Load() {
				msg := "FAILED"
				if m := bs.errMsg.Load(); m != nil {
					msg = "FAILED: " + *m
				}
				availW := st.AvailableWidth
				if availW > 0 && len(msg) > availW {
					msg = msg[:availW-3] + "..."
				}
				fmt.Fprintf(w, "%-*s", availW, msg)
				return nil
			}
			return normalFiller.Fill(w, st)
		}),
		mpb.BarPriority(priority),
		mpb.PrependDecorators(
			decor.Any(func(s decor.Statistics) string {
				if bs.failed.Load() {
					return fmt.Sprintf(" ERR  %s ", displayName)
				}
				if n := bs.seqNum.Load(); n > 0 {
					return fmt.Sprintf("%4d  %s ", n, displayName)
				}
				return fmt.Sprintf("      %s ", displayName)
			}, decor.WCSyncSpaceR),
		),
		mpb.AppendDecorators(
			decor.Any(func(s decor.Statistics) string {
				// Failed: error message is rendered in the bar area, nothing here
				if bs.failed.Load() {
					return ""
				}
				frozenNs := bs.elapsedNs.Load()
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
					return fmt.Sprintf("%3.0f%%  %s  %5s  %s/s",
						pct,
						formatSize(size),
						formatDuration(elapsed),
						formatSize(speed),
					)
				}
				// In-progress: show percentage, current/total, speed, and retry info
				pct := float64(0)
				if s.Total > 0 {
					pct = float64(s.Current) / float64(s.Total) * 100
				}
				retryInfo := ""
				if r := bs.retryCount.Load(); r > 0 {
					retryInfo = fmt.Sprintf("  (retry %d)", r)
				}
				elapsed := time.Since(bs.startTime).Seconds()
				if elapsed >= 1 && s.Current > 0 {
					speed := float64(s.Current) / elapsed
					return fmt.Sprintf("%3.0f%%  %s / %s  %s/s%s",
						pct,
						formatSize(float64(s.Current)),
						formatSize(float64(s.Total)),
						formatSize(speed),
						retryInfo,
					)
				}
				return fmt.Sprintf("%3.0f%%  %s / %s%s",
					pct,
					formatSize(float64(s.Current)),
					formatSize(float64(s.Total)),
					retryInfo,
				)
			}, decor.WCSyncSpace),
		),
	)

	// Set the real total without triggering completion (false).
	bar.SetTotal(job.Size, false)

	pr.currentBars[workerID] = bar
}

// CompleteTransfer marks a per-file bar as complete and increments the overall bar.
func (pr *ProgressReporter) CompleteTransfer(workerID int, job ObjectJob) {
	if workerID >= 0 && workerID < pr.numWorkers {
		bar := pr.currentBars[workerID]
		if bar != nil {
			bs := pr.currentState[workerID]
			if bs != nil {
				// Assign completion sequence number and freeze elapsed time
				// before triggering bar completion, so the final render shows both.
				bs.seqNum.Store(pr.completionSeq.Add(1))
				bs.elapsedNs.CompareAndSwap(0, int64(time.Since(bs.startTime)))
			}
			// Ensure bar shows exactly 100%. PopCompletedMode on the
			// container moves completed bars out of the active render area
			// into static scrollback, so each file appears once.
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

// FailTransfer marks a per-file bar as failed. The bar stays active (not popped)
// so it renders below completed bars. It will be completed in Complete() so that
// error lines appear at the bottom of the scrollback, below all successful transfers.
func (pr *ProgressReporter) FailTransfer(workerID int, job ObjectJob, err error) {
	if workerID >= 0 && workerID < pr.numWorkers {
		bar := pr.currentBars[workerID]
		bs := pr.currentState[workerID]
		if bar != nil && bs != nil {
			// Set error state — decorators will switch to showing error info.
			// Strip wrapping that includes the key name (e.g. "non-retryable
			// error for <key>: ..." or "all N attempts failed for <key>: ...")
			// since the key is already shown in the prepend decorator.
			errStr := err.Error()
			if idx := strings.Index(errStr, job.Key+": "); idx >= 0 {
				errStr = errStr[idx+len(job.Key)+2:]
			}
			if len(errStr) > 80 {
				errStr = errStr[:77] + "..."
			}
			bs.errMsg.Store(&errStr)
			bs.failed.Store(true)

			// Undo streamed bytes from this failed transfer so overall
			// speed/ETA calculations remain accurate.
			prev := bs.streamed.Swap(0)
			pr.streamedBytes.Add(-prev)

			// Reset bar to empty so it shows as [ ] not partially filled
			bar.SetCurrent(0)

			// Move bar to failed tracking — frees the workerID slot for reuse
			pr.failedMu.Lock()
			pr.failedBars = append(pr.failedBars, bar)
			pr.failedStates = append(pr.failedStates, bs)
			pr.failedMu.Unlock()
		} else if bar != nil {
			bar.Abort(true)
		}
		pr.currentBars[workerID] = nil
		pr.currentState[workerID] = nil
	}

	pr.failedFiles.Add(1)

	// Record full error for the completion summary
	pr.failedDetMu.Lock()
	pr.failedDetails = append(pr.failedDetails, FailedFile{Key: job.Key, Err: err.Error()})
	pr.failedDetMu.Unlock()
}

// RetryTransfer resets a per-file bar for a retry attempt. It undoes the
// streamed byte count from the previous attempt and resets the bar position
// so the progress display is accurate for the new attempt.
func (pr *ProgressReporter) RetryTransfer(workerID int, attempt int) {
	if workerID < 0 || workerID >= pr.numWorkers {
		return
	}
	bar := pr.currentBars[workerID]
	bs := pr.currentState[workerID]
	if bar == nil || bs == nil {
		return
	}

	// Undo streamed bytes from previous attempt
	prev := bs.streamed.Swap(0)
	pr.streamedBytes.Add(-prev)

	// Reset bar position and update retry count
	bar.SetCurrent(0)
	bs.retryCount.Store(int32(attempt))
	bs.startTime = time.Now()
}

// barCountingReader wraps an io.Reader and increments an mpb bar on each read.
// It also updates the reporter's streamedBytes counter so the overall speed
// and ETA reflect in-flight data, not just completed files.
type barCountingReader struct {
	r  io.Reader
	bar *mpb.Bar
	bs  *barState
	pr  *ProgressReporter
}

func (cr *barCountingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.bar.IncrBy(n)
		cr.bs.streamed.Add(int64(n))
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
	bs := pr.currentState[workerID]
	if bar == nil || bs == nil {
		return r
	}

	return &barCountingReader{r: r, bar: bar, bs: bs, pr: pr}
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
// Failed bars are completed last so they appear at the bottom of the scrollback,
// below all successful transfers. Any other still-active bars are aborted.
func (pr *ProgressReporter) Complete() {
	for i, bar := range pr.currentBars {
		if bar != nil {
			bar.Abort(true)
			pr.currentBars[i] = nil
		}
	}

	// Wait two render cycles so mpb pops all completed file bars to scrollback
	// before we add the failed bars. Without this, ordering can be incorrect.
	time.Sleep(350 * time.Millisecond)

	// Complete failed bars — they pop to scrollback below all previously
	// completed bars, creating a clear "errors at the bottom" layout.
	pr.failedMu.Lock()
	for _, bar := range pr.failedBars {
		bar.SetTotal(0, true)
	}
	pr.failedBars = nil
	pr.failedStates = nil
	pr.failedMu.Unlock()

	// Wait for failed bars to be popped before completing overall bar.
	time.Sleep(350 * time.Millisecond)

	pr.overallBar.SetTotal(pr.overallBar.Current(), true)
}

// Wait blocks until all progress bars complete rendering.
func (pr *ProgressReporter) Wait() {
	pr.container.Wait()
}

// formatSize returns a fixed-width (9 char) human-readable size string using
// binary units (KiB, MiB, GiB). Right-aligned for consistent column alignment.
func formatSize(bytes float64) string {
	var s string
	switch {
	case bytes >= 1024*1024*1024:
		s = fmt.Sprintf("%.1f GiB", bytes/(1024*1024*1024))
	case bytes >= 1024*1024:
		s = fmt.Sprintf("%.1f MiB", bytes/(1024*1024))
	case bytes >= 1024:
		s = fmt.Sprintf("%.1f KiB", bytes/1024)
	default:
		s = fmt.Sprintf("%.0f B", bytes)
	}
	return fmt.Sprintf("%10s", s)
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
	pr.failedDetMu.Lock()
	failed := make([]FailedFile, len(pr.failedDetails))
	copy(failed, pr.failedDetails)
	pr.failedDetMu.Unlock()

	return ProgressStats{
		CompletedFiles: pr.completedFiles.Load(),
		CompletedBytes: pr.completedBytes.Load(),
		FailedFiles:    pr.failedFiles.Load(),
		Failed:         failed,
	}
}
