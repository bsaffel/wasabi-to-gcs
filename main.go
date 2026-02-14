package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("shutdown signal received", slog.String("signal", sig.String()))
		cancel()

		<-time.After(30 * time.Second)
		slog.Error("graceful shutdown timed out, forcing exit")
		os.Exit(1)
	}()

	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("migration failed", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := ParseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	logger, logFile, err := setupLogger(cfg)
	if err != nil {
		return fmt.Errorf("setup logger: %w", err)
	}
	if logFile != nil {
		defer logFile.Close()
	}
	cfg.Logger = logger

	printBanner(cfg)

	state, err := NewStateManager(cfg.StateDir, logger)
	if err != nil {
		return fmt.Errorf("create state manager: %w", err)
	}
	defer state.Close()

	// Load manifest (skip if --rescan)
	var manifest *MigrationManifest
	if !cfg.Rescan {
		manifest, err = state.LoadManifest()
		if err != nil {
			return fmt.Errorf("load manifest: %w", err)
		}
	}

	if manifest != nil {
		printResumeInfo(manifest, state)
	}

	// Create clients
	wasabiClient, err := newWasabiClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create wasabi client: %w", err)
	}

	gcsClient, err := newGCSClient(ctx)
	if err != nil {
		return fmt.Errorf("create gcs client: %w", err)
	}
	defer gcsClient.Close()

	// Create progress reporter
	progress := NewProgressReporter(logger)

	// Pre-load overall bar from manifest if resuming
	if manifest != nil {
		progress.UpdateScanTotals(
			manifest.TotalObjects,
			manifest.TotalBytes,
			int64(state.CompletedCount()),
			state.CompletedBytes(),
		)
	}

	// Wire transfer engine
	engine := NewTransferEngine(cfg)
	engine.wasabi = wasabiClient
	engine.gcs = gcsClient
	engine.state = state
	engine.progress = progress

	// Create bounded jobs channel
	jobs := make(chan ObjectJob, cfg.Workers*2)

	// Create lister
	lister := &Lister{
		client:   wasabiClient,
		bucket:   cfg.WasabiBucket,
		prefix:   cfg.Prefix,
		state:    state,
		progress: progress,
		logger:   logger,
	}

	// Dry-run path: scan only, print summary
	if cfg.DryRun {
		fmt.Fprintf(os.Stderr, "DRY RUN — scanning without transferring\n\n")

		// Drain the channel (don't process, just count)
		drainDone := make(chan struct{})
		var dryRunObjects int64
		var dryRunBytes int64
		go func() {
			for job := range jobs {
				dryRunObjects++
				dryRunBytes += job.Size
			}
			close(drainDone)
		}()

		if err := lister.ScanAndStream(ctx, jobs); err != nil {
			close(jobs)
			return fmt.Errorf("scan: %w", err)
		}
		close(jobs)
		<-drainDone

		progress.Wait()

		fmt.Fprintf(os.Stderr, "\nDry Run Summary\n")
		fmt.Fprintf(os.Stderr, "===============\n")
		fmt.Fprintf(os.Stderr, "Total objects scanned:  %s\n", humanize(lister.totalObjects.Load()))
		fmt.Fprintf(os.Stderr, "Total bytes:           %s\n", humanizeBytes(lister.totalBytes.Load()))
		fmt.Fprintf(os.Stderr, "Already completed:     %s\n", humanize(lister.skippedObjects.Load()))
		fmt.Fprintf(os.Stderr, "Would transfer:        %s objects (%s)\n",
			humanize(dryRunObjects), humanizeBytes(dryRunBytes))
		return nil
	}

	// Normal path: run lister + pool concurrently
	startTime := time.Now()
	pool := NewWorkerPool(cfg.Workers, jobs, engine, state, progress, logger)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(jobs)
		return lister.ScanAndStream(ctx, jobs)
	})

	g.Go(func() error {
		return pool.Run(ctx)
	})

	if err := g.Wait(); err != nil {
		progress.Wait()
		return err
	}

	progress.Wait()
	printCompletionSummary(progress.Stats(), lister, startTime)

	return nil
}

// setupLogger creates a logger that writes to the state directory log file.
func setupLogger(cfg *Config) (*slog.Logger, *os.File, error) {
	logPath := filepath.Join(cfg.StateDir, "migration.log")
	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file: %w", err)
	}

	var output io.Writer = logFile

	if cfg.Verbose {
		output = io.MultiWriter(logFile, os.Stderr)
	}

	handler := slog.NewTextHandler(output, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	})

	logger := slog.New(handler)
	return logger, logFile, nil
}

func printBanner(cfg *Config) {
	fmt.Fprintf(os.Stderr, "Wasabi to GCS Migration\n")
	fmt.Fprintf(os.Stderr, "========================\n")
	fmt.Fprintf(os.Stderr, "Source:    wasabi://%s/%s\n", cfg.WasabiBucket, cfg.Prefix)
	fmt.Fprintf(os.Stderr, "Dest:      gs://%s/%s\n", cfg.GCSBucket, cfg.Prefix)
	fmt.Fprintf(os.Stderr, "Workers:   %d\n", cfg.Workers)
	fmt.Fprintf(os.Stderr, "State dir: %s\n", cfg.StateDir)
	fmt.Fprintf(os.Stderr, "Log file:  %s/migration.log\n", cfg.StateDir)
	fmt.Fprintf(os.Stderr, "\n")
}

func printResumeInfo(manifest *MigrationManifest, state *StateManager) {
	completedCount := state.CompletedCount()
	completedBytes := state.CompletedBytes()
	remainingCount := manifest.TotalObjects - int64(completedCount)
	remainingBytes := manifest.TotalBytes - completedBytes

	fmt.Fprintf(os.Stderr, "Resuming previous migration (scanned %s)\n",
		manifest.ScannedAt.Format("2006-01-02 15:04"))
	fmt.Fprintf(os.Stderr, "  Previously completed: %s objects (%s)\n",
		humanize(int64(completedCount)), humanizeBytes(completedBytes))
	fmt.Fprintf(os.Stderr, "  Estimated remaining:  %s objects (%s)\n",
		humanize(remainingCount), humanizeBytes(remainingBytes))
	fmt.Fprintf(os.Stderr, "\n")
}

func printCompletionSummary(stats ProgressStats, lister *Lister, startTime time.Time) {
	elapsed := time.Since(startTime)
	var avgSpeed float64
	if elapsed.Seconds() > 0 {
		avgSpeed = float64(stats.CompletedBytes) / elapsed.Seconds() / 1024 / 1024
	}

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Migration Complete\n")
	fmt.Fprintf(os.Stderr, "==================\n")
	fmt.Fprintf(os.Stderr, "Total objects:  %s\n", humanize(lister.totalObjects.Load()))
	fmt.Fprintf(os.Stderr, "Transferred:    %s (%s)\n",
		humanize(stats.CompletedFiles), humanizeBytes(stats.CompletedBytes))
	fmt.Fprintf(os.Stderr, "Skipped:        %s\n", humanize(lister.skippedObjects.Load()))
	fmt.Fprintf(os.Stderr, "Failed:         %s\n", humanize(stats.FailedFiles))
	fmt.Fprintf(os.Stderr, "Duration:       %s\n", elapsed.Truncate(time.Second))
	fmt.Fprintf(os.Stderr, "Avg speed:      %.1f MB/s\n", avgSpeed)
}

func humanize(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func humanizeBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1f TiB", float64(b)/float64(int64(1)<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/float64(int64(1)<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/float64(int64(1)<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/float64(int64(1)<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
