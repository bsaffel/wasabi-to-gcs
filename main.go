package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/grpclog"
)

// exitError carries a specific exit code through Cobra's error chain.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

// lipgloss styles — ANSI 16-color palette for broad terminal compatibility.
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("12")) // Bright blue

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8")). // Gray
			Width(14)

	valueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("15")) // White

	arrowStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8")) // Gray

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("2")) // Green

	warnStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("3")) // Yellow

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("1")). // Red
			Bold(true)

	dimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("8")) // Gray/dim
)

const flagGroupKey = "group"

// Custom usage template with grouped flags.
var usageTemplate = `Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}

Source (Wasabi):
{{flagsByGroup .Flags "source"}}
Destination (GCS):
{{flagsByGroup .Flags "destination"}}
Transfer Options:
{{flagsByGroup .Flags "transfer"}}
Output:
{{flagsByGroup .Flags "output"}}{{if .HasAvailableSubCommands}}
Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}

Use "{{.CommandPath}} [command] --help" for more information about a command.
`

func init() {
	cobra.AddTemplateFunc("flagsByGroup", func(fs *pflag.FlagSet, group string) string {
		var buf strings.Builder
		fs.VisitAll(func(f *pflag.Flag) {
			ann, ok := f.Annotations[flagGroupKey]
			if !ok || len(ann) == 0 || ann[0] != group {
				return
			}
			fmt.Fprintf(&buf, "      --%s", f.Name)
			varType, usage := pflag.UnquoteUsage(f)
			if varType != "" {
				fmt.Fprintf(&buf, " %s", varType)
			}
			padding := 30 - len(f.Name)
			if varType != "" {
				padding -= len(varType) + 1
			}
			if padding < 1 {
				padding = 1
			}
			fmt.Fprintf(&buf, "%s%s", strings.Repeat(" ", padding), usage)
			if f.DefValue != "" && f.DefValue != "false" && f.DefValue != "0" {
				fmt.Fprintf(&buf, " (default %s)", f.DefValue)
			}
			fmt.Fprintln(&buf)
		})
		return buf.String()
	})
}

func main() {
	if err := newRootCmd().Execute(); err != nil {
		var exitErr *exitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.code)
		}
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	cfg := &Config{
		Workers:    10,
		MaxRetries: 3,
		StateDir:   "./migration_state",
		LogLevel:   slog.LevelInfo,
	}

	var logLevel string

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate objects from Wasabi S3 to Google Cloud Storage",
		Long: `migrate streams objects from a Wasabi S3 bucket to a GCS bucket with
concurrent workers, resumability, progress tracking, and integrity verification.

Transfers are idempotent — re-running picks up where the previous run left off.
Each completed object is recorded in a local state directory, so interrupted
migrations resume without re-transferring.`,
		Example: `  # Basic migration
  migrate --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket my-source \
          --gcs-bucket my-destination

  # Migrate a specific prefix with 20 workers
  migrate --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket my-source \
          --gcs-bucket my-destination \
          --prefix "images/" \
          --workers 20

  # Dry run to see what would be transferred
  migrate --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket my-source \
          --gcs-bucket my-destination \
          --dry-run

  # Resume a previous migration, forcing a fresh scan
  migrate --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket my-source \
          --gcs-bucket my-destination \
          --rescan

  # Re-run a completed migration
  migrate --wasabi-endpoint https://s3.us-east-1.wasabisys.com \
          --wasabi-region us-east-1 \
          --wasabi-bucket my-source \
          --gcs-bucket my-destination \
          --force`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			resolveLogLevel(logLevel, cfg)

			if err := cfg.Validate(); err != nil {
				printStyledError(err)
				return &exitError{code: 2, err: err}
			}

			return runMigration(cfg)
		},
	}

	registerFlags(cmd, cfg, &logLevel)

	return cmd
}

func setFlagGroup(cmd *cobra.Command, flagName, group string) {
	cmd.Flags().SetAnnotation(flagName, flagGroupKey, []string{group})
}

func registerFlags(cmd *cobra.Command, cfg *Config, logLevel *string) {
	f := cmd.Flags()

	// Source (Wasabi)
	f.StringVar(&cfg.WasabiEndpoint, "wasabi-endpoint", "", "Wasabi S3 endpoint URL")
	f.StringVar(&cfg.WasabiRegion, "wasabi-region", "", "Wasabi region (e.g. us-east-1)")
	f.StringVar(&cfg.WasabiAccessKey, "wasabi-access-key", os.Getenv("WASABI_ACCESS_KEY"),
		"Wasabi access key [$WASABI_ACCESS_KEY]")
	f.StringVar(&cfg.WasabiSecretKey, "wasabi-secret-key", os.Getenv("WASABI_SECRET_KEY"),
		"Wasabi secret key [$WASABI_SECRET_KEY]")
	f.StringVar(&cfg.WasabiBucket, "wasabi-bucket", "", "Source Wasabi bucket name")

	// Destination (GCS)
	f.StringVar(&cfg.GCSProject, "gcs-project", "", "GCP project ID (optional)")
	f.StringVar(&cfg.GCSBucket, "gcs-bucket", "", "Destination GCS bucket name")

	// Transfer Options
	f.StringVar(&cfg.Prefix, "prefix", "", "Only migrate objects matching this key prefix")
	f.IntVar(&cfg.Workers, "workers", 10, "Number of concurrent transfer workers (1-100)")
	f.IntVar(&cfg.MaxRetries, "max-retries", 3, "Max retry attempts per object (0-10)")
	f.StringVar(&cfg.StateDir, "state-dir", "./migration_state", "Directory for resumable state tracking")
	f.BoolVar(&cfg.DryRun, "dry-run", false, "Scan and count objects without transferring")
	f.BoolVar(&cfg.Rescan, "rescan", false, "Force re-scan of source bucket, ignore cached manifest")
	f.BoolVar(&cfg.Force, "force", false, "Force restart of a completed migration")

	// Output
	f.BoolVar(&cfg.Verbose, "verbose", false, "Show structured log output alongside progress bars")
	f.StringVar(logLevel, "log-level", "info", "Log verbosity: debug, info, warn, error")

	// Group annotations for custom help template
	for _, name := range []string{"wasabi-endpoint", "wasabi-region", "wasabi-access-key", "wasabi-secret-key", "wasabi-bucket"} {
		setFlagGroup(cmd, name, "source")
	}
	for _, name := range []string{"gcs-project", "gcs-bucket"} {
		setFlagGroup(cmd, name, "destination")
	}
	for _, name := range []string{"prefix", "workers", "max-retries", "state-dir", "dry-run", "rescan", "force"} {
		setFlagGroup(cmd, name, "transfer")
	}
	for _, name := range []string{"verbose", "log-level"} {
		setFlagGroup(cmd, name, "output")
	}

	cmd.SetUsageTemplate(usageTemplate)
}

func resolveLogLevel(level string, cfg *Config) {
	switch strings.ToLower(level) {
	case "debug":
		cfg.LogLevel = slog.LevelDebug
	case "warn":
		cfg.LogLevel = slog.LevelWarn
	case "error":
		cfg.LogLevel = slog.LevelError
	default:
		cfg.LogLevel = slog.LevelInfo
	}
}

func runMigration(cfg *Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		slog.Info("shutdown signal received, finishing in-flight transfers…", slog.String("signal", sig.String()))
		cancel()

		// Restore default signal handling so the next Ctrl+C kills immediately.
		signal.Reset(syscall.SIGINT, syscall.SIGTERM)

		// Also force exit after a short timeout in case nothing else arrives.
		time.AfterFunc(2*time.Second, func() {
			slog.Error("graceful shutdown timed out, forcing exit")
			os.Exit(1)
		})
	}()

	logger, logFile, err := setupLogger(cfg)
	if err != nil {
		return fmt.Errorf("setup logger: %w", err)
	}
	if logFile != nil {
		defer logFile.Close()
	}
	cfg.Logger = logger

	// Redirect standard library and gRPC logging away from stderr.
	// Library code (GCS SDK, gRPC) writes to stderr via log.Print and
	// grpclog during connection establishment, which shifts mpb's cursor
	// tracking and leaves ghost progress bar lines at the top of the display.
	log.SetOutput(logFile)
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(logFile, logFile, logFile))

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

	// Discard manifest if it belongs to a different prefix or bucket
	if manifest != nil && (manifest.Prefix != cfg.Prefix || manifest.SourceBucket != cfg.WasabiBucket) {
		cfg.Logger.Info("prefix or bucket changed, starting fresh scan",
			slog.String("old_prefix", manifest.Prefix),
			slog.String("new_prefix", cfg.Prefix),
		)
		manifest = nil
	}

	// Detect already-completed migration
	if manifest != nil {
		completedCount := int64(state.CompletedCount())
		remaining := manifest.TotalObjects - completedCount
		if remaining <= 0 {
			if !cfg.Force {
				printAlreadyComplete(manifest, state)
				return &exitError{code: 0, err: fmt.Errorf("migration already complete; use --force to restart")}
			}
			// --force: clear completed state and re-scan
			if err := state.Reset(); err != nil {
				return fmt.Errorf("reset state: %w", err)
			}
			manifest = nil
			fmt.Fprintf(os.Stderr, "%s %s\n\n",
				warnStyle.Render("Force restart"),
				dimStyle.Render("cleared previous state, re-scanning"))
		} else {
			printResumeInfo(manifest, state)
		}
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

	// Redirect stderr (fd 2) to the log file so library writes (gRPC
	// connection setup, GCS SDK diagnostics) can't corrupt mpb's ANSI cursor
	// tracking. mpb draws to the saved terminal fd instead.
	termFd, err := syscall.Dup(int(os.Stderr.Fd()))
	if err != nil {
		return fmt.Errorf("dup stderr: %w", err)
	}
	terminal := os.NewFile(uintptr(termFd), "/dev/stderr")
	defer terminal.Close()
	syscall.Dup2(int(logFile.Fd()), int(os.Stderr.Fd()))
	restoreStderr := func() {
		syscall.Dup2(termFd, int(os.Stderr.Fd()))
	}
	defer restoreStderr()

	// Create progress reporter
	progress := NewProgressReporter(logger, cfg.Verbose, cfg.Workers, terminal)

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
		restoreStderr()
		fmt.Fprintln(os.Stderr, warnStyle.Render("DRY RUN")+" "+dimStyle.Render("scanning without transferring"))
		fmt.Fprintln(os.Stderr)

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

		if lister.totalObjects.Load() == 0 {
			printNoObjectsWarning(cfg)
			return nil
		}

		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, titleStyle.Render("Dry Run Summary"))
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "  %s  %s\n",
			labelStyle.Render("Scanned:"),
			valueStyle.Render(humanize(lister.totalObjects.Load())+" objects"))
		fmt.Fprintf(os.Stderr, "  %s  %s\n",
			labelStyle.Render("Total size:"),
			valueStyle.Render(humanizeBytes(lister.totalBytes.Load())))
		fmt.Fprintf(os.Stderr, "  %s  %s\n",
			labelStyle.Render("Completed:"),
			successStyle.Render(humanize(lister.skippedObjects.Load())))
		fmt.Fprintf(os.Stderr, "  %s  %s objects (%s)\n",
			labelStyle.Render("To transfer:"),
			warnStyle.Render(humanize(dryRunObjects)),
			warnStyle.Render(humanizeBytes(dryRunBytes)))
		fmt.Fprintln(os.Stderr)
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
		progress.Complete()
		progress.Wait()
		restoreStderr()
		return err
	}

	progress.Complete()
	progress.Wait()
	restoreStderr()

	if lister.totalObjects.Load() == 0 {
		printNoObjectsWarning(cfg)
		return nil
	}

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
	title := titleStyle.Render("Wasabi to GCS Migration")

	source := fmt.Sprintf("wasabi://%s/%s", cfg.WasabiBucket, cfg.Prefix)
	dest := fmt.Sprintf("gs://%s/%s", cfg.GCSBucket, cfg.Prefix)
	arrow := arrowStyle.Render("-->")

	lines := []string{
		title,
		"",
		fmt.Sprintf("  %s  %s  %s  %s",
			labelStyle.Render("Source:"),
			valueStyle.Render(source),
			arrow,
			valueStyle.Render(dest)),
		fmt.Sprintf("  %s  %s",
			labelStyle.Render("Workers:"),
			valueStyle.Render(fmt.Sprintf("%d", cfg.Workers))),
		fmt.Sprintf("  %s  %s",
			labelStyle.Render("State dir:"),
			dimStyle.Render(cfg.StateDir)),
		fmt.Sprintf("  %s  %s",
			labelStyle.Render("Log file:"),
			dimStyle.Render(cfg.StateDir+"/migration.log")),
		"",
	}

	fmt.Fprintln(os.Stderr, strings.Join(lines, "\n"))
}

func printAlreadyComplete(manifest *MigrationManifest, state *StateManager) {
	completedCount := state.CompletedCount()
	completedBytes := state.CompletedBytes()

	fmt.Fprintf(os.Stderr, "%s %s\n",
		warnStyle.Render("Already complete"),
		dimStyle.Render(fmt.Sprintf("(scanned %s)",
			manifest.ScannedAt.Format("2006-01-02 15:04"))))
	fmt.Fprintf(os.Stderr, "  %s %s objects (%s)\n",
		labelStyle.Render("Completed:"),
		successStyle.Render(humanize(int64(completedCount))),
		successStyle.Render(humanizeBytes(completedBytes)))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n",
		dimStyle.Render("Use --force to restart this migration, or --rescan to re-scan the source."))
	fmt.Fprintln(os.Stderr)
}

func printResumeInfo(manifest *MigrationManifest, state *StateManager) {
	completedCount := state.CompletedCount()
	completedBytes := state.CompletedBytes()
	remainingCount := manifest.TotalObjects - int64(completedCount)
	remainingBytes := manifest.TotalBytes - completedBytes

	fmt.Fprintf(os.Stderr, "%s %s\n",
		warnStyle.Render("Resuming"),
		dimStyle.Render(fmt.Sprintf("previous migration (scanned %s)",
			manifest.ScannedAt.Format("2006-01-02 15:04"))))
	fmt.Fprintf(os.Stderr, "  %s %s objects (%s)\n",
		labelStyle.Render("Completed:"),
		successStyle.Render(humanize(int64(completedCount))),
		successStyle.Render(humanizeBytes(completedBytes)))
	fmt.Fprintf(os.Stderr, "  %s %s objects (%s)\n",
		labelStyle.Render("Remaining:"),
		valueStyle.Render(humanize(remainingCount)),
		valueStyle.Render(humanizeBytes(remainingBytes)))
	fmt.Fprintln(os.Stderr)
}

func printNoObjectsWarning(cfg *Config) {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, warnStyle.Render("No objects found"))
	fmt.Fprintln(os.Stderr)
	source := fmt.Sprintf("wasabi://%s/%s", cfg.WasabiBucket, cfg.Prefix)
	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("Source:"),
		valueStyle.Render(source))
	if cfg.Prefix != "" {
		fmt.Fprintf(os.Stderr, "  %s  %s\n",
			labelStyle.Render("Prefix:"),
			valueStyle.Render(cfg.Prefix))
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n", dimStyle.Render("Check that the bucket name and prefix are correct."))
	fmt.Fprintln(os.Stderr)
}

func printCompletionSummary(stats ProgressStats, lister *Lister, startTime time.Time) {
	elapsed := time.Since(startTime)
	var avgSpeed float64
	if elapsed.Seconds() > 0 {
		avgSpeed = float64(stats.CompletedBytes) / elapsed.Seconds() / 1024 / 1024
	}

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, titleStyle.Render("Migration Complete"))
	fmt.Fprintln(os.Stderr)

	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("Total:"),
		valueStyle.Render(humanize(lister.totalObjects.Load())+" objects"))

	fmt.Fprintf(os.Stderr, "  %s  %s (%s)\n",
		labelStyle.Render("Transferred:"),
		successStyle.Render(humanize(stats.CompletedFiles)),
		successStyle.Render(humanizeBytes(stats.CompletedBytes)))

	skipped := lister.skippedObjects.Load()
	skippedLine := humanize(skipped)
	if skipped > 0 {
		skippedLine = warnStyle.Render(skippedLine)
	} else {
		skippedLine = dimStyle.Render(skippedLine)
	}
	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("Skipped:"),
		skippedLine)

	failedLine := humanize(stats.FailedFiles)
	if stats.FailedFiles > 0 {
		failedLine = errorStyle.Render(failedLine)
	} else {
		failedLine = dimStyle.Render(failedLine)
	}
	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("Failed:"),
		failedLine)

	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("Duration:"),
		valueStyle.Render(elapsed.Truncate(time.Second).String()))

	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("Avg speed:"),
		valueStyle.Render(fmt.Sprintf("%.1f MB/s", avgSpeed)))

	fmt.Fprintln(os.Stderr)
}

func printStyledError(err error) {
	errMsg := err.Error()
	lines := strings.Split(errMsg, "\n")

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, errorStyle.Render("Configuration Error"))
	fmt.Fprintln(os.Stderr)
	for _, line := range lines {
		if line != "" {
			fmt.Fprintf(os.Stderr, "  %s %s\n",
				errorStyle.Render("*"),
				valueStyle.Render(line))
		}
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, dimStyle.Render("  Run 'migrate --help' for usage information."))
	fmt.Fprintln(os.Stderr)
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
