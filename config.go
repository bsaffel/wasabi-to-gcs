package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Sentinel errors for fatal conditions
var (
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrPermissionDenied   = errors.New("permission denied")
	ErrRateLimited        = errors.New("rate limited")
	ErrInvalidCredentials = errors.New("invalid credentials")
)

// Config holds all configuration for the migration tool.
type Config struct {
	// Source (Wasabi)
	WasabiEndpoint  string
	WasabiRegion    string
	WasabiAccessKey string
	WasabiSecretKey string
	WasabiBucket    string

	// Destination (GCS)
	GCSProject string
	GCSBucket  string

	// Transfer settings
	Prefix     string
	Workers    int
	MaxRetries int
	StateDir   string
	DryRun     bool

	// Observability
	Logger   *slog.Logger
	LogLevel slog.Level

	// Console behavior
	Verbose bool
	Rescan  bool
}

// Validate checks all required fields and constraints.
func (c *Config) Validate() error {
	var errs []error

	if c.WasabiEndpoint == "" {
		errs = append(errs, errors.New("wasabi-endpoint is required"))
	}
	if c.WasabiRegion == "" {
		errs = append(errs, errors.New("wasabi-region is required"))
	}
	if c.WasabiAccessKey == "" {
		errs = append(errs, errors.New("wasabi-access-key is required (flag or WASABI_ACCESS_KEY env)"))
	}
	if c.WasabiSecretKey == "" {
		errs = append(errs, errors.New("wasabi-secret-key is required (flag or WASABI_SECRET_KEY env)"))
	}
	if c.WasabiBucket == "" {
		errs = append(errs, errors.New("wasabi-bucket is required"))
	}
	if c.GCSBucket == "" {
		errs = append(errs, errors.New("gcs-bucket is required"))
	}

	if c.Workers < 1 || c.Workers > 100 {
		errs = append(errs, fmt.Errorf("workers must be 1-100, got %d", c.Workers))
	}
	if c.MaxRetries < 0 || c.MaxRetries > 10 {
		errs = append(errs, fmt.Errorf("max-retries must be 0-10, got %d", c.MaxRetries))
	}

	if c.WasabiEndpoint != "" {
		if _, err := url.Parse(c.WasabiEndpoint); err != nil {
			errs = append(errs, fmt.Errorf("invalid wasabi-endpoint URL: %w", err))
		}
	}

	if c.StateDir != "" {
		if err := os.MkdirAll(c.StateDir, 0755); err != nil {
			errs = append(errs, fmt.Errorf("cannot create state-dir: %w", err))
		}
		testFile := filepath.Join(c.StateDir, ".write-test")
		if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
			errs = append(errs, fmt.Errorf("state-dir not writable: %w", err))
		}
		os.Remove(testFile)
	}

	return errors.Join(errs...)
}

// ParseFlags parses command-line flags and environment variables.
func ParseFlags() (*Config, error) {
	cfg := &Config{
		Workers:    10,
		MaxRetries: 3,
		StateDir:   "./migration_state",
		LogLevel:   slog.LevelInfo,
	}

	flag.StringVar(&cfg.WasabiEndpoint, "wasabi-endpoint", "", "Wasabi S3 endpoint URL")
	flag.StringVar(&cfg.WasabiRegion, "wasabi-region", "", "Wasabi region")
	flag.StringVar(&cfg.WasabiAccessKey, "wasabi-access-key", os.Getenv("WASABI_ACCESS_KEY"), "Wasabi access key (or WASABI_ACCESS_KEY env)")
	flag.StringVar(&cfg.WasabiSecretKey, "wasabi-secret-key", os.Getenv("WASABI_SECRET_KEY"), "Wasabi secret key (or WASABI_SECRET_KEY env)")
	flag.StringVar(&cfg.WasabiBucket, "wasabi-bucket", "", "Source Wasabi bucket")
	flag.StringVar(&cfg.GCSProject, "gcs-project", "", "GCP project ID")
	flag.StringVar(&cfg.GCSBucket, "gcs-bucket", "", "Destination GCS bucket")
	flag.StringVar(&cfg.Prefix, "prefix", "", "Object prefix filter")
	flag.IntVar(&cfg.Workers, "workers", 10, "Concurrent workers (1-100)")
	flag.IntVar(&cfg.MaxRetries, "max-retries", 3, "Max retries per object (0-10)")
	flag.StringVar(&cfg.StateDir, "state-dir", "./migration_state", "State directory for resumability")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "List objects without transferring")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Show log lines in terminal alongside progress bars")
	flag.BoolVar(&cfg.Rescan, "rescan", false, "Force re-enumeration of source bucket, ignore cached manifest")

	var logLevel string
	flag.StringVar(&logLevel, "log-level", "info", "Log level (debug/info/warn/error)")

	flag.Parse()

	switch strings.ToLower(logLevel) {
	case "debug":
		cfg.LogLevel = slog.LevelDebug
	case "info":
		cfg.LogLevel = slog.LevelInfo
	case "warn":
		cfg.LogLevel = slog.LevelWarn
	case "error":
		cfg.LogLevel = slog.LevelError
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}
