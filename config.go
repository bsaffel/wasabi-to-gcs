package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
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
	Speedtest  bool

	// Observability
	Logger   *slog.Logger
	LogLevel slog.Level

	// Console behavior
	Verbose bool
	Rescan  bool
	Force   bool
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

	if c.Workers < 1 || c.Workers > 128 {
		errs = append(errs, fmt.Errorf("workers must be 1-128, got %d", c.Workers))
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

