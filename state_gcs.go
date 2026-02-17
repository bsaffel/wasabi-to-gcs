package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// stateFiles are the files synced to/from GCS.
var stateFiles = []string{"completed.log", "manifest.json"}

// GCSStateSync downloads and uploads migration state files between a local
// directory and a GCS prefix. The existing StateManager always operates on
// local files; this layer wraps it with cloud persistence.
type GCSStateSync struct {
	bucket   string
	prefix   string
	stateDir string
	client   *storage.Client
	logger   *slog.Logger
}

// ParseGCSURL parses a gs://bucket/prefix/ URL into bucket and prefix.
func ParseGCSURL(raw string) (bucket, prefix string, err error) {
	if !strings.HasPrefix(raw, "gs://") {
		return "", "", fmt.Errorf("state-gcs must start with gs://, got %q", raw)
	}

	path := strings.TrimPrefix(raw, "gs://")
	if path == "" {
		return "", "", fmt.Errorf("state-gcs bucket name is empty")
	}

	parts := strings.SplitN(path, "/", 2)
	bucket = parts[0]
	if bucket == "" {
		return "", "", fmt.Errorf("state-gcs bucket name is empty")
	}

	if len(parts) == 2 {
		prefix = parts[1]
	}

	// Ensure prefix ends with / if non-empty (so object paths are clean)
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return bucket, prefix, nil
}

// NewGCSStateSync creates a GCSStateSync that syncs state between stateDir
// and gs://bucket/prefix/. It creates its own lightweight JSON-API GCS client.
func NewGCSStateSync(ctx context.Context, gcsURL, stateDir string, logger *slog.Logger) (*GCSStateSync, error) {
	bucket, prefix, err := ParseGCSURL(gcsURL)
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create gcs state client: %w", err)
	}

	return &GCSStateSync{
		bucket:   bucket,
		prefix:   prefix,
		stateDir: stateDir,
		client:   client,
		logger:   logger,
	}, nil
}

// Download fetches state files from GCS into the local state directory.
// Missing files in GCS are silently skipped (expected on first run).
func (s *GCSStateSync) Download(ctx context.Context) error {
	for _, name := range stateFiles {
		obj := s.client.Bucket(s.bucket).Object(s.prefix + name)
		r, err := obj.NewReader(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrObjectNotExist) {
				s.logger.Debug("state file not found in GCS, skipping", slog.String("file", name))
				continue
			}
			return fmt.Errorf("download %s from GCS: %w", name, err)
		}

		localPath := filepath.Join(s.stateDir, name)
		f, err := os.Create(localPath)
		if err != nil {
			r.Close()
			return fmt.Errorf("create local %s: %w", name, err)
		}

		if _, err := io.Copy(f, r); err != nil {
			f.Close()
			r.Close()
			return fmt.Errorf("write local %s: %w", name, err)
		}

		f.Close()
		r.Close()
		s.logger.Info("downloaded state from GCS", slog.String("file", name))
	}
	return nil
}

// Upload sends state files from the local state directory to GCS.
func (s *GCSStateSync) Upload(ctx context.Context) error {
	for _, name := range stateFiles {
		localPath := filepath.Join(s.stateDir, name)
		f, err := os.Open(localPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("open local %s: %w", name, err)
		}

		obj := s.client.Bucket(s.bucket).Object(s.prefix + name)
		w := obj.NewWriter(ctx)

		if _, err := io.Copy(w, f); err != nil {
			w.Close()
			f.Close()
			return fmt.Errorf("upload %s to GCS: %w", name, err)
		}

		if err := w.Close(); err != nil {
			f.Close()
			return fmt.Errorf("finalize %s upload: %w", name, err)
		}

		f.Close()
	}
	s.logger.Debug("uploaded state to GCS")
	return nil
}

// StartPeriodicSync launches a goroutine that uploads state to GCS at the
// given interval. The returned stop function cancels the goroutine and
// performs a final upload.
func (s *GCSStateSync) StartPeriodicSync(ctx context.Context, interval time.Duration, flushFn func()) func() {
	syncCtx, syncCancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-syncCtx.Done():
				return
			case <-ticker.C:
				if flushFn != nil {
					flushFn()
				}
				if err := s.Upload(syncCtx); err != nil {
					s.logger.Warn("periodic state sync failed", slog.String("error", err.Error()))
				}
			}
		}
	}()

	return func() {
		syncCancel()
		<-done

		// Final upload with a fresh context (parent may be cancelled)
		finalCtx, finalCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer finalCancel()

		if flushFn != nil {
			flushFn()
		}
		if err := s.Upload(finalCtx); err != nil {
			s.logger.Error("final state sync to GCS failed — remote state may be stale",
				slog.String("error", err.Error()))
		} else {
			s.logger.Info("final state synced to GCS")
		}
	}
}

// Close closes the underlying GCS client.
func (s *GCSStateSync) Close() error {
	return s.client.Close()
}
