package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// ObjectGetter is a narrow interface for testability — *s3.Client satisfies it.
type ObjectGetter interface {
	GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// TransferEngine handles streaming transfers from Wasabi to GCS with retries.
type TransferEngine struct {
	wasabi     ObjectGetter
	gcs        *storage.Client
	state      *StateManager
	progress   *ProgressReporter
	bufferPool *sync.Pool
	config     *Config
	logger     *slog.Logger

	maxRetries  int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

// NewTransferEngine creates a TransferEngine with the given configuration.
func NewTransferEngine(cfg *Config) *TransferEngine {
	return &TransferEngine{
		maxRetries:  cfg.MaxRetries,
		baseBackoff: 1 * time.Second,
		maxBackoff:  30 * time.Second,
		bufferPool: &sync.Pool{
			New: func() any {
				buf := make([]byte, 1*1024*1024) // 1MB buffers
				return &buf
			},
		},
		config: cfg,
		logger: cfg.Logger,
	}
}

// Transfer executes a transfer with retry logic.
func (te *TransferEngine) Transfer(ctx context.Context, workerID int, job ObjectJob) error {
	var lastErr error

	for attempt := 0; attempt <= te.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := te.calculateBackoff(attempt)
			te.logger.Debug("retrying transfer",
				slog.String("key", job.Key),
				slog.Int("attempt", attempt+1),
				slog.Duration("backoff", backoff),
			)

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := te.doTransfer(ctx, workerID, job)
		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryable(err) {
			return fmt.Errorf("non-retryable error for %s: %w", job.Key, err)
		}

		te.logger.Warn("transfer attempt failed",
			slog.String("key", job.Key),
			slog.Int("attempt", attempt+1),
			slog.Int("max_attempts", te.maxRetries+1),
			slog.Any("error", err),
		)
	}

	return fmt.Errorf("all %d attempts failed for %s: %w",
		te.maxRetries+1, job.Key, lastErr)
}

// calculateBackoff returns exponential backoff with full jitter.
func (te *TransferEngine) calculateBackoff(attempt int) time.Duration {
	backoff := te.baseBackoff * time.Duration(1<<uint(attempt-1))
	if backoff > te.maxBackoff {
		backoff = te.maxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(backoff)))
	return jitter
}

// doTransfer performs a single streaming transfer attempt.
func (te *TransferEngine) doTransfer(ctx context.Context, workerID int, job ObjectJob) error {
	// 1. Get object from Wasabi
	result, err := te.wasabi.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &te.config.WasabiBucket,
		Key:    &job.Key,
	})
	if err != nil {
		return fmt.Errorf("get from wasabi: %w", err)
	}
	defer result.Body.Close()

	// 2. Create GCS writer
	obj := te.gcs.Bucket(te.config.GCSBucket).Object(job.Key)
	writer := obj.NewWriter(ctx)
	writer.ChunkSize = 16 * 1024 * 1024 // 16MB chunks for resumable upload

	// Preserve content type if available
	if result.ContentType != nil {
		writer.ContentType = *result.ContentType
	}

	// 3. Get buffer from shared pool
	bufPtr := te.bufferPool.Get().(*[]byte)
	buf := *bufPtr
	defer te.bufferPool.Put(bufPtr)

	// 4. Stream with progress tracking and hashing
	hasher := md5.New()
	progressReader := te.progress.WrapReader(result.Body, workerID)

	// TeeReader: read once, write to both hasher and GCS
	teeReader := io.TeeReader(progressReader, hasher)

	written, copyErr := io.CopyBuffer(writer, teeReader, buf)

	// 5. CRITICAL: Always check Close() — this commits the upload
	closeErr := writer.Close()

	if copyErr != nil {
		return fmt.Errorf("copy data: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("finalize gcs upload: %w", closeErr)
	}

	// 6. Verify size
	if written != job.Size {
		if err := obj.Delete(ctx); err != nil {
			te.logger.Warn("failed to delete incomplete object",
				slog.String("key", job.Key),
				slog.Any("error", err),
			)
		}
		return fmt.Errorf("size mismatch: expected %d, wrote %d", job.Size, written)
	}

	// 7. Verify MD5 (skip multipart ETags containing "-")
	actualMD5 := hex.EncodeToString(hasher.Sum(nil))
	expectedMD5 := strings.Trim(job.ETag, "\"")

	if !strings.Contains(expectedMD5, "-") && actualMD5 != expectedMD5 {
		if err := obj.Delete(ctx); err != nil {
			te.logger.Warn("failed to delete corrupt object",
				slog.String("key", job.Key),
				slog.Any("error", err),
			)
		}
		return fmt.Errorf("md5 mismatch: expected %s, got %s", expectedMD5, actualMD5)
	}

	// 8. Mark completed
	te.state.MarkCompleted(job.Key, written, actualMD5)

	return nil
}

// isRetryable determines if an error should trigger a retry.
func isRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() // nolint: staticcheck // Temporary() is deprecated but Timeout() is fine
	}

	// AWS SDK errors
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "500", "502", "503", "504":
			return true
		case "429":
			return true
		case "RequestTimeout", "RequestTimeoutException":
			return true
		case "SlowDown":
			return true
		}
	}

	// GCS 5xx errors
	if strings.Contains(err.Error(), "googleapi: Error 5") {
		return true
	}

	return false
}
