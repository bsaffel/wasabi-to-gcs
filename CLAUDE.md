# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
make build          # Build binary → ./wasabi-to-gcs
make run            # Build and run (go run .)
make test           # Run tests (short mode)
make test-race      # Run tests with race detector
make test-integration  # Run integration tests (requires -tags=integration)
make lint           # Run golangci-lint (errcheck, govet, staticcheck, unused, gosimple, ineffassign)
make tidy           # go mod tidy
make clean          # Remove build artifacts
```

## Architecture

Single-package (`main`) Go CLI tool that migrates objects from Wasabi S3 to Google Cloud Storage. Uses Cobra for CLI, lipgloss for styled terminal output, and mpb for progress bars.

**Data flow:** `main.go:runMigration` orchestrates the pipeline:
1. **Lister** (`lister.go`) — paginated S3 ListObjectsV2 scan, streams `ObjectJob`s into a bounded channel. Updates progress bar denominators per-page so workers start immediately.
2. **WorkerPool** (`pool.go`) — slot-based semaphore (`chan int`) with errgroup. Each slot ID maps to a reusable per-worker progress bar. Fatal errors (bad credentials, missing bucket) abort the pool; transient errors are logged and skipped.
3. **TransferEngine** (`transfer.go`) — streams each object Wasabi→GCS with MD5 verification, size checks, exponential backoff with jitter. Uses `sync.Pool` for 1MB copy buffers. Deletes partially-written GCS objects on integrity failure.
4. **StateManager** (`state.go`) — append-only `completed.log` (TSV: timestamp, key, size, md5) + `manifest.json` for scan totals. Enables resumability: re-runs skip already-completed keys.
5. **ProgressReporter** (`progress.go`) — mpb-based: one overall bar + dynamic per-file bars. stderr fd is dup'd so library noise (gRPC, GCS SDK) goes to log file while mpb renders to the real terminal.

**Speedtest mode** (`speedtest.go`) — `--speedtest` flag runs a worker scaling analysis: tests Wasabi download and GCS upload throughput at concurrency levels 1, 2, 4, 8, 16, 32 (with early stopping on plateau), identifies the bottleneck, and recommends optimal `--workers` count. Internet baseline via Cloudflare for context.

**Key design details:**
- GCS uses gRPC client with connection pooling (`storage.NewGRPCClient`), not JSON API
- Wasabi credentials: `--wasabi-access-key`/`--wasabi-secret-key` flags or `WASABI_ACCESS_KEY`/`WASABI_SECRET_KEY` env vars
- GCS auth uses Application Default Credentials (no explicit key flags)
- `Config.Validate()` in `config.go` checks all required fields and constraints before any work begins
- Multipart ETags (containing "-") skip MD5 verification since they aren't single-object MD5s
