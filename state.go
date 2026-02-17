package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StateManager tracks completed transfers using an in-memory map backed
// by an append-only file. All writes are mutex-protected.
type StateManager struct {
	completed map[string]int64 // key → size in bytes
	mu        sync.Mutex
	file      *os.File

	logger   *slog.Logger
	stateDir string
}

// NewStateManager creates a StateManager, loading any existing state from disk.
func NewStateManager(stateDir string, logger *slog.Logger) (*StateManager, error) {
	sm := &StateManager{
		completed: make(map[string]int64),
		logger:    logger,
		stateDir:  stateDir,
	}

	if err := sm.load(); err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}

	file, err := os.OpenFile(
		filepath.Join(stateDir, "completed.log"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("open state file: %w", err)
	}
	sm.file = file

	return sm, nil
}

// MarkCompleted records a completed transfer. Mutex-protected, writes directly to file.
func (sm *StateManager) MarkCompleted(key string, size int64, md5 string) {
	sm.mu.Lock()
	sm.completed[key] = size
	fmt.Fprintf(sm.file, "%s\t%s\t%d\t%s\n",
		time.Now().Format(time.RFC3339),
		key,
		size,
		md5,
	)
	sm.mu.Unlock()
}

// IsCompleted checks if a key has already been transferred.
func (sm *StateManager) IsCompleted(key string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, exists := sm.completed[key]
	return exists
}

// CompletedCount returns the number of completed objects.
func (sm *StateManager) CompletedCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.completed)
}

// CompletedCountForPrefix returns the number of completed objects whose keys
// start with the given prefix. An empty prefix matches all keys.
func (sm *StateManager) CompletedCountForPrefix(prefix string) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if prefix == "" {
		return len(sm.completed)
	}
	var count int
	for key := range sm.completed {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

// CompletedBytes returns the total bytes of all completed objects.
func (sm *StateManager) CompletedBytes() int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var total int64
	for _, size := range sm.completed {
		total += size
	}
	return total
}

// CompletedBytesForPrefix returns the total bytes of completed objects whose
// keys start with the given prefix. An empty prefix matches all keys.
func (sm *StateManager) CompletedBytesForPrefix(prefix string) int64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var total int64
	for key, size := range sm.completed {
		if prefix == "" || strings.HasPrefix(key, prefix) {
			total += size
		}
	}
	return total
}

// MarkMatchingAsCompleted adds destination-verified objects to the completed set
// without re-transferring. Returns the number of new entries added.
func (sm *StateManager) MarkMatchingAsCompleted(destObjects map[string]ObjectInfo) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	added := 0
	for key, obj := range destObjects {
		if _, exists := sm.completed[key]; !exists {
			sm.completed[key] = obj.Size
			fmt.Fprintf(sm.file, "%s\t%s\t%d\t%s\n",
				time.Now().Format(time.RFC3339), key, obj.Size, "dest-verified")
			added++
		}
	}
	return added
}

// Reset clears all completed state, truncating the log file and in-memory map.
func (sm *StateManager) Reset() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.completed = make(map[string]int64)
	if sm.file != nil {
		if err := sm.file.Truncate(0); err != nil {
			return fmt.Errorf("truncate state file: %w", err)
		}
		if _, err := sm.file.Seek(0, 0); err != nil {
			return fmt.Errorf("seek state file: %w", err)
		}
	}
	return nil
}

// Flush syncs the state file to disk without closing it.
func (sm *StateManager) Flush() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.file != nil {
		sm.file.Sync()
	}
}

// Close flushes and closes the state file.
func (sm *StateManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.file != nil {
		sm.file.Sync()
		return sm.file.Close()
	}
	return nil
}

// load reads the append-only state file into memory on startup.
func (sm *StateManager) load() error {
	path := filepath.Join(sm.stateDir, "completed.log")

	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // First run
		}
		return fmt.Errorf("open state file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "\t", 4)
		if len(parts) < 3 {
			continue // Skip malformed lines
		}
		key := parts[1]
		size, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			continue
		}
		sm.completed[key] = size
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan state file: %w", err)
	}

	sm.logger.Info("state loaded",
		slog.Int("completed_objects", len(sm.completed)),
	)
	return nil
}

// MigrationManifest captures totals from a completed source bucket scan.
type MigrationManifest struct {
	TotalObjects int64     `json:"total_objects"`
	TotalBytes   int64     `json:"total_bytes"`
	ScannedAt    time.Time `json:"scanned_at"`
	SourceBucket string    `json:"source_bucket"`
	Prefix       string    `json:"prefix"`
}

// SaveManifest writes the manifest atomically to {stateDir}/manifest.json.
func (sm *StateManager) SaveManifest(manifest *MigrationManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	path := filepath.Join(sm.stateDir, "manifest.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}

	sm.logger.Info("manifest saved",
		slog.Int64("total_objects", manifest.TotalObjects),
		slog.Int64("total_bytes", manifest.TotalBytes),
	)
	return nil
}

// LoadManifest reads the manifest from disk. Returns nil if not found (first run).
func (sm *StateManager) LoadManifest() (*MigrationManifest, error) {
	path := filepath.Join(sm.stateDir, "manifest.json")

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var manifest MigrationManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}

	sm.logger.Info("manifest loaded",
		slog.Int64("total_objects", manifest.TotalObjects),
		slog.Int64("total_bytes", manifest.TotalBytes),
		slog.Time("scanned_at", manifest.ScannedAt),
	)
	return &manifest, nil
}
