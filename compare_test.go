package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompareObjects(t *testing.T) {
	tests := []struct {
		name             string
		source           map[string]ObjectInfo
		dest             map[string]ObjectInfo
		wantMatching     int
		wantOnlyInSource int
		wantOnlyInDest   int
		wantSizeMismatch int
	}{
		{
			name: "all matching",
			source: map[string]ObjectInfo{
				"a.txt": {Key: "a.txt", Size: 100},
				"b.txt": {Key: "b.txt", Size: 200},
			},
			dest: map[string]ObjectInfo{
				"a.txt": {Key: "a.txt", Size: 100},
				"b.txt": {Key: "b.txt", Size: 200},
			},
			wantMatching: 2,
		},
		{
			name: "only in source",
			source: map[string]ObjectInfo{
				"a.txt": {Key: "a.txt", Size: 100},
				"b.txt": {Key: "b.txt", Size: 200},
			},
			dest:             map[string]ObjectInfo{},
			wantOnlyInSource: 2,
		},
		{
			name:   "only in dest",
			source: map[string]ObjectInfo{},
			dest: map[string]ObjectInfo{
				"a.txt": {Key: "a.txt", Size: 100},
			},
			wantOnlyInDest: 1,
		},
		{
			name: "size mismatch",
			source: map[string]ObjectInfo{
				"a.txt": {Key: "a.txt", Size: 100},
			},
			dest: map[string]ObjectInfo{
				"a.txt": {Key: "a.txt", Size: 200},
			},
			wantSizeMismatch: 1,
		},
		{
			name: "mixed",
			source: map[string]ObjectInfo{
				"match.txt":    {Key: "match.txt", Size: 100},
				"source.txt":   {Key: "source.txt", Size: 200},
				"mismatch.txt": {Key: "mismatch.txt", Size: 300},
			},
			dest: map[string]ObjectInfo{
				"match.txt":    {Key: "match.txt", Size: 100},
				"dest.txt":     {Key: "dest.txt", Size: 400},
				"mismatch.txt": {Key: "mismatch.txt", Size: 999},
			},
			wantMatching:     1,
			wantOnlyInSource: 1,
			wantOnlyInDest:   1,
			wantSizeMismatch: 1,
		},
		{
			name:   "both empty",
			source: map[string]ObjectInfo{},
			dest:   map[string]ObjectInfo{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareObjects(tt.source, tt.dest)

			if got := len(result.Matching); got != tt.wantMatching {
				t.Errorf("Matching: got %d, want %d", got, tt.wantMatching)
			}
			if got := len(result.OnlyInSource); got != tt.wantOnlyInSource {
				t.Errorf("OnlyInSource: got %d, want %d", got, tt.wantOnlyInSource)
			}
			if got := len(result.OnlyInDest); got != tt.wantOnlyInDest {
				t.Errorf("OnlyInDest: got %d, want %d", got, tt.wantOnlyInDest)
			}
			if got := len(result.SizeMismatch); got != tt.wantSizeMismatch {
				t.Errorf("SizeMismatch: got %d, want %d", got, tt.wantSizeMismatch)
			}
		})
	}
}

func TestCompareObjectsTotals(t *testing.T) {
	source := map[string]ObjectInfo{
		"a.txt": {Key: "a.txt", Size: 100},
		"b.txt": {Key: "b.txt", Size: 200},
	}
	dest := map[string]ObjectInfo{
		"a.txt": {Key: "a.txt", Size: 100},
		"c.txt": {Key: "c.txt", Size: 300},
	}

	result := compareObjects(source, dest)

	if result.SourceTotal != 2 {
		t.Errorf("SourceTotal: got %d, want 2", result.SourceTotal)
	}
	if result.SourceBytes != 300 {
		t.Errorf("SourceBytes: got %d, want 300", result.SourceBytes)
	}
	if result.DestTotal != 2 {
		t.Errorf("DestTotal: got %d, want 2", result.DestTotal)
	}
	if result.DestBytes != 400 {
		t.Errorf("DestBytes: got %d, want 400", result.DestBytes)
	}
}

func TestCompareObjectsSorted(t *testing.T) {
	source := map[string]ObjectInfo{
		"c.txt": {Key: "c.txt", Size: 100},
		"a.txt": {Key: "a.txt", Size: 100},
		"b.txt": {Key: "b.txt", Size: 100},
	}
	dest := map[string]ObjectInfo{}

	result := compareObjects(source, dest)

	if len(result.OnlyInSource) != 3 {
		t.Fatalf("expected 3 OnlyInSource, got %d", len(result.OnlyInSource))
	}
	if result.OnlyInSource[0].Key != "a.txt" || result.OnlyInSource[1].Key != "b.txt" || result.OnlyInSource[2].Key != "c.txt" {
		t.Errorf("OnlyInSource not sorted: %v", result.OnlyInSource)
	}
}

func TestMarkMatchingAsCompleted(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	sm, err := NewStateManager(dir, logger)
	if err != nil {
		t.Fatalf("NewStateManager: %v", err)
	}
	defer sm.Close()

	// Pre-mark one key as already completed
	sm.MarkCompleted("existing.txt", 100, "abc123")

	destObjects := map[string]ObjectInfo{
		"existing.txt": {Key: "existing.txt", Size: 100},
		"new1.txt":     {Key: "new1.txt", Size: 200},
		"new2.txt":     {Key: "new2.txt", Size: 300},
	}

	added := sm.MarkMatchingAsCompleted(destObjects)

	if added != 2 {
		t.Errorf("added: got %d, want 2", added)
	}

	// Verify all three are now completed
	if !sm.IsCompleted("existing.txt") {
		t.Error("existing.txt should be completed")
	}
	if !sm.IsCompleted("new1.txt") {
		t.Error("new1.txt should be completed")
	}
	if !sm.IsCompleted("new2.txt") {
		t.Error("new2.txt should be completed")
	}

	// Verify the log file contains dest-verified entries
	data, err := os.ReadFile(filepath.Join(dir, "completed.log"))
	if err != nil {
		t.Fatalf("read completed.log: %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "dest-verified") {
		t.Error("completed.log should contain dest-verified entries")
	}
	if !strings.Contains(content, "new1.txt") {
		t.Error("completed.log should contain new1.txt")
	}
}

func TestMarkMatchingAsCompletedEmpty(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	sm, err := NewStateManager(dir, logger)
	if err != nil {
		t.Fatalf("NewStateManager: %v", err)
	}
	defer sm.Close()

	added := sm.MarkMatchingAsCompleted(map[string]ObjectInfo{})
	if added != 0 {
		t.Errorf("added: got %d, want 0", added)
	}
}
