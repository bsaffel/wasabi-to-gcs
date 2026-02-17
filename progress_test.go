package main

import (
	"testing"
	"time"
)

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{"sub-second", 500 * time.Millisecond, "0:00"},
		{"zero", 0, "0:00"},
		{"one second", 1 * time.Second, "0:01"},
		{"thirty seconds", 30 * time.Second, "0:30"},
		{"fifty-nine seconds", 59 * time.Second, "0:59"},
		{"one minute", 60 * time.Second, "1:00"},
		{"one minute thirty", 90 * time.Second, "1:30"},
		{"five minutes", 5 * time.Minute, "5:00"},
		{"twenty-five minutes nine seconds", 25*time.Minute + 9*time.Second, "25:09"},
		{"fifty-nine minutes fifty-nine seconds", 59*time.Minute + 59*time.Second, "59:59"},
		{"one hour", 60 * time.Minute, "1:00:00"},
		{"one hour five minutes", 65 * time.Minute, "1:05:00"},
		{"two hours thirty minutes", 150 * time.Minute, "2:30:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.duration)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.duration, got, tt.want)
			}
		})
	}
}

func TestFormatDurationDeterministic(t *testing.T) {
	// Verify formatDuration is deterministic: same input always produces
	// same output. This ensures the overall bar's elapsed time display
	// is stable (no jumping) since time.Since is monotonic.
	durations := []time.Duration{
		0,
		500 * time.Millisecond,
		1 * time.Second,
		30 * time.Second,
		59 * time.Second,
		60 * time.Second,
		90 * time.Second,
		5 * time.Minute,
		25*time.Minute + 9*time.Second,
		59*time.Minute + 59*time.Second,
		1 * time.Hour,
		1*time.Hour + 5*time.Minute,
		2*time.Hour + 30*time.Minute,
	}

	for _, d := range durations {
		first := formatDuration(d)
		for i := 0; i < 100; i++ {
			got := formatDuration(d)
			if got != first {
				t.Errorf("formatDuration(%v) not deterministic: got %q and %q", d, first, got)
				break
			}
		}
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		name  string
		bytes float64
		want  string
	}{
		{"zero bytes", 0, "       0 B"},
		{"small bytes", 512, "     512 B"},
		{"one KiB", 1024, "   1.0 KiB"},
		{"one MiB", 1024 * 1024, "   1.0 MiB"},
		{"one GiB", 1024 * 1024 * 1024, "   1.0 GiB"},
		{"mixed MiB", 1.5 * 1024 * 1024, "   1.5 MiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatSize(tt.bytes)
			if got != tt.want {
				t.Errorf("formatSize(%v) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}
