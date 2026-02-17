package main

import "testing"

func TestParseGCSURL(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{
			name:       "bucket only",
			input:      "gs://my-bucket",
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			name:       "bucket with trailing slash",
			input:      "gs://my-bucket/",
			wantBucket: "my-bucket",
			wantPrefix: "",
		},
		{
			name:       "bucket with prefix",
			input:      "gs://my-bucket/state/",
			wantBucket: "my-bucket",
			wantPrefix: "state/",
		},
		{
			name:       "bucket with nested prefix",
			input:      "gs://my-bucket/migration/state/",
			wantBucket: "my-bucket",
			wantPrefix: "migration/state/",
		},
		{
			name:       "prefix without trailing slash gets one",
			input:      "gs://my-bucket/state",
			wantBucket: "my-bucket",
			wantPrefix: "state/",
		},
		{
			name:    "not gs scheme",
			input:   "s3://my-bucket/state/",
			wantErr: true,
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "gs:// with no bucket",
			input:   "gs://",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, prefix, err := ParseGCSURL(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseGCSURL(%q) expected error, got bucket=%q prefix=%q", tt.input, bucket, prefix)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseGCSURL(%q) unexpected error: %v", tt.input, err)
				return
			}
			if bucket != tt.wantBucket {
				t.Errorf("ParseGCSURL(%q) bucket = %q, want %q", tt.input, bucket, tt.wantBucket)
			}
			if prefix != tt.wantPrefix {
				t.Errorf("ParseGCSURL(%q) prefix = %q, want %q", tt.input, prefix, tt.wantPrefix)
			}
		})
	}
}
