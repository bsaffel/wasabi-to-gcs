package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/grpclog"
)

// ObjectInfo describes an object in either source or destination.
type ObjectInfo struct {
	Key  string
	Size int64
	MD5  string // hex-encoded; empty if unavailable
}

// CompareResult holds the diff between source and destination.
type CompareResult struct {
	Matching     []ObjectInfo
	OnlyInSource []ObjectInfo
	OnlyInDest   []ObjectInfo
	SizeMismatch []SizeMismatchEntry
	SourceTotal  int64
	SourceBytes  int64
	DestTotal    int64
	DestBytes    int64
}

// SizeMismatchEntry records an object present in both but with different sizes.
type SizeMismatchEntry struct {
	Key        string
	SourceSize int64
	DestSize   int64
}

// runCompare is the top-level entry point for --compare mode.
func runCompare(cfg *Config) error {
	// Suppress gRPC and stdlib logging to prevent connection setup/teardown
	// writes from corrupting stderr output (same as runMigration does).
	log.SetOutput(io.Discard)
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Fprintln(os.Stderr, titleStyle.Render("Compare: Wasabi vs GCS"))
	fmt.Fprintln(os.Stderr)

	source := fmt.Sprintf("wasabi://%s/%s", cfg.WasabiBucket, cfg.Prefix)
	dest := fmt.Sprintf("gs://%s/%s", cfg.GCSBucket, cfg.Prefix)
	fmt.Fprintf(os.Stderr, "  %s  %s\n", labelStyle.Render("Source:"), valueStyle.Render(source))
	fmt.Fprintf(os.Stderr, "  %s  %s\n", labelStyle.Render("Destination:"), valueStyle.Render(dest))
	fmt.Fprintln(os.Stderr)

	// Create clients
	wasabiClient, err := newWasabiClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create wasabi client: %w", err)
	}

	gcsClient, err := newGCSClient(ctx, cfg.Workers)
	if err != nil {
		return fmt.Errorf("create gcs client: %w", err)
	}
	defer gcsClient.Close()

	// List both concurrently with progress updates
	var sourceObjects, destObjects map[string]ObjectInfo
	var sourceCount, destCount atomic.Int64

	// Print scanning status periodically
	statusDone := make(chan struct{})
	var statusMu sync.Mutex
	var statusDoneFlag bool
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				statusMu.Lock()
				if statusDoneFlag {
					statusMu.Unlock()
					return
				}
				statusMu.Unlock()
				fmt.Fprintf(os.Stderr, "\r  %s Wasabi: %s objects  |  GCS: %s objects",
					dimStyle.Render("Scanning..."),
					valueStyle.Render(humanize(sourceCount.Load())),
					valueStyle.Render(humanize(destCount.Load())))
			case <-statusDone:
				return
			}
		}
	}()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		sourceObjects, err = listWasabiObjects(gctx, wasabiClient, cfg.WasabiBucket, cfg.Prefix, &sourceCount)
		return err
	})

	g.Go(func() error {
		var err error
		destObjects, err = listGCSObjects(gctx, gcsClient, cfg.GCSBucket, cfg.Prefix, &destCount)
		return err
	})

	if err := g.Wait(); err != nil {
		statusMu.Lock()
		statusDoneFlag = true
		statusMu.Unlock()
		close(statusDone)
		fmt.Fprintln(os.Stderr) // clear status line
		fmt.Fprintf(os.Stderr, "\n%s %s\n\n", errorStyle.Render("Error:"), valueStyle.Render(err.Error()))
		return err
	}

	statusMu.Lock()
	statusDoneFlag = true
	statusMu.Unlock()
	close(statusDone)
	fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", 80)) // clear status line

	result := compareObjects(sourceObjects, destObjects)
	printCompareReport(result, cfg)

	return nil
}

// listGCSObjects lists all objects in a GCS bucket/prefix into a map.
func listGCSObjects(ctx context.Context, client *storage.Client, bucket, prefix string, counter *atomic.Int64) (map[string]ObjectInfo, error) {
	objects := make(map[string]ObjectInfo)

	query := &storage.Query{Prefix: prefix}
	query.SetAttrSelection([]string{"Name", "Size", "MD5Hash"})

	it := client.Bucket(bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list GCS objects: %w", err)
		}

		// Skip zero-size objects (folders)
		if attrs.Size == 0 {
			continue
		}

		obj := ObjectInfo{
			Key:  attrs.Name,
			Size: attrs.Size,
		}
		if len(attrs.MD5) > 0 {
			obj.MD5 = fmt.Sprintf("%x", attrs.MD5)
		}
		objects[attrs.Name] = obj
		if counter != nil {
			counter.Add(1)
		}
	}

	return objects, nil
}

// listWasabiObjects lists all objects in a Wasabi bucket/prefix into a map.
func listWasabiObjects(ctx context.Context, client *s3.Client, bucket, prefix string, counter *atomic.Int64) (map[string]ObjectInfo, error) {
	objects := make(map[string]ObjectInfo)

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list Wasabi objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Size == nil || *obj.Size == 0 {
				continue
			}

			key := *obj.Key
			info := ObjectInfo{
				Key:  key,
				Size: *obj.Size,
			}
			if obj.ETag != nil {
				etag := strings.Trim(*obj.ETag, "\"")
				// Only use ETag as MD5 if it's not a multipart upload
				if !strings.Contains(etag, "-") {
					info.MD5 = etag
				}
			}
			objects[key] = info
			if counter != nil {
				counter.Add(1)
			}
		}
	}

	return objects, nil
}

// compareObjects is a pure function that diffs source and dest by key+size.
func compareObjects(source, dest map[string]ObjectInfo) *CompareResult {
	result := &CompareResult{}

	for key, src := range source {
		result.SourceTotal++
		result.SourceBytes += src.Size

		if dst, exists := dest[key]; exists {
			if src.Size == dst.Size {
				result.Matching = append(result.Matching, src)
			} else {
				result.SizeMismatch = append(result.SizeMismatch, SizeMismatchEntry{
					Key:        key,
					SourceSize: src.Size,
					DestSize:   dst.Size,
				})
			}
		} else {
			result.OnlyInSource = append(result.OnlyInSource, src)
		}
	}

	for key, dst := range dest {
		result.DestTotal++
		result.DestBytes += dst.Size

		if _, exists := source[key]; !exists {
			result.OnlyInDest = append(result.OnlyInDest, dst)
		}
	}

	// Sort for deterministic output
	sort.Slice(result.OnlyInSource, func(i, j int) bool { return result.OnlyInSource[i].Key < result.OnlyInSource[j].Key })
	sort.Slice(result.OnlyInDest, func(i, j int) bool { return result.OnlyInDest[i].Key < result.OnlyInDest[j].Key })
	sort.Slice(result.SizeMismatch, func(i, j int) bool { return result.SizeMismatch[i].Key < result.SizeMismatch[j].Key })

	return result
}

// printCompareReport prints a styled comparison report to stderr.
func printCompareReport(result *CompareResult, cfg *Config) {
	fmt.Fprintln(os.Stderr, titleStyle.Render("Results"))
	fmt.Fprintln(os.Stderr)

	fmt.Fprintf(os.Stderr, "  %s  %s  (%s)\n",
		labelStyle.Render("Source:"),
		valueStyle.Render(humanize(result.SourceTotal)+" objects"),
		valueStyle.Render(humanizeBytes(result.SourceBytes)))

	fmt.Fprintf(os.Stderr, "  %s  %s  (%s)\n",
		labelStyle.Render("Destination:"),
		valueStyle.Render(humanize(result.DestTotal)+" objects"),
		valueStyle.Render(humanizeBytes(result.DestBytes)))

	fmt.Fprintln(os.Stderr)

	// Matching
	var matchingBytes int64
	for _, obj := range result.Matching {
		matchingBytes += obj.Size
	}
	fmt.Fprintf(os.Stderr, "  %s  %s  (%s)\n",
		labelStyle.Render("Matching:"),
		successStyle.Render(humanize(int64(len(result.Matching)))),
		successStyle.Render(humanizeBytes(matchingBytes)))

	// Need transfer (only in source)
	var needTransferBytes int64
	for _, obj := range result.OnlyInSource {
		needTransferBytes += obj.Size
	}
	fmt.Fprintf(os.Stderr, "  %s  %s  (%s)\n",
		labelStyle.Render("Need transfer:"),
		warnStyle.Render(humanize(int64(len(result.OnlyInSource)))),
		warnStyle.Render(humanizeBytes(needTransferBytes)))

	// Only in dest
	var onlyDestBytes int64
	for _, obj := range result.OnlyInDest {
		onlyDestBytes += obj.Size
	}
	fmt.Fprintf(os.Stderr, "  %s  %s  (%s)\n",
		labelStyle.Render("Only in dest:"),
		dimStyle.Render(humanize(int64(len(result.OnlyInDest)))),
		dimStyle.Render(humanizeBytes(onlyDestBytes)))

	// Size mismatch
	if len(result.SizeMismatch) > 0 {
		fmt.Fprintf(os.Stderr, "  %s  %s\n",
			labelStyle.Render("Size mismatch:"),
			errorStyle.Render(humanize(int64(len(result.SizeMismatch)))))
	} else {
		fmt.Fprintf(os.Stderr, "  %s  %s\n",
			labelStyle.Render("Size mismatch:"),
			dimStyle.Render("0"))
	}

	fmt.Fprintln(os.Stderr)

	// Verbose: show file details
	if cfg.Verbose {
		if len(result.OnlyInSource) > 0 {
			fmt.Fprintln(os.Stderr, warnStyle.Render("  Need transfer:"))
			printObjectList(result.OnlyInSource, 20)
		}

		if len(result.OnlyInDest) > 0 {
			fmt.Fprintln(os.Stderr, dimStyle.Render("  Only in destination:"))
			printObjectList(result.OnlyInDest, 20)
		}
	}

	// Always show size mismatches (integrity concern)
	if len(result.SizeMismatch) > 0 {
		fmt.Fprintln(os.Stderr, errorStyle.Render("  Size mismatches:"))
		limit := 20
		for i, entry := range result.SizeMismatch {
			if i >= limit {
				fmt.Fprintf(os.Stderr, "    %s\n",
					dimStyle.Render(fmt.Sprintf("... and %d more", len(result.SizeMismatch)-limit)))
				break
			}
			fmt.Fprintf(os.Stderr, "    %s  %s %s %s\n",
				valueStyle.Render(entry.Key),
				dimStyle.Render("source:"),
				valueStyle.Render(humanizeBytes(entry.SourceSize)),
				dimStyle.Render(fmt.Sprintf("dest: %s", humanizeBytes(entry.DestSize))))
		}
		fmt.Fprintln(os.Stderr)
	}
}

func printObjectList(objects []ObjectInfo, limit int) {
	for i, obj := range objects {
		if i >= limit {
			fmt.Fprintf(os.Stderr, "    %s\n",
				dimStyle.Render(fmt.Sprintf("... and %d more", len(objects)-limit)))
			break
		}
		fmt.Fprintf(os.Stderr, "    %s  %s\n",
			valueStyle.Render(obj.Key),
			dimStyle.Render(humanizeBytes(obj.Size)))
	}
	fmt.Fprintln(os.Stderr)
}
