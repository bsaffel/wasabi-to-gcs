package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/sync/errgroup"
)

const (
	speedtestGCSPrefix         = "__speedtest/"
	speedtestMaxObject         = 100 * 1024 * 1024 // 100 MiB cap for test downloads
	speedtestGCSChunkSize      = 16 * 1024 * 1024  // 16 MiB, matches transfer.go
	speedtestTimeout           = 5 * time.Minute
	speedtestPerWorkerDownload = 5 * 1024 * 1024 // 5 MiB per worker for download tests
	speedtestPerWorkerUpload   = 3 * 1024 * 1024 // 3 MiB per worker for upload tests
	scalingGainThreshold       = 0.10            // 10% improvement needed to recommend more workers
	stallThreshold             = 0.05            // 5% improvement threshold for early stopping
	stallCountForStop          = 2               // consecutive stalled levels before stopping
)

var scalingLevels = []int{1, 2, 4, 8, 16, 32}

// ScalingPoint records aggregate throughput at a given concurrency level.
type ScalingPoint struct {
	Workers    int
	Throughput float64 // bytes/sec
}

// SpeedtestResults holds measurements from the worker scaling analysis.
type SpeedtestResults struct {
	WasabiScaling []ScalingPoint
	GCSScaling    []ScalingPoint
	InternetBPS   float64

	WasabiSkipReason   string
	GCSSkipReason      string
	InternetSkipReason string

	TestObjectKey  string
	TestObjectSize int64

	RecommendedWorkers int
	RecommendReason    string
}

// zeroReader is an io.Reader that returns zero bytes.
type zeroReader struct {
	remaining int64
}

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.remaining <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > z.remaining {
		n = z.remaining
	}
	for i := int64(0); i < n; i++ {
		p[i] = 0
	}
	z.remaining -= n
	return int(n), nil
}

func runSpeedtest(cfg *Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), speedtestTimeout)
	defer cancel()

	// Print banner
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, titleStyle.Render("Speed Test"))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("Wasabi:"),
		valueStyle.Render(fmt.Sprintf("wasabi://%s/%s", cfg.WasabiBucket, cfg.Prefix)))
	fmt.Fprintf(os.Stderr, "  %s  %s\n",
		labelStyle.Render("GCS:"),
		valueStyle.Render(fmt.Sprintf("gs://%s/", cfg.GCSBucket)))
	fmt.Fprintln(os.Stderr)

	results := &SpeedtestResults{}

	// Create clients
	wasabiClient, err := newWasabiClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create wasabi client: %w", err)
	}

	gcsClient, err := newGCSClient(ctx)
	if err != nil {
		return fmt.Errorf("create gcs client: %w", err)
	}
	defer gcsClient.Close()

	// Find test object for Wasabi downloads
	testKey, testSize, err := findTestObject(ctx, wasabiClient, cfg.WasabiBucket, cfg.Prefix)
	if err != nil {
		results.WasabiSkipReason = err.Error()
		fmt.Fprintf(os.Stderr, "  %s %s\n",
			warnStyle.Render("Wasabi tests skipped:"),
			dimStyle.Render(err.Error()))
	} else {
		results.TestObjectKey = testKey
		results.TestObjectSize = testSize
		fmt.Fprintf(os.Stderr, "  %s  %s (%s)\n",
			dimStyle.Render("Test object:"),
			dimStyle.Render(testKey),
			dimStyle.Render(humanizeBytes(testSize)))

		if testSize < 1*1024*1024 {
			fmt.Fprintf(os.Stderr, "  %s\n",
				warnStyle.Render("Object < 1 MiB — download throughput may be understated"))
		}
	}

	// Run worker scaling sweep
	fmt.Fprintln(os.Stderr)
	if results.WasabiSkipReason != "" && results.GCSSkipReason != "" {
		fmt.Fprintf(os.Stderr, "  %s\n",
			warnStyle.Render("Cannot run scaling analysis — both Wasabi and GCS tests unavailable"))
	} else {
		runScalingSweep(ctx, results, wasabiClient, gcsClient, cfg)
	}

	// Internet baseline
	fmt.Fprintf(os.Stderr, "  Testing internet baseline... ")
	ibps, err := testInternetBaseline(ctx)
	if err != nil {
		results.InternetSkipReason = err.Error()
		fmt.Fprintln(os.Stderr, dimStyle.Render("skipped ("+err.Error()+")"))
	} else {
		results.InternetBPS = ibps
		fmt.Fprintln(os.Stderr, valueStyle.Render(formatSpeed(ibps)))
	}

	fmt.Fprintln(os.Stderr)

	// Compute recommendation
	results.RecommendedWorkers, results.RecommendReason = findOptimalWorkers(results)

	// Print summary
	printSpeedtestSummary(results, cfg.Workers)

	return nil
}

// runScalingSweep tests Wasabi download and GCS upload throughput at increasing
// concurrency levels and prints a table as each level completes.
func runScalingSweep(ctx context.Context, results *SpeedtestResults, wasabiClient *s3.Client, gcsClient *storage.Client, cfg *Config) {
	perWorkerDown := int64(speedtestPerWorkerDownload)
	if results.TestObjectSize > 0 && perWorkerDown > results.TestObjectSize {
		perWorkerDown = results.TestObjectSize
	}

	hasWasabi := results.WasabiSkipReason == ""
	hasGCS := results.GCSSkipReason == ""

	fmt.Fprintln(os.Stderr, "  "+titleStyle.Render("Worker Scaling"))
	fmt.Fprintln(os.Stderr)

	// Print table header
	hdr := fmt.Sprintf("  %7s", "Workers")
	if hasWasabi {
		hdr += fmt.Sprintf("  %14s", "Wasabi Down")
	}
	if hasGCS {
		hdr += fmt.Sprintf("  %14s", "GCS Up")
	}
	hdr += fmt.Sprintf("  %14s  %6s", "Effective", "Gain")
	fmt.Fprintln(os.Stderr, dimStyle.Render(hdr))

	stallCount := 0

	for _, n := range scalingLevels {
		var wasabiBPS, gcsBPS float64

		// Test Wasabi download at this concurrency
		if hasWasabi {
			bps, err := testWasabiAtConcurrency(ctx, wasabiClient,
				cfg.WasabiBucket, results.TestObjectKey, perWorkerDown, n)
			if err != nil {
				hasWasabi = false
				results.WasabiSkipReason = err.Error()
			} else {
				wasabiBPS = bps
			}
		}

		// Test GCS upload at this concurrency
		if hasGCS {
			bps, err := testGCSAtConcurrency(ctx, gcsClient,
				cfg.GCSBucket, int64(speedtestPerWorkerUpload), n)
			if err != nil {
				hasGCS = false
				results.GCSSkipReason = err.Error()
			} else {
				gcsBPS = bps
			}
		}

		results.WasabiScaling = append(results.WasabiScaling, ScalingPoint{n, wasabiBPS})
		results.GCSScaling = append(results.GCSScaling, ScalingPoint{n, gcsBPS})

		effective := minPositive(wasabiBPS, gcsBPS)

		// Gain vs previous level
		gainStr := "     —"
		idx := len(results.WasabiScaling)
		if idx >= 2 {
			prevEffective := minPositive(
				results.WasabiScaling[idx-2].Throughput,
				results.GCSScaling[idx-2].Throughput)
			if prevEffective > 0 {
				gain := (effective - prevEffective) / prevEffective * 100
				gainStr = fmt.Sprintf("%+5.0f%%", gain)
			}
		}

		// Print row
		row := fmt.Sprintf("  %7d", n)
		if results.WasabiSkipReason == "" || wasabiBPS > 0 {
			if wasabiBPS > 0 {
				row += fmt.Sprintf("  %14s", formatSpeed(wasabiBPS))
			} else {
				row += fmt.Sprintf("  %14s", "—")
			}
		}
		if results.GCSSkipReason == "" || gcsBPS > 0 {
			if gcsBPS > 0 {
				row += fmt.Sprintf("  %14s", formatSpeed(gcsBPS))
			} else {
				row += fmt.Sprintf("  %14s", "—")
			}
		}
		row += fmt.Sprintf("  %14s  %6s", formatSpeed(effective), gainStr)
		fmt.Fprintln(os.Stderr, row)

		// Early stop: 2 consecutive levels with < 5% improvement
		if idx >= 2 {
			prevEffective := minPositive(
				results.WasabiScaling[idx-2].Throughput,
				results.GCSScaling[idx-2].Throughput)
			if prevEffective > 0 {
				gain := (effective - prevEffective) / prevEffective
				if gain < stallThreshold {
					stallCount++
				} else {
					stallCount = 0
				}
			}
		}

		if stallCount >= stallCountForStop {
			fmt.Fprintf(os.Stderr, "  %s\n",
				dimStyle.Render("(stopped — throughput plateaued)"))
			break
		}

		// Stop if both legs failed mid-sweep
		if !hasWasabi && !hasGCS {
			break
		}
	}

	fmt.Fprintln(os.Stderr)
}

// testWasabiAtConcurrency measures aggregate Wasabi download throughput with
// N workers each downloading perWorkerSize bytes from the test object.
func testWasabiAtConcurrency(ctx context.Context, client *s3.Client, bucket, key string, perWorkerSize int64, workers int) (float64, error) {
	var totalBytes atomic.Int64

	rangeStr := fmt.Sprintf("bytes=0-%d", perWorkerSize-1)

	start := time.Now()
	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < workers; i++ {
		g.Go(func() error {
			output, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &key,
				Range:  &rangeStr,
			})
			if err != nil {
				return err
			}
			defer output.Body.Close()

			n, err := io.Copy(io.Discard, output.Body)
			totalBytes.Add(n)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	elapsed := time.Since(start).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	return float64(totalBytes.Load()) / elapsed, nil
}

// testGCSAtConcurrency measures aggregate GCS upload throughput with N workers
// each uploading perWorkerSize bytes. All test objects are cleaned up via defer.
func testGCSAtConcurrency(ctx context.Context, client *storage.Client, bucket string, perWorkerSize int64, workers int) (float64, error) {
	var totalBytes atomic.Int64
	objNames := make([]string, workers)

	defer func() {
		for _, name := range objNames {
			if name != "" {
				client.Bucket(bucket).Object(name).Delete(context.Background()) //nolint:errcheck
			}
		}
	}()

	start := time.Now()
	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < workers; i++ {
		objName := fmt.Sprintf("%sscale-%d-%d", speedtestGCSPrefix, time.Now().UnixNano(), i)
		objNames[i] = objName

		g.Go(func() error {
			obj := client.Bucket(bucket).Object(objName)
			w := obj.NewWriter(ctx)
			w.ChunkSize = speedtestGCSChunkSize

			n, err := io.Copy(w, &zeroReader{remaining: perWorkerSize})
			if err != nil {
				w.Close() //nolint:errcheck
				return err
			}

			if err := w.Close(); err != nil {
				return err
			}

			totalBytes.Add(n)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	elapsed := time.Since(start).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	return float64(totalBytes.Load()) / elapsed, nil
}

// findTestObject lists the first page of the bucket and picks the largest object (capped at 100 MiB).
func findTestObject(ctx context.Context, client *s3.Client, bucket, prefix string) (string, int64, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  &bucket,
		Prefix:  &prefix,
		MaxKeys: aws.Int32(1000),
	}

	output, err := client.ListObjectsV2(ctx, input)
	if err != nil {
		return "", 0, fmt.Errorf("list bucket: %w", err)
	}

	var bestKey string
	var bestSize int64

	for _, obj := range output.Contents {
		if obj.Size == nil || *obj.Size == 0 || obj.Key == nil {
			continue
		}
		if *obj.Size > bestSize {
			bestKey = *obj.Key
			bestSize = *obj.Size
		}
	}

	if bestKey == "" {
		return "", 0, fmt.Errorf("no objects found in wasabi://%s/%s", bucket, prefix)
	}

	if bestSize > speedtestMaxObject {
		bestSize = speedtestMaxObject
	}

	return bestKey, bestSize, nil
}

// testInternetBaseline downloads 10 MB from speed.cloudflare.com. Skips gracefully on failure.
func testInternetBaseline(ctx context.Context) (float64, error) {
	url := "https://speed.cloudflare.com/__down?bytes=10000000"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}

	start := time.Now()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("cloudflare unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("cloudflare returned %d", resp.StatusCode)
	}

	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return 0, err
	}

	elapsed := time.Since(start).Seconds()
	if elapsed < 0.001 {
		elapsed = 0.001
	}

	return float64(n) / elapsed, nil
}

// findOptimalWorkers analyzes the scaling data to recommend a worker count.
// It finds the "knee" where adding more workers stops yielding meaningful
// throughput gains (< 10% improvement over the previous level).
func findOptimalWorkers(results *SpeedtestResults) (int, string) {
	n := len(results.WasabiScaling)
	if len(results.GCSScaling) < n {
		n = len(results.GCSScaling)
	}
	if n == 0 {
		return 10, "no test data, using default"
	}

	// Compute effective throughput (min of both legs) at each level
	type point struct {
		workers   int
		effective float64
		wasabi    float64
		gcs      float64
	}
	points := make([]point, n)
	for i := 0; i < n; i++ {
		w := results.WasabiScaling[i].Throughput
		g := results.GCSScaling[i].Throughput
		points[i] = point{
			workers:   results.WasabiScaling[i].Workers,
			effective: minPositive(w, g),
			wasabi:    w,
			gcs:      g,
		}
	}

	// Find the knee: last level where improvement >= threshold
	optimal := points[0].workers
	for i := 1; i < len(points); i++ {
		prev := points[i-1].effective
		curr := points[i].effective
		if prev > 0 && (curr-prev)/prev >= scalingGainThreshold {
			optimal = points[i].workers
		}
	}

	// Identify the bottleneck at the optimal level
	var reason string
	for i := 0; i < n; i++ {
		if points[i].workers == optimal {
			w := points[i].wasabi
			g := points[i].gcs
			if w > 0 && g > 0 {
				if w < g {
					reason = fmt.Sprintf("Wasabi download is the bottleneck at ~%s", formatSpeed(w))
				} else {
					reason = fmt.Sprintf("GCS upload is the bottleneck at ~%s", formatSpeed(g))
				}
			} else if w > 0 {
				reason = fmt.Sprintf("Wasabi download at ~%s (GCS not tested)", formatSpeed(w))
			} else if g > 0 {
				reason = fmt.Sprintf("GCS upload at ~%s (Wasabi not tested)", formatSpeed(g))
			}
			break
		}
	}

	return optimal, reason
}

// minPositive returns the smaller of two values, ignoring zeros.
// If both are positive, returns the minimum. If only one is positive, returns that one.
func minPositive(a, b float64) float64 {
	if a > 0 && b > 0 {
		if a < b {
			return a
		}
		return b
	}
	if a > 0 {
		return a
	}
	return b
}

// printSpeedtestSummary prints the recommendation section after the scaling table.
func printSpeedtestSummary(results *SpeedtestResults, currentWorkers int) {
	resultLabel := lipgloss.NewStyle().
		Foreground(lipgloss.Color("8")).
		Width(20)

	fmt.Fprintln(os.Stderr, titleStyle.Render("Recommendation"))
	fmt.Fprintln(os.Stderr)

	// Internet baseline for context
	if results.InternetBPS > 0 {
		fmt.Fprintf(os.Stderr, "  %s%s\n",
			resultLabel.Render("Internet:"),
			valueStyle.Render(formatSpeed(results.InternetBPS)))
	}

	if results.RecommendedWorkers > 0 {
		recStr := fmt.Sprintf("--workers %d", results.RecommendedWorkers)
		if results.RecommendedWorkers == currentWorkers {
			recStr += " (current setting)"
		} else {
			recStr += fmt.Sprintf(" (currently %d)", currentWorkers)
		}
		fmt.Fprintf(os.Stderr, "  %s%s\n",
			resultLabel.Render("Optimal workers:"),
			successStyle.Render(recStr))

		if results.RecommendReason != "" {
			fmt.Fprintf(os.Stderr, "  %s%s\n",
				resultLabel.Render("Bottleneck:"),
				warnStyle.Render(results.RecommendReason))
		}

		// Show estimated throughput at recommended level
		n := len(results.WasabiScaling)
		if len(results.GCSScaling) < n {
			n = len(results.GCSScaling)
		}
		for i := 0; i < n; i++ {
			if results.WasabiScaling[i].Workers == results.RecommendedWorkers {
				eff := minPositive(
					results.WasabiScaling[i].Throughput,
					results.GCSScaling[i].Throughput)
				if eff > 0 {
					fmt.Fprintf(os.Stderr, "  %s%s\n",
						resultLabel.Render("Est. throughput:"),
						successStyle.Render(fmt.Sprintf("~%s with %d workers",
							formatSpeed(eff), results.RecommendedWorkers)))
				}
				break
			}
		}
	} else {
		fmt.Fprintf(os.Stderr, "  %s%s\n",
			resultLabel.Render("Optimal workers:"),
			dimStyle.Render("could not determine (insufficient test data)"))
	}

	fmt.Fprintln(os.Stderr)
}

// formatSpeed converts bytes/sec to "X.Y MiB/s".
func formatSpeed(bps float64) string {
	mib := bps / (1024 * 1024)
	return fmt.Sprintf("%.1f MiB/s", mib)
}
