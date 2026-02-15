package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
)

// newWasabiHTTPClient creates an HTTP client tuned for concurrent workers.
func newWasabiHTTPClient(workers int) *http.Client {
	maxConns := workers * 2
	if maxConns < 100 {
		maxConns = 100
	}

	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxConns,
			MaxIdleConnsPerHost: maxConns,
			MaxConnsPerHost:     0,

			IdleConnTimeout:   90 * time.Second,
			DisableKeepAlives: false,

			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,

			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
			ForceAttemptHTTP2:     true,
			DisableCompression:    false,
		},
	}
}

// newWasabiClient creates an S3 client configured for Wasabi.
func newWasabiClient(ctx context.Context, cfg *Config) (*s3.Client, error) {
	httpClient := newWasabiHTTPClient(cfg.Workers)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.WasabiRegion),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.WasabiAccessKey,
				cfg.WasabiSecretKey,
				"",
			),
		),
		awsconfig.WithHTTPClient(httpClient),
		awsconfig.WithRetryMaxAttempts(3),
		awsconfig.WithRetryMode(aws.RetryModeAdaptive),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.WasabiEndpoint)
		o.UsePathStyle = true
	}), nil
}

// newGCSClient creates a GCS client with gRPC connection pooling.
func newGCSClient(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewGRPCClient(ctx,
		option.WithGRPCConnectionPool(4),
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		return nil, fmt.Errorf("create gcs client: %w", err)
	}

	return client, nil
}
