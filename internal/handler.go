package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type Event struct {
	S3BucketName string  `json:"s3_bucket_name"`
	Folder       string  `json:"folder"`
	Find         *string `json:"find"`
}

type Response struct {
	Lang   string  `json:"lang"`
	Detail string  `json:"detail"`
	Result *string `json:"result"`
	Time   float32 `json:"time"`
}

var (
	s3Once       sync.Once
	s3DefaultCli *s3v2.Client
)

// getS3Client returns a cached default S3 client, created once per execution
// environment and reused across warm invocations.
func getS3Client() *s3v2.Client {
	s3Once.Do(func() {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			panic(fmt.Errorf("failed to load AWS config: %w", err))
		}
		s3DefaultCli = s3v2.NewFromConfig(cfg)
	})
	return s3DefaultCli
}

// maxConcurrency returns the worker pool size. It uses the MAX_CONCURRENCY env var override (1..256)
// if set; otherwise it derives the cap from the Lambda memory size using the shared formula:
// cap = max(8, min(64, memory_MB / 32)), where memory_MB comes from AWS_LAMBDA_FUNCTION_MEMORY_SIZE
// (default 1024 if unset).
func maxConcurrency() int {
	if v := os.Getenv("MAX_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n >= 1 {
			if n > 256 {
				n = 256
			}
			return n
		}
	}

	memMB := 1024
	if m := os.Getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"); m != "" {
		if n, err := strconv.Atoi(m); err == nil && n > 0 {
			memMB = n
		}
	}

	cap := memMB / 32
	if cap < 8 {
		cap = 8
	}
	if cap > 64 {
		cap = 64
	}
	return cap
}

func HandleRequest(ctx context.Context, event Event) (*Response, error) {
	start := time.Now()
	result, err := processor(ctx, event)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start).Seconds()

	response := Response{
		Lang:   "go",
		Detail: "aws-sdk-v2",
		Result: result,
		Time:   float32(math.Round(elapsed*10) / 10),
	}

	return &response, nil
}

func processor(ctx context.Context, event Event) (*string, error) {
	bucketName := event.S3BucketName
	svc := getS3Client()
	folder := event.Folder
	find := event.Find

	// List objects once (no pagination needed) as the bucket contains at most 1000 objects per requirements
	var keys []string
	listObjectsParams := &s3v2.ListObjectsV2Input{
		Bucket:  awsv2.String(bucketName),
		Prefix:  awsv2.String(folder),
		MaxKeys: awsv2.Int32(1000),
	}
	resp, err := svc.ListObjectsV2(ctx, listObjectsParams)
	if err != nil {
		return nil, err
	}
	for _, obj := range resp.Contents {
		if obj.Key != nil {
			keys = append(keys, *obj.Key)
		}
	}

	// Always download and fully read all objects' bodies to satisfy mandatory requirements.
	// If a find string is provided, return the first matching key; otherwise, return the count of objects.

	mc := maxConcurrency()
	sem := make(chan struct{}, mc)

	// Determine if we're in search mode (find-string provided)
	searchMode := find != nil

	var wg sync.WaitGroup
	// Track first match by original index to satisfy 'first' semantics while fully reading all bodies
	bestIdx := math.MaxInt
	var bestKey *string
	var mu sync.Mutex

	for i, key := range keys {
		i := i
		k := key
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			match, err := get(ctx, svc, bucketName, k, find)
			if err != nil {
				// Log and continue; do not fail entire batch
				fmt.Println("Error retrieving object:", err)
				return
			}
			if searchMode && match != nil {
				mu.Lock()
				if i < bestIdx {
					bestIdx = i
					bestKey = match
				}
				mu.Unlock()
			}
		}()
	}

	// Wait for all reads to complete
	wg.Wait()

	if !searchMode {
		result := strconv.Itoa(len(keys))
		return &result, nil
	}

	// Return the earliest match (may be nil if none found)
	return bestKey, nil
}

func get(ctx context.Context, svc *s3v2.Client, bucketName, key string, find *string) (*string, error) {
	getObjectParams := &s3v2.GetObjectInput{
		Bucket: awsv2.String(bucketName),
		Key:    awsv2.String(key),
	}
	response, err := svc.GetObject(ctx, getObjectParams)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if find == nil {
		// Count-only mode: fully read without allocating to keep memory low
		_, err = io.Copy(io.Discard, response.Body)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Search mode: fully read the body as raw bytes and search on bytes (no string decode)
	b, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if bytes.Contains(b, []byte(*find)) {
		return &key, nil
	}
	return nil, nil
}
