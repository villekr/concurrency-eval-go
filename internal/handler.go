package internal

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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

// getS3ClientForBucket returns a suitable S3 client. For standard buckets it returns a cached default client.
// For S3 Directory Buckets (S3 Express One Zone) it returns a specially configured client that targets the
// s3express endpoint and injects the required x-amz-region-set header.
func getS3ClientForBucket(bucketName string) *s3v2.Client {
	if isDirectoryBucket(bucketName) {
		return newS3ExpressClient(bucketName)
	}

	s3Once.Do(func() {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			panic(fmt.Errorf("failed to load AWS config: %w", err))
		}
		s3DefaultCli = s3v2.NewFromConfig(cfg)
	})
	return s3DefaultCli
}

func isDirectoryBucket(bucket string) bool {
	return strings.HasSuffix(bucket, "--x-s3") && strings.Contains(bucket, "--")
}

var azIDRe = regexp.MustCompile(`--([a-z0-9-]+)--x-s3$`)

func extractAZID(bucket string) (string, bool) {
	m := azIDRe.FindStringSubmatch(bucket)
	if len(m) == 2 {
		return m[1], true
	}
	return "", false
}

func newS3ExpressClient(bucketName string) *s3v2.Client {
	azID, ok := extractAZID(bucketName)
	if !ok {
		return getS3ClientForBucket("")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = deriveRegionFromAZID(azID)
		if region == "" {
			region = "us-east-1"
		}
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		panic(fmt.Errorf("failed to load AWS config for s3express: %w", err))
	}

	client := s3v2.NewFromConfig(cfg, func(o *s3v2.Options) {
		// Virtual-hosted–style addressing (required for directory buckets)
		o.UsePathStyle = false
		// Inject required header for directory buckets
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Build.Add(middleware.BuildMiddlewareFunc("AddRegionSet", func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (out middleware.BuildOutput, metadata middleware.Metadata, err error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Header.Set("x-amz-region-set", azID)
				}
				return next.HandleBuild(ctx, in)
			}), middleware.After)
		})
	})

	return client
}

func deriveRegionFromAZID(azID string) string {
	// Minimal mapping for common regions; prefer AWS_REGION env in Lambda.
	switch {
	case strings.HasPrefix(azID, "use1-"):
		return "us-east-1"
	case strings.HasPrefix(azID, "use2-"):
		return "us-east-2"
	case strings.HasPrefix(azID, "usw2-"):
		return "us-west-2"
	case strings.HasPrefix(azID, "eun1-"):
		return "eu-north-1"
	case strings.HasPrefix(azID, "euw1-"):
		return "eu-west-1"
	case strings.HasPrefix(azID, "euc1-"):
		return "eu-central-1"
	default:
		return ""
	}
}

// maxConcurrency returns the worker pool size, defaulting to 64, overridable via MAX_CONCURRENCY env var (1..256).
func maxConcurrency() int {
	v := os.Getenv("MAX_CONCURRENCY")
	if v == "" {
		return 64
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 64
	}
	if n > 256 {
		n = 256
	}
	return n
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
	svc := getS3ClientForBucket(bucketName)
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

	// Search mode: read the content once and check for substring
	b, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if strings.Contains(string(b), *find) {
		return &key, nil
	}
	return nil, nil
}
