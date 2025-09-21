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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
	s3DefaultCli *s3.S3
)

// getS3ClientForBucket returns a suitable S3 client. For standard buckets it returns a cached default client.
// For S3 Directory Buckets (S3 Express One Zone) it returns a specially configured client that targets the
// s3express endpoint and injects the required x-amz-region-set header.
func getS3ClientForBucket(bucketName string) *s3.S3 {
	if isDirectoryBucket(bucketName) {
		return newS3ExpressClient(bucketName)
	}

	s3Once.Do(func() {
		sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
		if err != nil {
			// In Lambda, panicking here will surface as init error; subsequent calls won't proceed.
			panic(fmt.Errorf("failed to create AWS session: %w", err))
		}
		s3DefaultCli = s3.New(sess)
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

func newS3ExpressClient(bucketName string) *s3.S3 {
	azID, ok := extractAZID(bucketName)
	if !ok {
		// Fallback to default client if pattern not matched
		return getS3ClientForBucket("")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		// Try derive rough region from AZ ID prefix like "eun1-az1" -> "eu-north-1"
		region = deriveRegionFromAZID(azID)
		if region == "" {
			region = "us-east-1"
		}
	}

	endpoint := fmt.Sprintf("https://s3express-%s.amazonaws.com", region)

	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable, Config: aws.Config{Region: aws.String(region)}})
	if err != nil {
		panic(fmt.Errorf("failed to create AWS session for s3express: %w", err))
	}

	svc := s3.New(sess, &aws.Config{
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(true),
	})

	// Inject required header for directory buckets
	svc.Handlers.Build.PushFront(func(r *request.Request) {
		r.HTTPRequest.Header.Set("x-amz-region-set", azID)
	})

	return svc
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

func HandleRequest(ctx context.Context, event Event) (*Response, error) {
	start := time.Now()
	result, err := processor(ctx, event)
	if err != nil {
		return nil, err
	}
	elapsed := time.Since(start).Seconds()

	response := Response{
		Lang:   "go",
		Detail: "aws-sdk",
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
	listObjectsParams := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(folder),
		MaxKeys: aws.Int64(1000),
	}
	resp, err := svc.ListObjectsV2WithContext(ctx, listObjectsParams)
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

	const maxConcurrent = 32
	sem := make(chan struct{}, maxConcurrent)

	// Always create a cancellable context; in count mode we just won't cancel early.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Determine if we're in search mode (find-string provided)
	searchMode := find != nil

	resultCh := make(chan *string, 1)
	var wg sync.WaitGroup

	for _, key := range keys {
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
				select {
				case resultCh <- match:
					cancel() // cancel remaining work on first match
				default:
				}
			}
		}()
	}

	if !searchMode {
		// No search requested: wait for all reads to complete, then return count
		wg.Wait()
		result := strconv.Itoa(len(keys))
		return &result, nil
	}

	// Search mode: allow early return on first match, otherwise nil when done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	if match, ok := <-resultCh; ok {
		return match, nil
	}
	return nil, nil
}

func get(ctx context.Context, svc *s3.S3, bucketName, key string, find *string) (*string, error) {
	getObjectParams := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	response, err := svc.GetObjectWithContext(ctx, getObjectParams)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	// Fully read the body (mandatory requirement)
	b, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	matched := find != nil && strings.Contains(string(b), *find)
	if matched {
		return &key, nil
	}
	return nil, nil
}
