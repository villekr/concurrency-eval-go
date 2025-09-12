package internal

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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
	s3Once   sync.Once
	s3Client *s3.S3
)

func getS3Client() *s3.S3 {
	s3Once.Do(func() {
		sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
		if err != nil {
			// In Lambda, panicking here will surface as init error; subsequent calls won't proceed.
			panic(fmt.Errorf("failed to create AWS session: %w", err))
		}
		s3Client = s3.New(sess)
	})
	return s3Client
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
	svc := getS3Client()
	bucketName := event.S3BucketName
	folder := event.Folder
	find := event.Find

	// List all objects with pagination
	var keys []string
	listObjectsParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(folder),
	}
	for {
		resp, err := svc.ListObjectsV2WithContext(ctx, listObjectsParams)
		if err != nil {
			return nil, err
		}
		for _, obj := range resp.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if aws.BoolValue(resp.IsTruncated) && resp.NextContinuationToken != nil {
			listObjectsParams.ContinuationToken = resp.NextContinuationToken
			continue
		}
		break
	}

	// If not searching for content, avoid downloads; just return count.
	if find == nil {
		result := strconv.Itoa(len(keys))
		return &result, nil
	}

	// Search concurrently with bounded parallelism and early cancellation on first match.
	const maxConcurrent = 32
	sem := make(chan struct{}, maxConcurrent)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
			if match != nil {
				select {
				case resultCh <- match:
					cancel() // cancel remaining work
				default:
				}
			}
		}()
	}

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

	// Only read the body if we need to search within it.
	b, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if find != nil {
		if strings.Contains(string(b), *find) {
			return &key, nil
		}
	}
	return nil, nil
}

func indexOfNonNil(slice []*string) *string {
	for _, v := range slice {
		if v != nil {
			return v
		}
	}
	return nil
}
