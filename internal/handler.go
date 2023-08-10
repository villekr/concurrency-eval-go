package internal

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
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

func HandleRequest(ctx context.Context, event Event) (*Response, error) {
	start := time.Now()
	result, err := processor(event)
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

func processor(event Event) (*string, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	bucketName := event.S3BucketName
	folder := event.Folder
	find := event.Find

	listObjectsParams := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(folder),
	}
	response, err := svc.ListObjectsV2(listObjectsParams)
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(response.Contents))
	for i, obj := range response.Contents {
		keys[i] = *obj.Key
	}

	responseChan := make(chan *string, len(keys))
	var wg sync.WaitGroup

	for _, key := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()

			body, err := get(svc, bucketName, key, find)
			if err != nil {
				fmt.Println("Error retrieving object:", err)
				return
			}
			responseChan <- body
		}(key)
	}

	go func() {
		wg.Wait()
		close(responseChan)
	}()

	var responses []*string
	for body := range responseChan {
		responses = append(responses, body)
	}
	if find != nil {
		return indexOfNonNil(responses), nil
	}
	result := strconv.Itoa(len(responses))
	return &result, nil
}

func get(svc *s3.S3, bucketName, key string, find *string) (*string, error) {
	getObjectParams := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	response, err := svc.GetObject(getObjectParams)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(response.Body)
	if err != nil {
		return nil, err
	}

	body := buf.String()
	if find != nil {
		index := strings.Index(body, *find)
		if index >= 0 {
			return &key, nil
		} else {
			return nil, nil
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
