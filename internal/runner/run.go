package main

import (
	"concurrency-eval/internal"
	"context"
	"fmt"
	"log"
	"os"
)

func main() {
	event := internal.Event{
		S3BucketName: os.Getenv("S3_BUCKET_NAME"),
		Folder:       os.Getenv("FOLDER"),
		Find:         nil,
	}

	ctx := context.TODO()
	response, err := internal.HandleRequest(ctx, event)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(response)

	find := os.Getenv("FIND")
	event.Find = &find
	response, err = internal.HandleRequest(ctx, event)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(response)
}
