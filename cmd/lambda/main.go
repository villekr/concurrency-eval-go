package main

import (
	"concurrency-eval/internal"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	lambda.Start(internal.HandleRequest)
}
