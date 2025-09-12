# Concurrency Evaluation - Go
Go code for [How Do You Like Your Lambda Concurrency](https://ville-karkkainen.medium.com/how-do-you-like-your-aws-lambda-concurrency-part-1-introduction-7a3f7ecfe4b5)-blog series.

## Requirements
- Go 1.25

## Directory structure
- cmd/
  - lambda/        # AWS Lambda entrypoint (main package)
  - runner/        # Local helper to run the handler from your machine (main package)
- internal/        # Private application/library code used by the commands
  - handler.go     # Lambda handler and core logic

This follows common Go conventions: each executable is under cmd/<name>, and shared/internal code lives under internal/.

## Build
- Lambda binary (linux/arm64 as used in CI):
  GOARCH=arm64 GOOS=linux go build -o build/lambda ./cmd/lambda

- Local runner:
  go run ./cmd/runner

## Environment variables used by runner
- S3_BUCKET_NAME
- FOLDER
- FIND (optional)