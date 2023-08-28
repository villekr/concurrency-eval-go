# Concurrency Evaluation - Go
Go code for [How Do You Like Your Lambda Concurrency](https://ville-karkkainen.medium.com/how-do-you-like-your-aws-lambda-concurrency-part-1-introduction-7a3f7ecfe4b5)-blog series.

# Requirements
* Go 1.20.6
* Docker

# Build Deployment Package

```
docker build -f docker/Dockerfile -t concurrency-eval-go .
docker run -it --rm -v $(pwd):/app concurrency-eval-go bash
cd /app
./scripts/build.sh
```
