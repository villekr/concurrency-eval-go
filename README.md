# Concurrency Evaluation - Go
Go code for [How Do You Like Your Lambda Concurrency](https://medium.com/@ville-karkkainen/how-do-you-like-your-lambda-concurrency-part-i-introduction-7a3f7ecfe4b5)-blog series.

# Requirements
* Go
* Docker

# Build Deployment Package

```
docker build -f docker/Dockerfile -t concurrency-eval-go .
docker run -it --rm -v $(pwd):/app concurrency-eval-go bash
cd /app
./scripts/build.sh
```
