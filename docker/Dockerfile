FROM --platform=linux/arm64 golang:1.20 as build
WORKDIR /app

COPY go.mod go.sum ./
COPY ./cmd ./cmd
COPY ./internal ./internal

RUN go build -tags lambda.norpc -o main ./cmd/main.go

FROM --platform=linux/arm64 public.ecr.aws/lambda/provided:al2023
COPY --from=build /app/main ./main
ENTRYPOINT [ "./main" ]