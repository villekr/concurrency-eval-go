FROM --platform=linux/arm64 golang:1.20
WORKDIR /temp

COPY go.mod go.sum ./
COPY ./cmd ./cmd
COPY ./internal ./internal
COPY ./scripts ./scripts

RUN cp main /build/cmd
#
#RUN go build -tags lambda.norpc -o main ./cmd/main.go
#
#FROM --platform=linux/arm64 public.ecr.aws/lambda/provided:al2023
#COPY --from=build /app/main ./main
#ENTRYPOINT [ "./main" ]