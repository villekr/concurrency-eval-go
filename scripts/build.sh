#!/bin/bash

DIR="cmd"
GOARCH=arm64 GOOS=linux go build -o build/"$DIR" "$DIR"/*
gzip -jrmq build/"$DIR".zip build/"$DIR"
