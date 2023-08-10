#!/bin/bash

DIR="cmd"
GOARCH=amd64 GOOS=linux go build -o build/"$DIR" "$DIR"/*
zip -jrmq build/"$DIR".zip build/"$DIR"
