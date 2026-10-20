#!/bin/sh
# needs protoc, protoc-gen-go and protoc-gen-go-grpc on PATH
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -e
protoc -I proto \
  --go_out=gen/brokerpb --go_opt=paths=source_relative \
  --go-grpc_out=gen/brokerpb --go-grpc_opt=paths=source_relative \
  proto/broker.proto
