build:
	go build -o bin/ ./cmd/...

test:
	go test -race ./...

proto:
	./gen.sh

.PHONY: build test proto
