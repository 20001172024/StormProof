.PHONY: all build test lint vet tidy

all: lint test

build:
	go build ./...

test:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...

tidy:
	go mod tidy
