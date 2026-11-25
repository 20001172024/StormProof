.PHONY: all build test lint vet tidy tools proto

# Pinned code-generation tools, installed into ./bin so they never clash with
# whatever is on the machine. Bump versions here and run `make tools`.
# These are the newest releases that build with the Go version in go.mod.
BUF_VERSION                := v1.65.0
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.0

BIN := $(CURDIR)/bin
export PATH := $(BIN):$(PATH)

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

# The stamp file's name contains every tool version, so the tools are
# rebuilt only when a version changes. CI caches ./bin on the same basis.
TOOLS_STAMP := $(BIN)/.tools-$(BUF_VERSION)-$(PROTOC_GEN_GO_VERSION)-$(PROTOC_GEN_GO_GRPC_VERSION)

tools: $(TOOLS_STAMP)

$(TOOLS_STAMP):
	rm -f $(BIN)/.tools-*
	GOBIN=$(BIN) go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	touch $@

# Regenerate Go code from proto/. Generated files are committed; CI runs this
# and fails if the result differs from what is checked in.
proto: tools
	buf lint
	buf generate
