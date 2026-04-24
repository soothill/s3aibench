GO ?= go
PKG := ./...
BIN := s3aibench
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -X github.com/darrensoothill/s3aibench/internal/version.Version=$(VERSION) \
           -X github.com/darrensoothill/s3aibench/internal/version.GitSHA=$(GIT_SHA)

.PHONY: build test coverage vet lint clean cross

build:
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/s3aibench

test:
	$(GO) test -race -count=1 $(PKG)

coverage:
	$(GO) test -race -count=1 -coverprofile=cover.out -covermode=atomic $(PKG)
	$(GO) tool cover -func=cover.out | tee coverage.txt
	./scripts/check-coverage.sh coverage.txt

vet:
	$(GO) vet $(PKG)

cross:
	GOOS=linux   GOARCH=amd64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/$(BIN)-linux-amd64 ./cmd/s3aibench
	GOOS=linux   GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/$(BIN)-linux-arm64 ./cmd/s3aibench
	GOOS=darwin  GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/$(BIN)-darwin-arm64 ./cmd/s3aibench

clean:
	rm -rf $(BIN) dist cover.out cover.html coverage.txt
