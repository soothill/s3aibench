GO ?= go
PKG := ./...
BIN := s3aibench
TOOLS_BIN := $(CURDIR)/.tools/bin
GOLANGCI_LINT_VERSION := $(shell cat .golangci-lint-version)
GOLANGCI_LINT ?= $(shell command -v golangci-lint 2>/dev/null || echo $(TOOLS_BIN)/golangci-lint)
GOLANGCI_LINT_TIMEOUT ?= 5m
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -X github.com/darrensoothill/s3aibench/internal/version.Version=$(VERSION) \
           -X github.com/darrensoothill/s3aibench/internal/version.GitSHA=$(GIT_SHA)

.PHONY: build test coverage vet lint install-lint clean cross

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

install-lint:
	mkdir -p $(TOOLS_BIN)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(TOOLS_BIN) $(GOLANGCI_LINT_VERSION)

lint:
	@if [ ! -x "$(GOLANGCI_LINT)" ]; then $(MAKE) install-lint; fi
	$(GOLANGCI_LINT) run --timeout=$(GOLANGCI_LINT_TIMEOUT)

cross:
	GOOS=linux   GOARCH=amd64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/$(BIN)-linux-amd64 ./cmd/s3aibench
	GOOS=linux   GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/$(BIN)-linux-arm64 ./cmd/s3aibench
	GOOS=darwin  GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o dist/$(BIN)-darwin-arm64 ./cmd/s3aibench

clean:
	rm -rf $(BIN) dist cover.out cover.html coverage.txt
