GO ?= go
VERSION ?= 0.1.0
COMMIT ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
COMMIT := $(COMMIT)
BUILD_TIME := $(BUILD_TIME)
LDFLAGS = -s -w -X 'main.version=$(VERSION) (commit=$(COMMIT) built=$(BUILD_TIME))'

.PHONY: build helpers test test-integration check release bundle

build: helpers
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/kube-node-migrate .

helpers:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v1 $(GO) build -trimpath -ldflags '-s -w' -o bin/kube-node-fs-linux-amd64 ./tools/kube-node-fs
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -ldflags '-s -w' -o bin/kube-node-fs-linux-arm64 ./tools/kube-node-fs

test:
	$(GO) test -race ./...

test-integration:
	$(GO) test -race -tags=integration ./...

check:
	@files="$$(gofmt -l main.go cmd internal tools)"; test -z "$$files" || { printf 'Run gofmt on:\n%s\n' "$$files"; exit 1; }
	$(GO) vet ./...

release: helpers
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v1 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/kube-node-migrate-linux-amd64 .
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/kube-node-migrate-linux-arm64 .

# Offline dependencies are supplied by the release builder, never downloaded by a migration.
PV_MIGRATE_DIR ?= $(CURDIR)/bin
bundle: release
	$(GO) run ./tools/check-bundle --pv-migrate-dir '$(PV_MIGRATE_DIR)' --version '$(VERSION)' --commit '$(COMMIT)' --built '$(BUILD_TIME)'
