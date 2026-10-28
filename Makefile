# Multi-module workspace. Every target runs per module so a failure names the module.
SHELL := /bin/bash
MODULES := $(shell find . -name go.mod -not -path './templates/*' -exec dirname {} \; | sort)
GOFLAGS ?=
RACE ?= -race

.PHONY: help modules fmt fmt-check vet build test test-short lint tidy tidy-check go-directive-check check release

# The consumers run Go 1.25; a dependency resolved at "latest" can silently
# raise a module's go directive above it. GO_FLOOR is the highest allowed.
GO_FLOOR ?= 1.25

help: ## list targets
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

modules: ## print the modules in the workspace
	@for m in $(MODULES); do echo $$m; done

fmt: ## gofmt every module in place
	@for m in $(MODULES); do (cd $$m && gofmt -w .); done

fmt-check: ## fail if any file is not gofmt-ed
	@out=$$(for m in $(MODULES); do (cd $$m && gofmt -l .); done); \
	if [ -n "$$out" ]; then echo "not gofmt-ed:"; echo "$$out"; exit 1; fi

vet: ## go vet every module
	@for m in $(MODULES); do echo "== vet $$m"; (cd $$m && go vet ./...) || exit 1; done

build: ## go build every module
	@for m in $(MODULES); do echo "== build $$m"; (cd $$m && go build ./...) || exit 1; done

test: ## go test -race every module (integration tests need Docker)
	@for m in $(MODULES); do echo "== test $$m"; (cd $$m && go test $(RACE) -count=1 -shuffle=on ./...) || exit 1; done

test-short: ## unit tests only; skips tests that need Docker
	@for m in $(MODULES); do echo "== test-short $$m"; (cd $$m && go test $(RACE) -short -count=1 -shuffle=on ./...) || exit 1; done

lint: ## golangci-lint every module with the root config
	@for m in $(MODULES); do echo "== lint $$m"; (cd $$m && golangci-lint run --config $(CURDIR)/.golangci.yml ./...) || exit 1; done

tidy: ## go mod tidy every module, then sync the workspace
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done
	@go work sync

tidy-check: tidy ## fail if tidy changed a tracked go.mod/go.sum (CI)
	@git diff --exit-code -- '*.mod' '*.sum' || (echo "go.mod/go.sum not tidy"; exit 1)

go-directive-check: ## fail if any go.mod requires a Go newer than GO_FLOOR
	@bad=$$(for m in $(MODULES); do v=$$(awk '$$1=="go"{print $$2}' $$m/go.mod); \
	  if [ "$$(printf '%s\n%s\n' "$(GO_FLOOR)" "$${v%.*}" | sort -V | tail -1)" != "$(GO_FLOOR)" ]; then echo "$$m: go $$v"; fi; done); \
	if [ -n "$$bad" ]; then echo "go directive above $(GO_FLOOR):"; echo "$$bad"; exit 1; fi

check: fmt-check vet build lint go-directive-check test ## everything CI runs

# Tag one module: make release MODULE=resilience VERSION=v0.1.0
release: ## tag <MODULE>/<VERSION> on the current commit and push the tag
	@test -n "$(MODULE)" || (echo "MODULE is required"; exit 1)
	@test -n "$(VERSION)" || (echo "VERSION is required"; exit 1)
	@test -f "$(MODULE)/go.mod" || (echo "$(MODULE)/go.mod not found"; exit 1)
	@git diff --quiet || (echo "working tree is dirty"; exit 1)
	git tag -a "$(MODULE)/$(VERSION)" -m "$(MODULE) $(VERSION)"
	git push origin "$(MODULE)/$(VERSION)"
