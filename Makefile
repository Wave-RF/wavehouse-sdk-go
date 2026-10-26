.DEFAULT_GOAL := ci

.PHONY: ci fix fmt vet lint test cover test-e2e shell

OS   := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)

GOLANGCI_LINT_VERSION := v2.11.4
GOLANGCI_LINT         := $(CURDIR)/.bin/$(OS)_$(ARCH)/golangci-lint-$(GOLANGCI_LINT_VERSION)

# golangci-lint panics type-checking a standard library newer than the Go it was built with, so lint on a fixed toolchain, not whatever is installed.
LINT_TOOLCHAIN := go1.26.6

# gofumpt runs through `go run` so go.mod keeps no dependencies, not even tool ones.
GOFUMPT := go run mvdan.cc/gofumpt@v0.9.2

# Total statement coverage must not fall below this percent. Measured 88.8% on import; floor is two points under.
COVERAGE_FLOOR := 86

ci: fmt vet lint cover shell ## Everything CI runs

# The version is in the file name, so bumping it reinstalls.
$(GOLANGCI_LINT):
	mkdir -p $(dir $@)
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b $(dir $@) $(GOLANGCI_LINT_VERSION)
	mv $(dir $@)golangci-lint $@

fmt: ## Fail if any Go file isn't gofumpt-formatted
	@out="$$($(GOFUMPT) -l .)"; if [ -n "$$out" ]; then echo "gofumpt would change:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...
	go vet -tags e2e ./...

lint: $(GOLANGCI_LINT)
	GOTOOLCHAIN=$(LINT_TOOLCHAIN) $(GOLANGCI_LINT) run ./...

test:
	go test -race ./...

cover: ## Race tests plus the coverage floor
	go test -race -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | awk -v floor=$(COVERAGE_FLOOR) \
		'/^total:/ { sub("%", "", $$3); printf "coverage %.1f%% (floor %d%%)\n", $$3, floor; if ($$3 + 0 < floor) exit 1 }'

shell: ## shellcheck every script, actionlint every workflow
	git ls-files -z '*.sh' | xargs -0 -r shellcheck -x
	actionlint

fix: $(GOLANGCI_LINT) ## Apply formatting and lint fixes
	$(GOFUMPT) -w .
	GOTOOLCHAIN=$(LINT_TOOLCHAIN) $(GOLANGCI_LINT) run --fix ./...

test-e2e: ## Live tests; needs a running server in WAVEHOUSE_URL
	@test -n "$$WAVEHOUSE_URL" || { echo "set WAVEHOUSE_URL to a running WaveHouse server"; exit 1; }
	go test -tags e2e -count=1 ./...
