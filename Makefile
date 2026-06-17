# HandoffKit, common tasks.
#
# Requires Go 1.22+ (the OpenAI SDK dependency). If your default `go` is older,
# override the toolchain per-invocation, e.g.:
#
#   make test GO=/opt/homebrew/bin/go
#
GO ?= go

# Where the Codex plugin scaffolder vendors a copy of the runtime. The `_src`
# dir starts with `_`, so the go tool excludes it from ./... (the vendored .go
# files are reference text, not part of this module's build).
PLUGIN_REF := plugins/handoffkit/skills/handoffkit-scaffold/references/_src

.DEFAULT_GOAL := help
.PHONY: help test test-integration build vet fmt tidy sync-plugin check-plugin-sync

help: ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

test: ## offline unit tests (no API), with the race detector
	$(GO) test -race ./...

test-integration: ## live API tests (needs OPENAI_API_KEY and/or `codex login`)
	$(GO) test -tags=integration ./llm/...

build: ## compile all packages + examples
	$(GO) build ./...

vet: ## go vet, including the integration-tagged tests
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...

fmt: ## gofmt all packages
	$(GO) fmt ./...

tidy: ## sync go.mod / go.sum
	$(GO) mod tidy

sync-plugin: ## re-vendor sketch + runtime into the Codex plugin scaffold (prevents drift)
	@rm -rf $(PLUGIN_REF)/sketch $(PLUGIN_REF)/runtime
	@mkdir -p $(PLUGIN_REF)/sketch $(PLUGIN_REF)/runtime
	cp $(filter-out %_test.go,$(wildcard sketch/*.go)) $(PLUGIN_REF)/sketch/
	cp $(filter-out %_test.go,$(wildcard runtime/*.go)) $(PLUGIN_REF)/runtime/
	@echo "synced sketch + runtime -> $(PLUGIN_REF)"

check-plugin-sync: ## fail if the plugin's vendored runtime has drifted (CI)
	@tmp=$$(mktemp -d); \
	mkdir -p $$tmp/sketch $$tmp/runtime; \
	cp $(filter-out %_test.go,$(wildcard sketch/*.go)) $$tmp/sketch/; \
	cp $(filter-out %_test.go,$(wildcard runtime/*.go)) $$tmp/runtime/; \
	if diff -r $$tmp $(PLUGIN_REF) >/dev/null 2>&1; then \
		echo "plugin scaffold in sync"; rm -rf $$tmp; \
	else \
		echo "ERROR: plugin scaffold out of sync, run 'make sync-plugin'"; \
		diff -r $$tmp $(PLUGIN_REF) || true; rm -rf $$tmp; exit 1; \
	fi
