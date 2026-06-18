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
override PLUGIN_REF_ROOT := plugins/handoffkit/skills/handoffkit-scaffold/references/_src
PLUGIN_REF := $(PLUGIN_REF_ROOT)

.DEFAULT_GOAL := help
.PHONY: help test test-integration test-integration-ci build vet fmt tidy sync-plugin check-plugin-sync

help: ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

test: ## offline unit tests (no API), uncached, with the race detector
	$(GO) test -race -count=1 ./...

test-integration: ## live API tests (needs OPENAI_API_KEY and/or `codex login`)
	$(GO) test -tags=integration ./llm/...

test-integration-ci: ## live API tests under race detector, fail if no backend is configured
	HANDOFFKIT_REQUIRE_INTEGRATION_BACKEND=1 $(GO) test -race -count=1 -tags=integration ./llm/...

build: ## compile all packages + examples
	$(GO) build ./...

vet: ## go vet, including the integration-tagged tests
	$(GO) vet ./...
	$(GO) vet -tags=integration ./...

fmt: ## gofmt all packages
	$(GO) fmt ./...

tidy: ## sync go.mod / go.sum
	$(GO) mod tidy

sync-plugin: ## re-vendor sketch + runtime, including tests, into the Codex plugin scaffold
	@set -eu; \
	ref='$(PLUGIN_REF)'; \
	root='$(PLUGIN_REF_ROOT)'; \
	if [ -z "$$ref" ] || [ "$$ref" = "." ] || [ "$$ref" = "/" ]; then \
		echo "ERROR: unsafe PLUGIN_REF '$$ref'"; exit 1; \
	fi; \
	root_abs=$$(cd "$$root" && pwd -P); \
	ref_abs=$$(cd "$$ref" 2>/dev/null && pwd -P) || { echo "ERROR: PLUGIN_REF must exist under $$root"; exit 1; }; \
	sketch_abs=$$(cd sketch && pwd -P); \
	runtime_abs=$$(cd runtime && pwd -P); \
	if [ "$$ref_abs" = "$$sketch_abs" ] || [ "$$ref_abs" = "$$runtime_abs" ]; then \
		echo "ERROR: PLUGIN_REF must not point at canonical source directories"; exit 1; \
	fi; \
	case "$$ref_abs" in "$$root_abs"|$$root_abs/*) ;; \
		*) echo "ERROR: PLUGIN_REF must be inside $$root"; exit 1 ;; \
	esac; \
	rm -rf "$$ref/sketch" "$$ref/runtime"; \
	mkdir -p "$$ref/sketch" "$$ref/runtime"; \
	cp $(wildcard sketch/*.go) "$$ref/sketch/"; \
	cp $(wildcard runtime/*.go) "$$ref/runtime/"; \
	echo "synced sketch + runtime, including tests -> $$ref"

check-plugin-sync: ## fail if the plugin's vendored runtime has drifted (CI)
	@tmp=$$(mktemp -d); \
	mkdir -p $$tmp/sketch $$tmp/runtime; \
	cp $(wildcard sketch/*.go) $$tmp/sketch/; \
	cp $(wildcard runtime/*.go) $$tmp/runtime/; \
	if diff -r $$tmp $(PLUGIN_REF) >/dev/null 2>&1; then \
		echo "plugin scaffold in sync"; rm -rf $$tmp; \
	else \
		echo "ERROR: plugin scaffold out of sync, run 'make sync-plugin'"; \
		diff -r $$tmp $(PLUGIN_REF) || true; rm -rf $$tmp; exit 1; \
	fi
