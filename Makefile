# lsm-rust — developer task runner.
#
# Run `make` or `make help` for the list of targets. The `check` target runs
# the same gates as CI (format, lint, tests, docs), so you can reproduce a
# green build locally before opening a pull request.

CARGO ?= cargo
RUSTDOCFLAGS_STRICT := -D warnings

# Address/data used by the `serve` and `serve-metrics` convenience targets.
ADDR ?= 127.0.0.1:6379
METRICS_ADDR ?= 127.0.0.1:9898
DATA ?= ./data

.DEFAULT_GOAL := help

##@ General

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build & run

.PHONY: build
build: ## Build the crate (debug).
	$(CARGO) build

.PHONY: release
release: ## Build the crate (release, optimized).
	$(CARGO) build --release

.PHONY: run
run: ## Run the scripted demo binary.
	$(CARGO) run --release -- demo

.PHONY: serve
serve: ## Serve the store over the Redis protocol (ADDR, DATA overridable).
	$(CARGO) run --release -- serve --addr $(ADDR) --data $(DATA)

.PHONY: serve-metrics
serve-metrics: ## Serve RESP + a Prometheus /metrics endpoint (METRICS_ADDR overridable).
	$(CARGO) run --release -- serve --addr $(ADDR) --data $(DATA) --metrics-addr $(METRICS_ADDR)

##@ Quality gates (run by CI)

.PHONY: check
check: fmt-check lint test doc ## Run every CI gate: format, lint, tests, docs.

.PHONY: fmt
fmt: ## Format the code in place.
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting without modifying files.
	$(CARGO) fmt --all -- --check

.PHONY: lint
lint: ## Run clippy with warnings denied.
	$(CARGO) clippy --all-targets --all-features -- -D warnings

.PHONY: test
test: ## Run the full test suite (unit + integration + doc tests).
	$(CARGO) test --all-features

.PHONY: test-recovery
test-recovery: ## Run the crash-recovery integration suite only.
	$(CARGO) test --test recovery

.PHONY: doc
doc: ## Build the API docs, denying doc warnings.
	RUSTDOCFLAGS="$(RUSTDOCFLAGS_STRICT)" $(CARGO) doc --no-deps --all-features

.PHONY: doc-open
doc-open: ## Build and open the API docs in a browser.
	RUSTDOCFLAGS="$(RUSTDOCFLAGS_STRICT)" $(CARGO) doc --no-deps --all-features --open

##@ Extras (require tooling)

.PHONY: bench
bench: ## Run the criterion benchmark suite.
	$(CARGO) bench

.PHONY: coverage
coverage: ## Measure coverage with cargo-tarpaulin (writes cobertura.xml).
	$(CARGO) tarpaulin --all-features --workspace --timeout 120 --out xml

.PHONY: audit
audit: ## Audit dependencies for known vulnerabilities (cargo-audit).
	$(CARGO) audit

.PHONY: install-tools
install-tools: ## Install the optional dev tools used by coverage/audit.
	$(CARGO) install cargo-tarpaulin cargo-audit

##@ Housekeeping

.PHONY: clean
clean: ## Remove build artifacts and the demo data directory.
	$(CARGO) clean
	rm -rf $(DATA)
