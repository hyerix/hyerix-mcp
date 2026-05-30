APP_NATS_SRC ?= ../app/src-tauri/src/nats

.PHONY: dev test build release sync clippy fmt fmt-check check

dev:
	cargo run -- --nats-url nats://localhost:4222

build:
	cargo build

release:
	cargo build --release

test:
	cargo test --all

clippy:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

check: fmt-check clippy test

sync:
	@echo "Diffing hyerix-mcp/src/nats against $(APP_NATS_SRC)"
	@echo "(this is the manual drift surface; both copies live until ~15 tools or v2.0)"
	@for f in streams.rs messages.rs kv.rs; do \
		if [ -f "$(APP_NATS_SRC)/$$f" ]; then \
			echo "--- diff: $$f ---"; \
			diff -u "$(APP_NATS_SRC)/$$f" "src/nats/$$f" || true; \
		fi; \
	done
