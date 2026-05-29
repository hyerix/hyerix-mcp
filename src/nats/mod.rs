// Source-of-truth lives at `hyerix/app/src-tauri/src/nats/` (private).
// This is a hand-copied slice covering only the 8 launch tools.
// Manual sync until ~15 tools or v2.0 — then extract `hyerix-core`.
// Run `make sync` (or `just sync`) to surface drift.

pub mod connection;
pub mod consumers;
pub mod health;
pub mod kv;
pub mod messages;
pub mod object_store;
pub mod streams;
pub mod types;
