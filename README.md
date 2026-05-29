# hyerix-mcp

[![CI](https://github.com/hyerix/hyerix-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/hyerix/hyerix-mcp/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/hyerix/hyerix-mcp?color=00D4AA&label=release)](https://github.com/hyerix/hyerix-mcp/releases)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-00D4AA.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

**Hyerix's MCP surface for NATS.** A stdio Model Context Protocol server that lets any MCP-capable AI agent talk to a NATS cluster: inspect streams and consumers, browse messages, walk KV and Object Store, check cluster health.

This is Hyerix's own surface for the protocol — not "an integration with" any one agent host. Point it at your NATS cluster and any MCP client (Claude Desktop, Cursor, Claude Code, Windsurf, or any other MCP client) can drive it.

---

## Install

### Pre-built binary

Download the binary for your platform from the [releases page](https://github.com/hyerix/hyerix-mcp/releases), unzip, and add it to your `PATH`.

### From source

```sh
cargo install --git https://github.com/hyerix/hyerix-mcp
```

### Homebrew

Homebrew formula coming once we tap a stable release cadence.

---

## Quick start

Once `hyerix-mcp` is on your `PATH`, add it to your agent host's MCP config.

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "hyerix": {
      "command": "hyerix-mcp",
      "args": ["--nats-url", "nats://localhost:4222"]
    }
  }
}
```

### Cursor

Edit `~/.cursor/mcp.json` with the same `mcpServers` block as above.

### Claude Code

Claude Code is configured via CLI instead of a JSON file:

```sh
claude mcp add hyerix hyerix-mcp -- --nats-url nats://localhost:4222
```

Restart your agent host, then ask it: *"what JetStream streams are on my cluster?"* You should see a tool call to `list_streams` and a reply naming your streams (or an empty list if JetStream isn't enabled on the connection). If the agent reports it can't see any `hyerix.*` tools, check the agent host's MCP log — usually a `PATH` issue.

---

## Why hyerix-mcp

- **Read-safe by default.** The only mutating tool is `publish_message`, and it's off until `--allow-publish` is set. Other servers in this space let an agent that misreads a description publish, delete a KV bucket, or drop a consumer unsupervised. We treat that as a footgun, not a feature.
- **Rust, ~5MB single binary.** No Node + `node_modules`, no Go binary 6× the size. Drop it on `PATH` and go.
- **No embedded NATS surprises.** Other implementations bundle an in-process NATS server "for demos." `hyerix-mcp` connects to *your* cluster, full stop.
- **Drift-synced from production code.** The NATS client paths in this repo are slices of the same code shipping inside the Hyerix desktop app, exercised by paying customers daily.
- **CI quality gates.** Every commit must pass `cargo clippy -D warnings`, `cargo fmt --check`, the unit suite, and an integration test that spawns a real `nats-server` and walks the protocol. Signed multi-platform releases (macOS arm64/x86_64, Linux x86_64/arm64, Windows x86_64) wired from day one.

---

## Tools

`hyerix-mcp` ships **14 tools** at v1 — 13 read-only and one mutating tool (gated, off by default). Every tool that scans or collects is bounded server-side (timeouts, message caps, byte caps) so an agent can't blow its context window or your cluster's bandwidth by passing huge limits.

| Group | Tool | What it does |
|-------|------|--------------|
| Core | `subscribe` | Collect messages from a subject until timeout OR max_messages — bounded at 60s / 100 msgs. |
| Core | `request_reply` | Send one request, return single reply (or timeout). Bounded at 30s. |
| Core | `publish_message` | Publish to a subject. **Off by default** — see below. |
| Streams | `list_streams` | List every JetStream stream. Start here. |
| Streams | `get_stream` | Detail one stream by name. |
| Consumers | `list_consumers` | List a stream's consumers — find lagging ones. |
| Consumers | `get_consumer` | Detail one consumer: ack pending, redeliveries, lag. |
| Messages | `browse_messages` | Peek at recent messages without consuming them. |
| KV | `kv_get` | Read a KV key + revision. |
| KV | `kv_list_keys` | List keys in a KV bucket with optional substring filter — keys + revisions + sizes, no values. |
| Object Store | `obj_list_buckets` | List every Object Store bucket on the cluster. |
| Object Store | `obj_list` | List objects in one bucket — metadata only, no payloads. |
| Object Store | `obj_get` | Fetch one object's bytes + metadata. Capped at 1 MiB. |
| Health | `get_cluster_health` | One-call rollup: servers, leader, totals. |

Hyerix's full NATS surface (50+ operations) lives in the [desktop app](https://hyerix.ai#download). The MCP server exposes the agent-friendly slice.

<details>
<summary><strong>What a tool call actually returns</strong> — example: <code>list_streams</code></summary>

Agents call MCP tools, and the response comes back as structured JSON text. Here's the literal payload `list_streams` returns when run against a cluster with a handful of order, payment, and KV streams (trimmed to 4 representative entries):

```json
[
  {
    "name": "ORDERS_US",
    "subjects": ["orders.us.>"],
    "messages": 310955,
    "bytes": 154902173,
    "consumer_count": 1,
    "retention": "limits",
    "storage": "file"
  },
  {
    "name": "PAYMENTS",
    "subjects": ["payments.>"],
    "messages": 1333695,
    "bytes": 360246270,
    "consumer_count": 2,
    "retention": "limits",
    "storage": "file"
  },
  {
    "name": "KV_feature-flags",
    "subjects": ["$KV.feature-flags.>"],
    "messages": 19,
    "bytes": 1994,
    "consumer_count": 0,
    "retention": "limits",
    "storage": "file"
  },
  {
    "name": "OBJ_invoices",
    "subjects": ["$O.invoices.C.>", "$O.invoices.M.>"],
    "messages": 50,
    "bytes": 605884,
    "consumer_count": 0,
    "retention": "limits",
    "storage": "file"
  }
]
```

That's what your agent sees — concrete enough to ground answers, small enough not to blow context. KV and Object Store buckets show up here too (NATS stores them as backing streams prefixed `$KV.` / `$O.`); use `kv_list_keys` / `obj_list_buckets` for the bucket-level view.

</details>

### `publish_message` — opt-in

`publish_message` is the only mutating tool, and it's disabled by default. An agent that misreads a description can publish to the wrong subject; we'd rather make that an explicit choice than a default. Start the server with `--allow-publish` to enable it:

```json
{
  "mcpServers": {
    "hyerix": {
      "command": "hyerix-mcp",
      "args": ["--nats-url", "nats://localhost:4222", "--allow-publish"]
    }
  }
}
```

You can also set `HYERIX_MCP_ALLOW_PUBLISH=1` in the agent host's `env` block for this server.

> [!WARNING]
> Enabling `--allow-publish` lets the agent publish to any subject your NATS credentials can reach. Scope the credentials accordingly.

---

## Configuration

All flags can also be passed via environment variables.

| Flag | Env var | Notes |
|------|---------|-------|
| `--nats-url` | `NATS_URL` | Required. e.g. `nats://localhost:4222`, `tls://example:4222` |
| `--creds` | `NATS_CREDS` | Path to a NATS 2.x credentials file (JWT auth) |
| `--nkey` | `NATS_NKEY` | Path to a NATS NKey seed file (single line starting with `SU…`) |
| `--user` | `NATS_USER` | Legacy user/pass |
| `--pass` | `NATS_PASSWORD` | Legacy user/pass |
| `--token` | `NATS_TOKEN` | Token auth |
| `--allow-publish` | `HYERIX_MCP_ALLOW_PUBLISH` | Enable `publish_message` |

- **TLS:** automatically enforced if the URL starts with `tls://` or `nats+tls://`.
- **Auth precedence:** creds file > NKey > token > user/pass.

### Compatibility

- **NATS server:** 2.10+. JetStream operations require JetStream enabled on the connected account. KV and Object Store require server 2.10+ with JetStream.
- **Platforms:** macOS arm64/x86_64, Linux x86_64/arm64, Windows x86_64. Signed release binaries on every tag.

### No license, no signup

The MCP server itself does not require a Hyerix license. Run it freely. For dashboards, topology graphs, and Signal AI on top of the same NATS code path, see [Hyerix](https://hyerix.ai#download).

### Transport

stdio-only at v1.x — HTTP/SSE is on the roadmap once there's a real need (hosted instances, remote agents).

---

## Where hyerix-mcp deliberately stops

These are design choices, not gaps.

- **No `create_*` / `delete_*` / `update_*`.** Destructive ops are too easy for an agent to call wrong from a single description. The v1 surface is read-heavy on purpose — `publish_message` is the line and it's gated.
- **No Object Store write or KV write.** By design until we see real agent telemetry showing the need. The read surface (`kv_get`, `kv_list_keys`, `obj_list_buckets`, `obj_list`, `obj_get`) is enough to inspect; mutation goes through your normal NATS tooling.
- **No Signal AI as a tool.** Signal AI is a Hyerix product feature — its value is the UI (chips, grounded answers, intent display). Exposing it via MCP just makes the LLM call another LLM with worse context. The desktop is the right surface for it.
- **JWT chain depth & operator-mode `$SYS` access** beyond a single creds file are not yet wired up. File an issue if you need it.

---

## Issues & contributions

Found a bug, hit a limitation, or want a tool we don't yet expose? Open an issue at [github.com/hyerix/hyerix-mcp/issues](https://github.com/hyerix/hyerix-mcp/issues).

## Security

Security issues: please **don't** open a public issue. See [SECURITY.md](./SECURITY.md) or email `security@hyerix.ai`.

---

## Built by Hyerix

[Hyerix](https://hyerix.ai) is the AI-native desktop GUI for NATS. The MCP server is its surface for AI agents — same NATS code path, different transport. The canonical NATS client code lives inside the (private) Hyerix desktop app; this repo carries a hand-synced slice covering the launch tools.

License: Apache-2.0.

<sub>Hyerix /ˈhaɪ.rɪks/ — rhymes with "high tricks". [@hyerixAI](https://x.com/hyerixAI)</sub>
