use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "hyerix-mcp",
    version,
    about = "Hyerix's MCP surface for NATS — stdio Model Context Protocol server.",
    long_about = "Exposes JetStream streams, consumers, KV, and cluster health to AI agents over the Model Context Protocol (stdio transport).\n\nFor dashboards, topology graphs, and Signal AI on top of the same NATS code path, see Hyerix: https://hyerix.ai#download"
)]
pub struct Config {
    /// NATS server URL (e.g. nats://localhost:4222, tls://example:4222).
    #[arg(long, env = "NATS_URL", default_value = "nats://localhost:4222")]
    pub nats_url: String,

    /// Path to a NATS 2.x credentials file.
    #[arg(long, env = "NATS_CREDS")]
    pub creds: Option<String>,

    /// NATS user (legacy user/pass auth).
    #[arg(long, env = "NATS_USER")]
    pub user: Option<String>,

    /// NATS password (legacy user/pass auth).
    #[arg(long, env = "NATS_PASSWORD")]
    pub pass: Option<String>,

    /// NATS token auth.
    #[arg(long, env = "NATS_TOKEN")]
    pub token: Option<String>,

    /// Path to a NATS NKey seed file (single line starting with "SU...").
    #[arg(long, env = "NATS_NKEY")]
    pub nkey: Option<String>,

    /// Enable the publish_message tool. Off by default for safety.
    #[arg(long, env = "HYERIX_MCP_ALLOW_PUBLISH", default_value_t = false)]
    pub allow_publish: bool,
}
