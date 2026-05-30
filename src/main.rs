use anyhow::Result;
use clap::Parser;
use rmcp::{transport::stdio, ServiceExt};
use tracing_subscriber::EnvFilter;

use hyerix_mcp_lib::{config::Config, nats::connection::LazyClient, tools::HyerixMcp};

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("HYERIX_MCP_LOG")
                .unwrap_or_else(|_| EnvFilter::new("hyerix_mcp_lib=info,warn")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();

    tracing::info!(nats_url = %cfg.nats_url, allow_publish = cfg.allow_publish, "hyerix-mcp starting");

    let allow_publish = cfg.allow_publish;
    let client = LazyClient::new(cfg);
    let server = HyerixMcp::new(client, allow_publish);
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
