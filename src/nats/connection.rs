use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_nats::{Client, ConnectOptions};
use tokio::sync::Mutex;

use crate::config::Config;

#[derive(Clone)]
pub struct LazyClient {
    cfg: Arc<Config>,
    inner: Arc<Mutex<Option<Client>>>,
}

impl LazyClient {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg: Arc::new(cfg),
            inner: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn get(&self) -> Result<Client> {
        let mut guard = self.inner.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }
        let client = connect(&self.cfg).await?;
        *guard = Some(client.clone());
        Ok(client)
    }
}

pub async fn connect(cfg: &Config) -> Result<Client> {
    let options = build_options(cfg).await?;
    let client = options
        .connect(&cfg.nats_url)
        .await
        .map_err(|e| anyhow!("failed to connect to NATS at {}: {e}", cfg.nats_url))?;
    Ok(client)
}

async fn build_options(cfg: &Config) -> Result<ConnectOptions> {
    let creds_set = cfg.creds.is_some();
    let nkey_set = cfg.nkey.is_some();
    let user_set = cfg.user.is_some() || cfg.pass.is_some();
    let token_set = cfg.token.is_some();

    let mut options = if creds_set {
        let path = cfg.creds.as_ref().unwrap();
        ConnectOptions::with_credentials_file(PathBuf::from(path))
            .await
            .map_err(|e| anyhow!("failed to load credentials file '{path}': {e}"))?
    } else if nkey_set {
        let path = cfg.nkey.as_ref().unwrap();
        let raw = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| anyhow!("failed to read nkey file '{path}': {e}"))?;
        let seed = raw
            .lines()
            .map(str::trim)
            .find(|l| l.starts_with("SU") || l.starts_with("SO") || l.starts_with("SA"))
            .ok_or_else(|| anyhow!("no NKey seed (SU.../SO.../SA...) found in '{path}'"))?
            .to_string();
        ConnectOptions::with_nkey(seed)
    } else if token_set {
        ConnectOptions::new().token(cfg.token.clone().unwrap())
    } else if user_set {
        let u = cfg.user.clone().unwrap_or_default();
        let p = cfg.pass.clone().unwrap_or_default();
        ConnectOptions::with_user_and_password(u, p)
    } else {
        ConnectOptions::new()
    };

    if cfg.nats_url.starts_with("tls://") || cfg.nats_url.starts_with("nats+tls://") {
        options = options.require_tls(true);
    }

    options = options.name("hyerix-mcp");

    Ok(options)
}
