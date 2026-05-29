use anyhow::{anyhow, Result};
use async_nats::jetstream;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures::TryStreamExt;

use super::types::{KvEntry, KvKeyEntry, KvKeyListResult};

pub async fn get_value(client: &async_nats::Client, bucket: &str, key: &str) -> Result<KvEntry> {
    let js = jetstream::new(client.clone());
    let store = js
        .get_key_value(bucket)
        .await
        .map_err(|e| anyhow!("failed to open KV bucket '{bucket}': {e}"))?;
    let entry = store
        .entry(key)
        .await
        .map_err(|e| anyhow!("failed to read key '{key}' from bucket '{bucket}': {e}"))?
        .ok_or_else(|| anyhow!("key '{key}' not found in bucket '{bucket}'"))?;

    let (value, encoding) = match std::str::from_utf8(&entry.value) {
        Ok(s) => (s.to_string(), "utf8".to_string()),
        Err(_) => (BASE64.encode(&entry.value), "base64".to_string()),
    };

    Ok(KvEntry {
        bucket: entry.bucket.clone(),
        key: entry.key.clone(),
        value,
        value_encoding: encoding,
        revision: entry.revision,
        created: entry
            .created
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
    })
}

pub async fn list_keys(
    client: &async_nats::Client,
    bucket: &str,
    filter: Option<&str>,
    limit: usize,
) -> Result<KvKeyListResult> {
    let cap = limit.clamp(1, 1000);
    let js = jetstream::new(client.clone());
    let store = js
        .get_key_value(bucket)
        .await
        .map_err(|e| anyhow!("failed to open KV bucket '{bucket}': {e}"))?;

    let mut names = store
        .keys()
        .await
        .map_err(|e| anyhow!("failed to list keys for bucket '{bucket}': {e}"))?;

    let mut out: Vec<KvKeyEntry> = Vec::with_capacity(cap);
    while let Some(name) = names
        .try_next()
        .await
        .map_err(|e| anyhow!("error iterating keys: {e}"))?
    {
        if let Some(needle) = filter {
            if !name.contains(needle) {
                continue;
            }
        }
        let (revision, value_size_bytes) = match store
            .entry(&name)
            .await
            .map_err(|e| anyhow!("failed to read key '{name}': {e}"))?
        {
            Some(e) => (e.revision, e.value.len()),
            None => continue,
        };
        out.push(KvKeyEntry {
            key: name,
            revision,
            value_size_bytes,
        });
        if out.len() >= cap {
            break;
        }
    }

    Ok(KvKeyListResult {
        bucket: bucket.into(),
        returned: out.len(),
        keys: out,
    })
}
