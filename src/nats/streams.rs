use anyhow::{anyhow, Result};
use async_nats::jetstream::{
    self,
    stream::{RetentionPolicy, StorageType},
};
use futures::TryStreamExt;

use super::types::{StreamDetail, StreamSummary};

pub async fn list_streams(
    client: &async_nats::Client,
    filter: Option<&str>,
) -> Result<Vec<StreamSummary>> {
    let js = jetstream::new(client.clone());
    let mut iter = js.streams();
    let mut out = Vec::new();
    while let Some(info) = iter
        .try_next()
        .await
        .map_err(|e| anyhow!("failed to iterate jetstream streams: {e}"))?
    {
        if let Some(needle) = filter {
            if !info.config.name.contains(needle) {
                continue;
            }
        }
        out.push(StreamSummary {
            name: info.config.name.clone(),
            subjects: info.config.subjects.clone(),
            messages: info.state.messages,
            bytes: info.state.bytes,
            consumer_count: info.state.consumer_count as i32,
            retention: retention_str(info.config.retention),
            storage: storage_str(info.config.storage),
        });
    }
    Ok(out)
}

pub async fn get_stream(client: &async_nats::Client, name: &str) -> Result<StreamDetail> {
    let js = jetstream::new(client.clone());
    let mut stream = js
        .get_stream(name)
        .await
        .map_err(|e| anyhow!("failed to get stream '{name}': {e}"))?;
    let info = stream
        .info()
        .await
        .map_err(|e| anyhow!("failed to read info for stream '{name}': {e}"))?;
    let cfg = &info.config;
    let state = &info.state;
    Ok(StreamDetail {
        name: cfg.name.clone(),
        description: cfg.description.clone(),
        subjects: cfg.subjects.clone(),
        retention: retention_str(cfg.retention),
        storage: storage_str(cfg.storage),
        num_replicas: cfg.num_replicas as i32,
        max_bytes: cfg.max_bytes,
        max_age_secs: cfg.max_age.as_secs(),
        messages: state.messages,
        bytes: state.bytes,
        first_seq: state.first_sequence,
        last_seq: state.last_sequence,
        consumer_count: state.consumer_count as i32,
        created: info
            .created
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
    })
}

fn retention_str(r: RetentionPolicy) -> String {
    match r {
        RetentionPolicy::Limits => "limits".into(),
        RetentionPolicy::Interest => "interest".into(),
        RetentionPolicy::WorkQueue => "workQueue".into(),
    }
}

fn storage_str(s: StorageType) -> String {
    match s {
        StorageType::File => "file".into(),
        StorageType::Memory => "memory".into(),
    }
}
