use anyhow::{anyhow, Result};
use async_nats::jetstream::{self, stream::StorageType};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures::TryStreamExt;
use tokio::io::AsyncReadExt;

use super::types::{ObjectStoreBucket, ObjectStoreEntry, ObjectStoreObject};

const OBJ_STREAM_PREFIX: &str = "OBJ_";
const OBJ_GET_MAX_BYTES: usize = 1024 * 1024;

pub async fn list_buckets(client: &async_nats::Client) -> Result<Vec<ObjectStoreBucket>> {
    let js = jetstream::new(client.clone());
    let mut iter = js.streams();
    let mut out = Vec::new();
    while let Some(info) = iter
        .try_next()
        .await
        .map_err(|e| anyhow!("failed to iterate jetstream streams: {e}"))?
    {
        let name = info.config.name.clone();
        let Some(bucket) = name.strip_prefix(OBJ_STREAM_PREFIX) else {
            continue;
        };
        out.push(ObjectStoreBucket {
            bucket_name: bucket.to_string(),
            description: if info.config.description.is_some() {
                info.config.description.clone()
            } else {
                None
            },
            total_objects: info.state.messages,
            total_bytes: info.state.bytes,
            ttl_seconds: info.config.max_age.as_secs(),
            storage: storage_str(info.config.storage),
        });
    }
    Ok(out)
}

pub async fn list_objects(
    client: &async_nats::Client,
    bucket: &str,
    limit: usize,
) -> Result<Vec<ObjectStoreEntry>> {
    let cap = limit.clamp(1, 1000);
    let js = jetstream::new(client.clone());
    let store = js
        .get_object_store(bucket)
        .await
        .map_err(|e| anyhow!("failed to open object store '{bucket}': {e}"))?;

    let mut iter = store
        .list()
        .await
        .map_err(|e| anyhow!("failed to list objects in '{bucket}': {e}"))?;
    let mut out = Vec::new();
    while let Some(info) = iter
        .try_next()
        .await
        .map_err(|e| anyhow!("error iterating objects: {e}"))?
    {
        if info.deleted {
            continue;
        }
        out.push(ObjectStoreEntry {
            name: info.name.clone(),
            size_bytes: info.size,
            mtime: info.modified.and_then(|t| {
                t.format(&time::format_description::well_known::Rfc3339)
                    .ok()
            }),
            chunks: info.chunks,
            digest: info.digest.clone(),
            description: info.description.clone(),
        });
        if out.len() >= cap {
            break;
        }
    }
    Ok(out)
}

pub async fn get_object(
    client: &async_nats::Client,
    bucket: &str,
    name: &str,
) -> Result<ObjectStoreObject> {
    let js = jetstream::new(client.clone());
    let store = js
        .get_object_store(bucket)
        .await
        .map_err(|e| anyhow!("failed to open object store '{bucket}': {e}"))?;

    let info = store
        .info(name)
        .await
        .map_err(|e| anyhow!("failed to read info for object '{name}': {e}"))?;
    if info.deleted {
        return Err(anyhow!("object '{name}' has been deleted from '{bucket}'"));
    }
    if info.size > OBJ_GET_MAX_BYTES {
        return Err(anyhow!(
            "object '{name}' is {} bytes — too large to return (cap {} bytes). \
             Use a NATS client or Hyerix Desktop to fetch it.",
            info.size,
            OBJ_GET_MAX_BYTES
        ));
    }

    let mut obj = store
        .get(name)
        .await
        .map_err(|e| anyhow!("failed to open object '{name}': {e}"))?;
    let mut buf = Vec::with_capacity(info.size);
    obj.read_to_end(&mut buf)
        .await
        .map_err(|e| anyhow!("failed to read object body: {e}"))?;

    let (payload, encoding) = match std::str::from_utf8(&buf) {
        Ok(s) => (s.to_string(), "utf8".to_string()),
        Err(_) => (BASE64.encode(&buf), "base64".to_string()),
    };

    Ok(ObjectStoreObject {
        name: info.name.clone(),
        bucket: info.bucket.clone(),
        size_bytes: info.size,
        mtime: info.modified.and_then(|t| {
            t.format(&time::format_description::well_known::Rfc3339)
                .ok()
        }),
        chunks: info.chunks,
        digest: info.digest.clone(),
        description: info.description.clone(),
        payload,
        payload_encoding: encoding,
    })
}

fn storage_str(s: StorageType) -> String {
    match s {
        StorageType::File => "file".into(),
        StorageType::Memory => "memory".into(),
    }
}
