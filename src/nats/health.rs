use std::time::Duration;

use anyhow::Result;
use async_nats::jetstream;
use futures::{StreamExt, TryStreamExt};

use super::types::{ClusterHealth, ConnectedServer};

pub async fn cluster_health(client: &async_nats::Client) -> Result<ClusterHealth> {
    let si = client.server_info();
    let connected = ConnectedServer {
        server_id: si.server_id.clone(),
        server_name: si.server_name.clone(),
        version: si.version.clone(),
        host: si.host.clone(),
        port: si.port,
    };

    let mut server_count = 0usize;
    let mut jetstream_enabled_count = 0usize;
    let mut cluster_leader: Option<String> = None;
    if let Ok(servers) = ping_servers(client).await {
        server_count = servers.len();
        for srv in &servers {
            if srv.jetstream {
                jetstream_enabled_count += 1;
            }
            if cluster_leader.is_none() {
                if let Some(name) = &srv.cluster_leader {
                    cluster_leader = Some(name.clone());
                }
            }
        }
    }
    if server_count == 0 {
        server_count = 1;
        jetstream_enabled_count = 1;
    }

    let js = jetstream::new(client.clone());
    let mut total_streams = 0i32;
    let mut total_consumers = 0i32;
    let mut total_messages = 0u64;
    let mut total_bytes = 0u64;
    let mut leaderless_groups = Vec::new();

    if let Ok(mut iter) =
        std::panic::AssertUnwindSafe(async { Ok::<_, anyhow::Error>(js.streams()) }).await
    {
        while let Ok(Some(info)) = iter.try_next().await {
            total_streams += 1;
            total_consumers += info.state.consumer_count as i32;
            total_messages += info.state.messages;
            total_bytes += info.state.bytes;
            if let Some(cluster) = &info.cluster {
                if cluster.leader.is_none() && !cluster.replicas.is_empty() {
                    leaderless_groups.push(info.config.name.clone());
                }
            }
        }
    }

    Ok(ClusterHealth {
        server_count,
        jetstream_enabled_count,
        cluster_leader,
        leaderless_groups,
        total_streams,
        total_consumers,
        total_messages,
        total_bytes,
        connected_server: connected,
    })
}

struct PingedServer {
    jetstream: bool,
    cluster_leader: Option<String>,
}

async fn ping_servers(client: &async_nats::Client) -> Result<Vec<PingedServer>> {
    let inbox = client.new_inbox();
    let mut sub = client.subscribe(inbox.clone()).await?;
    client
        .publish_with_reply("$SYS.REQ.SERVER.PING", inbox, "".into())
        .await?;
    client.flush().await?;

    let mut servers = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, sub.next()).await {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
            let server = &v["server"];
            let stats = &v["statsz"];
            let jetstream = stats["jetstream"].as_bool().unwrap_or(false)
                || stats["jetstream"]["enabled"].as_bool().unwrap_or(false);
            let cluster_leader = v["server"]["cluster"]["leader"]
                .as_str()
                .map(|s| s.to_string())
                .or_else(|| server["cluster"].as_str().map(|s| s.to_string()));
            servers.push(PingedServer {
                jetstream,
                cluster_leader,
            });
        }
    }
    Ok(servers)
}
