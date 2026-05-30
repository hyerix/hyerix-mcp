use async_nats::HeaderMap;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    schemars, tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use serde::Deserialize;

use crate::nats::{connection::LazyClient, consumers, health, kv, messages, object_store, streams};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListStreamsArgs {
    /// Optional substring match against stream name.
    #[serde(default)]
    pub filter: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetStreamArgs {
    /// Stream name (case-sensitive).
    pub name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ListConsumersArgs {
    /// Stream name whose consumers to list.
    pub stream_name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetConsumerArgs {
    pub stream_name: String,
    pub consumer_name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct BrowseMessagesArgs {
    pub stream_name: String,
    /// Max messages to return (1-200). Default 20.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Optional NATS subject pattern, e.g. "orders.eu.*".
    #[serde(default)]
    pub subject_filter: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct KvGetArgs {
    pub bucket: String,
    pub key: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PublishMessageArgs {
    /// NATS subject (e.g. 'orders.eu.created'). No wildcards.
    pub subject: String,
    /// Message body. UTF-8 string or base64-encoded bytes (see payload_encoding).
    pub payload: String,
    /// Payload encoding: "utf8" (default) or "base64".
    #[serde(default)]
    pub payload_encoding: Option<String>,
    /// Optional key-value headers.
    #[serde(default)]
    pub headers: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SubscribeArgs {
    /// NATS subject or wildcard (e.g. 'orders.eu.*').
    pub subject: String,
    /// Stop after this many seconds (1-60). Default 10.
    #[serde(default)]
    pub timeout_seconds: Option<u32>,
    /// Stop after this many messages (1-100). Default 10.
    #[serde(default)]
    pub max_messages: Option<u32>,
    /// Optional queue group name — distribute messages with other subscribers in the same group.
    #[serde(default)]
    pub queue_group: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RequestReplyArgs {
    /// NATS subject of the request. No wildcards.
    pub subject: String,
    /// Request body. UTF-8 string or base64-encoded bytes (see payload_encoding).
    pub payload: String,
    /// Payload encoding: "utf8" (default) or "base64".
    #[serde(default)]
    pub payload_encoding: Option<String>,
    /// Optional key-value headers attached to the request.
    #[serde(default)]
    pub headers: Option<std::collections::BTreeMap<String, String>>,
    /// Give up after this many seconds (1-30). Default 5.
    #[serde(default)]
    pub timeout_seconds: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct KvListKeysArgs {
    pub bucket: String,
    /// Optional substring match on the key name.
    #[serde(default)]
    pub filter: Option<String>,
    /// Max keys to return (1-1000). Default 100.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ObjListArgs {
    pub bucket: String,
    /// Max objects to return (1-1000). Default 100.
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ObjGetArgs {
    pub bucket: String,
    pub name: String,
}

#[derive(Clone)]
pub struct HyerixMcp {
    client: LazyClient,
    allow_publish: bool,
    #[allow(dead_code)]
    tool_router: ToolRouter<HyerixMcp>,
}

#[tool_router]
impl HyerixMcp {
    pub fn new(client: LazyClient, allow_publish: bool) -> Self {
        Self {
            client,
            allow_publish,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "List all JetStream streams on the connected NATS cluster. Returns name, subjects, message count, byte size, consumer count, retention, and storage for each. Start here when an agent asks 'what streams exist' or before any per-stream operation."
    )]
    async fn list_streams(
        &self,
        Parameters(args): Parameters<ListStreamsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match streams::list_streams(&client, args.filter.as_deref()).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("list_streams failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Get detailed info for one JetStream stream: subjects, retention, storage, replicas, message count, first/last sequence. Call after list_streams to drill into one stream by name."
    )]
    async fn get_stream(
        &self,
        Parameters(args): Parameters<GetStreamArgs>,
    ) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match streams::get_stream(&client, &args.name).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("get_stream failed: {e:#}"))),
        }
    }

    #[tool(
        description = "List all consumers attached to a JetStream stream. Returns name, kind (push/pull), ack pending, num pending, num redelivered, num waiting. Use this to find lagging or unhealthy consumers."
    )]
    async fn list_consumers(
        &self,
        Parameters(args): Parameters<ListConsumersArgs>,
    ) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match consumers::list_consumers(&client, &args.stream_name).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("list_consumers failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Get detailed info for one consumer: ack wait, max deliver, filter subjects, current ack pending, num pending, num redelivered, last delivered sequence. The diagnostic payload for 'why is this consumer falling behind?'"
    )]
    async fn get_consumer(
        &self,
        Parameters(args): Parameters<GetConsumerArgs>,
    ) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match consumers::get_consumer(&client, &args.stream_name, &args.consumer_name).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("get_consumer failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Read recent messages from a JetStream stream without consuming them. Returns subject, payload (utf-8 if decodable, else base64), headers, sequence, timestamp. Use this to inspect what's actually flowing on a stream."
    )]
    async fn browse_messages(
        &self,
        Parameters(args): Parameters<BrowseMessagesArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(20) as usize;
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match messages::browse_messages(
            &client,
            &args.stream_name,
            limit,
            args.subject_filter.as_deref(),
        )
        .await
        {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("browse_messages failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Get the current value and revision of a key in a JetStream KV bucket. Returns value (utf-8 if decodable, else base64), revision, and created timestamp. Returns isError=true if the bucket or key does not exist."
    )]
    async fn kv_get(
        &self,
        Parameters(args): Parameters<KvGetArgs>,
    ) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match kv::get_value(&client, &args.bucket, &args.key).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("kv_get failed: {e:#}"))),
        }
    }

    #[tool(
        description = "One-call rollup of cluster health: server count, JetStream enabled servers, current cluster leader, any leaderless RAFT groups, total streams, total consumers, total messages, total bytes. Use this as the first call when an agent asks 'is the cluster ok?'"
    )]
    async fn get_cluster_health(&self) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match health::cluster_health(&client).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("get_cluster_health failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Publish a single message to a NATS subject. Use only when the user has explicitly asked to publish — this is a mutation. Returns the published subject and payload size on success. Does NOT confirm delivery to any consumer. Disabled by default; the server must be started with --allow-publish."
    )]
    async fn publish_message(
        &self,
        Parameters(args): Parameters<PublishMessageArgs>,
    ) -> Result<CallToolResult, McpError> {
        if !self.allow_publish {
            return Ok(error_result(
                "publish_message is disabled. Restart hyerix-mcp with --allow-publish to enable."
                    .into(),
            ));
        }
        let encoding = args
            .payload_encoding
            .as_deref()
            .unwrap_or("utf8")
            .to_lowercase();
        let payload_bytes = match encoding.as_str() {
            "utf8" => args.payload.into_bytes(),
            "base64" => match BASE64.decode(args.payload.as_bytes()) {
                Ok(b) => b,
                Err(e) => return Ok(error_result(format!("invalid base64 payload: {e}"))),
            },
            other => {
                return Ok(error_result(format!(
                    "unknown payload_encoding '{other}' (use 'utf8' or 'base64')"
                )));
            }
        };

        let headers_map = args.headers.map(|m| {
            let mut hm = HeaderMap::new();
            for (k, v) in m {
                hm.insert(k.as_str(), v.as_str());
            }
            hm
        });

        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match messages::publish_message(&client, &args.subject, payload_bytes, headers_map).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("publish_message failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Subscribe to a NATS subject (wildcards allowed) and collect messages until either timeout_seconds elapses OR max_messages are received, whichever comes first. Bounded: timeout caps at 60s, max_messages at 100. Use this to capture a live sample of what is flowing on a subject — not for long-lived streaming."
    )]
    async fn subscribe(
        &self,
        Parameters(args): Parameters<SubscribeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let timeout = args.timeout_seconds.unwrap_or(10) as u64;
        let max_messages = args.max_messages.unwrap_or(10) as usize;
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match messages::subscribe_collect(
            &client,
            &args.subject,
            timeout,
            max_messages,
            args.queue_group.as_deref(),
        )
        .await
        {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("subscribe failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Send one request to a NATS subject and return the single reply (or time out). Use this to probe a service over NATS request/reply — e.g. ping a health endpoint, ask a $SRV-registered service. Bounded: timeout caps at 30s."
    )]
    async fn request_reply(
        &self,
        Parameters(args): Parameters<RequestReplyArgs>,
    ) -> Result<CallToolResult, McpError> {
        let encoding = args
            .payload_encoding
            .as_deref()
            .unwrap_or("utf8")
            .to_lowercase();
        let payload_bytes = match encoding.as_str() {
            "utf8" => args.payload.into_bytes(),
            "base64" => match BASE64.decode(args.payload.as_bytes()) {
                Ok(b) => b,
                Err(e) => return Ok(error_result(format!("invalid base64 payload: {e}"))),
            },
            other => {
                return Ok(error_result(format!(
                    "unknown payload_encoding '{other}' (use 'utf8' or 'base64')"
                )));
            }
        };

        let headers_map = args.headers.map(|m| {
            let mut hm = HeaderMap::new();
            for (k, v) in m {
                hm.insert(k.as_str(), v.as_str());
            }
            hm
        });

        let timeout = args.timeout_seconds.unwrap_or(5) as u64;
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match messages::request_reply(&client, &args.subject, payload_bytes, headers_map, timeout)
            .await
        {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("request_reply failed: {e:#}"))),
        }
    }

    #[tool(
        description = "List keys in a JetStream KV bucket with optional substring filter. Returns key name, current revision, and value size in bytes for each — NOT the values themselves. Call kv_get per key to read values. Bounded: returns at most 1000 keys."
    )]
    async fn kv_list_keys(
        &self,
        Parameters(args): Parameters<KvListKeysArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(100) as usize;
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match kv::list_keys(&client, &args.bucket, args.filter.as_deref(), limit).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("kv_list_keys failed: {e:#}"))),
        }
    }

    #[tool(
        description = "List every JetStream Object Store bucket on the cluster. Returns bucket name, description, total objects, total bytes, TTL seconds, and storage backend for each. Start here before obj_list or obj_get."
    )]
    async fn obj_list_buckets(&self) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match object_store::list_buckets(&client).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("obj_list_buckets failed: {e:#}"))),
        }
    }

    #[tool(
        description = "List objects in one Object Store bucket — metadata only (name, size, mtime, chunks, digest, description). Does NOT download payloads. Use obj_get for that. Bounded: returns at most 1000 entries."
    )]
    async fn obj_list(
        &self,
        Parameters(args): Parameters<ObjListArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(100) as usize;
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match object_store::list_objects(&client, &args.bucket, limit).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("obj_list failed: {e:#}"))),
        }
    }

    #[tool(
        description = "Fetch one object's bytes plus metadata from an Object Store bucket. Returns payload as utf-8 if decodable, otherwise base64. Hard-capped at 1 MiB — returns isError if the object is larger, to protect the agent's context window."
    )]
    async fn obj_get(
        &self,
        Parameters(args): Parameters<ObjGetArgs>,
    ) -> Result<CallToolResult, McpError> {
        let client = match self.client.get().await {
            Ok(c) => c,
            Err(e) => return Ok(error_result(format!("connect failed: {e:#}"))),
        };
        match object_store::get_object(&client, &args.bucket, &args.name).await {
            Ok(v) => json_result(&v),
            Err(e) => Ok(error_result(format!("obj_get failed: {e:#}"))),
        }
    }
}

#[tool_handler]
impl ServerHandler for HyerixMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder().enable_tools().build(),
        )
        .with_server_info(
            Implementation::new("hyerix-mcp", env!("CARGO_PKG_VERSION"))
                .with_website_url("https://hyerix.ai"),
        )
        .with_instructions(
            "Hyerix's MCP surface for NATS. Start with list_streams or get_cluster_health to orient. \
             More from Hyerix at https://hyerix.ai"
                .to_string(),
        )
    }
}

fn json_result<T: serde::Serialize>(value: &T) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string_pretty(value)
        .unwrap_or_else(|e| format!("{{\"error\":\"failed to serialize result: {e}\"}}"));
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

fn error_result(message: String) -> CallToolResult {
    CallToolResult::error(vec![Content::text(message)])
}
