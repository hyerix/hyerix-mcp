use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct StreamSummary {
    pub name: String,
    pub subjects: Vec<String>,
    pub messages: u64,
    pub bytes: u64,
    pub consumer_count: i32,
    pub retention: String,
    pub storage: String,
}

#[derive(Debug, Serialize)]
pub struct StreamDetail {
    pub name: String,
    pub description: Option<String>,
    pub subjects: Vec<String>,
    pub retention: String,
    pub storage: String,
    pub num_replicas: i32,
    pub max_bytes: i64,
    pub max_age_secs: u64,
    pub messages: u64,
    pub bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub consumer_count: i32,
    pub created: String,
}

#[derive(Debug, Serialize)]
pub struct ConsumerSummary {
    pub name: String,
    pub stream_name: String,
    pub kind: String,
    pub ack_pending: i64,
    pub num_pending: u64,
    pub num_redelivered: i64,
    pub num_waiting: i64,
}

#[derive(Debug, Serialize)]
pub struct ConsumerDetail {
    pub name: String,
    pub stream_name: String,
    pub kind: String,
    pub durable_name: Option<String>,
    pub filter_subject: Option<String>,
    pub filter_subjects: Vec<String>,
    pub ack_policy: String,
    pub ack_wait_secs: Option<f64>,
    pub max_deliver: Option<i64>,
    pub deliver_policy: String,
    pub replay_policy: String,
    pub ack_pending: i64,
    pub num_pending: u64,
    pub num_redelivered: i64,
    pub num_waiting: i64,
    pub last_delivered_stream_seq: u64,
    pub last_delivered_consumer_seq: u64,
    pub ack_floor_stream_seq: u64,
    pub ack_floor_consumer_seq: u64,
    pub created: String,
}

#[derive(Debug, Serialize)]
pub struct BrowsedMessage {
    pub sequence: u64,
    pub subject: String,
    pub payload: String,
    pub payload_encoding: String,
    pub payload_size: usize,
    pub headers: Vec<MessageHeader>,
    pub timestamp: String,
}

#[derive(Debug, Serialize)]
pub struct MessageHeader {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct BrowseResult {
    pub stream_name: String,
    pub messages: Vec<BrowsedMessage>,
    pub returned: usize,
    pub has_more: bool,
    pub total_messages: u64,
}

#[derive(Debug, Serialize)]
pub struct KvEntry {
    pub bucket: String,
    pub key: String,
    pub value: String,
    pub value_encoding: String,
    pub revision: u64,
    pub created: String,
}

#[derive(Debug, Serialize)]
pub struct ClusterHealth {
    pub server_count: usize,
    pub jetstream_enabled_count: usize,
    pub cluster_leader: Option<String>,
    pub leaderless_groups: Vec<String>,
    pub total_streams: i32,
    pub total_consumers: i32,
    pub total_messages: u64,
    pub total_bytes: u64,
    pub connected_server: ConnectedServer,
}

#[derive(Debug, Serialize)]
pub struct ConnectedServer {
    pub server_id: String,
    pub server_name: String,
    pub version: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Serialize)]
pub struct PublishResult {
    pub subject: String,
    pub payload_size: usize,
    pub jetstream_stream: Option<String>,
    pub jetstream_sequence: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ReceivedMessage {
    pub subject: String,
    pub reply: Option<String>,
    pub payload: String,
    pub payload_encoding: String,
    pub payload_size: usize,
    pub headers: Vec<MessageHeader>,
}

#[derive(Debug, Serialize)]
pub struct SubscribeResult {
    pub subject: String,
    pub queue_group: Option<String>,
    pub timeout_seconds: u64,
    pub max_messages: usize,
    pub received: usize,
    pub timed_out: bool,
    pub messages: Vec<ReceivedMessage>,
}

#[derive(Debug, Serialize)]
pub struct KvKeyEntry {
    pub key: String,
    pub revision: u64,
    pub value_size_bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct KvKeyListResult {
    pub bucket: String,
    pub returned: usize,
    pub keys: Vec<KvKeyEntry>,
}

#[derive(Debug, Serialize)]
pub struct ObjectStoreBucket {
    pub bucket_name: String,
    pub description: Option<String>,
    pub total_objects: u64,
    pub total_bytes: u64,
    pub ttl_seconds: u64,
    pub storage: String,
}

#[derive(Debug, Serialize)]
pub struct ObjectStoreEntry {
    pub name: String,
    pub size_bytes: usize,
    pub mtime: Option<String>,
    pub chunks: usize,
    pub digest: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ObjectStoreObject {
    pub name: String,
    pub bucket: String,
    pub size_bytes: usize,
    pub mtime: Option<String>,
    pub chunks: usize,
    pub digest: Option<String>,
    pub description: Option<String>,
    pub payload: String,
    pub payload_encoding: String,
}
