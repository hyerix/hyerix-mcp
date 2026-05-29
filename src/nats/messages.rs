use std::time::Duration;

use anyhow::{anyhow, Result};
use async_nats::jetstream::{
    self,
    consumer::{AckPolicy, DeliverPolicy},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures::{StreamExt, TryStreamExt};

use super::types::{
    BrowseResult, BrowsedMessage, MessageHeader, PublishResult, ReceivedMessage, SubscribeResult,
};

pub async fn browse_messages(
    client: &async_nats::Client,
    stream_name: &str,
    limit: usize,
    subject_filter: Option<&str>,
) -> Result<BrowseResult> {
    let limit = limit.clamp(1, 200);
    let js = jetstream::new(client.clone());
    let mut stream = js
        .get_stream(stream_name)
        .await
        .map_err(|e| anyhow!("failed to get stream '{stream_name}': {e}"))?;
    let info = stream
        .info()
        .await
        .map_err(|e| anyhow!("failed to read info for stream '{stream_name}': {e}"))?;
    let total = info.state.messages;
    let first_seq = info.state.first_sequence;
    let last_seq = info.state.last_sequence;

    if total == 0 || first_seq == 0 {
        return Ok(BrowseResult {
            stream_name: stream_name.into(),
            messages: Vec::new(),
            returned: 0,
            has_more: false,
            total_messages: total,
        });
    }

    let filter_str = subject_filter.unwrap_or("").to_string();
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            deliver_policy: DeliverPolicy::ByStartSequence {
                start_sequence: first_seq,
            },
            filter_subject: filter_str,
            ack_policy: AckPolicy::None,
            inactive_threshold: std::time::Duration::from_secs(30),
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow!("failed to create browse consumer: {e}"))?;

    let mut batch = consumer
        .fetch()
        .max_messages(limit)
        .expires(std::time::Duration::from_secs(5))
        .messages()
        .await
        .map_err(|e| anyhow!("failed to fetch messages: {e}"))?;

    let mut messages = Vec::with_capacity(limit);
    while let Some(msg) = batch
        .try_next()
        .await
        .map_err(|e| anyhow!("error reading message batch: {e}"))?
    {
        messages.push(convert_message(&msg)?);
        if messages.len() >= limit {
            break;
        }
    }

    let returned = messages.len();
    let has_more = messages
        .last()
        .map(|m| m.sequence < last_seq)
        .unwrap_or(false);

    Ok(BrowseResult {
        stream_name: stream_name.into(),
        messages,
        returned,
        has_more,
        total_messages: total,
    })
}

pub async fn publish_message(
    client: &async_nats::Client,
    subject: &str,
    payload: Vec<u8>,
    headers: Option<async_nats::HeaderMap>,
) -> Result<PublishResult> {
    let payload_size = payload.len();
    let js = jetstream::new(client.clone());

    let js_result = if let Some(ref hdrs) = headers {
        js.publish_with_headers(subject.to_string(), hdrs.clone(), payload.clone().into())
            .await
    } else {
        js.publish(subject.to_string(), payload.clone().into())
            .await
    };

    if let Ok(ack_future) = js_result {
        if let Ok(ack) = ack_future.await {
            return Ok(PublishResult {
                subject: subject.into(),
                payload_size,
                jetstream_stream: Some(ack.stream.clone()),
                jetstream_sequence: Some(ack.sequence),
            });
        }
    }

    let core_result = if let Some(hdrs) = headers {
        client
            .publish_with_headers(subject.to_string(), hdrs, payload.into())
            .await
    } else {
        client.publish(subject.to_string(), payload.into()).await
    };

    core_result.map_err(|e| anyhow!("failed to publish to '{subject}': {e}"))?;

    Ok(PublishResult {
        subject: subject.into(),
        payload_size,
        jetstream_stream: None,
        jetstream_sequence: None,
    })
}

pub async fn subscribe_collect(
    client: &async_nats::Client,
    subject: &str,
    timeout_seconds: u64,
    max_messages: usize,
    queue_group: Option<&str>,
) -> Result<SubscribeResult> {
    let timeout = timeout_seconds.clamp(1, 60);
    let cap = max_messages.clamp(1, 100);

    let mut sub = match queue_group {
        Some(q) if !q.is_empty() => client
            .queue_subscribe(subject.to_string(), q.to_string())
            .await
            .map_err(|e| anyhow!("failed to queue-subscribe to '{subject}' ({q}): {e}"))?,
        _ => client
            .subscribe(subject.to_string())
            .await
            .map_err(|e| anyhow!("failed to subscribe to '{subject}': {e}"))?,
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
    let mut messages = Vec::with_capacity(cap);
    let mut timed_out = false;
    while messages.len() < cap {
        match tokio::time::timeout_at(deadline, sub.next()).await {
            Ok(Some(msg)) => messages.push(convert_core_message(&msg)),
            Ok(None) => break,
            Err(_) => {
                timed_out = true;
                break;
            }
        }
    }

    let _ = sub.unsubscribe().await;

    Ok(SubscribeResult {
        subject: subject.into(),
        queue_group: queue_group.filter(|q| !q.is_empty()).map(|q| q.to_string()),
        timeout_seconds: timeout,
        max_messages: cap,
        received: messages.len(),
        timed_out,
        messages,
    })
}

pub async fn request_reply(
    client: &async_nats::Client,
    subject: &str,
    payload: Vec<u8>,
    headers: Option<async_nats::HeaderMap>,
    timeout_seconds: u64,
) -> Result<ReceivedMessage> {
    let timeout = timeout_seconds.clamp(1, 30);
    let fut = async {
        if let Some(hdrs) = headers {
            client
                .request_with_headers(subject.to_string(), hdrs, payload.into())
                .await
        } else {
            client.request(subject.to_string(), payload.into()).await
        }
    };
    let reply = tokio::time::timeout(Duration::from_secs(timeout), fut)
        .await
        .map_err(|_| anyhow!("request to '{subject}' timed out after {timeout}s"))?
        .map_err(|e| anyhow!("request to '{subject}' failed: {e}"))?;
    Ok(convert_core_message(&reply))
}

fn convert_core_message(msg: &async_nats::Message) -> ReceivedMessage {
    let bytes = &msg.payload;
    let (payload, encoding) = match std::str::from_utf8(bytes) {
        Ok(s) => (s.to_string(), "utf8".to_string()),
        Err(_) => (BASE64.encode(bytes), "base64".to_string()),
    };
    let headers = match &msg.headers {
        Some(map) => map
            .iter()
            .flat_map(|(name, values)| {
                values.iter().map(move |v| MessageHeader {
                    key: name.to_string(),
                    value: v.to_string(),
                })
            })
            .collect(),
        None => Vec::new(),
    };
    ReceivedMessage {
        subject: msg.subject.to_string(),
        reply: msg.reply.as_ref().map(|s| s.to_string()),
        payload,
        payload_encoding: encoding,
        payload_size: bytes.len(),
        headers,
    }
}

fn convert_message(msg: &async_nats::jetstream::Message) -> Result<BrowsedMessage> {
    let info = msg
        .info()
        .map_err(|e| anyhow!("failed to parse message info: {e}"))?;
    let sequence = info.stream_sequence;
    let timestamp = info
        .published
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();

    let payload_bytes = &msg.payload;
    let payload_size = payload_bytes.len();

    let (payload, encoding) = match std::str::from_utf8(payload_bytes) {
        Ok(s) => (s.to_string(), "utf8".to_string()),
        Err(_) => (BASE64.encode(payload_bytes), "base64".to_string()),
    };

    let headers = match &msg.headers {
        Some(map) => map
            .iter()
            .flat_map(|(name, values)| {
                values.iter().map(move |v| MessageHeader {
                    key: name.to_string(),
                    value: v.to_string(),
                })
            })
            .collect(),
        None => Vec::new(),
    };

    Ok(BrowsedMessage {
        sequence,
        subject: msg.subject.to_string(),
        payload,
        payload_encoding: encoding,
        payload_size,
        headers,
        timestamp,
    })
}
