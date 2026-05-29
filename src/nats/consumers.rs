use anyhow::{anyhow, Result};
use async_nats::jetstream::{
    self,
    consumer::{AckPolicy, DeliverPolicy, ReplayPolicy},
};
use futures::TryStreamExt;

use super::types::{ConsumerDetail, ConsumerSummary};

pub async fn list_consumers(
    client: &async_nats::Client,
    stream_name: &str,
) -> Result<Vec<ConsumerSummary>> {
    let js = jetstream::new(client.clone());
    let stream = js
        .get_stream(stream_name)
        .await
        .map_err(|e| anyhow!("failed to get stream '{stream_name}': {e}"))?;
    let mut iter = stream.consumers();
    let mut out = Vec::new();
    while let Some(info) = iter
        .try_next()
        .await
        .map_err(|e| anyhow!("failed to list consumers for stream '{stream_name}': {e}"))?
    {
        out.push(ConsumerSummary {
            name: info.name.clone(),
            stream_name: info.stream_name.clone(),
            kind: kind_str(&info.config),
            ack_pending: info.num_ack_pending as i64,
            num_pending: info.num_pending,
            num_redelivered: info.num_redelivered as i64,
            num_waiting: info.num_waiting as i64,
        });
    }
    Ok(out)
}

pub async fn get_consumer(
    client: &async_nats::Client,
    stream_name: &str,
    consumer_name: &str,
) -> Result<ConsumerDetail> {
    let js = jetstream::new(client.clone());
    let stream = js
        .get_stream(stream_name)
        .await
        .map_err(|e| anyhow!("failed to get stream '{stream_name}': {e}"))?;
    let mut consumer: async_nats::jetstream::consumer::Consumer<
        async_nats::jetstream::consumer::Config,
    > = stream.get_consumer(consumer_name).await.map_err(|e| {
        anyhow!("failed to get consumer '{consumer_name}' on stream '{stream_name}': {e}")
    })?;
    let info = consumer
        .info()
        .await
        .map_err(|e| anyhow!("failed to read consumer info for '{consumer_name}': {e}"))?;
    let cfg = &info.config;
    Ok(ConsumerDetail {
        name: info.name.clone(),
        stream_name: info.stream_name.clone(),
        kind: kind_str(cfg),
        durable_name: cfg.durable_name.clone(),
        filter_subject: if cfg.filter_subject.is_empty() {
            None
        } else {
            Some(cfg.filter_subject.clone())
        },
        filter_subjects: cfg.filter_subjects.clone(),
        ack_policy: ack_policy_str(cfg.ack_policy),
        ack_wait_secs: {
            let s = cfg.ack_wait.as_secs_f64();
            if s > 0.0 {
                Some(s)
            } else {
                None
            }
        },
        max_deliver: if cfg.max_deliver > 0 {
            Some(cfg.max_deliver)
        } else {
            None
        },
        deliver_policy: deliver_policy_str(cfg.deliver_policy),
        replay_policy: replay_policy_str(cfg.replay_policy),
        ack_pending: info.num_ack_pending as i64,
        num_pending: info.num_pending,
        num_redelivered: info.num_redelivered as i64,
        num_waiting: info.num_waiting as i64,
        last_delivered_stream_seq: info.delivered.stream_sequence,
        last_delivered_consumer_seq: info.delivered.consumer_sequence,
        ack_floor_stream_seq: info.ack_floor.stream_sequence,
        ack_floor_consumer_seq: info.ack_floor.consumer_sequence,
        created: info
            .created
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default(),
    })
}

fn kind_str(cfg: &async_nats::jetstream::consumer::Config) -> String {
    if cfg.deliver_subject.is_some() {
        "push".into()
    } else {
        "pull".into()
    }
}

fn ack_policy_str(p: AckPolicy) -> String {
    match p {
        AckPolicy::None => "none".into(),
        AckPolicy::All => "all".into(),
        AckPolicy::Explicit => "explicit".into(),
    }
}

fn deliver_policy_str(p: DeliverPolicy) -> String {
    match p {
        DeliverPolicy::All => "all".into(),
        DeliverPolicy::Last => "last".into(),
        DeliverPolicy::New => "new".into(),
        DeliverPolicy::ByStartSequence { .. } => "byStartSequence".into(),
        DeliverPolicy::ByStartTime { .. } => "byStartTime".into(),
        DeliverPolicy::LastPerSubject => "lastPerSubject".into(),
    }
}

fn replay_policy_str(p: ReplayPolicy) -> String {
    match p {
        ReplayPolicy::Instant => "instant".into(),
        ReplayPolicy::Original => "original".into(),
    }
}
