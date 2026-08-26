use anyhow::Result;
use cln_plugin::Plugin;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

const MAX_DYNAMIC_KEYS: usize = 128;
const MAX_TRACKED_PEERS: usize = 2048;
const MAX_PENDING_INVOICES: usize = 4096;
pub const LATENCY_BUCKETS: [u64; 9] = [1, 5, 10, 30, 60, 300, 900, 3600, u64::MAX];

#[derive(Clone, Default)]
pub struct EventState(pub Arc<Mutex<EventCounters>>);

#[derive(Default)]
pub struct EventCounters {
    pub notifications: BTreeMap<String, u64>,
    pub warnings: BTreeMap<(String, String), u64>,
    pub plugin_events: BTreeMap<(String, String), u64>,
    pub channel_transitions: BTreeMap<(String, String), u64>,
    pub forwards: BTreeMap<String, u64>,
    pub coin_movements: BTreeMap<(String, String), u64>,
    pub latest_block_height: Option<u64>,
    pub latest_block_time: Option<u64>,
    pub peers: BTreeMap<String, PeerConnection>,
    pub forwarding_channels: BTreeMap<String, ForwardingFlow>,
    pub invoice_events: BTreeMap<String, u64>,
    pub invoice_created_at: BTreeMap<String, u64>,
    pub invoice_latency: LatencyHistogram,
    pub sendpay_events: BTreeMap<String, u64>,
    pub sendpay_latency: LatencyHistogram,
}

#[derive(Default)]
pub struct PeerConnection {
    pub connected_since: Option<u64>,
    pub connected_seconds_total: u64,
    pub connects: u64,
    pub disconnects: u64,
}

#[derive(Default)]
pub struct ForwardingFlow {
    pub received_msat: u64,
    pub sent_msat: u64,
    pub fees_msat: u64,
}

#[derive(Default)]
pub struct LatencyHistogram {
    pub buckets: [u64; LATENCY_BUCKETS.len()],
    pub count: u64,
    pub sum_seconds: u64,
}

impl LatencyHistogram {
    fn observe(&mut self, seconds: u64) {
        self.count = self.count.saturating_add(1);
        self.sum_seconds = self.sum_seconds.saturating_add(seconds);
        for (index, upper) in LATENCY_BUCKETS.iter().enumerate() {
            if seconds <= *upper {
                self.buckets[index] = self.buckets[index].saturating_add(1);
            }
        }
    }
}

fn bounded_increment<K: Ord + Clone>(map: &mut BTreeMap<K, u64>, key: K, overflow: K) {
    let selected = if map.contains_key(&key) || map.len() < MAX_DYNAMIC_KEYS {
        key
    } else {
        overflow
    };
    *map.entry(selected).or_default() += 1;
}

fn field<'a>(body: &'a Value, name: &str) -> &'a str {
    body.get(name).and_then(Value::as_str).unwrap_or("unknown")
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn amount_msat(value: Option<&Value>) -> u64 {
    value
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str()?.strip_suffix("msat")?.parse().ok())
                .or_else(|| value.get("msat")?.as_u64())
        })
        .unwrap_or(0)
}

pub async fn on_notification(plugin: Plugin<crate::AppState>, value: Value) -> Result<()> {
    let Some((topic, body)) = value.as_object().and_then(|o| o.iter().next()) else {
        return Ok(());
    };
    if topic == "shutdown" {
        let _ = plugin.shutdown();
        return Ok(());
    }

    if matches!(topic.as_str(), "plugin_started" | "plugin_stopped") {
        plugin.state().plugin_metrics.invalidate().await;
    }

    let mut counters = plugin.state().events.0.lock().await;
    bounded_increment(
        &mut counters.notifications,
        topic.clone(),
        "other".to_owned(),
    );
    match topic.as_str() {
        "warning" => {
            let key = (
                field(body, "level").to_owned(),
                field(body, "source").to_owned(),
            );
            bounded_increment(
                &mut counters.warnings,
                key,
                ("unknown".to_owned(), "other".to_owned()),
            );
        }
        "plugin_started" | "plugin_stopped" => {
            let action = topic.trim_start_matches("plugin_").to_owned();
            let key = (field(body, "plugin_name").to_owned(), action);
            bounded_increment(
                &mut counters.plugin_events,
                key,
                ("other".to_owned(), "unknown".to_owned()),
            );
        }
        "channel_state_changed" => {
            let key = (
                field(body, "new_state").to_owned(),
                field(body, "cause").to_owned(),
            );
            bounded_increment(
                &mut counters.channel_transitions,
                key,
                ("other".to_owned(), "unknown".to_owned()),
            );
        }
        "forward_event" => {
            let key = field(body, "status").to_owned();
            bounded_increment(&mut counters.forwards, key.clone(), "other".to_owned());
            if key == "settled" {
                let inbound = field(body, "in_channel").to_owned();
                let outbound = field(body, "out_channel").to_owned();
                if counters.forwarding_channels.len() < MAX_DYNAMIC_KEYS
                    || counters.forwarding_channels.contains_key(&inbound)
                {
                    let flow = counters.forwarding_channels.entry(inbound).or_default();
                    flow.received_msat = flow
                        .received_msat
                        .saturating_add(amount_msat(body.get("in_msat")));
                    flow.fees_msat = flow
                        .fees_msat
                        .saturating_add(amount_msat(body.get("fee_msat")));
                }
                if outbound != "unknown"
                    && (counters.forwarding_channels.len() < MAX_DYNAMIC_KEYS
                        || counters.forwarding_channels.contains_key(&outbound))
                {
                    let flow = counters.forwarding_channels.entry(outbound).or_default();
                    flow.sent_msat = flow
                        .sent_msat
                        .saturating_add(amount_msat(body.get("out_msat")));
                }
            }
        }
        "coin_movement" => {
            let key = (
                field(body, "type").to_owned(),
                field(body, "primary_tag").to_owned(),
            );
            bounded_increment(
                &mut counters.coin_movements,
                key,
                ("other".to_owned(), "other".to_owned()),
            );
        }
        "block_added" => {
            counters.latest_block_height = body.get("height").and_then(Value::as_u64);
            counters.latest_block_time = Some(now());
        }
        "connect" => {
            let peer = field(body, "id").to_owned();
            if counters.peers.len() < MAX_TRACKED_PEERS || counters.peers.contains_key(&peer) {
                let connection = counters.peers.entry(peer).or_default();
                connection.connects = connection.connects.saturating_add(1);
                connection.connected_since.get_or_insert_with(now);
            }
        }
        "disconnect" => {
            let peer = field(body, "id").to_owned();
            if counters.peers.len() < MAX_TRACKED_PEERS || counters.peers.contains_key(&peer) {
                let connection = counters.peers.entry(peer).or_default();
                connection.disconnects = connection.disconnects.saturating_add(1);
                if let Some(since) = connection.connected_since.take() {
                    connection.connected_seconds_total = connection
                        .connected_seconds_total
                        .saturating_add(now().saturating_sub(since));
                }
            }
        }
        "invoice_creation" => {
            *counters
                .invoice_events
                .entry("created".to_owned())
                .or_default() += 1;
            let label = field(body, "label").to_owned();
            if counters.invoice_created_at.len() < MAX_PENDING_INVOICES
                || counters.invoice_created_at.contains_key(&label)
            {
                counters.invoice_created_at.insert(label, now());
            }
        }
        "invoice_payment" => {
            *counters
                .invoice_events
                .entry("paid".to_owned())
                .or_default() += 1;
            if let Some(created_at) = counters.invoice_created_at.remove(field(body, "label")) {
                counters
                    .invoice_latency
                    .observe(now().saturating_sub(created_at));
            }
        }
        "sendpay_success" => {
            *counters
                .sendpay_events
                .entry("success".to_owned())
                .or_default() += 1;
            if let (Some(created), Some(completed)) = (
                body.get("created_at").and_then(Value::as_u64),
                body.get("completed_at").and_then(Value::as_u64),
            ) {
                counters
                    .sendpay_latency
                    .observe(completed.saturating_sub(created));
            }
        }
        "sendpay_failure" => {
            *counters
                .sendpay_events
                .entry("failure".to_owned())
                .or_default() += 1;
            let data = body.get("data").unwrap_or(body);
            if let (Some(created), Some(completed)) = (
                data.get("created_at").and_then(Value::as_u64),
                data.get("completed_at").and_then(Value::as_u64),
            ) {
                counters
                    .sendpay_latency
                    .observe(completed.saturating_sub(created));
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{LatencyHistogram, MAX_DYNAMIC_KEYS, bounded_increment};
    use std::collections::BTreeMap;

    #[test]
    fn dynamic_labels_are_bounded() {
        let mut values = BTreeMap::new();
        for index in 0..MAX_DYNAMIC_KEYS + 5 {
            bounded_increment(&mut values, format!("key-{index}"), "other".to_owned());
        }
        assert_eq!(values.len(), MAX_DYNAMIC_KEYS + 1);
        assert_eq!(values["other"], 5);
    }

    #[test]
    fn latency_histogram_buckets_are_cumulative() {
        let mut histogram = LatencyHistogram::default();
        histogram.observe(7);
        assert_eq!(histogram.count, 1);
        assert_eq!(histogram.sum_seconds, 7);
        assert_eq!(histogram.buckets[1], 0);
        assert_eq!(histogram.buckets[2], 1);
        assert_eq!(histogram.buckets.last(), Some(&1));
    }
}
