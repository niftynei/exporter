use anyhow::{Result, anyhow, ensure};
use serde::Deserialize;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_LIMIT: u64 = 10_000;
pub const MAX_LIMIT: u64 = 50_000;

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChannelSample {
    pub channel_key: String,
    pub channel_id: Option<String>,
    pub short_channel_id: Option<String>,
    pub peer_id: String,
    pub state: String,
    pub connected: bool,
    pub reestablished: bool,
    pub capacity_msat: u64,
    pub to_us_msat: u64,
    pub spendable_msat: u64,
    pub receivable_msat: u64,
    pub htlc_in_count: u64,
    pub htlc_out_count: u64,
    pub htlc_in_msat: u64,
    pub htlc_out_msat: u64,
}

pub fn amount_msat(value: Option<&Value>) -> u64 {
    value
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str()?.strip_suffix("msat")?.parse().ok())
                .or_else(|| value.get("msat")?.as_u64())
        })
        .unwrap_or(0)
}

impl ChannelSample {
    pub fn from_value(value: &Value, index: usize) -> Self {
        let peer_id = value
            .get("peer_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let channel_id = value
            .get("channel_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let short_channel_id = value
            .get("short_channel_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let channel_key = channel_id
            .clone()
            .or_else(|| short_channel_id.clone())
            .unwrap_or_else(|| format!("pending:{peer_id}:{index}"));
        let mut htlc_in_count = 0u64;
        let mut htlc_out_count = 0u64;
        let mut htlc_in_msat = 0u64;
        let mut htlc_out_msat = 0u64;
        for htlc in value
            .get("htlcs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let amount = amount_msat(htlc.get("amount_msat"));
            if htlc.get("direction").and_then(Value::as_str) == Some("in") {
                htlc_in_count = htlc_in_count.saturating_add(1);
                htlc_in_msat = htlc_in_msat.saturating_add(amount);
            } else {
                htlc_out_count = htlc_out_count.saturating_add(1);
                htlc_out_msat = htlc_out_msat.saturating_add(amount);
            }
        }
        Self {
            channel_key,
            channel_id,
            short_channel_id,
            peer_id,
            state: value
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned(),
            connected: value
                .get("peer_connected")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            reestablished: value
                .get("reestablished")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            capacity_msat: amount_msat(value.get("total_msat")),
            to_us_msat: amount_msat(value.get("to_us_msat")),
            spendable_msat: amount_msat(value.get("spendable_msat")),
            receivable_msat: amount_msat(value.get("receivable_msat")),
            htlc_in_count,
            htlc_out_count,
            htlc_in_msat,
            htlc_out_msat,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RangeQuery {
    #[serde(default)]
    pub start: Option<u64>,
    #[serde(default)]
    pub end: Option<u64>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u64,
}

fn default_limit() -> u64 {
    DEFAULT_LIMIT
}

impl RangeQuery {
    pub fn parse(value: Value) -> Result<Self> {
        let query = if value.is_array() {
            let values = value.as_array().expect("checked array");
            Self {
                start: values.first().and_then(Value::as_u64),
                end: values.get(1).and_then(Value::as_u64),
                channel: values.get(2).and_then(Value::as_str).map(str::to_owned),
                limit: values
                    .get(3)
                    .and_then(Value::as_u64)
                    .unwrap_or(DEFAULT_LIMIT),
            }
        } else {
            serde_json::from_value(value)?
        };
        query.validate()?;
        Ok(query)
    }

    pub fn bounds(&self) -> (u64, u64) {
        (self.start.unwrap_or(0), self.end.unwrap_or_else(now))
    }

    fn validate(&self) -> Result<()> {
        let (start, end) = self.bounds();
        ensure!(start <= end, "start must not be after end");
        ensure!(
            (1..=MAX_LIMIT).contains(&self.limit),
            "limit must be between 1 and {MAX_LIMIT}"
        );
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct PairQuery {
    #[serde(flatten)]
    pub range: RangeQuery,
    #[serde(default = "default_interval")]
    pub interval: u64,
    #[serde(default)]
    pub in_channel: Option<String>,
    #[serde(default)]
    pub out_channel: Option<String>,
    #[serde(default)]
    pub min_ppm: Option<f64>,
    #[serde(default)]
    pub max_ppm: Option<f64>,
}

fn default_interval() -> u64 {
    3_600
}

impl PairQuery {
    pub fn parse(value: Value) -> Result<Self> {
        let query = if let Some(values) = value.as_array() {
            Self {
                range: RangeQuery {
                    start: values.first().and_then(Value::as_u64),
                    end: values.get(1).and_then(Value::as_u64),
                    channel: None,
                    limit: values
                        .get(7)
                        .and_then(Value::as_u64)
                        .unwrap_or(DEFAULT_LIMIT),
                },
                interval: values
                    .get(2)
                    .and_then(Value::as_u64)
                    .unwrap_or_else(default_interval),
                in_channel: values.get(3).and_then(Value::as_str).map(str::to_owned),
                out_channel: values.get(4).and_then(Value::as_str).map(str::to_owned),
                min_ppm: values.get(5).and_then(Value::as_f64),
                max_ppm: values.get(6).and_then(Value::as_f64),
            }
        } else {
            serde_json::from_value(value)
                .map_err(|error| anyhow!("invalid cln-history-pairs parameters: {error}"))?
        };
        query.range.validate()?;
        ensure!(
            (60..=31_536_000).contains(&query.interval),
            "interval must be between 60 seconds and 1 year"
        );
        if let (Some(min), Some(max)) = (query.min_ppm, query.max_ppm) {
            ensure!(min <= max, "min_ppm must not exceed max_ppm");
        }
        Ok(query)
    }
}

#[derive(Debug, Deserialize)]
pub struct EventQuery {
    #[serde(flatten)]
    pub range: RangeQuery,
    #[serde(default)]
    pub event_type: Option<String>,
}

impl EventQuery {
    pub fn parse(value: Value) -> Result<Self> {
        let query = if let Some(values) = value.as_array() {
            Self {
                range: RangeQuery {
                    start: values.first().and_then(Value::as_u64),
                    end: values.get(1).and_then(Value::as_u64),
                    channel: values.get(2).and_then(Value::as_str).map(str::to_owned),
                    limit: values
                        .get(4)
                        .and_then(Value::as_u64)
                        .unwrap_or(DEFAULT_LIMIT),
                },
                event_type: values.get(3).and_then(Value::as_str).map(str::to_owned),
            }
        } else {
            serde_json::from_value(value)?
        };
        query.range.validate()?;
        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use super::{ChannelSample, PairQuery, RangeQuery};
    use serde_json::json;

    #[test]
    fn parses_channel_liquidity_and_directional_htlcs() {
        let sample = ChannelSample::from_value(
            &json!({
                "peer_id": "peer", "channel_id": "full", "short_channel_id": "1x2x3",
                "state": "CHANNELD_NORMAL", "peer_connected": true, "reestablished": true,
                "total_msat": 1_000_000, "to_us_msat": 600_000,
                "spendable_msat": {"msat": 590_000}, "receivable_msat": "390000msat",
                "htlcs": [
                    {"direction": "in", "amount_msat": 10_000},
                    {"direction": "out", "amount_msat": "20000msat"}
                ]
            }),
            0,
        );
        assert_eq!(sample.channel_key, "full");
        assert_eq!(sample.spendable_msat, 590_000);
        assert_eq!((sample.htlc_in_count, sample.htlc_out_count), (1, 1));
        assert_eq!(
            (sample.htlc_in_msat, sample.htlc_out_msat),
            (10_000, 20_000)
        );
    }

    #[test]
    fn validates_query_bounds_and_limits() {
        assert!(RangeQuery::parse(json!({"start": 20, "end": 10})).is_err());
        assert!(RangeQuery::parse(json!({"limit": 0})).is_err());
        assert!(PairQuery::parse(json!({"interval": 59})).is_err());
    }
}
