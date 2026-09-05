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
pub struct ChannelAlias {
    pub kind: &'static str,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct ChannelSample {
    pub channel_key: String,
    pub aliases: Vec<ChannelAlias>,
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
    pub blockheight: Option<u64>,
    pub htlc_min_expiry: Option<u64>,
    pub htlc_max_expiry: Option<u64>,
    pub htlc_hooked_count: u64,
    pub htlc_trimmed_count: u64,
    pub max_accepted_htlcs: Option<u64>,
    pub local_policy: ChannelPolicy,
    pub remote_policy: ChannelPolicy,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ChannelPolicy {
    pub fee_base_msat: Option<u64>,
    pub fee_ppm: Option<u64>,
    pub htlc_minimum_msat: Option<u64>,
    pub htlc_maximum_msat: Option<u64>,
    pub cltv_expiry_delta: Option<u64>,
}

impl PartialEq for ChannelSample {
    fn eq(&self, other: &Self) -> bool {
        self.channel_key == other.channel_key
            && self.channel_id == other.channel_id
            && self.short_channel_id == other.short_channel_id
            && self.peer_id == other.peer_id
            && self.state == other.state
            && self.connected == other.connected
            && self.reestablished == other.reestablished
            && self.capacity_msat == other.capacity_msat
            && self.to_us_msat == other.to_us_msat
            && self.spendable_msat == other.spendable_msat
            && self.receivable_msat == other.receivable_msat
            && self.htlc_in_count == other.htlc_in_count
            && self.htlc_out_count == other.htlc_out_count
            && self.htlc_in_msat == other.htlc_in_msat
            && self.htlc_out_msat == other.htlc_out_msat
            && self.blockheight == other.blockheight
            && self.htlc_min_expiry == other.htlc_min_expiry
            && self.htlc_max_expiry == other.htlc_max_expiry
            && self.htlc_hooked_count == other.htlc_hooked_count
            && self.htlc_trimmed_count == other.htlc_trimmed_count
            && self.max_accepted_htlcs == other.max_accepted_htlcs
            && self.local_policy == other.local_policy
            && self.remote_policy == other.remote_policy
    }
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
    pub fn from_value(value: &Value, index: usize, blockheight: Option<u64>) -> Self {
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
        let funding_outpoint = value
            .get("funding_txid")
            .and_then(Value::as_str)
            .zip(value.get("funding_outnum").and_then(Value::as_u64))
            .map(|(txid, outnum)| format!("{txid}:{outnum}"));
        let channel_key = channel_id
            .clone()
            .or_else(|| short_channel_id.clone())
            .or_else(|| funding_outpoint.clone())
            .unwrap_or_else(|| format!("pending:{peer_id}:{index}"));
        let mut aliases = Vec::new();
        if let Some(value) = &channel_id {
            aliases.push(ChannelAlias {
                kind: "channel_id",
                value: value.clone(),
            });
        }
        if let Some(value) = &short_channel_id {
            aliases.push(ChannelAlias {
                kind: "short_channel_id",
                value: value.clone(),
            });
        }
        if let Some(value) = funding_outpoint {
            aliases.push(ChannelAlias {
                kind: "funding_outpoint",
                value,
            });
        }
        for (field, kind) in [("local", "local_alias"), ("remote", "remote_alias")] {
            if let Some(alias) = value
                .get("alias")
                .or_else(|| value.get("aliases"))
                .and_then(|aliases| aliases.get(field))
                .and_then(Value::as_str)
            {
                if !aliases.iter().any(|item| item.value == alias) {
                    aliases.push(ChannelAlias {
                        kind,
                        value: alias.to_owned(),
                    });
                }
            }
        }
        let mut htlc_in_count = 0u64;
        let mut htlc_out_count = 0u64;
        let mut htlc_in_msat = 0u64;
        let mut htlc_out_msat = 0u64;
        let mut htlc_min_expiry = None;
        let mut htlc_max_expiry = None;
        let mut htlc_hooked_count = 0u64;
        let mut htlc_trimmed_count = 0u64;
        for htlc in value
            .get("htlcs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let amount = amount_msat(htlc.get("amount_msat"));
            if let Some(expiry) = htlc.get("expiry").and_then(Value::as_u64) {
                htlc_min_expiry =
                    Some(htlc_min_expiry.map_or(expiry, |current: u64| current.min(expiry)));
                htlc_max_expiry =
                    Some(htlc_max_expiry.map_or(expiry, |current: u64| current.max(expiry)));
            }
            if htlc.get("status").and_then(Value::as_str).is_some() {
                htlc_hooked_count = htlc_hooked_count.saturating_add(1);
            }
            if htlc.get("local_trimmed").and_then(Value::as_bool) == Some(true) {
                htlc_trimmed_count = htlc_trimmed_count.saturating_add(1);
            }
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
            aliases,
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
            // Block height is only meaningful for interpreting pending HTLC
            // expiries. Omitting it for idle channels keeps a new block from
            // turning every polling pass into a channel change point.
            blockheight: if htlc_in_count + htlc_out_count > 0 {
                blockheight
            } else {
                None
            },
            htlc_min_expiry,
            htlc_max_expiry,
            htlc_hooked_count,
            htlc_trimmed_count,
            max_accepted_htlcs: value.get("max_accepted_htlcs").and_then(Value::as_u64),
            local_policy: ChannelPolicy::from_value(
                value
                    .get("updates")
                    .and_then(|updates| updates.get("local")),
            ),
            remote_policy: ChannelPolicy::from_value(
                value
                    .get("updates")
                    .and_then(|updates| updates.get("remote")),
            ),
        }
    }
}

impl ChannelPolicy {
    fn from_value(value: Option<&Value>) -> Self {
        Self {
            fee_base_msat: value.and_then(|value| optional_amount_msat(value.get("fee_base_msat"))),
            fee_ppm: value
                .and_then(|value| value.get("fee_proportional_millionths"))
                .and_then(Value::as_u64),
            htlc_minimum_msat: value
                .and_then(|value| optional_amount_msat(value.get("htlc_minimum_msat"))),
            htlc_maximum_msat: value
                .and_then(|value| optional_amount_msat(value.get("htlc_maximum_msat"))),
            cltv_expiry_delta: value
                .and_then(|value| value.get("cltv_expiry_delta"))
                .and_then(Value::as_u64),
        }
    }
}

fn optional_amount_msat(value: Option<&Value>) -> Option<u64> {
    value.map(|value| amount_msat(Some(value)))
}

#[derive(Debug, Deserialize)]
pub struct RangeQuery {
    #[serde(default)]
    pub start: Option<u64>,
    #[serde(default)]
    pub end: Option<u64>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub cursor: Option<u64>,
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
                cursor: values.get(4).and_then(Value::as_u64),
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
pub struct SampleQuery {
    #[serde(flatten)]
    pub range: RangeQuery,
    #[serde(default)]
    pub interval: Option<u64>,
}

impl SampleQuery {
    pub fn parse(value: Value) -> Result<Self> {
        let query = if value.is_array() {
            let interval = value
                .as_array()
                .and_then(|values| values.get(5))
                .and_then(Value::as_u64);
            Self {
                range: RangeQuery::parse(value)?,
                interval,
            }
        } else {
            serde_json::from_value(value)?
        };
        query.range.validate()?;
        if let Some(interval) = query.interval {
            ensure!(
                (60..=31_536_000).contains(&interval),
                "interval must be between 60 seconds and 1 year"
            );
        }
        Ok(query)
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
                    cursor: values.get(8).and_then(Value::as_u64),
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

#[derive(Debug, Deserialize)]
pub struct ProbeQuery {
    #[serde(flatten)]
    pub range: RangeQuery,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
    #[serde(default)]
    pub interval: Option<u64>,
}

impl ProbeQuery {
    pub fn parse(value: Value) -> Result<Self> {
        let query = if let Some(values) = value.as_array() {
            Self {
                range: RangeQuery {
                    start: values.first().and_then(Value::as_u64),
                    end: values.get(1).and_then(Value::as_u64),
                    channel: values.get(2).and_then(Value::as_str).map(str::to_owned),
                    cursor: values.get(6).and_then(Value::as_u64),
                    limit: values
                        .get(5)
                        .and_then(Value::as_u64)
                        .unwrap_or(DEFAULT_LIMIT),
                },
                status: values.get(3).and_then(Value::as_str).map(str::to_owned),
                interval: values.get(4).and_then(Value::as_u64),
                destination: None,
            }
        } else {
            serde_json::from_value(value)
                .map_err(|error| anyhow!("invalid cln-history-probes parameters: {error}"))?
        };
        query.range.validate()?;
        if let Some(interval) = query.interval {
            ensure!(
                (60..=31_536_000).contains(&interval),
                "interval must be between 60 seconds and 1 year"
            );
        }
        if let Some(status) = query.status.as_deref() {
            ensure!(
                matches!(
                    status,
                    "destination_reached" | "route_failed" | "unexpected_settlement"
                ),
                "status must be destination_reached, route_failed, or unexpected_settlement"
            );
        }
        Ok(query)
    }
}

impl EventQuery {
    pub fn parse(value: Value) -> Result<Self> {
        let query = if let Some(values) = value.as_array() {
            Self {
                range: RangeQuery {
                    start: values.first().and_then(Value::as_u64),
                    end: values.get(1).and_then(Value::as_u64),
                    channel: values.get(2).and_then(Value::as_str).map(str::to_owned),
                    cursor: values.get(5).and_then(Value::as_u64),
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
    use super::{ChannelSample, PairQuery, ProbeQuery, RangeQuery, SampleQuery};
    use serde_json::json;

    #[test]
    fn parses_channel_liquidity_and_directional_htlcs() {
        let sample = ChannelSample::from_value(
            &json!({
                "peer_id": "peer", "channel_id": "full", "short_channel_id": "1x2x3",
                "alias": {"local": "9x9x1", "remote": "9x9x2"},
                "state": "CHANNELD_NORMAL", "peer_connected": true, "reestablished": true,
                "total_msat": 1_000_000, "to_us_msat": 600_000,
                "spendable_msat": {"msat": 590_000}, "receivable_msat": "390000msat",
                "max_accepted_htlcs": 30,
                "updates": {
                    "local": {"fee_base_msat": 1_000, "fee_proportional_millionths": 650,
                        "htlc_minimum_msat": 1, "htlc_maximum_msat": 990_000,
                        "cltv_expiry_delta": 34},
                    "remote": {"fee_base_msat": 0, "fee_proportional_millionths": 40,
                        "htlc_minimum_msat": 1_000, "htlc_maximum_msat": 900_000,
                        "cltv_expiry_delta": 144}
                },
                "htlcs": [
                    {"direction": "in", "amount_msat": 10_000, "expiry": 190,
                        "status": "waiting on htlc_accepted"},
                    {"direction": "out", "amount_msat": "20000msat", "expiry": 210,
                        "local_trimmed": true}
                ]
            }),
            0,
            Some(180),
        );
        assert_eq!(sample.channel_key, "full");
        assert_eq!(sample.aliases.len(), 4);
        assert_eq!(sample.spendable_msat, 590_000);
        assert_eq!((sample.htlc_in_count, sample.htlc_out_count), (1, 1));
        assert_eq!(
            (sample.htlc_in_msat, sample.htlc_out_msat),
            (10_000, 20_000)
        );
        assert_eq!(sample.blockheight, Some(180));
        assert_eq!(
            (sample.htlc_min_expiry, sample.htlc_max_expiry),
            (Some(190), Some(210))
        );
        assert_eq!(
            (sample.htlc_hooked_count, sample.htlc_trimmed_count),
            (1, 1)
        );
        assert_eq!(sample.max_accepted_htlcs, Some(30));
        assert_eq!(sample.local_policy.fee_ppm, Some(650));
        assert_eq!(sample.remote_policy.cltv_expiry_delta, Some(144));
    }

    #[test]
    fn validates_probe_query_filters_and_buckets() {
        let query = ProbeQuery::parse(json!({
            "start": 1_000, "end": 2_000, "channel": "1x2x3",
            "status": "destination_reached", "interval": 300, "limit": 25
        }))
        .unwrap();
        assert_eq!(query.range.channel.as_deref(), Some("1x2x3"));
        assert_eq!(query.interval, Some(300));
        assert!(ProbeQuery::parse(json!({"interval": 30})).is_err());
        assert!(ProbeQuery::parse(json!({"status": "pending"})).is_err());
    }

    #[test]
    fn validates_query_bounds_and_limits() {
        assert!(RangeQuery::parse(json!({"start": 20, "end": 10})).is_err());
        assert!(RangeQuery::parse(json!({"limit": 0})).is_err());
        assert!(PairQuery::parse(json!({"interval": 59})).is_err());
        assert!(SampleQuery::parse(json!({"interval": 59})).is_err());
    }
}
