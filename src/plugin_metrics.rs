use crate::{AppState, rpc};
use anyhow::{Result, bail, ensure};
use futures::future::join_all;
use serde::Deserialize;
use serde_json::{Number, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

const DISCOVERY_TTL: Duration = Duration::from_secs(60);
const MAX_COLLECTORS: usize = 64;
const MAX_RESPONSE_BYTES: usize = 1_048_576;
const MAX_FAMILIES: usize = 256;
const MAX_SAMPLES: usize = 10_000;
const MAX_LABELS: usize = 16;
const MAX_NAME_BYTES: usize = 128;
const MAX_HELP_BYTES: usize = 512;
const MAX_LABEL_VALUE_BYTES: usize = 512;

#[derive(Clone, Default)]
pub struct Registry(Arc<Mutex<Cache>>);

#[derive(Default)]
struct Cache {
    methods: Vec<String>,
    refreshed_at: Option<Instant>,
}

impl Registry {
    pub async fn invalidate(&self) {
        self.0.lock().await.refreshed_at = None;
    }

    async fn methods(&self, state: &AppState) -> Result<Vec<String>> {
        {
            let cache = self.0.lock().await;
            if cache
                .refreshed_at
                .is_some_and(|refreshed| refreshed.elapsed() < DISCOVERY_TTL)
            {
                return Ok(cache.methods.clone());
            }
        }

        let response = rpc::call(&state.rpc_path, "help", json!({}), state.rpc_timeout).await?;
        let methods = discover_methods(&response)?;
        let mut cache = self.0.lock().await;
        cache.methods.clone_from(&methods);
        cache.refreshed_at = Some(Instant::now());
        Ok(methods)
    }
}

pub struct Batch {
    pub discovery_duration: Duration,
    pub discovery_error: Option<anyhow::Error>,
    pub collections: Vec<Collection>,
}

pub struct Collection {
    pub rpc: String,
    pub duration: Duration,
    pub result: Result<PluginMetrics>,
}

pub struct PluginMetrics {
    pub namespace: String,
    pub families: Vec<Family>,
    pub sample_count: usize,
}

pub struct Family {
    pub name: String,
    pub help: String,
    pub kind: MetricType,
    pub samples: Vec<Sample>,
}

pub struct Sample {
    pub labels: BTreeMap<String, String>,
    pub value: Number,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    Counter,
    Gauge,
}

impl MetricType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
        }
    }
}

#[derive(Deserialize)]
struct WireMetrics {
    version: u64,
    namespace: String,
    families: Vec<WireFamily>,
}

#[derive(Deserialize)]
struct WireFamily {
    name: String,
    help: String,
    #[serde(rename = "type")]
    kind: MetricType,
    samples: Vec<WireSample>,
}

#[derive(Deserialize)]
struct WireSample {
    #[serde(default)]
    labels: BTreeMap<String, String>,
    value: Number,
}

pub async fn collect(state: &AppState) -> Batch {
    let discovery_started = Instant::now();
    let methods = match state.plugin_metrics.methods(state).await {
        Ok(methods) => methods,
        Err(error) => {
            return Batch {
                discovery_duration: discovery_started.elapsed(),
                discovery_error: Some(error),
                collections: Vec::new(),
            };
        }
    };
    let discovery_duration = discovery_started.elapsed();
    let collections = join_all(methods.into_iter().map(|method| async move {
        let started = Instant::now();
        let result = rpc::call(&state.rpc_path, &method, json!({}), state.rpc_timeout)
            .await
            .and_then(|value| validate_response(&method, value));
        Collection {
            rpc: method,
            duration: started.elapsed(),
            result,
        }
    }))
    .await;

    Batch {
        discovery_duration,
        discovery_error: None,
        collections,
    }
}

fn discover_methods(response: &Value) -> Result<Vec<String>> {
    let help = response
        .get("help")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("help RPC response has no help array"))?;
    let mut methods = BTreeSet::new();
    for entry in help {
        let Some(method) = entry
            .get("command")
            .and_then(Value::as_str)
            .and_then(|command| command.split_whitespace().next())
        else {
            continue;
        };
        if method.ends_with("-metrics") && valid_rpc_name(method) {
            methods.insert(method.to_owned());
        }
    }
    ensure!(
        methods.len() <= MAX_COLLECTORS,
        "found more than {MAX_COLLECTORS} plugin metrics RPCs"
    );
    Ok(methods.into_iter().collect())
}

fn validate_response(method: &str, value: Value) -> Result<PluginMetrics> {
    ensure!(
        serde_json::to_vec(&value)?.len() <= MAX_RESPONSE_BYTES,
        "{method} response exceeds {MAX_RESPONSE_BYTES} bytes"
    );
    let wire: WireMetrics = serde_json::from_value(value)?;
    ensure!(
        wire.version == 1,
        "{method} uses unsupported metrics version {}",
        wire.version
    );
    ensure!(
        valid_metric_name(&wire.namespace),
        "{method} returned an invalid namespace"
    );
    let expected_namespace = method
        .strip_suffix("-metrics")
        .expect("discovery only selects -metrics methods")
        .replace('-', "_");
    ensure!(
        wire.namespace == expected_namespace,
        "{method} namespace must be '{expected_namespace}'"
    );
    ensure!(
        wire.families.len() <= MAX_FAMILIES,
        "{method} returned more than {MAX_FAMILIES} metric families"
    );

    let mut names = BTreeSet::new();
    let mut sample_count = 0_usize;
    let mut families = Vec::with_capacity(wire.families.len());
    for family in wire.families {
        ensure!(
            family.name.len() <= MAX_NAME_BYTES,
            "metric family name is too long"
        );
        ensure!(
            valid_metric_name(&family.name),
            "invalid metric family '{}'",
            family.name
        );
        ensure!(
            names.insert(family.name.clone()),
            "duplicate metric family '{}'",
            family.name
        );
        ensure!(
            family.help.len() <= MAX_HELP_BYTES,
            "help for '{}' is too long",
            family.name
        );
        ensure!(
            !family.help.contains(['\n', '\r']),
            "help for '{}' contains a newline",
            family.name
        );
        if matches!(family.kind, MetricType::Counter) {
            ensure!(
                family.name.ends_with("_total"),
                "counter '{}' must end in _total",
                family.name
            );
        }
        sample_count = sample_count
            .checked_add(family.samples.len())
            .ok_or_else(|| anyhow::anyhow!("sample count overflow"))?;
        ensure!(
            sample_count <= MAX_SAMPLES,
            "{method} returned more than {MAX_SAMPLES} samples"
        );
        for sample in &family.samples {
            ensure!(
                sample.labels.len() <= MAX_LABELS,
                "sample in '{}' has too many labels",
                family.name
            );
            for (name, value) in &sample.labels {
                ensure!(valid_metric_name(name), "invalid label name '{name}'");
                ensure!(
                    value.len() <= MAX_LABEL_VALUE_BYTES,
                    "label '{name}' value is too long"
                );
            }
        }
        families.push(Family {
            name: family.name,
            help: family.help,
            kind: family.kind,
            samples: family
                .samples
                .into_iter()
                .map(|sample| Sample {
                    labels: sample.labels,
                    value: sample.value,
                })
                .collect(),
        });
    }

    if families.is_empty() {
        bail!("{method} returned no metric families");
    }
    Ok(PluginMetrics {
        namespace: wire.namespace,
        families,
        sample_count,
    })
}

fn valid_rpc_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_NAME_BYTES
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn valid_metric_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_NAME_BYTES
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_only_metrics_rpcs() {
        let methods = discover_methods(&json!({"help": [
            {"command": "tracker-metrics "},
            {"command": "pay bolt11 [amount_msat]"},
            {"command": "foo-metrics [unexpected]"},
            {"command": "bad.name-metrics"}
        ]}))
        .unwrap();
        assert_eq!(methods, ["foo-metrics", "tracker-metrics"]);
    }

    #[test]
    fn validates_a_metrics_response() {
        let metrics = validate_response(
            "tracker-metrics",
            json!({
                "version": 1,
                "namespace": "tracker",
                "families": [{
                    "name": "reorgs_total",
                    "help": "Handled reorganizations.",
                    "type": "counter",
                    "samples": [{"labels": {"kind": "chain"}, "value": 2}]
                }]
            }),
        )
        .unwrap();
        assert_eq!(metrics.namespace, "tracker");
        assert_eq!(metrics.sample_count, 1);
    }

    #[test]
    fn rejects_namespace_spoofing_and_bad_counters() {
        let spoofed = json!({
            "version": 1,
            "namespace": "wallet",
            "families": [{
                "name": "healthy", "help": "Health.", "type": "gauge",
                "samples": [{"value": 1}]
            }]
        });
        assert!(validate_response("tracker-metrics", spoofed).is_err());
        let counter = json!({
            "version": 1,
            "namespace": "tracker",
            "families": [{
                "name": "reorgs", "help": "Reorgs.", "type": "counter",
                "samples": [{"value": 1}]
            }]
        });
        assert!(validate_response("tracker-metrics", counter).is_err());
    }
}
