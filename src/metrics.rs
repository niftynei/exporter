use crate::{AppState, plugin_metrics, rpc};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Default)]
struct Metrics {
    output: String,
    family_names: BTreeSet<String>,
    sample_names: BTreeSet<String>,
}

impl Metrics {
    fn family(&mut self, name: &str, help: &str, kind: &str) {
        self.family_names.insert(name.to_owned());
        let help = help.replace('\\', "\\\\").replace('\n', "\\n");
        let _ = writeln!(self.output, "# HELP {name} {help}");
        let _ = writeln!(self.output, "# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl std::fmt::Display) {
        self.sample_names.insert(name.to_owned());
        self.output.push_str(name);
        if !labels.is_empty() {
            self.output.push('{');
            for (index, (key, value)) in labels.iter().enumerate() {
                if index != 0 {
                    self.output.push(',');
                }
                let _ = write!(self.output, "{key}=\"{}\"", escape_label(value));
            }
            self.output.push('}');
        }
        let _ = writeln!(self.output, " {value}");
    }
}

fn render_plugin_metrics(
    metrics: &mut Metrics,
    plugin: &plugin_metrics::PluginMetrics,
) -> anyhow::Result<()> {
    let prefix = format!("cln_{}", plugin.namespace);
    let names = plugin
        .families
        .iter()
        .map(|family| format!("{prefix}_{}", family.name))
        .collect::<Vec<_>>();
    for name in &names {
        anyhow::ensure!(
            !metrics.family_names.contains(name) && !metrics.sample_names.contains(name),
            "metric family '{name}' conflicts with an exporter metric"
        );
    }
    for (family, name) in plugin.families.iter().zip(names) {
        metrics.family(&name, &family.help, family.kind.as_str());
        for sample in &family.samples {
            let labels = sample
                .labels
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>();
            metrics.sample(&name, &labels, &sample.value);
        }
    }
    Ok(())
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn msat(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.strip_suffix("msat")?.parse().ok())
        .or_else(|| value.get("msat")?.as_u64())
}

fn leading_u64(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        value
            .as_str()?
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .ok()
    })
}

fn percentage_ratio(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.strip_suffix('%')?.parse::<f64>().ok())
        .map(|percent| percent / 100.0)
}

fn number(value: &Value, pointer: &str) -> u64 {
    value.pointer(pointer).and_then(Value::as_u64).unwrap_or(0)
}

fn boolean(value: &Value, pointer: &str) -> u8 {
    u8::from(
        value
            .pointer(pointer)
            .and_then(Value::as_bool)
            .unwrap_or(false),
    )
}

fn record_collector(metrics: &mut Metrics, collector: &str, duration: Duration, success: bool) {
    metrics.sample(
        "cln_exporter_collector_success",
        &[("collector", collector)],
        u8::from(success),
    );
    metrics.sample(
        "cln_exporter_collector_duration_seconds",
        &[("collector", collector)],
        duration.as_secs_f64(),
    );
}

fn collect_node(metrics: &mut Metrics, value: &Value) {
    let id = value.get("id").and_then(Value::as_str).unwrap_or("unknown");
    let network = value
        .get("network")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let version = value
        .get("version")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    metrics.sample(
        "cln_node_info",
        &[("id", id), ("network", network), ("version", version)],
        1,
    );
    metrics.sample(
        "cln_node_blockheight",
        &[],
        value
            .get("blockheight")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    );
    metrics.sample(
        "cln_node_bitcoin_synced",
        &[],
        u8::from(value.get("warning_bitcoind_sync").is_none()),
    );
    metrics.sample(
        "cln_node_lightning_synced",
        &[],
        u8::from(value.get("warning_lightningd_sync").is_none()),
    );
    if let Some(fees) = value.get("fees_collected_msat").and_then(msat) {
        metrics.sample("cln_routing_fees_collected_msat", &[], fees);
    }
}

fn collect_funds(metrics: &mut Metrics, value: &Value) {
    let outputs = value
        .get("outputs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    let mut totals = BTreeMap::<&str, u64>::new();
    for output in outputs {
        let status = output
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        *totals.entry(status).or_default() += output.get("amount_msat").and_then(msat).unwrap_or(0);
    }
    for (status, total) in totals {
        metrics.sample("cln_wallet_onchain_msat", &[("status", status)], total);
    }
}

fn channel_is_anchor(channel: &Value) -> bool {
    channel
        .pointer("/channel_type/names")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|name| name.contains("anchor"))
}

fn collect_channels(
    metrics: &mut Metrics,
    value: &Value,
    target_percent: u64,
    blockheight: u64,
    htlc_warning_blocks: u64,
    feerates: Option<&Value>,
) {
    let Some(channels) = value.get("channels").and_then(Value::as_array) else {
        return;
    };
    let mut states = BTreeMap::<&str, u64>::new();
    for (index, channel) in channels.iter().enumerate() {
        let peer = channel
            .get("peer_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let fallback = format!("pending-{index}");
        let channel_id = channel
            .get("short_channel_id")
            .or_else(|| channel.get("channel_id"))
            .and_then(Value::as_str)
            .unwrap_or(&fallback);
        let labels = [("channel", channel_id), ("peer", peer)];
        let state = channel
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        *states.entry(state).or_default() += 1;
        let onchain = matches!(
            state,
            "AWAITING_UNILATERAL" | "FUNDING_SPEND_SEEN" | "ONCHAIN"
        );
        metrics.sample(
            "cln_channel_connected",
            &labels,
            boolean(channel, "/peer_connected"),
        );
        metrics.sample(
            "cln_channel_state",
            &[labels[0], labels[1], ("state", state)],
            1,
        );
        metrics.sample("cln_channel_onchain", &labels, u8::from(onchain));

        let capacity = channel.get("total_msat").and_then(msat).unwrap_or(0);
        let spendable = channel.get("spendable_msat").and_then(msat).unwrap_or(0);
        let receivable = channel.get("receivable_msat").and_then(msat).unwrap_or(0);
        let target = capacity.saturating_mul(target_percent) / 100;
        metrics.sample("cln_channel_capacity_msat", &labels, capacity);
        metrics.sample("cln_channel_spendable_msat", &labels, spendable);
        metrics.sample("cln_channel_receivable_msat", &labels, receivable);
        if let Some(funding) = channel.get("funding") {
            if let Some(fee) = funding.get("fee_paid_msat").and_then(msat) {
                metrics.sample("cln_channel_funding_fee_paid_msat", &labels, fee);
            }
            if let Some(fee) = funding.get("fee_rcvd_msat").and_then(msat) {
                metrics.sample("cln_channel_funding_fee_received_msat", &labels, fee);
            }
        }
        metrics.sample(
            "cln_channel_outbound_liquidity_ratio",
            &labels,
            if capacity == 0 {
                0.0
            } else {
                spendable as f64 / capacity as f64
            },
        );
        metrics.sample(
            "cln_channel_inbound_liquidity_ratio",
            &labels,
            if capacity == 0 {
                0.0
            } else {
                receivable as f64 / capacity as f64
            },
        );
        metrics.sample(
            "cln_channel_outbound_liquidity_shortfall_msat",
            &labels,
            target.saturating_sub(spendable),
        );
        metrics.sample(
            "cln_channel_inbound_liquidity_shortfall_msat",
            &labels,
            target.saturating_sub(receivable),
        );
        let htlcs = channel
            .get("htlcs")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        metrics.sample("cln_channel_htlcs", &labels, htlcs);
        let expiries = channel
            .get("htlcs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|htlc| htlc.get("expiry").and_then(Value::as_u64))
            .collect::<Vec<_>>();
        if let Some(expiry) = expiries.iter().min() {
            metrics.sample("cln_channel_earliest_htlc_expiry_height", &labels, expiry);
            metrics.sample(
                "cln_channel_earliest_htlc_expiry_blocks",
                &labels,
                expiry.saturating_sub(blockheight),
            );
        }
        metrics.sample(
            "cln_channel_htlcs_near_expiry",
            &labels,
            expiries
                .iter()
                .filter(|expiry| expiry.saturating_sub(blockheight) <= htlc_warning_blocks)
                .count(),
        );
        if let Some(max) = channel.get("max_accepted_htlcs").and_then(Value::as_u64) {
            metrics.sample(
                "cln_channel_htlc_slot_utilization_ratio",
                &labels,
                if max == 0 {
                    0.0
                } else {
                    htlcs as f64 / max as f64
                },
            );
        }

        if let Some(current) = channel.pointer("/feerate/perkw").and_then(Value::as_u64) {
            metrics.sample("cln_channel_commitment_feerate_perkw", &labels, current);
            if let Some(perkw) = feerates.and_then(|rates| rates.get("perkw")) {
                let purpose = if channel_is_anchor(channel) {
                    "unilateral_anchor_close"
                } else {
                    "unilateral_close"
                };
                if let Some(recommended) = perkw.get(purpose).and_then(Value::as_u64) {
                    metrics.sample(
                        "cln_channel_feerate_competitiveness_ratio",
                        &labels,
                        if recommended == 0 {
                            0.0
                        } else {
                            current as f64 / recommended as f64
                        },
                    );
                }
                let acceptable = perkw
                    .get("min_acceptable")
                    .and_then(Value::as_u64)
                    .zip(perkw.get("max_acceptable").and_then(Value::as_u64))
                    .is_some_and(|(min, max)| current >= min && current <= max);
                metrics.sample(
                    "cln_channel_feerate_acceptable",
                    &labels,
                    u8::from(acceptable),
                );
            }
        }

        let inflights = channel
            .get("inflight")
            .and_then(Value::as_array)
            .map_or(&[][..], Vec::as_slice);
        metrics.sample("cln_channel_splice_inflight", &labels, inflights.len());
        for (index, inflight) in inflights.iter().enumerate() {
            let candidate = index.to_string();
            let splice_labels = [labels[0], labels[1], ("candidate", candidate.as_str())];
            metrics.sample(
                "cln_channel_splice_amount_sat",
                &splice_labels,
                inflight
                    .get("splice_amount")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            );
            metrics.sample(
                "cln_channel_splice_feerate_perkw",
                &splice_labels,
                inflight.get("feerate").and_then(leading_u64).unwrap_or(0),
            );
            metrics.sample(
                "cln_channel_splice_total_funding_msat",
                &splice_labels,
                inflight
                    .get("total_funding_msat")
                    .and_then(msat)
                    .unwrap_or(0),
            );
        }
    }
    for (state, count) in states {
        metrics.sample("cln_channels", &[("state", state)], count);
    }
}

fn collect_peers(metrics: &mut Metrics, value: &Value) {
    for peer in value
        .get("peers")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let id = peer.get("id").and_then(Value::as_str).unwrap_or("unknown");
        metrics.sample(
            "cln_peer_connected",
            &[("peer", id)],
            boolean(peer, "/connected"),
        );
    }
}

fn collect_feerates(metrics: &mut Metrics, value: &Value) {
    if let Some(perkw) = value.get("perkw") {
        for name in [
            "opening",
            "mutual_close",
            "unilateral_close",
            "unilateral_anchor_close",
            "penalty",
            "splice",
            "floor",
            "min_acceptable",
            "max_acceptable",
        ] {
            if let Some(rate) = perkw.get(name).and_then(Value::as_u64) {
                metrics.sample("cln_bitcoin_feerate_perkw", &[("purpose", name)], rate);
            }
        }
        for estimate in perkw
            .get("estimates")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let blocks = estimate
                .get("blockcount")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .to_string();
            for (kind, field) in [("raw", "feerate"), ("smoothed", "smoothed_feerate")] {
                metrics.sample(
                    "cln_bitcoin_feerate_estimate_perkw",
                    &[("blocks", &blocks), ("kind", kind)],
                    estimate.get(field).and_then(Value::as_u64).unwrap_or(0),
                );
            }
        }
    }
    if let Some(estimates) = value
        .get("onchain_fee_estimates")
        .and_then(Value::as_object)
    {
        for (kind, amount) in estimates {
            if let Some(satoshis) = amount.as_u64() {
                metrics.sample(
                    "cln_bitcoin_onchain_fee_estimate_sat",
                    &[("transaction", kind)],
                    satoshis,
                );
            }
        }
    }
    metrics.sample(
        "cln_bitcoin_feerates_available",
        &[],
        u8::from(value.get("warning_missing_feerates").is_none()),
    );
}

fn collect_plugins(metrics: &mut Metrics, value: &Value) {
    let mut backup_active = false;
    for plugin in value
        .get("plugins")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = plugin
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if name
            .rsplit('/')
            .next()
            .is_some_and(|basename| basename == "backup" || basename == "backup.py")
            && boolean(plugin, "/active") == 1
        {
            backup_active = true;
        }
        metrics.sample(
            "cln_plugin_active",
            &[("plugin", name)],
            boolean(plugin, "/active"),
        );
        metrics.sample(
            "cln_plugin_dynamic",
            &[("plugin", name)],
            boolean(plugin, "/dynamic"),
        );
    }
    metrics.sample("cln_backup_plugin_active", &[], u8::from(backup_active));
    metrics.sample("cln_backup_freshness_supported", &[], 0);
}

fn collect_anchor_tank(
    metrics: &mut Metrics,
    funds: &Value,
    channels: &Value,
    feerates: &Value,
    configs: &Value,
) {
    let confirmed_msat = funds
        .get("outputs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|output| {
            output.get("status").and_then(Value::as_str) == Some("confirmed")
                && !output
                    .get("reserved")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
        })
        .filter_map(|output| output.get("amount_msat").and_then(msat))
        .sum::<u64>();
    let anchor_channels = channels
        .get("channels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|channel| {
            channel_is_anchor(channel)
                && !matches!(
                    channel.get("state").and_then(Value::as_str),
                    Some("CLOSED" | "CLOSINGD_COMPLETE")
                )
        })
        .count() as u64;
    let close_fee_msat = feerates
        .pointer("/onchain_fee_estimates/unilateral_close_satoshis")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .saturating_mul(1000);
    let estimated_required = close_fee_msat.saturating_mul(anchor_channels);
    let configured_required = configs
        .pointer("/configs/min-emergency-msat/value_msat")
        .and_then(msat)
        .unwrap_or(0);
    let required = estimated_required.max(configured_required);
    metrics.sample("cln_anchor_channels", &[], anchor_channels);
    metrics.sample("cln_anchor_tank_available_msat", &[], confirmed_msat);
    metrics.sample(
        "cln_anchor_tank_configured_minimum_msat",
        &[],
        configured_required,
    );
    metrics.sample(
        "cln_anchor_tank_estimated_required_msat",
        &[],
        estimated_required,
    );
    metrics.sample(
        "cln_anchor_tank_shortfall_msat",
        &[],
        required.saturating_sub(confirmed_msat),
    );
    metrics.sample(
        "cln_anchor_tank_coverage_ratio",
        &[],
        if required == 0 {
            1.0
        } else {
            confirmed_msat as f64 / required as f64
        },
    );
}

fn collect_liquidity_ad(metrics: &mut Metrics, value: &Value) {
    let Some(node) = value
        .get("nodes")
        .and_then(Value::as_array)
        .and_then(|nodes| nodes.first())
    else {
        metrics.sample("cln_liquidity_advertisement_enabled", &[], 0);
        return;
    };
    let Some(ad) = node.get("option_will_fund") else {
        metrics.sample("cln_liquidity_advertisement_enabled", &[], 0);
        return;
    };
    metrics.sample("cln_liquidity_advertisement_enabled", &[], 1);
    if let Some(value) = ad.get("lease_fee_base_msat").and_then(msat) {
        metrics.sample("cln_liquidity_ad_lease_fee_base_msat", &[], value);
    }
    for (metric, field) in [
        ("cln_liquidity_ad_lease_fee_basis", "lease_fee_basis"),
        ("cln_liquidity_ad_funding_weight", "funding_weight"),
        (
            "cln_liquidity_ad_channel_fee_max_proportional_thousandths",
            "channel_fee_max_proportional_thousandths",
        ),
    ] {
        if let Some(value) = ad.get(field).and_then(Value::as_u64) {
            metrics.sample(metric, &[], value);
        }
    }
    if let Some(value) = ad.get("channel_fee_max_base_msat").and_then(msat) {
        metrics.sample("cln_liquidity_ad_channel_fee_max_base_msat", &[], value);
    }
}

fn collect_tracker(metrics: &mut Metrics, value: &Value) {
    metrics.sample("cln_tracker_healthy", &[], boolean(value, "/healthy"));
    for status in ["active", "syncing", "deleting"] {
        metrics.sample(
            "cln_tracker_descriptors",
            &[("status", status)],
            number(value, &format!("/descriptors/{status}")),
        );
    }
    metrics.sample(
        "cln_tracker_pending_movements",
        &[],
        number(value, "/descriptors/pending_movements"),
    );
    for incident in value
        .get("incidents")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        metrics.sample(
            "cln_tracker_incident",
            &[
                (
                    "descriptor",
                    incident
                        .get("descriptor")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                ),
                (
                    "code",
                    incident
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                ),
                (
                    "operation",
                    incident
                        .get("operation")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown"),
                ),
            ],
            1,
        );
    }
    metrics.sample(
        "cln_tracker_bwatch_height",
        &[],
        number(value, "/bwatch/current_height"),
    );
    metrics.sample(
        "cln_tracker_bwatch_lag_blocks",
        &[],
        number(value, "/bwatch/lag"),
    );
    let active_rescans = value
        .pointer("/bwatch/active_rescans")
        .and_then(Value::as_array);
    let rescan_count = active_rescans.map_or(0, Vec::len);
    let rescan_blocks_processed = active_rescans
        .into_iter()
        .flatten()
        .filter_map(|rescan| rescan.get("blocks_processed").and_then(Value::as_u64))
        .sum::<u64>();
    let rescan_blocks_total = active_rescans
        .into_iter()
        .flatten()
        .filter_map(|rescan| rescan.get("blocks_total").and_then(Value::as_u64))
        .sum::<u64>();
    metrics.sample("cln_tracker_bwatch_active_rescans", &[], rescan_count);
    metrics.sample(
        "cln_tracker_bwatch_rescan_blocks_processed",
        &[],
        rescan_blocks_processed,
    );
    metrics.sample(
        "cln_tracker_bwatch_rescan_blocks_total",
        &[],
        rescan_blocks_total,
    );
    metrics.sample(
        "cln_tracker_bwatch_rescan_progress_ratio",
        &[],
        if rescan_blocks_total == 0 {
            0.0
        } else {
            rescan_blocks_processed as f64 / rescan_blocks_total as f64
        },
    );
    for (metric, field) in [
        (
            "cln_tracker_reconciliation_failures_total",
            "reconciliation_failures",
        ),
        (
            "cln_tracker_bookkeeper_failures_total",
            "bookkeeper_failures",
        ),
        ("cln_tracker_reorgs_total", "reorgs"),
    ] {
        metrics.sample(metric, &[], number(value, &format!("/counters/{field}")));
    }
}

fn collect_bookkeeper(metrics: &mut Metrics, value: &Value) {
    const MAX_ACCOUNTS: usize = 256;
    let mut overflow = BTreeMap::<&str, u64>::new();
    for (index, account) in value
        .get("accounts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let name = account
            .get("account")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        for balance in account
            .get("balances")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let coin = balance
                .get("coin_type")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let amount = balance.get("balance_msat").and_then(msat).unwrap_or(0);
            if index >= MAX_ACCOUNTS {
                *overflow.entry(coin).or_default() += amount;
                continue;
            }
            metrics.sample(
                "cln_bookkeeper_balance_msat",
                &[("account", name), ("coin", coin)],
                amount,
            );
        }
    }
    for (coin, amount) in overflow {
        metrics.sample(
            "cln_bookkeeper_balance_msat",
            &[("account", "__other__"), ("coin", coin)],
            amount,
        );
    }
}

fn collect_channel_apy(metrics: &mut Metrics, value: &Value) {
    for channel in value
        .get("channels_apy")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let account = channel
            .get("account")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let labels = [("account", account)];
        for (metric, field) in [
            ("cln_channel_routed_in_msat", "routed_in_msat"),
            ("cln_channel_routed_out_msat", "routed_out_msat"),
            ("cln_channel_lease_fee_paid_msat", "lease_fee_paid_msat"),
            ("cln_channel_lease_fee_earned_msat", "lease_fee_earned_msat"),
        ] {
            metrics.sample(
                metric,
                &labels,
                channel.get(field).and_then(msat).unwrap_or(0),
            );
        }
        for (metric, field) in [
            ("cln_channel_apy_ratio", "apy_total"),
            ("cln_channel_lease_apy_ratio", "apy_lease"),
            ("cln_channel_inbound_utilization_ratio", "utilization_in"),
            ("cln_channel_outbound_utilization_ratio", "utilization_out"),
        ] {
            if let Some(ratio) = channel.get(field).and_then(percentage_ratio) {
                metrics.sample(metric, &labels, ratio);
            }
        }
    }
}

async fn timed_rpc(
    state: &AppState,
    method: &str,
    params: Value,
) -> (Duration, anyhow::Result<Value>) {
    let started = Instant::now();
    let result = rpc::call(&state.rpc_path, method, params, state.rpc_timeout).await;
    (started.elapsed(), result)
}

fn finish_collector(
    metrics: &mut Metrics,
    collector: &str,
    result: &(Duration, anyhow::Result<Value>),
    render: impl FnOnce(&mut Metrics, &Value),
) {
    let (duration, result) = result;
    let success = result.is_ok();
    if let Ok(value) = result {
        render(metrics, value);
    }
    record_collector(metrics, collector, *duration, success);
}

async fn collect_events(metrics: &mut Metrics, state: &AppState) {
    let events = state.events.0.lock().await;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    for (topic, count) in &events.notifications {
        metrics.sample("cln_notifications_total", &[("topic", topic)], count);
    }
    for ((level, source), count) in &events.warnings {
        metrics.sample(
            "cln_warnings_total",
            &[("level", level), ("source", source)],
            count,
        );
    }
    for ((plugin, action), count) in &events.plugin_events {
        metrics.sample(
            "cln_plugin_events_total",
            &[("plugin", plugin), ("action", action)],
            count,
        );
    }
    for ((state_name, cause), count) in &events.channel_transitions {
        metrics.sample(
            "cln_channel_state_changes_total",
            &[("state", state_name), ("cause", cause)],
            count,
        );
    }
    for (status, count) in &events.forwards {
        metrics.sample("cln_forward_events_total", &[("status", status)], count);
    }
    for ((kind, tag), count) in &events.coin_movements {
        metrics.sample(
            "cln_coin_movements_total",
            &[("type", kind), ("tag", tag)],
            count,
        );
    }
    for (peer, connection) in &events.peers {
        let connected_seconds = connection.connected_seconds_total.saturating_add(
            connection
                .connected_since
                .map_or(0, |since| now.saturating_sub(since)),
        );
        metrics.sample(
            "cln_peer_connection_seconds_total",
            &[("peer", peer)],
            connected_seconds,
        );
        metrics.sample(
            "cln_peer_connections_total",
            &[("peer", peer)],
            connection.connects,
        );
        metrics.sample(
            "cln_peer_disconnects_total",
            &[("peer", peer)],
            connection.disconnects,
        );
    }
    for (channel, flow) in &events.forwarding_channels {
        metrics.sample(
            "cln_channel_forward_received_msat_total",
            &[("channel", channel)],
            flow.received_msat,
        );
        metrics.sample(
            "cln_channel_forward_sent_msat_total",
            &[("channel", channel)],
            flow.sent_msat,
        );
        metrics.sample(
            "cln_channel_forward_fees_msat_total",
            &[("channel", channel)],
            flow.fees_msat,
        );
        metrics.sample(
            "cln_channel_forward_net_liquidity_drift_msat",
            &[("channel", channel)],
            i128::from(flow.received_msat) - i128::from(flow.sent_msat),
        );
    }
    for (event, count) in &events.invoice_events {
        metrics.sample("cln_invoice_events_total", &[("event", event)], count);
    }
    let created = events.invoice_events.get("created").copied().unwrap_or(0);
    let paid = events.invoice_latency.count;
    metrics.sample(
        "cln_invoice_observed_payment_ratio",
        &[],
        if created == 0 {
            0.0
        } else {
            paid as f64 / created as f64
        },
    );
    render_histogram(
        metrics,
        "cln_invoice_payment_latency_seconds",
        &events.invoice_latency,
    );
    for (outcome, count) in &events.sendpay_events {
        metrics.sample("cln_sendpay_attempts_total", &[("outcome", outcome)], count);
    }
    render_histogram(
        metrics,
        "cln_sendpay_latency_seconds",
        &events.sendpay_latency,
    );
    if let Some(height) = events.latest_block_height {
        metrics.sample("cln_latest_block_notification_height", &[], height);
    }
    if let Some(time) = events.latest_block_time {
        metrics.sample("cln_latest_block_notification_timestamp_seconds", &[], time);
    }
}

fn render_histogram(
    metrics: &mut Metrics,
    name: &str,
    histogram: &crate::events::LatencyHistogram,
) {
    metrics.family(name, "Observed event latency in seconds.", "histogram");
    for (upper, count) in crate::events::LATENCY_BUCKETS
        .iter()
        .zip(histogram.buckets.iter())
    {
        let label = if *upper == u64::MAX {
            "+Inf".to_owned()
        } else {
            upper.to_string()
        };
        metrics.sample(&format!("{name}_bucket"), &[("le", &label)], count);
    }
    metrics.sample(&format!("{name}_sum"), &[], histogram.sum_seconds);
    metrics.sample(&format!("{name}_count"), &[], histogram.count);
}

pub async fn render(state: &AppState) -> String {
    let mut metrics = Metrics::default();
    metrics.family(
        "cln_exporter_up",
        "Whether the exporter process is running.",
        "gauge",
    );
    metrics.family(
        "cln_exporter_collector_success",
        "Whether the most recent collector RPC succeeded.",
        "gauge",
    );
    metrics.family(
        "cln_exporter_collector_duration_seconds",
        "Duration of the most recent collector RPC.",
        "gauge",
    );
    metrics.family(
        "cln_exporter_plugin_collector_success",
        "Whether a discovered plugin metrics RPC returned valid, non-conflicting metrics.",
        "gauge",
    );
    metrics.family(
        "cln_exporter_plugin_collector_duration_seconds",
        "Duration of a discovered plugin metrics RPC.",
        "gauge",
    );
    metrics.family(
        "cln_exporter_plugin_collector_samples",
        "Number of samples accepted from a discovered plugin metrics RPC.",
        "gauge",
    );
    metrics.sample("cln_exporter_up", &[], 1);

    let (
        node,
        funds,
        channels,
        peers,
        feerates,
        configs,
        plugins,
        tracker,
        bookkeeper,
        channel_apy,
        discovered_metrics,
    ) = tokio::join!(
        timed_rpc(state, "getinfo", json!({})),
        timed_rpc(state, "listfunds", json!({})),
        timed_rpc(state, "listpeerchannels", json!({})),
        timed_rpc(state, "listpeers", json!({})),
        timed_rpc(state, "feerates", json!({"style": "perkw"})),
        timed_rpc(
            state,
            "listconfigs",
            json!({"config": "min-emergency-msat"})
        ),
        timed_rpc(state, "plugin", json!({"subcommand": "list"})),
        timed_rpc(state, "tracker-health", json!({})),
        timed_rpc(state, "bkpr-listbalances", json!({})),
        timed_rpc(state, "bkpr-channelsapy", json!({})),
        plugin_metrics::collect(state),
    );

    finish_collector(&mut metrics, "node", &node, collect_node);
    finish_collector(&mut metrics, "funds", &funds, collect_funds);
    finish_collector(&mut metrics, "peers", &peers, collect_peers);
    finish_collector(&mut metrics, "feerates", &feerates, collect_feerates);
    finish_collector(&mut metrics, "configs", &configs, |_, _| {});
    let target = state.liquidity_target_percent;
    let blockheight = node
        .1
        .as_ref()
        .ok()
        .and_then(|value| value.get("blockheight"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let feerates_value = feerates.1.as_ref().ok();
    finish_collector(&mut metrics, "channels", &channels, |metrics, value| {
        collect_channels(
            metrics,
            value,
            target,
            blockheight,
            state.htlc_warning_blocks,
            feerates_value,
        )
    });
    finish_collector(&mut metrics, "plugins", &plugins, collect_plugins);
    finish_collector(&mut metrics, "bookkeeper", &bookkeeper, collect_bookkeeper);
    finish_collector(
        &mut metrics,
        "channel_apy",
        &channel_apy,
        collect_channel_apy,
    );
    if let (Ok(funds), Ok(channels), Ok(feerates), Ok(configs)) =
        (&funds.1, &channels.1, &feerates.1, &configs.1)
    {
        collect_anchor_tank(&mut metrics, funds, channels, feerates, configs);
    }

    let liquidity_ad = if let Some(id) = node
        .1
        .as_ref()
        .ok()
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
    {
        timed_rpc(state, "listnodes", json!({"id": id})).await
    } else {
        (
            Duration::ZERO,
            Err(anyhow::anyhow!("getinfo did not return a node id")),
        )
    };
    finish_collector(
        &mut metrics,
        "liquidity_ad",
        &liquidity_ad,
        collect_liquidity_ad,
    );
    collect_events(&mut metrics, state).await;
    record_collector(
        &mut metrics,
        "plugin_metrics_discovery",
        discovered_metrics.discovery_duration,
        discovered_metrics.discovery_error.is_none(),
    );
    if let Some(error) = &discovered_metrics.discovery_error {
        log::warn!("plugin metrics discovery failed: {error:#}");
    }
    let mut tracker_metrics_rendered = false;
    for collection in discovered_metrics.collections {
        let rpc = collection.rpc;
        let sample_count = collection
            .result
            .as_ref()
            .map_or(0, |plugin| plugin.sample_count);
        let rendered = collection.result.and_then(|plugin| {
            render_plugin_metrics(&mut metrics, &plugin)?;
            if plugin.namespace == "tracker" {
                tracker_metrics_rendered = true;
            }
            Ok(())
        });
        if let Err(error) = &rendered {
            log::warn!("plugin metrics collector {rpc} failed: {error:#}");
        }
        metrics.sample(
            "cln_exporter_plugin_collector_success",
            &[("rpc", &rpc)],
            u8::from(rendered.is_ok()),
        );
        metrics.sample(
            "cln_exporter_plugin_collector_duration_seconds",
            &[("rpc", &rpc)],
            collection.duration.as_secs_f64(),
        );
        metrics.sample(
            "cln_exporter_plugin_collector_samples",
            &[("rpc", &rpc)],
            if rendered.is_ok() { sample_count } else { 0 },
        );
    }
    if !tracker_metrics_rendered {
        finish_collector(&mut metrics, "tracker", &tracker, collect_tracker);
    }
    metrics.output
}

#[cfg(test)]
mod tests {
    use super::{
        Metrics, collect_anchor_tank, collect_channels, collect_tracker, escape_label, msat,
        percentage_ratio, render_plugin_metrics,
    };
    use serde_json::json;

    #[test]
    fn labels_are_escaped() {
        assert_eq!(escape_label("a\\b\n\"c"), "a\\\\b\\n\\\"c");
    }

    #[test]
    fn amounts_accept_cln_wire_shapes() {
        assert_eq!(msat(&json!(42)), Some(42));
        assert_eq!(msat(&json!("42msat")), Some(42));
        assert_eq!(msat(&json!({"msat": 42})), Some(42));
    }

    #[test]
    fn bookkeeper_percentages_become_ratios() {
        assert_eq!(percentage_ratio(&json!("12.5%")), Some(0.125));
    }

    #[test]
    fn samples_use_prometheus_text_format() {
        let mut metrics = Metrics::default();
        metrics.sample("example", &[("label", "value")], 3);
        assert_eq!(metrics.output, "example{label=\"value\"} 3\n");
    }

    #[test]
    fn channel_metrics_include_deadlines_fees_and_splices() {
        let channels = json!({"channels": [{
            "peer_id": "peer",
            "short_channel_id": "1x2x3",
            "state": "CHANNELD_AWAITING_SPLICE",
            "peer_connected": true,
            "total_msat": "1000000msat",
            "spendable_msat": "400000msat",
            "receivable_msat": "500000msat",
            "channel_type": {"names": ["anchors/even"]},
            "feerate": {"perkw": 500},
            "htlcs": [{"expiry": 105}],
            "inflight": [{
                "funding_txid": "not-a-label",
                "feerate": "750perkw",
                "splice_amount": -1000,
                "total_funding_msat": "900000msat"
            }]
        }]});
        let feerates = json!({"perkw": {
            "unilateral_anchor_close": 1000,
            "min_acceptable": 250,
            "max_acceptable": 2000
        }});
        let mut metrics = Metrics::default();
        collect_channels(&mut metrics, &channels, 20, 100, 12, Some(&feerates));
        assert!(metrics.output.contains(
            "cln_channel_earliest_htlc_expiry_blocks{channel=\"1x2x3\",peer=\"peer\"} 5"
        ));
        assert!(metrics.output.contains(
            "cln_channel_feerate_competitiveness_ratio{channel=\"1x2x3\",peer=\"peer\"} 0.5"
        ));
        assert!(metrics.output.contains(
            "cln_channel_splice_amount_sat{channel=\"1x2x3\",peer=\"peer\",candidate=\"0\"} -1000"
        ));
        assert!(!metrics.output.contains("not-a-label"));
    }

    #[test]
    fn anchor_tank_uses_configured_or_estimated_requirement() {
        let funds = json!({"outputs": [{
            "status": "confirmed", "reserved": false, "amount_msat": "100000msat"
        }]});
        let channels = json!({"channels": [{
            "state": "CHANNELD_NORMAL", "channel_type": {"names": ["anchors/even"]}
        }]});
        let feerates = json!({"onchain_fee_estimates": {
            "unilateral_close_satoshis": 200
        }});
        let configs = json!({"configs": {"min-emergency-msat": {
            "value_msat": "150000msat"
        }}});
        let mut metrics = Metrics::default();
        collect_anchor_tank(&mut metrics, &funds, &channels, &feerates, &configs);
        assert!(
            metrics
                .output
                .contains("cln_anchor_tank_estimated_required_msat 200000")
        );
        assert!(
            metrics
                .output
                .contains("cln_anchor_tank_shortfall_msat 100000")
        );
    }

    #[test]
    fn plugin_metrics_are_namespaced_and_collisions_are_rejected() {
        let plugin = crate::plugin_metrics::PluginMetrics {
            namespace: "tracker".to_owned(),
            sample_count: 1,
            families: vec![crate::plugin_metrics::Family {
                name: "healthy".to_owned(),
                help: "Tracker health.".to_owned(),
                kind: crate::plugin_metrics::MetricType::Gauge,
                samples: vec![crate::plugin_metrics::Sample {
                    labels: std::collections::BTreeMap::new(),
                    value: 1.into(),
                }],
            }],
        };
        let mut metrics = Metrics::default();
        render_plugin_metrics(&mut metrics, &plugin).unwrap();
        assert!(metrics.output.contains("cln_tracker_healthy 1"));
        assert!(render_plugin_metrics(&mut metrics, &plugin).is_err());
    }

    #[test]
    fn tracker_fallback_includes_aggregate_rescan_progress() {
        let health = json!({
            "healthy": true,
            "bwatch": {
                "active_rescans": [
                    {"blocks_processed": 25, "blocks_total": 100},
                    {"blocks_processed": 50, "blocks_total": 100}
                ]
            }
        });
        let mut metrics = Metrics::default();
        collect_tracker(&mut metrics, &health);

        assert!(
            metrics
                .output
                .contains("cln_tracker_bwatch_active_rescans 2")
        );
        assert!(
            metrics
                .output
                .contains("cln_tracker_bwatch_rescan_blocks_processed 75")
        );
        assert!(
            metrics
                .output
                .contains("cln_tracker_bwatch_rescan_blocks_total 200")
        );
        assert!(
            metrics
                .output
                .contains("cln_tracker_bwatch_rescan_progress_ratio 0.375")
        );
    }
}
