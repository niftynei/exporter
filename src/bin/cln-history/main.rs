mod db;
mod model;
mod node_rpc;

use anyhow::{Context, Result, ensure};
use cln_plugin::{
    Builder, Error, Plugin, RpcMethodBuilder,
    options::{DefaultIntegerConfigOption, DefaultStringConfigOption},
};
use db::HistoryDb;
use model::{EventQuery, PairQuery, SampleQuery, now};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};

const OPT_DB_FILE: DefaultStringConfigOption = DefaultStringConfigOption::new_str_with_default(
    "history-db-file",
    "cln-history.sqlite3",
    "SQLite filename, relative to the lightning RPC directory unless absolute",
);
const OPT_SAMPLE_INTERVAL: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption::new_i64_with_default(
        "history-sample-interval",
        300,
        "Seconds between channel snapshots",
    );
const OPT_RETENTION_DAYS: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption::new_i64_with_default(
        "history-retention-days",
        730,
        "Days to retain channel samples, events, and forwarding-pair buckets",
    );
const OPT_RPC_TIMEOUT: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption::new_i64_with_default(
        "history-rpc-timeout",
        10,
        "Timeout for internal CLN RPC calls, in seconds",
    );

#[derive(Clone)]
struct HistoryState {
    db: HistoryDb,
    rpc_path: PathBuf,
    rpc_timeout: Duration,
    sample_interval: Duration,
}

async fn history_status(plugin: Plugin<HistoryState>, _params: Value) -> Result<Value, Error> {
    plugin.state().db.status()
}

async fn history_metrics(plugin: Plugin<HistoryState>, _params: Value) -> Result<Value, Error> {
    plugin.state().db.metrics()
}

async fn history_channels(plugin: Plugin<HistoryState>, params: Value) -> Result<Value, Error> {
    let query = SampleQuery::parse(params)?;
    plugin.state().db.channel_samples(&query)
}

async fn history_pairs(plugin: Plugin<HistoryState>, params: Value) -> Result<Value, Error> {
    let query = PairQuery::parse(params)?;
    plugin.state().db.forward_pairs(&query)
}

async fn history_events(plugin: Plugin<HistoryState>, params: Value) -> Result<Value, Error> {
    let query = EventQuery::parse(params)?;
    plugin.state().db.events(&query)
}

async fn history_htlcs(plugin: Plugin<HistoryState>, params: Value) -> Result<Value, Error> {
    let query = SampleQuery::parse(params)?;
    plugin.state().db.htlc_samples(&query)
}

async fn on_notification(plugin: Plugin<HistoryState>, value: Value) -> Result<()> {
    let Some((topic, body)) = value.as_object().and_then(|object| object.iter().next()) else {
        return Ok(());
    };
    if topic == "shutdown" {
        let _ = plugin.shutdown();
        return Ok(());
    }
    match topic.as_str() {
        "channel_state_changed" => plugin.state().db.record_channel_event(now(), body)?,
        "forward_event" => plugin.state().db.record_forward_event(now(), body)?,
        _ => {}
    }
    Ok(())
}

async fn collect_once(state: &HistoryState) {
    let collected_at = now();
    let result = node_rpc::call(
        &state.rpc_path,
        "listpeerchannels",
        json!({}),
        state.rpc_timeout,
    )
    .await
    .and_then(|value| state.db.record_channel_snapshot(collected_at, &value));
    if let Err(error) = result {
        log::warn!("cln-history snapshot failed: {error:#}");
        if let Err(record_error) = state.db.record_collection_failure(collected_at, &error) {
            log::warn!("cln-history could not record collection failure: {record_error:#}");
        }
    }
}

async fn collect_loop(state: HistoryState) -> Result<()> {
    let mut interval = tokio::time::interval(state.sample_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        collect_once(&state).await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let Some(configured) = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(OPT_DB_FILE)
        .option(OPT_SAMPLE_INTERVAL)
        .option(OPT_RETENTION_DAYS)
        .option(OPT_RPC_TIMEOUT)
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("cln-history-status", history_status)
                .description("Show cln-history health, coverage, and storage statistics"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("history-metrics", history_metrics)
                .description("Return bounded cln-history metrics for Prometheus exporters"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("cln-history-channels", history_channels)
                .description("Return channel balance and availability change points")
                .usage("[start] [end] [channel] [limit] [cursor] [interval]"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("cln-history-pairs", history_pairs)
                .description("Return aggregated forwarding results by incoming/outgoing channel pair")
                .usage("[start] [end] [interval] [in_channel] [out_channel] [min_ppm] [max_ppm] [limit] [cursor]"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("cln-history-htlcs", history_htlcs)
                .description("Return historical pending-HTLC channel aggregates")
                .usage("[start] [end] [channel] [limit] [cursor] [interval]"),
        )
        .rpcmethod_from_builder(
            RpcMethodBuilder::new("cln-history-events", history_events)
                .description("Return channel lifecycle and state-transition events")
                .usage("[start] [end] [channel] [event_type] [limit] [cursor]"),
        )
        .subscribe("*", on_notification)
        .dynamic()
        .configure()
        .await?
    else {
        return Ok(());
    };

    let sample_interval = configured.option(&OPT_SAMPLE_INTERVAL)?;
    let retention_days = configured.option(&OPT_RETENTION_DAYS)?;
    let rpc_timeout = configured.option(&OPT_RPC_TIMEOUT)?;
    ensure!(
        sample_interval >= 30,
        "history-sample-interval must be at least 30 seconds"
    );
    ensure!(
        retention_days >= 1,
        "history-retention-days must be positive"
    );
    ensure!(rpc_timeout > 0, "history-rpc-timeout must be positive");

    let configuration = configured.configuration();
    let rpc_path = PathBuf::from(&configuration.lightning_dir).join(&configuration.rpc_file);
    let requested_db = PathBuf::from(configured.option(&OPT_DB_FILE)?);
    let db_path = if requested_db.is_absolute() {
        requested_db
    } else {
        rpc_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new(&configuration.lightning_dir))
            .join(requested_db)
    };
    let state = HistoryState {
        db: HistoryDb::open(db_path, retention_days as u64)
            .context("initializing cln-history database")?,
        rpc_path,
        rpc_timeout: Duration::from_secs(rpc_timeout as u64),
        sample_interval: Duration::from_secs(sample_interval as u64),
    };
    let plugin = configured.start(state.clone()).await?;
    tokio::select! {
        result = plugin.join() => result,
        result = collect_loop(state) => result,
    }
}
