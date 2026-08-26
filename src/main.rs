mod events;
mod metrics;
mod plugin_metrics;
mod rpc;

use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use cln_plugin::{
    Builder,
    options::{DefaultIntegerConfigOption, DefaultStringConfigOption},
};
use serde_json::json;
use std::{path::PathBuf, time::Duration};
use tokio::net::TcpListener;

const OPT_LISTEN: DefaultStringConfigOption = DefaultStringConfigOption::new_str_with_default(
    "prometheus-listen",
    "127.0.0.1:9750",
    "Address and port for the Prometheus exporter",
);
const OPT_RPC_TIMEOUT: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption::new_i64_with_default(
        "prometheus-rpc-timeout",
        5,
        "Per-collector CLN RPC timeout in seconds",
    );
const OPT_LIQUIDITY_TARGET: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption::new_i64_with_default(
        "prometheus-liquidity-target-percent",
        20,
        "Minimum desired inbound and outbound liquidity per channel, as a percentage of capacity",
    );
const OPT_HTLC_WARNING_BLOCKS: DefaultIntegerConfigOption =
    DefaultIntegerConfigOption::new_i64_with_default(
        "prometheus-htlc-warning-blocks",
        12,
        "Warn-window for unresolved HTLC expiry metrics, in blocks",
    );

#[derive(Clone)]
pub struct AppState {
    rpc_path: PathBuf,
    rpc_timeout: Duration,
    liquidity_target_percent: u64,
    htlc_warning_blocks: u64,
    events: events::EventState,
    plugin_metrics: plugin_metrics::Registry,
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        metrics::render(&state).await,
    )
        .into_response()
}

async fn health_handler(State(state): State<AppState>) -> Response {
    match rpc::call(&state.rpc_path, "getinfo", json!({}), state.rpc_timeout).await {
        Ok(_) => (StatusCode::OK, axum::Json(json!({"healthy": true}))).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({"healthy": false, "error": error.to_string()})),
        )
            .into_response(),
    }
}

async fn serve(state: AppState, listener: TcpListener) -> Result<()> {
    let address = listener.local_addr().context("reading exporter address")?;
    log::info!("CLN exporter listening on http://{address}/metrics");
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(health_handler))
        .with_state(state);
    axum::serve(listener, app)
        .await
        .context("serving Prometheus HTTP endpoint")
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let Some(configured) = Builder::new(tokio::io::stdin(), tokio::io::stdout())
        .option(OPT_LISTEN)
        .option(OPT_RPC_TIMEOUT)
        .option(OPT_LIQUIDITY_TARGET)
        .option(OPT_HTLC_WARNING_BLOCKS)
        .subscribe("*", events::on_notification)
        .dynamic()
        .configure()
        .await?
    else {
        return Ok(());
    };

    let listen = configured.option(&OPT_LISTEN)?;
    let rpc_timeout = configured.option(&OPT_RPC_TIMEOUT)?;
    let liquidity_target = configured.option(&OPT_LIQUIDITY_TARGET)?;
    let htlc_warning_blocks = configured.option(&OPT_HTLC_WARNING_BLOCKS)?;
    ensure!(rpc_timeout > 0, "prometheus-rpc-timeout must be positive");
    ensure!(
        (0..=50).contains(&liquidity_target),
        "prometheus-liquidity-target-percent must be between 0 and 50"
    );
    ensure!(
        htlc_warning_blocks > 0,
        "prometheus-htlc-warning-blocks must be positive"
    );
    let configuration = configured.configuration();
    let state = AppState {
        rpc_path: PathBuf::from(&configuration.lightning_dir).join(&configuration.rpc_file),
        rpc_timeout: Duration::from_secs(rpc_timeout as u64),
        liquidity_target_percent: liquidity_target as u64,
        htlc_warning_blocks: htlc_warning_blocks as u64,
        events: events::EventState::default(),
        plugin_metrics: plugin_metrics::Registry::default(),
    };
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding Prometheus listener to {listen}"))?;
    let plugin = configured.start(state.clone()).await?;
    tokio::select! {
        result = plugin.join() => result,
        result = serve(state, listener) => result,
    }
}
