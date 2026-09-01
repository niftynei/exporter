# CLN exporter

`cln-exporter` is a Rust Core Lightning plugin that exposes bounded-cardinality
Prometheus metrics on one HTTP listener. Other plugins, including Tracker and
Bookkeeper, remain portless and are queried through CLN's RPC interface.

## Build

~~~console
nix build
~~~

The package contains `result/bin/cln-exporter` and `result/bin/cln-history`. A
development shell is also included:

~~~console
nix develop
cargo test
cargo clippy --all-targets -- -D warnings
~~~

## Lightweight history plugin

`cln-history` keeps compact channel history on the CLN node for dashboard
clients that connect over the existing CLN RPC or Commando connection. It does
not require Prometheus. Channel liquidity and pending-HTLC aggregates are
recorded when they change, with an hourly checkpoint; forwarding results are
stored in five-minute channel-pair buckets. The default retention is 730 days.

The SQLite database uses WAL mode and defaults to `cln-history.sqlite3` beside
the node's RPC file. It stores channel and peer identifiers, aggregate amounts,
states, and failure causes. It does not store payment hashes, preimages,
invoices, or raw notification payloads.

Add the plugin to the CLN configuration:

~~~text
plugin=/absolute/path/to/exporter/result/bin/cln-history
history-db-file=cln-history.sqlite3
history-sample-interval=300
history-retention-days=730
history-rpc-timeout=10
~~~

The plugin provides read-only dashboard RPCs:

- `history-metrics`: bounded storage, collection-health, coverage, and row-count
  metrics automatically discovered by `cln-exporter` and exposed with the
  `cln_history_` prefix.
- `cln-history-status`: database health, coverage, and row counts.
- `cln-history-channels`: channel balance, connectivity, and availability
  change points. The most recent point before the requested range is included
  as a baseline. Set `interval` to return the last state in each time bucket.
- `cln-history-pairs`: forwarding count, volume, fees, and effective ppm by
  incoming/outgoing channel pair and requested time interval.
- `cln-history-htlcs`: historical per-channel pending-HTLC counts and amounts.
- `cln-history-events`: channel state changes, disappearance, and reappearance.

All query RPCs accept Unix-second `start` and `end` bounds plus an optional
`limit` (10,000 by default, 50,000 maximum). List responses include
`pagination.has_more` and `pagination.next_cursor`; pass the returned cursor to
fetch the next page. Channel and HTLC queries accept `interval` between 60
seconds and one year. Forward-pair queries use the same field for their
aggregation interval.

Each channel has a durable `channel_identity`. The plugin records its channel
IDs, public SCIDs, and local/remote aliases with first/last-seen timestamps.
When an identifier changes during a splice, any surviving alias keeps the same
identity. It deliberately does not merge channels based only on peer ID, since
a peer may have multiple channels. Pair results include identity details for
both sides.

History begins when the plugin starts; it does not backfill older CLN or
Bookkeeper records. For example:

~~~console
lightning-cli cln-history-status
lightning-cli -k cln-history-channels start=1754006400 channel=1x2x3 interval=3600 limit=1000
lightning-cli -k cln-history-channels start=1754006400 cursor=1234 interval=3600 limit=1000
lightning-cli -k cln-history-pairs start=1754006400 interval=86400 min_ppm=100
lightning-cli -k cln-history-events start=1754006400 event_type=state_changed
~~~

## Configuration

~~~text
plugin=/absolute/path/to/exporter/result/bin/cln-exporter
prometheus-listen=127.0.0.1:9750
prometheus-rpc-timeout=5
prometheus-liquidity-target-percent=20
prometheus-htlc-warning-blocks=12
~~~

The listener provides:

- `GET /metrics`: Prometheus text exposition.
- `GET /healthz`: HTTP 200 when the exporter can call CLN's `getinfo`, otherwise
  HTTP 503.

The liquidity target is applied independently to inbound and outbound usable
liquidity. For a 1,000,000 msat channel and a 20% target, spendable liquidity
below 200,000 msat produces an outbound shortfall; receivable liquidity below
200,000 msat produces an inbound shortfall.

## Deploy and verify

The older Python Prometheus plugin uses the same default port. Stop it or remove
it from the CLN configuration before starting this exporter.

Build, then smoke-test the exporter dynamically:

~~~console
nix build
lightning-cli -k plugin \
  subcommand=start \
  plugin=/absolute/path/to/exporter/result/bin/cln-exporter \
  prometheus-listen=127.0.0.1:9750 \
  prometheus-rpc-timeout=5 \
  prometheus-liquidity-target-percent=20 \
  prometheus-htlc-warning-blocks=12

curl -fsS http://127.0.0.1:9750/healthz
curl -fsS http://127.0.0.1:9750/metrics | grep cln_exporter_collector_success
~~~

Once verified, put the options from the Configuration section in CLN's config
and restart CLN. Use `plugin`, not `important-plugin`: losing monitoring should
not terminate a node holding funds.

Configure Prometheus on the same host with:

~~~yaml
scrape_configs:
  - job_name: cln
    scrape_interval: 15s
    scrape_timeout: 10s
    static_configs:
      - targets: [127.0.0.1:9750]
        labels:
          node: my-cln-node
~~~

Add `prometheus-alerts.yml` under Prometheus's `rule_files` and reload
Prometheus. `ClnBackupPluginInactive` assumes the community backup plugin is
required; remove that alert if the node intentionally uses another backup
strategy.

## Grafana defaults

The `grafana/` directory contains a provisioned Prometheus data source and a
starter **Core Lightning Overview** dashboard. It covers node and collector
health, wallet and anchor reserves, channel liquidity, routing flow, payment
latency, peer reliability, HTLC and feerate safety, and Tracker health.

The dashboard expects each Prometheus target to have a stable `node` label:

~~~yaml
static_configs:
  - targets: [127.0.0.1:9750]
    labels:
      node: my-cln-node
~~~

For a conventional Grafana installation, install the files as follows:

~~~text
grafana/provisioning/datasources/prometheus.yml
  -> /etc/grafana/provisioning/datasources/prometheus.yml
grafana/provisioning/dashboards/cln.yml
  -> /etc/grafana/provisioning/dashboards/cln.yml
grafana/dashboards/cln-overview.json
  -> /var/lib/grafana/dashboards/cln/cln-overview.json
~~~

Restart Grafana after installing them. The data source defaults to
`http://127.0.0.1:9090`. In Docker Compose, change that URL to the Prometheus
service name (commonly `http://prometheus:9090`) and mount the two provisioning
directories and the dashboard directory at the same container paths. The
dashboard uses the fixed data-source UID `prometheus`, so preserve that UID if
an existing data source is substituted.

The bundled Prometheus alert rules remain the single alert-evaluation source.
The Grafana dashboard annotates firing `ALERTS` series instead of defining a
duplicate set of Grafana-managed alerts.

If Prometheus runs in a container, `127.0.0.1` refers to the container itself.
Bind the exporter to the CLN host's private/container interface and use that
hostname from Prometheus. The endpoint has no authentication and exposes node,
peer, channel and Bookkeeper account identifiers, so do not publish it directly
to the internet.

## Collectors

- Node identity, version, blockheight, CLN/Bitcoin sync and routing fees.
- On-chain wallet balances grouped by output status.
- Channel state, connectivity, capacity, spendable/receivable liquidity,
  liquidity ratios and target shortfalls, HTLC count, slot pressure and nearest
  HTLC expiry.
- Peer connection state, observed connected time, reconnects and disconnects.
- Bitcoin feerates by purpose and confirmation target, estimated transaction
  fees, and per-channel commitment feerate competitiveness.
- Anchor-channel count, confirmed unreserved wallet funds, configured emergency
  reserve, estimated simultaneous-close fee requirement, coverage and shortfall.
- Inflight splice count, amount, feerate and resulting funding amount.
- Our advertised `option_will_fund` liquidity-ad terms, channel funding fees,
  lease fees paid/earned, utilization and Bookkeeper channel APY.
- Per-channel settled forwarding inflow, outflow, fees and net liquidity drift.
- Incoming invoice creation/payment totals and observed payment latency, plus
  outgoing sendpay success/failure and latency.
- Installed plugin active/dynamic state.
- Tracker health, descriptor lifecycle, incidents, bwatch lag, active historical
  rescan progress and process-lifetime failure counters through the automatically
  discovered `tracker-metrics` RPC. The older `tracker-health` adapter remains
  as a compatibility fallback.
- Bookkeeper account balances when `bkpr-listbalances` is available.
- Warning/error, plugin lifecycle, channel state transition, forwarding,
  coin-movement and generic notification counters from CLN's event stream.
- Exporter collector success and duration.

Optional RPC collectors are isolated. A failing discovered collector reports
`cln_exporter_plugin_collector_success{rpc="..."} 0`; built-in and compatibility
collectors use `cln_exporter_collector_success`. Neither failure prevents the
remaining metrics from being scraped.

## Plugin metrics RPCs

The exporter discovers plugin RPC methods ending in `-metrics` from CLN's
`help` response. Discovery is cached for 60 seconds and invalidated by
`plugin_started` and `plugin_stopped` notifications. Discovered collectors run
concurrently with the built-in collectors, use the configured RPC timeout, and
cannot make the complete scrape fail.

A participating RPC takes no parameters and returns structured JSON:

~~~json
{
  "version": 1,
  "namespace": "example",
  "families": [
    {
      "name": "jobs_total",
      "help": "Jobs completed by this plugin.",
      "type": "counter",
      "samples": [
        { "labels": { "result": "success" }, "value": 3 }
      ]
    }
  ]
}
~~~

For an RPC named `example-metrics`, the namespace must be `example`; hyphens in
the RPC stem become underscores. Family and label names use Prometheus
identifier syntax. Version 1 supports `gauge` and `counter`, and counter names
must end in `_total`. The exporter prefixes every family with `cln_<namespace>_`.

Responses are limited to 1 MiB, 256 families, 10,000 samples, and 16 labels per
sample. Names, help text, and label values are also bounded. Invalid schemas,
namespace spoofing, duplicate families, and collisions with exporter metrics
are rejected as a unit. Raw Prometheus exposition text is not accepted.

Collector behavior is visible through:

~~~prometheus
cln_exporter_plugin_collector_success{rpc="example-metrics"} 1
cln_exporter_plugin_collector_duration_seconds{rpc="example-metrics"} 0.004
cln_exporter_plugin_collector_samples{rpc="example-metrics"} 1
~~~

## Cardinality policy

Channel, peer and plugin names are bounded by node state. Bookkeeper account
series are capped at 256, with additional balances aggregated as `__other__`.
Warning messages, payment hashes, transaction IDs, outpoints, descriptors and
arbitrary error text are never metric labels. Dynamically observed notification
topics and sources are capped, with excess values grouped under `other`.

Notification and Tracker operational counters reset with their owning plugin.
Durable incidents and pending work are exposed by Tracker through its RPC.

## Current upstream visibility limits

CLN's public RPCs expose unresolved HTLC expiry heights and whether a channel is
onchain, but not the internal sweep `deadline_block` values maintained by
`onchain_control.c`. The exporter therefore reports the exact structured HTLC
deadlines and onchain state. Exact sweep-resolution deadlines require a new CLN
RPC rather than parsing human-readable channel status messages.

The community backup plugin writes synchronously in the `db_write` hook and
terminates CLN if writing fails, but exposes no status RPC or last-success
timestamp. `cln_backup_plugin_active` and warning notifications are therefore
truthful; `cln_backup_freshness_supported` remains zero until that plugin gains
a status RPC.

Invoice latency is observed only for invoices both created and paid while this
exporter process is running. Forwarding-flow, peer-flap and notification metrics
have the same process-lifetime scope; Prometheus handles their counter resets.
