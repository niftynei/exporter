use crate::model::{ChannelSample, EventQuery, PairQuery, ProbeQuery, SampleQuery, amount_msat};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
};

const SCHEMA_VERSION: u64 = 5;
const CHECKPOINT_SECONDS: u64 = 3_600;
const FORWARD_BUCKET_SECONDS: u64 = 300;

#[derive(Clone)]
pub struct HistoryDb {
    path: PathBuf,
    retention_seconds: u64,
}

impl HistoryDb {
    pub fn open(path: PathBuf, retention_days: u64) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let db = Self {
            path,
            retention_seconds: retention_days.saturating_mul(86_400),
        };
        db.connection()?;
        Ok(db)
    }

    fn connection(&self) -> Result<Connection> {
        let mut connection = Connection::open(&self.path)
            .with_context(|| format!("opening {}", self.path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_meta (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS channels (
                 channel_key TEXT PRIMARY KEY,
                 channel_id TEXT,
                 short_channel_id TEXT,
                 peer_id TEXT NOT NULL,
                 first_seen INTEGER NOT NULL,
                 last_seen INTEGER NOT NULL,
                 last_state TEXT NOT NULL,
                 disappeared_at INTEGER
             );
             CREATE TABLE IF NOT EXISTS channel_samples (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ts INTEGER NOT NULL,
                 channel_key TEXT NOT NULL,
                 channel_id TEXT,
                 short_channel_id TEXT,
                 peer_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 connected INTEGER NOT NULL,
                 reestablished INTEGER NOT NULL,
                 capacity_msat INTEGER NOT NULL,
                 to_us_msat INTEGER NOT NULL,
                 spendable_msat INTEGER NOT NULL,
                 receivable_msat INTEGER NOT NULL,
                 htlc_in_count INTEGER NOT NULL,
                 htlc_out_count INTEGER NOT NULL,
                 htlc_in_msat INTEGER NOT NULL,
                 htlc_out_msat INTEGER NOT NULL,
                 blockheight INTEGER,
                 htlc_min_expiry INTEGER,
                 htlc_max_expiry INTEGER,
                 htlc_hooked_count INTEGER NOT NULL DEFAULT 0,
                 htlc_trimmed_count INTEGER NOT NULL DEFAULT 0,
                 max_accepted_htlcs INTEGER,
                 local_fee_base_msat INTEGER,
                 local_fee_ppm INTEGER,
                 local_htlc_minimum_msat INTEGER,
                 local_htlc_maximum_msat INTEGER,
                 local_cltv_expiry_delta INTEGER,
                 remote_fee_base_msat INTEGER,
                 remote_fee_ppm INTEGER,
                 remote_htlc_minimum_msat INTEGER,
                 remote_htlc_maximum_msat INTEGER,
                 remote_cltv_expiry_delta INTEGER
             );
             CREATE INDEX IF NOT EXISTS channel_samples_channel_ts
                 ON channel_samples(channel_key, ts);
             CREATE TABLE IF NOT EXISTS channel_events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ts INTEGER NOT NULL,
                 event_type TEXT NOT NULL,
                 channel_key TEXT,
                 channel_id TEXT,
                 short_channel_id TEXT,
                 peer_id TEXT,
                 old_state TEXT,
                 new_state TEXT,
                 cause TEXT,
                 direction TEXT,
                 reason TEXT
             );
             CREATE INDEX IF NOT EXISTS channel_events_ts
                 ON channel_events(ts);
             CREATE TABLE IF NOT EXISTS forward_pair_buckets (
                 bucket_start INTEGER NOT NULL,
                 in_channel TEXT NOT NULL,
                 out_channel TEXT NOT NULL,
                 status TEXT NOT NULL,
                 failcode INTEGER NOT NULL DEFAULT 0,
                 failreason TEXT NOT NULL DEFAULT '',
                 forward_count INTEGER NOT NULL,
                 in_msat INTEGER NOT NULL,
                 out_msat INTEGER NOT NULL,
                 fee_msat INTEGER NOT NULL,
                 PRIMARY KEY(bucket_start, in_channel, out_channel, status, failcode, failreason)
             );
             CREATE INDEX IF NOT EXISTS forward_pair_buckets_time
                 ON forward_pair_buckets(bucket_start);
             CREATE TABLE IF NOT EXISTS collector_runs (
                 ts INTEGER PRIMARY KEY,
                 success INTEGER NOT NULL,
                 channel_count INTEGER,
                 error TEXT
             );
             CREATE TABLE IF NOT EXISTS channel_aliases (
                 alias_type TEXT NOT NULL,
                 alias TEXT NOT NULL,
                 channel_key TEXT NOT NULL,
                 first_seen INTEGER NOT NULL,
                 last_seen INTEGER NOT NULL,
                 PRIMARY KEY(alias_type, alias)
             );
             CREATE INDEX IF NOT EXISTS channel_aliases_channel_key
                 ON channel_aliases(channel_key);
             CREATE TABLE IF NOT EXISTS probe_events (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ts INTEGER NOT NULL,
                 started_at INTEGER,
                 duration_ms INTEGER,
                 plugin_version TEXT,
                 status TEXT NOT NULL,
                 destination_reached INTEGER NOT NULL,
                 destination TEXT,
                 delivered_msat INTEGER NOT NULL,
                 amount_at_source_msat INTEGER NOT NULL,
                 total_fee_msat INTEGER NOT NULL,
                 lower_bound_msat INTEGER,
                 failure_code INTEGER,
                 failcode INTEGER,
                 failcodename TEXT,
                 erring_index INTEGER,
                 erring_channel TEXT,
                 erring_direction INTEGER,
                 erring_node TEXT
             );
             CREATE INDEX IF NOT EXISTS probe_events_ts ON probe_events(ts);
             CREATE INDEX IF NOT EXISTS probe_events_destination_ts
                 ON probe_events(destination, ts);
             CREATE TABLE IF NOT EXISTS probe_hops (
                 probe_id INTEGER NOT NULL REFERENCES probe_events(id) ON DELETE CASCADE,
                 hop_index INTEGER NOT NULL,
                 channel_key TEXT,
                 short_channel_id TEXT NOT NULL,
                 direction INTEGER NOT NULL,
                 node_id_out TEXT NOT NULL,
                 amount_out_msat INTEGER NOT NULL,
                 cltv_out INTEGER NOT NULL,
                 status TEXT NOT NULL,
                 PRIMARY KEY(probe_id, hop_index)
             );
             CREATE INDEX IF NOT EXISTS probe_hops_channel
                 ON probe_hops(channel_key, short_channel_id);",
        )?;
        migrate(&mut connection)?;
        Ok(connection)
    }

    #[cfg(test)]
    pub fn record_channel_snapshot(&self, ts: u64, response: &Value) -> Result<()> {
        self.record_channel_snapshot_at_height(ts, response, None)
    }

    pub fn record_channel_snapshot_at_height(
        &self,
        ts: u64,
        response: &Value,
        blockheight: Option<u64>,
    ) -> Result<()> {
        let channels = response
            .get("channels")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("listpeerchannels response has no channels array"))?;
        let mut samples = channels
            .iter()
            .enumerate()
            .map(|(index, value)| ChannelSample::from_value(value, index, blockheight))
            .collect::<Vec<_>>();
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for sample in &mut samples {
            resolve_sample_identity(&transaction, ts, sample)?;
            self.record_sample(&transaction, ts, sample)?;
        }
        self.record_disappearances(&transaction, ts, &samples)?;
        transaction.execute(
            "INSERT OR REPLACE INTO collector_runs(ts, success, channel_count, error)
             VALUES (?1, 1, ?2, NULL)",
            params![to_i64(ts)?, to_i64(samples.len() as u64)?],
        )?;
        self.prune(&transaction, ts)?;
        transaction.commit()?;
        Ok(())
    }

    fn record_sample(
        &self,
        transaction: &Transaction<'_>,
        ts: u64,
        sample: &ChannelSample,
    ) -> Result<()> {
        let previous = latest_sample(transaction, &sample.channel_key)?;
        let should_checkpoint = previous
            .as_ref()
            .is_none_or(|(previous_ts, _)| ts.saturating_sub(*previous_ts) >= CHECKPOINT_SECONDS);
        let changed = previous
            .as_ref()
            .is_none_or(|(_, previous)| previous != sample);
        let disappeared_at = transaction
            .query_row(
                "SELECT disappeared_at FROM channels WHERE channel_key = ?1",
                [&sample.channel_key],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()?
            .flatten();
        transaction.execute(
            "INSERT INTO channels(
                 channel_key, channel_id, short_channel_id, peer_id,
                 first_seen, last_seen, last_state, disappeared_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, NULL)
             ON CONFLICT(channel_key) DO UPDATE SET
                 channel_id = excluded.channel_id,
                 short_channel_id = excluded.short_channel_id,
                 peer_id = excluded.peer_id,
                 last_seen = excluded.last_seen,
                 last_state = excluded.last_state,
                 disappeared_at = NULL",
            params![
                sample.channel_key,
                sample.channel_id,
                sample.short_channel_id,
                sample.peer_id,
                to_i64(ts)?,
                sample.state,
            ],
        )?;
        if disappeared_at.is_some() {
            insert_event(
                transaction,
                ts,
                "reappeared",
                Some(sample),
                None,
                Some(&sample.state),
                None,
            )?;
        }
        if changed || should_checkpoint {
            insert_sample(transaction, ts, sample)?;
        }
        Ok(())
    }

    fn record_disappearances(
        &self,
        transaction: &Transaction<'_>,
        ts: u64,
        samples: &[ChannelSample],
    ) -> Result<()> {
        let present = samples
            .iter()
            .map(|sample| sample.channel_key.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut statement = transaction.prepare(
            "SELECT channel_key, channel_id, short_channel_id, peer_id, last_state
             FROM channels WHERE disappeared_at IS NULL",
        )?;
        let known = statement
            .query_map([], |row| {
                Ok(ChannelSample {
                    channel_key: row.get(0)?,
                    aliases: Vec::new(),
                    channel_id: row.get(1)?,
                    short_channel_id: row.get(2)?,
                    peer_id: row.get(3)?,
                    state: row.get(4)?,
                    connected: false,
                    reestablished: false,
                    capacity_msat: 0,
                    to_us_msat: 0,
                    spendable_msat: 0,
                    receivable_msat: 0,
                    htlc_in_count: 0,
                    htlc_out_count: 0,
                    htlc_in_msat: 0,
                    htlc_out_msat: 0,
                    blockheight: None,
                    htlc_min_expiry: None,
                    htlc_max_expiry: None,
                    htlc_hooked_count: 0,
                    htlc_trimmed_count: 0,
                    max_accepted_htlcs: None,
                    local_policy: Default::default(),
                    remote_policy: Default::default(),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        for channel in known {
            if !present.contains(channel.channel_key.as_str()) {
                transaction.execute(
                    "UPDATE channels SET disappeared_at = ?2 WHERE channel_key = ?1",
                    params![channel.channel_key, to_i64(ts)?],
                )?;
                insert_event(
                    transaction,
                    ts,
                    "disappeared",
                    Some(&channel),
                    Some(&channel.state),
                    None,
                    None,
                )?;
            }
        }
        Ok(())
    }

    pub fn record_collection_failure(&self, ts: u64, error: &anyhow::Error) -> Result<()> {
        let message = format!("{error:#}");
        self.connection()?.execute(
            "INSERT OR REPLACE INTO collector_runs(ts, success, channel_count, error)
             VALUES (?1, 0, NULL, ?2)",
            params![to_i64(ts)?, message.chars().take(1_000).collect::<String>()],
        )?;
        Ok(())
    }

    pub fn record_channel_event(&self, ts: u64, value: &Value) -> Result<()> {
        let channel_id = text(value, &["channel_id"]);
        let short_channel_id = text(value, &["short_channel_id"]);
        let peer_id = text(value, &["peer_id"]);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let channel_key = [&channel_id, &short_channel_id]
            .into_iter()
            .flatten()
            .find_map(|alias| resolve_alias(&transaction, alias).transpose())
            .transpose()?
            .or_else(|| channel_id.clone())
            .or_else(|| short_channel_id.clone())
            .or_else(|| peer_id.clone());
        if let Some(key) = channel_key.as_deref() {
            if let Some(alias) = channel_id.as_deref() {
                record_alias(&transaction, ts, "channel_id", alias, key)?;
            }
            if let Some(alias) = short_channel_id.as_deref() {
                record_alias(&transaction, ts, "short_channel_id", alias, key)?;
            }
        }
        transaction.execute(
            "INSERT INTO channel_events(
                 ts, event_type, channel_key, channel_id, short_channel_id,
                 peer_id, old_state, new_state, cause
             ) VALUES (?1, 'state_changed', ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                to_i64(ts)?,
                channel_key,
                channel_id,
                short_channel_id,
                peer_id,
                text(value, &["old_state"]),
                text(value, &["new_state"]),
                text(value, &["cause"]),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_forward_event(&self, ts: u64, value: &Value) -> Result<()> {
        let Some(in_alias) = text(value, &["in_channel"]) else {
            return Ok(());
        };
        let out_alias = text(value, &["out_channel"]).unwrap_or_else(|| "unknown".to_owned());
        let status = text(value, &["status"]).unwrap_or_else(|| "unknown".to_owned());
        let failcode = value.get("failcode").and_then(Value::as_u64).unwrap_or(0);
        let failreason = text(value, &["failreason"]).unwrap_or_default();
        let bucket = ts / FORWARD_BUCKET_SECONDS * FORWARD_BUCKET_SECONDS;
        let in_msat = amount_msat(value.get("in_msat"));
        let out_msat = amount_msat(value.get("out_msat"));
        let fee_msat = value
            .get("fee_msat")
            .map(|value| amount_msat(Some(value)))
            .unwrap_or_else(|| in_msat.saturating_sub(out_msat));
        let connection = self.connection()?;
        let in_channel = resolve_alias(&connection, &in_alias)?.unwrap_or(in_alias);
        let out_channel = resolve_alias(&connection, &out_alias)?.unwrap_or(out_alias);
        connection.execute(
            "INSERT INTO forward_pair_buckets(
                 bucket_start, in_channel, out_channel, status, failcode, failreason,
                 forward_count, in_msat, out_msat, fee_msat
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8, ?9)
             ON CONFLICT(bucket_start, in_channel, out_channel, status, failcode, failreason) DO UPDATE SET
                 forward_count = forward_count + 1,
                 in_msat = in_msat + excluded.in_msat,
                 out_msat = out_msat + excluded.out_msat,
                 fee_msat = fee_msat + excluded.fee_msat",
            params![
                to_i64(bucket)?,
                in_channel,
                out_channel,
                status,
                to_i64(failcode)?,
                failreason,
                to_i64(in_msat)?,
                to_i64(out_msat)?,
                to_i64(fee_msat)?,
            ],
        )?;
        Ok(())
    }

    pub fn record_peer_connection_event(
        &self,
        ts: u64,
        event_type: &str,
        value: &Value,
    ) -> Result<()> {
        let Some(peer_id) = text(value, &["id", "peer_id"]) else {
            return Ok(());
        };
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO channel_events(
                 ts, event_type, peer_id, direction, reason
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                to_i64(ts)?,
                event_type,
                peer_id,
                text(value, &["direction"]),
                text(value, &["reason"]),
            ],
        )?;
        Ok(())
    }

    pub fn record_probe_result(&self, received_at: u64, value: &Value) -> Result<()> {
        let status = text(value, &["status"]).context("probe result has no status")?;
        if status == "pending" || status == "pending_after_restart" {
            return Ok(());
        }
        if !matches!(
            status.as_str(),
            "destination_reached" | "route_failed" | "unexpected_settlement"
        ) {
            bail!("unknown terminal probe status: {status}");
        }
        let route = value
            .get("route")
            .and_then(Value::as_array)
            .context("probe result has no route")?;
        if route.is_empty() {
            return Ok(());
        }
        let ts = value
            .get("observed_at")
            .and_then(Value::as_u64)
            .unwrap_or(received_at);
        let failure = value.get("failure").unwrap_or(&Value::Null);
        let destination = route
            .last()
            .and_then(|hop| hop.get("node_id_out"))
            .and_then(Value::as_str);
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO probe_events(
                 ts, started_at, duration_ms, plugin_version, status,
                 destination_reached, destination, delivered_msat,
                 amount_at_source_msat, total_fee_msat, lower_bound_msat,
                 failure_code, failcode, failcodename, erring_index,
                 erring_channel, erring_direction, erring_node
             ) VALUES (
                 ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                 ?12, ?13, ?14, ?15, ?16, ?17, ?18
             )",
            params![
                to_i64(ts)?,
                optional_i64(value.get("started_at").and_then(Value::as_u64))?,
                optional_i64(value.get("duration_ms").and_then(Value::as_u64))?,
                text(value, &["plugin_version"]),
                status,
                value
                    .get("destination_reached")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                destination,
                to_i64(amount_msat(value.get("delivered_msat")))?,
                to_i64(amount_msat(value.get("amount_at_source_msat")))?,
                to_i64(amount_msat(value.get("total_fee_msat")))?,
                optional_i64(
                    value
                        .get("lower_bound_msat")
                        .filter(|amount| !amount.is_null())
                        .map(|amount| amount_msat(Some(amount))),
                )?,
                failure.get("code").and_then(Value::as_i64),
                failure.get("failcode").and_then(Value::as_i64),
                text(failure, &["failcodename"]),
                failure.get("erring_index").and_then(Value::as_i64),
                text(failure, &["erring_channel"]),
                failure.get("erring_direction").and_then(Value::as_i64),
                text(failure, &["erring_node"]),
            ],
        )?;
        let probe_id = transaction.last_insert_rowid();
        for (index, hop) in route.iter().enumerate() {
            let directed = text(hop, &["short_channel_id_dir"])
                .context("probe hop has no short_channel_id_dir")?;
            let (scid, direction) = directed
                .rsplit_once('/')
                .context("probe hop channel must include a direction")?;
            let direction = direction
                .parse::<u64>()
                .context("parsing probe hop direction")?;
            let channel_key = resolve_alias(&transaction, scid)?;
            transaction.execute(
                "INSERT INTO probe_hops(
                     probe_id, hop_index, channel_key, short_channel_id, direction,
                     node_id_out, amount_out_msat, cltv_out, status
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    probe_id,
                    to_i64(index as u64)?,
                    channel_key,
                    scid,
                    to_i64(direction)?,
                    text(hop, &["node_id_out"]).context("probe hop has no destination")?,
                    to_i64(amount_msat(hop.get("amount_out_msat")))?,
                    to_i64(hop.get("cltv_out").and_then(Value::as_u64).unwrap_or(0))?,
                    text(hop, &["status"]).unwrap_or_else(|| "unknown".to_owned()),
                ],
            )?;
        }
        self.prune(&transaction, ts)?;
        transaction.commit()?;
        Ok(())
    }

    fn prune(&self, transaction: &Transaction<'_>, ts: u64) -> Result<()> {
        let cutoff = to_i64(ts.saturating_sub(self.retention_seconds))?;
        transaction.execute(
            "DELETE FROM channel_samples
             WHERE ts < ?1 AND id NOT IN (
                 SELECT MAX(id) FROM channel_samples WHERE ts < ?1 GROUP BY channel_key
             )",
            [cutoff],
        )?;
        transaction.execute("DELETE FROM channel_events WHERE ts < ?1", [cutoff])?;
        transaction.execute(
            "DELETE FROM forward_pair_buckets WHERE bucket_start < ?1",
            [cutoff],
        )?;
        transaction.execute("DELETE FROM collector_runs WHERE ts < ?1", [cutoff])?;
        transaction.execute("DELETE FROM probe_events WHERE ts < ?1", [cutoff])?;
        Ok(())
    }

    pub fn status(&self) -> Result<Value> {
        let connection = self.connection()?;
        let (oldest, newest): (Option<i64>, Option<i64>) = connection.query_row(
            "SELECT MIN(ts), MAX(ts) FROM collector_runs WHERE success = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let last_run = connection
            .query_row(
                "SELECT ts, success, channel_count, error FROM collector_runs ORDER BY ts DESC LIMIT 1",
                [],
                |row| Ok(json!({
                    "timestamp": row.get::<_, i64>(0)?,
                    "success": row.get::<_, bool>(1)?,
                    "channel_count": row.get::<_, Option<i64>>(2)?,
                    "error": row.get::<_, Option<String>>(3)?,
                })),
            )
            .optional()?;
        Ok(json!({
            "api_version": 2,
            "schema_version": SCHEMA_VERSION,
            "database": self.path.display().to_string(),
            "database_bytes": file_bytes(&self.path),
            "wal_bytes": file_bytes(&sidecar_path(&self.path, "-wal")),
            "storage_bytes": storage_bytes(&self.path),
            "retention_days": self.retention_seconds / 86_400,
            "change_only": true,
            "checkpoint_seconds": CHECKPOINT_SECONDS,
            "forward_bucket_seconds": FORWARD_BUCKET_SECONDS,
            "coverage": {"oldest": oldest, "newest": newest},
            "last_collection": last_run,
            "counts": {
                "channels": count(&connection, "channels")?,
                "channel_samples": count(&connection, "channel_samples")?,
                "channel_events": count(&connection, "channel_events")?,
                "peer_connection_events": count_where(&connection, "channel_events", "event_type IN ('connect', 'disconnect')")?,
                "forward_pair_buckets": count(&connection, "forward_pair_buckets")?,
                "probe_events": count(&connection, "probe_events")?,
                "successful_probe_events": count_where(&connection, "probe_events", "status = 'destination_reached'")?,
                "channel_aliases": count(&connection, "channel_aliases")?,
            }
        }))
    }

    pub fn metrics(&self) -> Result<Value> {
        let connection = self.connection()?;
        let (oldest_success, newest_success): (Option<i64>, Option<i64>) = connection.query_row(
            "SELECT MIN(ts), MAX(ts) FROM collector_runs WHERE success = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let last_success = newest_success.unwrap_or(0);
        let last_run = connection
            .query_row(
                "SELECT ts, success FROM collector_runs ORDER BY ts DESC LIMIT 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, bool>(1)?)),
            )
            .optional()?;
        let (last_collection, collection_success) = last_run
            .map(|(timestamp, success)| (timestamp, i64::from(success)))
            .unwrap_or((0, 0));
        let now = to_i64(crate::model::now())?;
        let collection_age = if last_success > 0 {
            now.saturating_sub(last_success)
        } else {
            0
        };
        let coverage = match (oldest_success, newest_success) {
            (Some(oldest), Some(newest)) => newest.saturating_sub(oldest),
            _ => 0,
        };
        let page_count = pragma_i64(&connection, "page_count")?;
        let freelist_count = pragma_i64(&connection, "freelist_count")?;
        Ok(json!({
            "version": 1,
            "namespace": "history",
            "families": [
                gauge("database_bytes", "Size of the main cln-history SQLite database file in bytes.", file_bytes(&self.path)),
                gauge("wal_bytes", "Size of the cln-history SQLite write-ahead log in bytes.", file_bytes(&sidecar_path(&self.path, "-wal"))),
                gauge("storage_bytes", "Total cln-history SQLite database, WAL, and shared-memory disk usage in bytes.", storage_bytes(&self.path)),
                gauge("database_pages", "Number of pages in the cln-history SQLite database.", page_count),
                gauge("database_freelist_pages", "Number of unused pages in the cln-history SQLite database.", freelist_count),
                gauge("schema_version", "Current cln-history SQLite schema version.", SCHEMA_VERSION),
                gauge("channels", "Number of channels known to cln-history.", count(&connection, "channels")?),
                gauge("active_channels", "Number of channels present in the latest cln-history snapshot.", count_where(&connection, "channels", "disappeared_at IS NULL")?),
                gauge("disappeared_channels", "Number of historical channels absent from the latest cln-history snapshot.", count_where(&connection, "channels", "disappeared_at IS NOT NULL")?),
                gauge("channel_samples", "Number of retained channel liquidity change points and checkpoints.", count(&connection, "channel_samples")?),
                gauge("channel_events", "Number of retained channel lifecycle and state events.", count(&connection, "channel_events")?),
                gauge("peer_connection_events", "Number of retained exact peer connect and disconnect notifications.", count_where(&connection, "channel_events", "event_type IN ('connect', 'disconnect')")?),
                gauge("forward_pair_buckets", "Number of retained forwarding channel-pair buckets.", count(&connection, "forward_pair_buckets")?),
                gauge("probe_events", "Number of retained terminal route-liquidity probe observations.", count(&connection, "probe_events")?),
                gauge("successful_probe_events", "Number of retained probes which reached their destination and failed safely there.", count_where(&connection, "probe_events", "status = 'destination_reached'")?),
                gauge("failed_probe_events", "Number of retained probes which failed before reaching their destination.", count_where(&connection, "probe_events", "status = 'route_failed'")?),
                gauge("channel_aliases", "Number of durable channel identifier aliases known to cln-history.", count(&connection, "channel_aliases")?),
                gauge("collector_runs", "Number of retained cln-history collection attempts.", count(&connection, "collector_runs")?),
                gauge("retained_collection_failures", "Number of failed collection attempts in the retention window.", count_where(&connection, "collector_runs", "success = 0")?),
                gauge("collection_success", "Whether the most recent cln-history collection attempt succeeded.", collection_success),
                gauge("last_collection_timestamp_seconds", "Unix timestamp of the most recent cln-history collection attempt.", last_collection),
                gauge("last_successful_collection_timestamp_seconds", "Unix timestamp of the most recent successful cln-history collection.", last_success),
                gauge("last_successful_collection_age_seconds", "Age of the most recent successful cln-history collection in seconds.", collection_age),
                gauge("history_coverage_seconds", "Elapsed time covered by retained successful cln-history collections.", coverage),
                gauge("retention_seconds", "Configured cln-history retention window in seconds.", self.retention_seconds),
            ]
        }))
    }

    pub fn channel_samples(&self, query: &SampleQuery) -> Result<Value> {
        let (start, end) = query.range.bounds();
        let connection = self.connection()?;
        let page = load_samples(&connection, query, false)?;
        Ok(json!({
            "api_version": 2,
            "start": start, "end": end, "change_only": true,
            "interval": query.interval,
            "samples": page.items,
            "pagination": {"next_cursor": page.next_cursor, "has_more": page.has_more},
        }))
    }

    pub fn htlc_samples(&self, query: &SampleQuery) -> Result<Value> {
        let (start, end) = query.range.bounds();
        let connection = self.connection()?;
        let page = load_samples(&connection, query, true)?;
        Ok(json!({
            "api_version": 2,
            "start": start, "end": end, "interval": query.interval,
            "samples": page.items,
            "pagination": {"next_cursor": page.next_cursor, "has_more": page.has_more},
        }))
    }

    pub fn forward_pairs(&self, query: &PairQuery) -> Result<Value> {
        let (start, end) = query.range.bounds();
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT (bucket_start / ?1) * ?1 AS period,
                    in_channel, out_channel, status, failcode, failreason,
                    SUM(forward_count), SUM(in_msat), SUM(out_msat), SUM(fee_msat)
             FROM forward_pair_buckets
             WHERE bucket_start BETWEEN ?2 AND ?3
             GROUP BY period, in_channel, out_channel, status, failcode, failreason
             ORDER BY period, in_channel, out_channel, status, failcode, failreason",
        )?;
        let rows = statement
            .query_map(
                params![to_i64(query.interval)?, to_i64(start)?, to_i64(end)?],
                |row| {
                    let failcode = row.get::<_, i64>(4)?;
                    let failreason = row.get::<_, String>(5)?;
                    let out_msat = row.get::<_, i64>(8)?;
                    let fee_msat = row.get::<_, i64>(9)?;
                    let ppm = if out_msat > 0 {
                        fee_msat as f64 * 1_000_000.0 / out_msat as f64
                    } else {
                        0.0
                    };
                    Ok((
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        ppm,
                        json!({
                            "period": row.get::<_, i64>(0)?,
                            "in_channel": row.get::<_, String>(1)?,
                            "out_channel": row.get::<_, String>(2)?,
                            "status": row.get::<_, String>(3)?,
                            "failcode": (failcode != 0).then_some(failcode),
                            "failreason": (!failreason.is_empty()).then_some(failreason),
                            "forward_count": row.get::<_, i64>(6)?,
                            "in_msat": row.get::<_, i64>(7)?,
                            "out_msat": out_msat,
                            "fee_msat": fee_msat,
                            "effective_ppm": ppm,
                        }),
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let in_filter = query
            .in_channel
            .as_deref()
            .map(|value| {
                resolve_alias(&connection, value).map(|key| key.unwrap_or_else(|| value.to_owned()))
            })
            .transpose()?;
        let out_filter = query
            .out_channel
            .as_deref()
            .map(|value| {
                resolve_alias(&connection, value).map(|key| key.unwrap_or_else(|| value.to_owned()))
            })
            .transpose()?;
        let mut values = rows
            .into_iter()
            .filter(|(incoming, outgoing, ppm, _)| {
                matches_channel(in_filter.as_deref(), incoming)
                    && matches_channel(out_filter.as_deref(), outgoing)
                    && query.min_ppm.is_none_or(|minimum| *ppm >= minimum)
                    && query.max_ppm.is_none_or(|maximum| *ppm <= maximum)
            })
            .skip(query.range.cursor.unwrap_or(0) as usize)
            .take(query.range.limit as usize + 1)
            .map(|(_, _, _, value)| value)
            .collect::<Vec<_>>();
        let has_more = values.len() > query.range.limit as usize;
        values.truncate(query.range.limit as usize);
        for value in &mut values {
            attach_pair_identity(&connection, value)?;
        }
        let next_cursor = has_more.then(|| query.range.cursor.unwrap_or(0) + values.len() as u64);
        Ok(json!({
            "api_version": 2,
            "start": start, "end": end, "interval": query.interval,
            "pairs": values,
            "pagination": {"next_cursor": next_cursor, "has_more": has_more},
        }))
    }

    pub fn events(&self, query: &EventQuery) -> Result<Value> {
        let (start, end) = query.range.bounds();
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, ts, event_type, channel_key, channel_id, short_channel_id,
                    peer_id, old_state, new_state, cause, direction, reason
             FROM channel_events WHERE ts BETWEEN ?1 AND ?2 ORDER BY ts, id",
        )?;
        let rows = statement
            .query_map(params![to_i64(start)?, to_i64(end)?], |row| {
                Ok(json!({
                    "cursor": row.get::<_, i64>(0)?,
                    "timestamp": row.get::<_, i64>(1)?,
                    "event_type": row.get::<_, String>(2)?,
                    "channel_identity": row.get::<_, Option<String>>(3)?,
                    "channel_key": row.get::<_, Option<String>>(3)?,
                    "channel_id": row.get::<_, Option<String>>(4)?,
                    "short_channel_id": row.get::<_, Option<String>>(5)?,
                    "peer_id": row.get::<_, Option<String>>(6)?,
                    "old_state": row.get::<_, Option<String>>(7)?,
                    "new_state": row.get::<_, Option<String>>(8)?,
                    "cause": row.get::<_, Option<String>>(9)?,
                    "direction": row.get::<_, Option<String>>(10)?,
                    "reason": row.get::<_, Option<String>>(11)?,
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let resolved_filter = query
            .range
            .channel
            .as_deref()
            .map(|value| {
                resolve_alias(&connection, value).map(|key| key.unwrap_or_else(|| value.to_owned()))
            })
            .transpose()?;
        let mut values = rows
            .into_iter()
            .filter(|value| {
                value["cursor"]
                    .as_u64()
                    .is_some_and(|id| id > query.range.cursor.unwrap_or(0))
                    && query
                        .event_type
                        .as_deref()
                        .is_none_or(|kind| value["event_type"] == kind)
                    && resolved_filter.as_deref().is_none_or(|channel| {
                        ["channel_key", "channel_id", "short_channel_id", "peer_id"]
                            .iter()
                            .any(|field| value[*field] == channel)
                    })
            })
            .take(query.range.limit as usize + 1)
            .collect::<Vec<_>>();
        let has_more = values.len() > query.range.limit as usize;
        values.truncate(query.range.limit as usize);
        let next_cursor = has_more
            .then(|| values.last().and_then(|value| value["cursor"].as_u64()))
            .flatten();
        Ok(json!({
            "api_version": 2,
            "start": start, "end": end, "events": values,
            "pagination": {"next_cursor": next_cursor, "has_more": has_more},
        }))
    }

    pub fn probe_events(&self, query: &ProbeQuery) -> Result<Value> {
        let (start, end) = query.range.bounds();
        let connection = self.connection()?;
        let resolved_channel = query
            .range
            .channel
            .as_deref()
            .map(|value| {
                resolve_alias(&connection, value).map(|key| key.unwrap_or_else(|| value.to_owned()))
            })
            .transpose()?;
        let mut statement = connection.prepare(
            "SELECT id, ts, started_at, duration_ms, plugin_version, status,
                    destination_reached, destination, delivered_msat,
                    amount_at_source_msat, total_fee_msat, lower_bound_msat,
                    failure_code, failcode, failcodename, erring_index,
                    erring_channel, erring_direction, erring_node
             FROM probe_events WHERE ts BETWEEN ?1 AND ?2 ORDER BY ts, id",
        )?;
        let rows = statement
            .query_map(params![to_i64(start)?, to_i64(end)?], |row| {
                Ok(json!({
                    "cursor": row.get::<_, i64>(0)?,
                    "timestamp": row.get::<_, i64>(1)?,
                    "started_at": row.get::<_, Option<i64>>(2)?,
                    "duration_ms": row.get::<_, Option<i64>>(3)?,
                    "plugin_version": row.get::<_, Option<String>>(4)?,
                    "status": row.get::<_, String>(5)?,
                    "destination_reached": row.get::<_, bool>(6)?,
                    "destination": row.get::<_, Option<String>>(7)?,
                    "delivered_msat": row.get::<_, i64>(8)?,
                    "amount_at_source_msat": row.get::<_, i64>(9)?,
                    "total_fee_msat": row.get::<_, i64>(10)?,
                    "lower_bound_msat": row.get::<_, Option<i64>>(11)?,
                    "failure": {
                        "code": row.get::<_, Option<i64>>(12)?,
                        "failcode": row.get::<_, Option<i64>>(13)?,
                        "failcodename": row.get::<_, Option<String>>(14)?,
                        "erring_index": row.get::<_, Option<i64>>(15)?,
                        "erring_channel": row.get::<_, Option<String>>(16)?,
                        "erring_direction": row.get::<_, Option<i64>>(17)?,
                        "erring_node": row.get::<_, Option<String>>(18)?,
                    }
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut events = Vec::new();
        for mut event in rows {
            let probe_id = event["cursor"].as_i64().unwrap_or(0);
            let hops = load_probe_hops(&connection, probe_id)?;
            let route_key = hops
                .iter()
                .filter_map(|hop| hop["short_channel_id_dir"].as_str())
                .collect::<Vec<_>>()
                .join(">");
            let channel_matches = resolved_channel.as_deref().is_none_or(|channel| {
                hops.iter().any(|hop| {
                    hop["channel_identity"] == channel
                        || hop["short_channel_id"] == channel
                        || hop["node_id_out"] == channel
                })
            });
            if !channel_matches
                || query
                    .status
                    .as_deref()
                    .is_some_and(|status| event["status"] != status)
                || query
                    .destination
                    .as_deref()
                    .is_some_and(|destination| event["destination"] != destination)
            {
                continue;
            }
            let object = event.as_object_mut().expect("probe event is an object");
            object.insert("route_key".to_owned(), json!(route_key));
            object.insert("route".to_owned(), json!(hops));
            events.push(event);
        }
        let aggregated = query.interval.is_some();
        let mut values = if let Some(interval) = query.interval {
            aggregate_probe_events(events, interval)
                .into_iter()
                .skip(query.range.cursor.unwrap_or(0) as usize)
                .take(query.range.limit as usize + 1)
                .collect::<Vec<_>>()
        } else {
            events
                .into_iter()
                .filter(|event| {
                    event["cursor"].as_u64().unwrap_or(0) > query.range.cursor.unwrap_or(0)
                })
                .take(query.range.limit as usize + 1)
                .collect::<Vec<_>>()
        };
        let has_more = values.len() > query.range.limit as usize;
        values.truncate(query.range.limit as usize);
        let next_cursor = has_more.then(|| {
            if aggregated {
                query.range.cursor.unwrap_or(0) + values.len() as u64
            } else {
                values
                    .last()
                    .and_then(|value| value["cursor"].as_u64())
                    .unwrap_or(0)
            }
        });
        Ok(json!({
            "api_version": 2,
            "start": start, "end": end, "interval": query.interval,
            "probes": values,
            "pagination": {"next_cursor": next_cursor, "has_more": has_more},
        }))
    }
}

#[derive(Debug)]
struct ProbeAggregate {
    period: u64,
    route_key: String,
    route: Value,
    attempts: u64,
    destination_reached: u64,
    route_failed: u64,
    unexpected_settlement: u64,
    max_success_msat: Option<u64>,
    smallest_failed_msat: Option<u64>,
    last_success_at: Option<u64>,
    last_observed_at: u64,
    failures: BTreeMap<String, u64>,
}

fn aggregate_probe_events(events: Vec<Value>, interval: u64) -> Vec<Value> {
    let mut groups = BTreeMap::<(u64, String), ProbeAggregate>::new();
    for event in events {
        let timestamp = event["timestamp"].as_u64().unwrap_or(0);
        let period = timestamp / interval * interval;
        let route_key = event["route_key"].as_str().unwrap_or_default().to_owned();
        let entry = groups
            .entry((period, route_key.clone()))
            .or_insert_with(|| ProbeAggregate {
                period,
                route_key,
                route: event["route"].clone(),
                attempts: 0,
                destination_reached: 0,
                route_failed: 0,
                unexpected_settlement: 0,
                max_success_msat: None,
                smallest_failed_msat: None,
                last_success_at: None,
                last_observed_at: timestamp,
                failures: BTreeMap::new(),
            });
        entry.attempts += 1;
        entry.last_observed_at = entry.last_observed_at.max(timestamp);
        let amount = event["delivered_msat"].as_u64().unwrap_or(0);
        match event["status"].as_str().unwrap_or_default() {
            "destination_reached" => {
                entry.destination_reached += 1;
                entry.max_success_msat = Some(
                    entry
                        .max_success_msat
                        .map_or(amount, |current| current.max(amount)),
                );
                entry.last_success_at = Some(
                    entry
                        .last_success_at
                        .map_or(timestamp, |current| current.max(timestamp)),
                );
            }
            "unexpected_settlement" => entry.unexpected_settlement += 1,
            _ => {
                entry.route_failed += 1;
                entry.smallest_failed_msat = Some(
                    entry
                        .smallest_failed_msat
                        .map_or(amount, |current| current.min(amount)),
                );
                let failure = event["failure"]["failcodename"]
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| {
                        event["failure"]["failcode"]
                            .as_i64()
                            .map(|code| code.to_string())
                    })
                    .unwrap_or_else(|| "unknown".to_owned());
                *entry.failures.entry(failure).or_default() += 1;
            }
        }
    }
    groups
        .into_values()
        .map(|entry| {
            json!({
                "period": entry.period,
                "route_key": entry.route_key,
                "route": entry.route,
                "attempts": entry.attempts,
                "destination_reached": entry.destination_reached,
                "route_failed": entry.route_failed,
                "unexpected_settlement": entry.unexpected_settlement,
                "max_success_msat": entry.max_success_msat,
                "smallest_failed_msat": entry.smallest_failed_msat,
                "last_success_at": entry.last_success_at,
                "last_observed_at": entry.last_observed_at,
                "failures": entry.failures,
            })
        })
        .collect()
}

fn migrate(connection: &mut Connection) -> Result<()> {
    let version = connection
        .query_row(
            "SELECT value FROM schema_meta WHERE key = 'version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|value| value.parse::<u64>())
        .transpose()
        .context("parsing cln-history schema version")?;
    match version {
        Some(SCHEMA_VERSION) => return Ok(()),
        None | Some(1) | Some(2) | Some(3) | Some(4) => {}
        Some(version) if version > SCHEMA_VERSION => {
            bail!("database schema {version} is newer than this cln-history binary")
        }
        Some(version) => bail!("unsupported cln-history database schema {version}"),
    }
    let forward_buckets_need_v3 =
        !table_has_column(connection, "forward_pair_buckets", "failcode")?;
    let channel_samples_need_v4 = !table_has_column(connection, "channel_samples", "blockheight")?;
    let channel_events_need_v4 = !table_has_column(connection, "channel_events", "direction")?;
    let transaction = connection.transaction()?;
    if version.is_none() || version == Some(1) {
        transaction.execute_batch(
            "INSERT OR IGNORE INTO channel_aliases(alias_type, alias, channel_key, first_seen, last_seen)
                 SELECT 'identity', channel_key, channel_key, first_seen, last_seen FROM channels;
             INSERT OR IGNORE INTO channel_aliases(alias_type, alias, channel_key, first_seen, last_seen)
                 SELECT 'channel_id', channel_id, channel_key, first_seen, last_seen
                 FROM channels WHERE channel_id IS NOT NULL;
             INSERT OR IGNORE INTO channel_aliases(alias_type, alias, channel_key, first_seen, last_seen)
                 SELECT 'short_channel_id', short_channel_id, channel_key, first_seen, last_seen
                 FROM channels WHERE short_channel_id IS NOT NULL;",
        )?;
    }
    if forward_buckets_need_v3 {
        transaction.execute_batch(
            "DROP TABLE IF EXISTS forward_pair_buckets_v3;
             CREATE TABLE forward_pair_buckets_v3 (
                 bucket_start INTEGER NOT NULL,
                 in_channel TEXT NOT NULL,
                 out_channel TEXT NOT NULL,
                 status TEXT NOT NULL,
                 failcode INTEGER NOT NULL DEFAULT 0,
                 failreason TEXT NOT NULL DEFAULT '',
                 forward_count INTEGER NOT NULL,
                 in_msat INTEGER NOT NULL,
                 out_msat INTEGER NOT NULL,
                 fee_msat INTEGER NOT NULL,
                 PRIMARY KEY(bucket_start, in_channel, out_channel, status, failcode, failreason)
             );
             INSERT INTO forward_pair_buckets_v3(
                 bucket_start, in_channel, out_channel, status, failcode, failreason,
                 forward_count, in_msat, out_msat, fee_msat
             ) SELECT bucket_start, in_channel, out_channel, status, 0, '',
                      forward_count, in_msat, out_msat, fee_msat
               FROM forward_pair_buckets;
             DROP TABLE forward_pair_buckets;
             ALTER TABLE forward_pair_buckets_v3 RENAME TO forward_pair_buckets;
             CREATE INDEX forward_pair_buckets_time ON forward_pair_buckets(bucket_start);",
        )?;
    }
    if channel_samples_need_v4 {
        transaction.execute_batch(
            "DROP TABLE IF EXISTS channel_samples_v4;
             CREATE TABLE channel_samples_v4 (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ts INTEGER NOT NULL,
                 channel_key TEXT NOT NULL,
                 channel_id TEXT,
                 short_channel_id TEXT,
                 peer_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 connected INTEGER NOT NULL,
                 reestablished INTEGER NOT NULL,
                 capacity_msat INTEGER NOT NULL,
                 to_us_msat INTEGER NOT NULL,
                 spendable_msat INTEGER NOT NULL,
                 receivable_msat INTEGER NOT NULL,
                 htlc_in_count INTEGER NOT NULL,
                 htlc_out_count INTEGER NOT NULL,
                 htlc_in_msat INTEGER NOT NULL,
                 htlc_out_msat INTEGER NOT NULL,
                 blockheight INTEGER,
                 htlc_min_expiry INTEGER,
                 htlc_max_expiry INTEGER,
                 htlc_hooked_count INTEGER NOT NULL DEFAULT 0,
                 htlc_trimmed_count INTEGER NOT NULL DEFAULT 0,
                 max_accepted_htlcs INTEGER,
                 local_fee_base_msat INTEGER,
                 local_fee_ppm INTEGER,
                 local_htlc_minimum_msat INTEGER,
                 local_htlc_maximum_msat INTEGER,
                 local_cltv_expiry_delta INTEGER,
                 remote_fee_base_msat INTEGER,
                 remote_fee_ppm INTEGER,
                 remote_htlc_minimum_msat INTEGER,
                 remote_htlc_maximum_msat INTEGER,
                 remote_cltv_expiry_delta INTEGER
             );
             INSERT INTO channel_samples_v4(
                 id, ts, channel_key, channel_id, short_channel_id, peer_id, state,
                 connected, reestablished, capacity_msat, to_us_msat, spendable_msat,
                 receivable_msat, htlc_in_count, htlc_out_count, htlc_in_msat, htlc_out_msat
             ) SELECT id, ts, channel_key, channel_id, short_channel_id, peer_id, state,
                      connected, reestablished, capacity_msat, to_us_msat, spendable_msat,
                      receivable_msat, htlc_in_count, htlc_out_count, htlc_in_msat, htlc_out_msat
               FROM channel_samples;
             DROP TABLE channel_samples;
             ALTER TABLE channel_samples_v4 RENAME TO channel_samples;
             CREATE INDEX channel_samples_channel_ts ON channel_samples(channel_key, ts);",
        )?;
    }
    if channel_events_need_v4 {
        transaction.execute_batch(
            "ALTER TABLE channel_events ADD COLUMN direction TEXT;
             ALTER TABLE channel_events ADD COLUMN reason TEXT;",
        )?;
    }
    transaction.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', ?1)",
        [SCHEMA_VERSION.to_string()],
    )?;
    transaction.commit()?;
    Ok(())
}

fn table_has_column(connection: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns.iter().any(|candidate| candidate == column))
}

fn resolve_sample_identity(
    transaction: &Transaction<'_>,
    ts: u64,
    sample: &mut ChannelSample,
) -> Result<()> {
    let mut stable_key = None;
    for alias in &sample.aliases {
        if let Some(existing) = resolve_alias(transaction, &alias.value)? {
            stable_key = Some(existing);
            break;
        }
    }
    let stable_key = stable_key.unwrap_or_else(|| sample.channel_key.clone());
    sample.channel_key = stable_key.clone();
    record_alias(transaction, ts, "identity", &stable_key, &stable_key)?;
    for alias in &sample.aliases {
        record_alias(transaction, ts, alias.kind, &alias.value, &stable_key)?;
    }
    Ok(())
}

fn record_alias(
    connection: &Connection,
    ts: u64,
    alias_type: &str,
    alias: &str,
    channel_key: &str,
) -> Result<()> {
    connection.execute(
        "INSERT INTO channel_aliases(alias_type, alias, channel_key, first_seen, last_seen)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(alias_type, alias) DO UPDATE SET
             last_seen = MAX(last_seen, excluded.last_seen)",
        params![alias_type, alias, channel_key, to_i64(ts)?],
    )?;
    Ok(())
}

fn resolve_alias(connection: &Connection, alias: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT channel_key FROM channel_aliases WHERE alias = ?1
             ORDER BY CASE alias_type WHEN 'identity' THEN 0 WHEN 'channel_id' THEN 1 ELSE 2 END
             LIMIT 1",
            [alias],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
}

fn insert_sample(transaction: &Transaction<'_>, ts: u64, sample: &ChannelSample) -> Result<()> {
    transaction.execute(
        "INSERT INTO channel_samples(
             ts, channel_key, channel_id, short_channel_id, peer_id, state,
             connected, reestablished, capacity_msat, to_us_msat,
             spendable_msat, receivable_msat, htlc_in_count, htlc_out_count,
             htlc_in_msat, htlc_out_msat, blockheight, htlc_min_expiry,
             htlc_max_expiry, htlc_hooked_count, htlc_trimmed_count,
             max_accepted_htlcs, local_fee_base_msat, local_fee_ppm,
             local_htlc_minimum_msat, local_htlc_maximum_msat, local_cltv_expiry_delta,
             remote_fee_base_msat, remote_fee_ppm, remote_htlc_minimum_msat,
             remote_htlc_maximum_msat, remote_cltv_expiry_delta
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
             ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32
         )",
        params![
            to_i64(ts)?,
            sample.channel_key,
            sample.channel_id,
            sample.short_channel_id,
            sample.peer_id,
            sample.state,
            sample.connected,
            sample.reestablished,
            to_i64(sample.capacity_msat)?,
            to_i64(sample.to_us_msat)?,
            to_i64(sample.spendable_msat)?,
            to_i64(sample.receivable_msat)?,
            to_i64(sample.htlc_in_count)?,
            to_i64(sample.htlc_out_count)?,
            to_i64(sample.htlc_in_msat)?,
            to_i64(sample.htlc_out_msat)?,
            optional_i64(sample.blockheight)?,
            optional_i64(sample.htlc_min_expiry)?,
            optional_i64(sample.htlc_max_expiry)?,
            to_i64(sample.htlc_hooked_count)?,
            to_i64(sample.htlc_trimmed_count)?,
            optional_i64(sample.max_accepted_htlcs)?,
            optional_i64(sample.local_policy.fee_base_msat)?,
            optional_i64(sample.local_policy.fee_ppm)?,
            optional_i64(sample.local_policy.htlc_minimum_msat)?,
            optional_i64(sample.local_policy.htlc_maximum_msat)?,
            optional_i64(sample.local_policy.cltv_expiry_delta)?,
            optional_i64(sample.remote_policy.fee_base_msat)?,
            optional_i64(sample.remote_policy.fee_ppm)?,
            optional_i64(sample.remote_policy.htlc_minimum_msat)?,
            optional_i64(sample.remote_policy.htlc_maximum_msat)?,
            optional_i64(sample.remote_policy.cltv_expiry_delta)?,
        ],
    )?;
    Ok(())
}

fn latest_sample(
    connection: &Connection,
    channel_key: &str,
) -> Result<Option<(u64, ChannelSample)>> {
    connection
        .query_row(
            "SELECT ts, channel_key, channel_id, short_channel_id, peer_id, state,
                    connected, reestablished, capacity_msat, to_us_msat,
                    spendable_msat, receivable_msat, htlc_in_count, htlc_out_count,
                    htlc_in_msat, htlc_out_msat, blockheight, htlc_min_expiry,
                    htlc_max_expiry, htlc_hooked_count, htlc_trimmed_count,
                    max_accepted_htlcs, local_fee_base_msat, local_fee_ppm,
                    local_htlc_minimum_msat, local_htlc_maximum_msat, local_cltv_expiry_delta,
                    remote_fee_base_msat, remote_fee_ppm, remote_htlc_minimum_msat,
                    remote_htlc_maximum_msat, remote_cltv_expiry_delta
             FROM channel_samples WHERE channel_key = ?1 ORDER BY ts DESC, id DESC LIMIT 1",
            [channel_key],
            |row| Ok((from_i64(row.get(0)?), sample_from_row(row, 1)?)),
        )
        .optional()
        .map_err(Into::into)
}

fn sample_from_row(row: &rusqlite::Row<'_>, offset: usize) -> rusqlite::Result<ChannelSample> {
    Ok(ChannelSample {
        channel_key: row.get(offset)?,
        aliases: Vec::new(),
        channel_id: row.get(offset + 1)?,
        short_channel_id: row.get(offset + 2)?,
        peer_id: row.get(offset + 3)?,
        state: row.get(offset + 4)?,
        connected: row.get(offset + 5)?,
        reestablished: row.get(offset + 6)?,
        capacity_msat: from_i64(row.get(offset + 7)?),
        to_us_msat: from_i64(row.get(offset + 8)?),
        spendable_msat: from_i64(row.get(offset + 9)?),
        receivable_msat: from_i64(row.get(offset + 10)?),
        htlc_in_count: from_i64(row.get(offset + 11)?),
        htlc_out_count: from_i64(row.get(offset + 12)?),
        htlc_in_msat: from_i64(row.get(offset + 13)?),
        htlc_out_msat: from_i64(row.get(offset + 14)?),
        blockheight: from_optional_i64(row.get(offset + 15)?),
        htlc_min_expiry: from_optional_i64(row.get(offset + 16)?),
        htlc_max_expiry: from_optional_i64(row.get(offset + 17)?),
        htlc_hooked_count: from_i64(row.get(offset + 18)?),
        htlc_trimmed_count: from_i64(row.get(offset + 19)?),
        max_accepted_htlcs: from_optional_i64(row.get(offset + 20)?),
        local_policy: crate::model::ChannelPolicy {
            fee_base_msat: from_optional_i64(row.get(offset + 21)?),
            fee_ppm: from_optional_i64(row.get(offset + 22)?),
            htlc_minimum_msat: from_optional_i64(row.get(offset + 23)?),
            htlc_maximum_msat: from_optional_i64(row.get(offset + 24)?),
            cltv_expiry_delta: from_optional_i64(row.get(offset + 25)?),
        },
        remote_policy: crate::model::ChannelPolicy {
            fee_base_msat: from_optional_i64(row.get(offset + 26)?),
            fee_ppm: from_optional_i64(row.get(offset + 27)?),
            htlc_minimum_msat: from_optional_i64(row.get(offset + 28)?),
            htlc_maximum_msat: from_optional_i64(row.get(offset + 29)?),
            cltv_expiry_delta: from_optional_i64(row.get(offset + 30)?),
        },
    })
}

struct SampleRow {
    id: u64,
    timestamp: u64,
    bucket_start: Option<u64>,
    sample: ChannelSample,
}

struct Page {
    items: Vec<Value>,
    next_cursor: Option<u64>,
    has_more: bool,
}

fn load_samples(connection: &Connection, query: &SampleQuery, htlcs_only: bool) -> Result<Page> {
    let (start, end) = query.range.bounds();
    let cursor = query.range.cursor.unwrap_or(0);
    let resolved_filter = query
        .range
        .channel
        .as_deref()
        .map(|channel| {
            resolve_alias(connection, channel)
                .map(|value| value.unwrap_or_else(|| channel.to_owned()))
        })
        .transpose()?;
    let mut samples = Vec::<SampleRow>::new();
    let mut baselines = HashMap::<String, SampleRow>::new();
    let mut statement = connection.prepare(
        "SELECT id, ts, channel_key, channel_id, short_channel_id, peer_id, state,
                connected, reestablished, capacity_msat, to_us_msat,
                spendable_msat, receivable_msat, htlc_in_count, htlc_out_count,
                htlc_in_msat, htlc_out_msat, blockheight, htlc_min_expiry,
                htlc_max_expiry, htlc_hooked_count, htlc_trimmed_count,
                max_accepted_htlcs, local_fee_base_msat, local_fee_ppm,
                local_htlc_minimum_msat, local_htlc_maximum_msat, local_cltv_expiry_delta,
                remote_fee_base_msat, remote_fee_ppm, remote_htlc_minimum_msat,
                remote_htlc_maximum_msat, remote_cltv_expiry_delta
         FROM channel_samples WHERE ts <= ?1 ORDER BY ts, id",
    )?;
    for row in statement.query_map([to_i64(end)?], |row| {
        Ok(SampleRow {
            id: from_i64(row.get(0)?),
            timestamp: from_i64(row.get(1)?),
            bucket_start: None,
            sample: sample_from_row(row, 2)?,
        })
    })? {
        let row = row?;
        if !sample_matches(resolved_filter.as_deref(), &row.sample) {
            continue;
        }
        if row.timestamp < start {
            if cursor == 0 {
                baselines.insert(row.sample.channel_key.clone(), row);
            }
        } else if row.id > cursor
            && (!htlcs_only || row.sample.htlc_in_count > 0 || row.sample.htlc_out_count > 0)
        {
            samples.push(row);
        }
    }
    if let Some(interval) = query.interval {
        let mut buckets = BTreeMap::<(String, u64), SampleRow>::new();
        for mut row in samples {
            let bucket =
                start.saturating_add((row.timestamp.saturating_sub(start) / interval) * interval);
            row.bucket_start = Some(bucket);
            let key = (row.sample.channel_key.clone(), bucket);
            match buckets.get_mut(&key) {
                Some(existing) if htlcs_only => {
                    let existing_count =
                        existing.sample.htlc_in_count + existing.sample.htlc_out_count;
                    let row_count = row.sample.htlc_in_count + row.sample.htlc_out_count;
                    if row_count >= existing_count {
                        *existing = row;
                    }
                }
                Some(existing) => {
                    *existing = row;
                }
                None => {
                    buckets.insert(key, row);
                }
            }
        }
        samples = buckets.into_values().collect();
    }
    samples.extend(baselines.into_values());
    samples.sort_by_key(|row| row.id);
    let has_more = samples.len() > query.range.limit as usize;
    samples.truncate(query.range.limit as usize);
    let next_cursor = has_more.then(|| samples.last().map(|row| row.id)).flatten();
    let items = samples
        .into_iter()
        .map(|row| {
            let mut value = sample_json(
                row.id,
                row.timestamp,
                row.bucket_start,
                &row.sample,
                htlcs_only,
            );
            value
                .as_object_mut()
                .expect("sample is a JSON object")
                .insert(
                    "channel_details".to_owned(),
                    channel_details(connection, &row.sample.channel_key)?,
                );
            Ok(value)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Page {
        items,
        next_cursor,
        has_more,
    })
}

fn sample_json(
    id: u64,
    timestamp: u64,
    bucket_start: Option<u64>,
    sample: &ChannelSample,
    htlcs_only: bool,
) -> Value {
    let mut value = json!({
        "cursor": id,
        "timestamp": timestamp,
        "bucket_start": bucket_start,
        "channel_identity": sample.channel_key,
        "channel_key": sample.channel_key,
        "channel_id": sample.channel_id,
        "short_channel_id": sample.short_channel_id,
        "peer_id": sample.peer_id,
        "htlc_in_count": sample.htlc_in_count,
        "htlc_out_count": sample.htlc_out_count,
        "htlc_in_msat": sample.htlc_in_msat,
        "htlc_out_msat": sample.htlc_out_msat,
        "blockheight": sample.blockheight,
        "htlc_min_expiry": sample.htlc_min_expiry,
        "htlc_max_expiry": sample.htlc_max_expiry,
        "htlc_hooked_count": sample.htlc_hooked_count,
        "htlc_trimmed_count": sample.htlc_trimmed_count,
        "max_accepted_htlcs": sample.max_accepted_htlcs,
    });
    if !htlcs_only {
        let object = value.as_object_mut().expect("JSON object");
        object.extend(
            json!({
                "state": sample.state,
                "connected": sample.connected,
                "reestablished": sample.reestablished,
                "capacity_msat": sample.capacity_msat,
                "to_us_msat": sample.to_us_msat,
                "spendable_msat": sample.spendable_msat,
                "receivable_msat": sample.receivable_msat,
                "local_policy": {
                    "fee_base_msat": sample.local_policy.fee_base_msat,
                    "fee_ppm": sample.local_policy.fee_ppm,
                    "htlc_minimum_msat": sample.local_policy.htlc_minimum_msat,
                    "htlc_maximum_msat": sample.local_policy.htlc_maximum_msat,
                    "cltv_expiry_delta": sample.local_policy.cltv_expiry_delta,
                },
                "remote_policy": {
                    "fee_base_msat": sample.remote_policy.fee_base_msat,
                    "fee_ppm": sample.remote_policy.fee_ppm,
                    "htlc_minimum_msat": sample.remote_policy.htlc_minimum_msat,
                    "htlc_maximum_msat": sample.remote_policy.htlc_maximum_msat,
                    "cltv_expiry_delta": sample.remote_policy.cltv_expiry_delta,
                },
            })
            .as_object()
            .expect("JSON object")
            .clone(),
        );
    }
    value
}

fn insert_event(
    transaction: &Transaction<'_>,
    ts: u64,
    event_type: &str,
    sample: Option<&ChannelSample>,
    old_state: Option<&str>,
    new_state: Option<&str>,
    cause: Option<&str>,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO channel_events(
             ts, event_type, channel_key, channel_id, short_channel_id,
             peer_id, old_state, new_state, cause
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            to_i64(ts)?,
            event_type,
            sample.map(|value| &value.channel_key),
            sample.and_then(|value| value.channel_id.as_deref()),
            sample.and_then(|value| value.short_channel_id.as_deref()),
            sample.map(|value| value.peer_id.as_str()),
            old_state,
            new_state,
            cause,
        ],
    )?;
    Ok(())
}

fn count(connection: &Connection, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    Ok(connection.query_row(&sql, [], |row| row.get(0))?)
}

fn attach_pair_identity(connection: &Connection, value: &mut Value) -> Result<()> {
    let incoming = value
        .get("in_channel")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let outgoing = value
        .get("out_channel")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let object = value.as_object_mut().expect("pair is a JSON object");
    object.insert("in_channel_identity".to_owned(), json!(incoming));
    object.insert("out_channel_identity".to_owned(), json!(outgoing));
    object.insert(
        "in_channel_details".to_owned(),
        channel_details(connection, &incoming)?,
    );
    object.insert(
        "out_channel_details".to_owned(),
        channel_details(connection, &outgoing)?,
    );
    Ok(())
}

fn load_probe_hops(connection: &Connection, probe_id: i64) -> Result<Vec<Value>> {
    let mut statement = connection.prepare(
        "SELECT hop_index, channel_key, short_channel_id, direction,
                node_id_out, amount_out_msat, cltv_out, status
         FROM probe_hops WHERE probe_id = ?1 ORDER BY hop_index",
    )?;
    let rows = statement
        .query_map([probe_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    rows.into_iter()
        .map(
            |(
                hop_index,
                channel_key,
                short_channel_id,
                direction,
                node_id_out,
                amount_out_msat,
                cltv_out,
                status,
            )| {
                let identity = channel_key
                    .as_deref()
                    .unwrap_or(&short_channel_id)
                    .to_owned();
                Ok(json!({
                    "hop_index": hop_index,
                    "channel_identity": identity,
                    "channel_details": channel_details(connection, &identity)?,
                    "short_channel_id": short_channel_id,
                    "short_channel_id_dir": format!("{short_channel_id}/{direction}"),
                    "direction": direction,
                    "node_id_out": node_id_out,
                    "amount_out_msat": amount_out_msat,
                    "cltv_out": cltv_out,
                    "status": status,
                }))
            },
        )
        .collect()
}

fn channel_details(connection: &Connection, channel_key: &str) -> Result<Value> {
    let current = connection
        .query_row(
            "SELECT channel_id, short_channel_id, peer_id, first_seen, last_seen, disappeared_at
             FROM channels WHERE channel_key = ?1",
            [channel_key],
            |row| {
                Ok(json!({
                    "channel_id": row.get::<_, Option<String>>(0)?,
                    "short_channel_id": row.get::<_, Option<String>>(1)?,
                    "peer_id": row.get::<_, String>(2)?,
                    "first_seen": row.get::<_, i64>(3)?,
                    "last_seen": row.get::<_, i64>(4)?,
                    "disappeared_at": row.get::<_, Option<i64>>(5)?,
                }))
            },
        )
        .optional()?;
    let mut statement = connection.prepare(
        "SELECT alias_type, alias, first_seen, last_seen FROM channel_aliases
         WHERE channel_key = ?1 ORDER BY alias_type, first_seen, alias",
    )?;
    let aliases = statement
        .query_map([channel_key], |row| {
            Ok(json!({
                "type": row.get::<_, String>(0)?, "value": row.get::<_, String>(1)?,
                "first_seen": row.get::<_, i64>(2)?, "last_seen": row.get::<_, i64>(3)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({"identity": channel_key, "current": current, "aliases": aliases}))
}

fn count_where(connection: &Connection, table: &str, condition: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE {condition}");
    Ok(connection.query_row(&sql, [], |row| row.get(0))?)
}

fn pragma_i64(connection: &Connection, pragma: &str) -> Result<i64> {
    let sql = format!("PRAGMA {pragma}");
    Ok(connection.query_row(&sql, [], |row| row.get(0))?)
}

fn gauge(name: &str, help: &str, value: impl serde::Serialize) -> Value {
    json!({
        "name": name,
        "help": help,
        "type": "gauge",
        "samples": [{"value": value}],
    })
}

fn sidecar_path(path: &std::path::Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn file_bytes(path: &std::path::Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn storage_bytes(path: &std::path::Path) -> u64 {
    file_bytes(path)
        .saturating_add(file_bytes(&sidecar_path(path, "-wal")))
        .saturating_add(file_bytes(&sidecar_path(path, "-shm")))
}

fn text(value: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .find_map(|field| value.get(field)?.as_str().map(str::to_owned))
}

fn matches_channel(filter: Option<&str>, value: &str) -> bool {
    filter.is_none_or(|filter| filter == value)
}

fn sample_matches(filter: Option<&str>, sample: &ChannelSample) -> bool {
    filter.is_none_or(|filter| {
        sample.channel_key == filter
            || sample.channel_id.as_deref() == Some(filter)
            || sample.short_channel_id.as_deref() == Some(filter)
            || sample.peer_id == filter
    })
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("value exceeds SQLite INTEGER range")
}

fn from_i64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn optional_i64(value: Option<u64>) -> Result<Option<i64>> {
    value.map(to_i64).transpose()
}

fn from_optional_i64(value: Option<i64>) -> Option<u64> {
    value.map(from_i64)
}

#[cfg(test)]
mod tests {
    use super::{HistoryDb, resolve_alias};
    use crate::model::{EventQuery, PairQuery, ProbeQuery, SampleQuery};
    use serde_json::json;

    fn database() -> (tempfile::TempDir, HistoryDb) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database =
            HistoryDb::open(directory.path().join("history.sqlite3"), 30).expect("open database");
        (directory, database)
    }

    fn channels(to_us: u64) -> serde_json::Value {
        json!({"channels": [{
            "peer_id": "peer", "channel_id": "channel", "short_channel_id": "1x2x3",
            "state": "CHANNELD_NORMAL", "peer_connected": true, "reestablished": true,
            "total_msat": 1_000_000, "to_us_msat": to_us,
            "spendable_msat": to_us.saturating_sub(10_000),
            "receivable_msat": 990_000u64.saturating_sub(to_us), "htlcs": []
        }]})
    }

    fn channel_version(
        channel_id: &str,
        scid: &str,
        local_alias: &str,
        to_us: u64,
    ) -> serde_json::Value {
        json!({"channels": [{
            "peer_id": "peer", "channel_id": channel_id, "short_channel_id": scid,
            "alias": {"local": local_alias},
            "state": "CHANNELD_NORMAL", "peer_connected": true, "reestablished": true,
            "total_msat": 1_000_000, "to_us_msat": to_us,
            "spendable_msat": to_us.saturating_sub(10_000),
            "receivable_msat": 990_000u64.saturating_sub(to_us), "htlcs": []
        }]})
    }

    fn probe_result(timestamp: u64, status: &str, amount_msat: u64) -> serde_json::Value {
        json!({
            "observed_at": timestamp,
            "started_at": timestamp.saturating_sub(2),
            "duration_ms": 2_000,
            "plugin_version": "0.1.0",
            "status": status,
            "destination_reached": status == "destination_reached",
            "delivered_msat": amount_msat,
            "amount_at_source_msat": amount_msat + 100,
            "total_fee_msat": 100,
            "lower_bound_msat": if status == "destination_reached" {
                json!(amount_msat)
            } else {
                serde_json::Value::Null
            },
            "failure": if status == "destination_reached" {
                json!({"code": 203, "failcodename": "WIRE_INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS",
                    "erring_index": 2, "erring_channel": "2x2x2", "erring_direction": 0,
                    "erring_node": "destination"})
            } else {
                json!({"code": 204, "failcode": 4103,
                    "failcodename": "WIRE_TEMPORARY_CHANNEL_FAILURE", "erring_index": 1,
                    "erring_channel": "1x2x3", "erring_direction": 0,
                    "erring_node": "middle"})
            },
            "route": [
                {"node_id_out": "middle", "short_channel_id_dir": "1x2x3/0",
                    "amount_out_msat": amount_msat + 100, "cltv_out": 144,
                    "status": "reached"},
                {"node_id_out": "destination", "short_channel_id_dir": "2x2x2/0",
                    "amount_out_msat": amount_msat, "cltv_out": 110,
                    "status": if status == "destination_reached" { "reached" } else { "failed" }}
            ]
        })
    }

    #[test]
    fn snapshots_are_change_only_with_hourly_checkpoints() {
        let (_directory, database) = database();
        database
            .record_channel_snapshot(1_000, &channels(600_000))
            .unwrap();
        database
            .record_channel_snapshot(1_300, &channels(600_000))
            .unwrap();
        database
            .record_channel_snapshot(1_600, &channels(500_000))
            .unwrap();
        database
            .record_channel_snapshot(5_300, &channels(500_000))
            .unwrap();
        let value = database
            .channel_samples(&SampleQuery::parse(json!({})).unwrap())
            .unwrap();
        assert_eq!(value["samples"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn new_blocks_do_not_create_idle_channel_change_points() {
        let (_directory, database) = database();
        database
            .record_channel_snapshot_at_height(1_000, &channels(600_000), Some(900_000))
            .unwrap();
        database
            .record_channel_snapshot_at_height(1_300, &channels(600_000), Some(900_001))
            .unwrap();
        let value = database
            .channel_samples(&SampleQuery::parse(json!({})).unwrap())
            .unwrap();
        assert_eq!(value["samples"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn aggregates_forward_pairs_and_effective_ppm() {
        let (_directory, database) = database();
        let forward = json!({
            "status": "settled", "in_channel": "1x1x1", "out_channel": "2x2x2",
            "in_msat": 1_000_650, "out_msat": 1_000_000, "fee_msat": 650
        });
        database.record_forward_event(1_001, &forward).unwrap();
        database.record_forward_event(1_100, &forward).unwrap();
        let query = PairQuery::parse(json!({"start": 0, "end": 2_000, "interval": 3600})).unwrap();
        let value = database.forward_pairs(&query).unwrap();
        assert_eq!(value["pairs"][0]["forward_count"], 2);
        assert_eq!(value["pairs"][0]["fee_msat"], 1_300);
        assert_eq!(value["pairs"][0]["effective_ppm"], 650.0);
    }

    #[test]
    fn preserves_forward_failure_reasons_and_ingress_only_failures() {
        let (_directory, database) = database();
        for (failcode, failreason) in [
            (4103, "WIRE_TEMPORARY_CHANNEL_FAILURE"),
            (4103, "WIRE_TEMPORARY_CHANNEL_FAILURE"),
            (8194, "WIRE_TEMPORARY_NODE_FAILURE"),
        ] {
            database
                .record_forward_event(
                    1_001,
                    &json!({
                        "status": "local_failed", "in_channel": "1x1x1",
                        "in_msat": 250_000, "failcode": failcode, "failreason": failreason
                    }),
                )
                .unwrap();
        }
        let value = database
            .forward_pairs(&PairQuery::parse(json!({"start": 0, "end": 2_000})).unwrap())
            .unwrap();
        let pairs = value["pairs"].as_array().unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0]["out_channel"], "unknown");
        assert_eq!(pairs[0]["failcode"], 4103);
        assert_eq!(pairs[0]["failreason"], "WIRE_TEMPORARY_CHANNEL_FAILURE");
        assert_eq!(pairs[0]["forward_count"], 2);
        assert_eq!(pairs[1]["failcode"], 8194);
    }

    #[test]
    fn migrates_v2_forward_buckets_without_inventing_failure_details() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.sqlite3");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO schema_meta VALUES ('version', '2');
                 CREATE TABLE forward_pair_buckets(
                     bucket_start INTEGER NOT NULL, in_channel TEXT NOT NULL,
                     out_channel TEXT NOT NULL, status TEXT NOT NULL,
                     forward_count INTEGER NOT NULL, in_msat INTEGER NOT NULL,
                     out_msat INTEGER NOT NULL, fee_msat INTEGER NOT NULL,
                     PRIMARY KEY(bucket_start, in_channel, out_channel, status)
                 );
                 INSERT INTO forward_pair_buckets VALUES (
                     900, '1x1x1', '2x2x2', 'local_failed', 4, 1000000, 0, 0
                 );",
            )
            .unwrap();
        drop(connection);
        let database = HistoryDb::open(path, 30).unwrap();
        let value = database
            .forward_pairs(&PairQuery::parse(json!({"start": 0, "end": 2_000})).unwrap())
            .unwrap();
        assert_eq!(database.status().unwrap()["schema_version"], 5);
        assert_eq!(value["pairs"][0]["forward_count"], 4);
        assert_eq!(value["pairs"][0]["failcode"], serde_json::Value::Null);
        assert_eq!(value["pairs"][0]["failreason"], serde_json::Value::Null);
    }

    #[test]
    fn detects_channel_disappearance_and_reappearance() {
        let (_directory, database) = database();
        database
            .record_channel_snapshot(1_000, &channels(600_000))
            .unwrap();
        database
            .record_channel_snapshot(1_300, &json!({"channels": []}))
            .unwrap();
        database
            .record_channel_snapshot(1_600, &channels(600_000))
            .unwrap();
        let status = database.status().unwrap();
        assert_eq!(status["counts"]["channel_events"], 2);
    }

    #[test]
    fn records_exact_peer_connection_notifications() {
        let (_directory, database) = database();
        database
            .record_peer_connection_event(
                1_000,
                "connect",
                &json!({"id": "peer", "direction": "out"}),
            )
            .unwrap();
        database
            .record_peer_connection_event(
                1_120,
                "disconnect",
                &json!({"id": "peer", "reason": "socket closed"}),
            )
            .unwrap();

        let value = database
            .events(&EventQuery::parse(json!({"start": 900, "end": 1_200})).unwrap())
            .unwrap();
        let events = value["events"].as_array().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["event_type"], "connect");
        assert_eq!(events[0]["peer_id"], "peer");
        assert_eq!(events[0]["direction"], "out");
        assert_eq!(events[1]["event_type"], "disconnect");
        assert_eq!(events[1]["reason"], "socket closed");
        assert_eq!(
            database.status().unwrap()["counts"]["peer_connection_events"],
            2
        );
    }

    #[test]
    fn migrates_v3_channel_samples_and_events_to_v4() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.sqlite3");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO schema_meta VALUES ('version', '3');
                 CREATE TABLE channel_samples(
                     id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
                     channel_key TEXT NOT NULL, channel_id TEXT, short_channel_id TEXT,
                     peer_id TEXT NOT NULL, state TEXT NOT NULL, connected INTEGER NOT NULL,
                     reestablished INTEGER NOT NULL, capacity_msat INTEGER NOT NULL,
                     to_us_msat INTEGER NOT NULL, spendable_msat INTEGER NOT NULL,
                     receivable_msat INTEGER NOT NULL, htlc_in_count INTEGER NOT NULL,
                     htlc_out_count INTEGER NOT NULL, htlc_in_msat INTEGER NOT NULL,
                     htlc_out_msat INTEGER NOT NULL
                 );
                 INSERT INTO channel_samples VALUES (
                     7, 1000, 'channel', 'channel', '1x2x3', 'peer',
                     'CHANNELD_NORMAL', 1, 1, 1000000, 600000, 590000, 390000,
                     0, 0, 0, 0
                 );
                 CREATE TABLE channel_events(
                     id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL,
                     event_type TEXT NOT NULL, channel_key TEXT, channel_id TEXT,
                     short_channel_id TEXT, peer_id TEXT, old_state TEXT,
                     new_state TEXT, cause TEXT
                 );
                 INSERT INTO channel_events VALUES (
                     3, 1000, 'state_changed', 'channel', 'channel', '1x2x3',
                     'peer', 'A', 'B', 'local'
                 );",
            )
            .unwrap();
        drop(connection);

        let database = HistoryDb::open(path, 30).unwrap();
        let samples = database
            .channel_samples(&SampleQuery::parse(json!({})).unwrap())
            .unwrap();
        let events = database
            .events(&EventQuery::parse(json!({})).unwrap())
            .unwrap();
        assert_eq!(database.status().unwrap()["schema_version"], 5);
        assert_eq!(samples["samples"][0]["cursor"], 7);
        assert_eq!(samples["samples"][0]["to_us_msat"], 600_000);
        assert_eq!(
            samples["samples"][0]["blockheight"],
            serde_json::Value::Null
        );
        assert_eq!(events["events"][0]["cursor"], 3);
        assert_eq!(events["events"][0]["direction"], serde_json::Value::Null);
    }

    #[test]
    fn keeps_identity_when_channel_ids_change_but_an_alias_survives() {
        let (_directory, database) = database();
        database
            .record_channel_snapshot(
                1_000,
                &channel_version("channel-a", "1x1x1", "9x9x9", 600_000),
            )
            .unwrap();
        database
            .record_channel_snapshot(
                1_300,
                &channel_version("channel-b", "2x2x2", "9x9x9", 500_000),
            )
            .unwrap();
        let page = database
            .channel_samples(&SampleQuery::parse(json!({})).unwrap())
            .unwrap();
        let identities = page["samples"]
            .as_array()
            .unwrap()
            .iter()
            .map(|sample| sample["channel_identity"].as_str().unwrap())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(identities, std::collections::HashSet::from(["channel-a"]));
        assert_eq!(database.status().unwrap()["counts"]["channels"], 1);

        database
            .record_forward_event(
                1_400,
                &json!({
                    "status": "settled", "in_channel": "2x2x2", "out_channel": "unknown",
                    "in_msat": 1_001, "out_msat": 1_000, "fee_msat": 1
                }),
            )
            .unwrap();
        let pairs = database
            .forward_pairs(&PairQuery::parse(json!({"start": 0, "end": 2_000})).unwrap())
            .unwrap();
        assert_eq!(pairs["pairs"][0]["in_channel_identity"], "channel-a");
    }

    #[test]
    fn paginates_samples_without_overlap() {
        let (_directory, database) = database();
        for (timestamp, balance) in [(1_000, 600_000), (1_300, 500_000), (1_600, 400_000)] {
            database
                .record_channel_snapshot(timestamp, &channels(balance))
                .unwrap();
        }
        let first = database
            .channel_samples(&SampleQuery::parse(json!({"limit": 2})).unwrap())
            .unwrap();
        assert_eq!(first["samples"].as_array().unwrap().len(), 2);
        assert_eq!(first["pagination"]["has_more"], true);
        let cursor = first["pagination"]["next_cursor"].as_u64().unwrap();
        let second = database
            .channel_samples(&SampleQuery::parse(json!({"limit": 2, "cursor": cursor})).unwrap())
            .unwrap();
        assert_eq!(second["samples"].as_array().unwrap().len(), 1);
        assert_eq!(second["pagination"]["has_more"], false);
        assert!(second["samples"][0]["cursor"].as_u64().unwrap() > cursor);
    }

    #[test]
    fn returns_last_channel_state_in_each_time_bucket() {
        let (_directory, database) = database();
        for (timestamp, balance) in [(1_000, 600_000), (1_300, 500_000), (1_600, 400_000)] {
            database
                .record_channel_snapshot(timestamp, &channels(balance))
                .unwrap();
        }
        let page = database
            .channel_samples(
                &SampleQuery::parse(json!({
                    "start": 1_000, "end": 2_000, "interval": 600
                }))
                .unwrap(),
            )
            .unwrap();
        assert_eq!(page["samples"].as_array().unwrap().len(), 2);
        assert_eq!(page["samples"][0]["bucket_start"], 1_000);
        assert_eq!(page["samples"][0]["to_us_msat"], 500_000);
        assert_eq!(page["samples"][1]["bucket_start"], 1_600);
    }

    #[test]
    fn migrates_v1_channel_identifiers_into_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("history.sqlite3");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO schema_meta VALUES ('version', '1');
             CREATE TABLE channels(
                 channel_key TEXT PRIMARY KEY, channel_id TEXT, short_channel_id TEXT,
                 peer_id TEXT NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL,
                 last_state TEXT NOT NULL, disappeared_at INTEGER
             );
             INSERT INTO channels VALUES (
                 'stable', 'channel-id', '1x2x3', 'peer', 1000, 1300,
                 'CHANNELD_NORMAL', NULL
             );",
            )
            .unwrap();
        drop(connection);
        let database = HistoryDb::open(path, 30).unwrap();
        let connection = database.connection().unwrap();
        assert_eq!(
            resolve_alias(&connection, "channel-id").unwrap().as_deref(),
            Some("stable")
        );
        assert_eq!(
            resolve_alias(&connection, "1x2x3").unwrap().as_deref(),
            Some("stable")
        );
        assert_eq!(database.status().unwrap()["schema_version"], 5);
    }

    #[test]
    fn paginates_forward_pairs_and_events() {
        let (_directory, database) = database();
        for (timestamp, incoming) in [(1_000, "1x1x1"), (5_000, "2x2x2"), (9_000, "3x3x3")] {
            database
                .record_forward_event(
                    timestamp,
                    &json!({
                        "status": "settled", "in_channel": incoming, "out_channel": "9x9x9",
                        "in_msat": 1_001, "out_msat": 1_000, "fee_msat": 1
                    }),
                )
                .unwrap();
            database
                .record_channel_event(
                    timestamp,
                    &json!({
                        "channel_id": incoming, "peer_id": "peer",
                        "old_state": "A", "new_state": "B", "cause": "local"
                    }),
                )
                .unwrap();
        }
        let pairs = database
            .forward_pairs(
                &PairQuery::parse(json!({
                    "start": 0, "end": 10_000, "interval": 3600, "limit": 2
                }))
                .unwrap(),
            )
            .unwrap();
        assert_eq!(pairs["pairs"].as_array().unwrap().len(), 2);
        let pair_cursor = pairs["pagination"]["next_cursor"].as_u64().unwrap();
        let pairs = database
            .forward_pairs(
                &PairQuery::parse(json!({
                    "start": 0, "end": 10_000, "interval": 3600, "limit": 2,
                    "cursor": pair_cursor
                }))
                .unwrap(),
            )
            .unwrap();
        assert_eq!(pairs["pairs"].as_array().unwrap().len(), 1);

        let events = database
            .events(
                &EventQuery::parse(json!({
                    "start": 0, "end": 10_000, "limit": 2
                }))
                .unwrap(),
            )
            .unwrap();
        let event_cursor = events["pagination"]["next_cursor"].as_u64().unwrap();
        let events = database
            .events(
                &EventQuery::parse(json!({
                    "start": 0, "end": 10_000, "limit": 2, "cursor": event_cursor
                }))
                .unwrap(),
            )
            .unwrap();
        assert_eq!(events["events"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn records_probe_routes_with_durable_channel_identity() {
        let (_directory, database) = database();
        database
            .record_channel_snapshot(900, &channels(600_000))
            .unwrap();
        database
            .record_probe_result(1_001, &probe_result(1_000, "destination_reached", 500_000))
            .unwrap();

        let page = database
            .probe_events(
                &ProbeQuery::parse(json!({
                    "start": 900, "end": 1_100, "channel": "1x2x3"
                }))
                .unwrap(),
            )
            .unwrap();
        let probe = &page["probes"][0];
        assert_eq!(page["api_version"], 2);
        assert_eq!(probe["status"], "destination_reached");
        assert_eq!(probe["destination"], "destination");
        assert_eq!(probe["route_key"], "1x2x3/0>2x2x2/0");
        assert_eq!(probe["route"][0]["channel_identity"], "channel");
        assert_eq!(
            probe["route"][0]["channel_details"]["current"]["peer_id"],
            "peer"
        );
        assert!(probe.get("payment_hash").is_none());
        assert_eq!(database.status().unwrap()["counts"]["probe_events"], 1);
        assert_eq!(
            database.status().unwrap()["counts"]["successful_probe_events"],
            1
        );
    }

    #[test]
    fn aggregates_probe_bounds_and_failure_reasons() {
        let (_directory, database) = database();
        database
            .record_probe_result(1_001, &probe_result(1_000, "destination_reached", 500_000))
            .unwrap();
        database
            .record_probe_result(1_101, &probe_result(1_100, "route_failed", 750_000))
            .unwrap();

        let page = database
            .probe_events(
                &ProbeQuery::parse(json!({
                    "start": 900, "end": 1_200, "interval": 600
                }))
                .unwrap(),
            )
            .unwrap();
        let aggregate = &page["probes"][0];
        assert_eq!(aggregate["attempts"], 2);
        assert_eq!(aggregate["destination_reached"], 1);
        assert_eq!(aggregate["route_failed"], 1);
        assert_eq!(aggregate["max_success_msat"], 500_000);
        assert_eq!(aggregate["smallest_failed_msat"], 750_000);
        assert_eq!(aggregate["failures"]["WIRE_TEMPORARY_CHANNEL_FAILURE"], 1);
    }

    #[test]
    fn paginates_raw_probe_results_without_overlap() {
        let (_directory, database) = database();
        for timestamp in [1_000, 1_100, 1_200] {
            database
                .record_probe_result(
                    timestamp,
                    &probe_result(timestamp, "destination_reached", timestamp * 10),
                )
                .unwrap();
        }
        let first = database
            .probe_events(&ProbeQuery::parse(json!({"limit": 2})).unwrap())
            .unwrap();
        assert_eq!(first["probes"].as_array().unwrap().len(), 2);
        let cursor = first["pagination"]["next_cursor"].as_u64().unwrap();
        let second = database
            .probe_events(&ProbeQuery::parse(json!({"limit": 2, "cursor": cursor})).unwrap())
            .unwrap();
        assert_eq!(second["probes"].as_array().unwrap().len(), 1);
        assert!(second["probes"][0]["cursor"].as_u64().unwrap() > cursor);
    }

    #[test]
    fn metrics_report_storage_rows_and_collection_health() {
        let (_directory, database) = database();
        database
            .record_channel_snapshot(1_000, &channels(600_000))
            .unwrap();
        database
            .record_collection_failure(1_300, &anyhow::anyhow!("test failure"))
            .unwrap();
        let metrics = database.metrics().unwrap();
        assert_eq!(metrics["version"], 1);
        assert_eq!(metrics["namespace"], "history");
        let families = metrics["families"].as_array().unwrap();
        let values = families
            .iter()
            .map(|family| {
                (
                    family["name"].as_str().unwrap(),
                    family["samples"][0]["value"].clone(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        assert!(values["database_bytes"].as_u64().unwrap() > 0);
        assert_eq!(values["channels"], 1);
        assert_eq!(values["channel_samples"], 1);
        assert_eq!(values["probe_events"], 0);
        assert_eq!(values["retained_collection_failures"], 1);
        assert_eq!(values["collection_success"], 0);
        assert_eq!(
            values["last_successful_collection_timestamp_seconds"],
            1_000
        );
    }
}
