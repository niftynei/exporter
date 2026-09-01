use crate::model::{ChannelSample, EventQuery, PairQuery, SampleQuery, amount_msat};
use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
};

const SCHEMA_VERSION: u64 = 2;
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
                 htlc_out_msat INTEGER NOT NULL
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
                 cause TEXT
             );
             CREATE INDEX IF NOT EXISTS channel_events_ts
                 ON channel_events(ts);
             CREATE TABLE IF NOT EXISTS forward_pair_buckets (
                 bucket_start INTEGER NOT NULL,
                 in_channel TEXT NOT NULL,
                 out_channel TEXT NOT NULL,
                 status TEXT NOT NULL,
                 forward_count INTEGER NOT NULL,
                 in_msat INTEGER NOT NULL,
                 out_msat INTEGER NOT NULL,
                 fee_msat INTEGER NOT NULL,
                 PRIMARY KEY(bucket_start, in_channel, out_channel, status)
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
                 ON channel_aliases(channel_key);",
        )?;
        migrate(&mut connection)?;
        Ok(connection)
    }

    pub fn record_channel_snapshot(&self, ts: u64, response: &Value) -> Result<()> {
        let channels = response
            .get("channels")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("listpeerchannels response has no channels array"))?;
        let mut samples = channels
            .iter()
            .enumerate()
            .map(|(index, value)| ChannelSample::from_value(value, index))
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
        let Some(out_alias) = text(value, &["out_channel"]) else {
            return Ok(());
        };
        let status = text(value, &["status"]).unwrap_or_else(|| "unknown".to_owned());
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
                 bucket_start, in_channel, out_channel, status,
                 forward_count, in_msat, out_msat, fee_msat
             ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7)
             ON CONFLICT(bucket_start, in_channel, out_channel, status) DO UPDATE SET
                 forward_count = forward_count + 1,
                 in_msat = in_msat + excluded.in_msat,
                 out_msat = out_msat + excluded.out_msat,
                 fee_msat = fee_msat + excluded.fee_msat",
            params![
                to_i64(bucket)?,
                in_channel,
                out_channel,
                status,
                to_i64(in_msat)?,
                to_i64(out_msat)?,
                to_i64(fee_msat)?,
            ],
        )?;
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
                "forward_pair_buckets": count(&connection, "forward_pair_buckets")?,
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
                gauge("forward_pair_buckets", "Number of retained forwarding channel-pair buckets.", count(&connection, "forward_pair_buckets")?),
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
                    in_channel, out_channel, status,
                    SUM(forward_count), SUM(in_msat), SUM(out_msat), SUM(fee_msat)
             FROM forward_pair_buckets
             WHERE bucket_start BETWEEN ?2 AND ?3
             GROUP BY period, in_channel, out_channel, status
             ORDER BY period, in_channel, out_channel, status",
        )?;
        let rows = statement
            .query_map(
                params![to_i64(query.interval)?, to_i64(start)?, to_i64(end)?],
                |row| {
                    let out_msat = row.get::<_, i64>(6)?;
                    let fee_msat = row.get::<_, i64>(7)?;
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
                            "forward_count": row.get::<_, i64>(4)?,
                            "in_msat": row.get::<_, i64>(5)?,
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
                    peer_id, old_state, new_state, cause
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
        None | Some(1) => {}
        Some(version) if version > SCHEMA_VERSION => {
            bail!("database schema {version} is newer than this cln-history binary")
        }
        Some(version) => bail!("unsupported cln-history database schema {version}"),
    }
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
    transaction.execute(
        "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', ?1)",
        [SCHEMA_VERSION.to_string()],
    )?;
    transaction.commit()?;
    Ok(())
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
             htlc_in_msat, htlc_out_msat
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
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
                    htlc_in_msat, htlc_out_msat
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
                htlc_in_msat, htlc_out_msat
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

#[cfg(test)]
mod tests {
    use super::{HistoryDb, resolve_alias};
    use crate::model::{EventQuery, PairQuery, SampleQuery};
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
        assert_eq!(database.status().unwrap()["schema_version"], 2);
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
        assert_eq!(values["retained_collection_failures"], 1);
        assert_eq!(values["collection_success"], 0);
        assert_eq!(
            values["last_successful_collection_timestamp_seconds"],
            1_000
        );
    }
}
