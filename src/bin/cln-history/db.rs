use crate::model::{ChannelSample, EventQuery, PairQuery, RangeQuery, amount_msat};
use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::{collections::HashMap, fs, path::PathBuf};

const SCHEMA_VERSION: u64 = 1;
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
        let connection = Connection::open(&self.path)
            .with_context(|| format!("opening {}", self.path.display()))?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_meta (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', '1');
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
             );",
        )?;
        Ok(connection)
    }

    pub fn record_channel_snapshot(&self, ts: u64, response: &Value) -> Result<()> {
        let channels = response
            .get("channels")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("listpeerchannels response has no channels array"))?;
        let samples = channels
            .iter()
            .enumerate()
            .map(|(index, value)| ChannelSample::from_value(value, index))
            .collect::<Vec<_>>();
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for sample in &samples {
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
        let channel_key = channel_id
            .clone()
            .or_else(|| short_channel_id.clone())
            .or_else(|| peer_id.clone());
        self.connection()?.execute(
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
        Ok(())
    }

    pub fn record_forward_event(&self, ts: u64, value: &Value) -> Result<()> {
        let Some(in_channel) = text(value, &["in_channel"]) else {
            return Ok(());
        };
        let Some(out_channel) = text(value, &["out_channel"]) else {
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
        self.connection()?.execute(
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
            "schema_version": SCHEMA_VERSION,
            "database": self.path.display().to_string(),
            "database_bytes": fs::metadata(&self.path).map(|metadata| metadata.len()).unwrap_or(0),
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
            }
        }))
    }

    pub fn channel_samples(&self, query: &RangeQuery) -> Result<Value> {
        let (start, end) = query.bounds();
        let connection = self.connection()?;
        let rows = load_samples(&connection, query, false)?;
        Ok(json!({
            "start": start, "end": end, "change_only": true,
            "samples": rows,
        }))
    }

    pub fn htlc_samples(&self, query: &RangeQuery) -> Result<Value> {
        let (start, end) = query.bounds();
        let connection = self.connection()?;
        let rows = load_samples(&connection, query, true)?;
        Ok(json!({"start": start, "end": end, "samples": rows}))
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
        let values = rows
            .into_iter()
            .filter(|(incoming, outgoing, ppm, _)| {
                matches_channel(query.in_channel.as_deref(), incoming)
                    && matches_channel(query.out_channel.as_deref(), outgoing)
                    && query.min_ppm.is_none_or(|minimum| *ppm >= minimum)
                    && query.max_ppm.is_none_or(|maximum| *ppm <= maximum)
            })
            .take(query.range.limit as usize)
            .map(|(_, _, _, value)| value)
            .collect::<Vec<_>>();
        Ok(json!({
            "start": start, "end": end, "interval": query.interval,
            "pairs": values,
        }))
    }

    pub fn events(&self, query: &EventQuery) -> Result<Value> {
        let (start, end) = query.range.bounds();
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT ts, event_type, channel_key, channel_id, short_channel_id,
                    peer_id, old_state, new_state, cause
             FROM channel_events WHERE ts BETWEEN ?1 AND ?2 ORDER BY ts, id",
        )?;
        let rows = statement
            .query_map(params![to_i64(start)?, to_i64(end)?], |row| {
                Ok(json!({
                    "timestamp": row.get::<_, i64>(0)?,
                    "event_type": row.get::<_, String>(1)?,
                    "channel_key": row.get::<_, Option<String>>(2)?,
                    "channel_id": row.get::<_, Option<String>>(3)?,
                    "short_channel_id": row.get::<_, Option<String>>(4)?,
                    "peer_id": row.get::<_, Option<String>>(5)?,
                    "old_state": row.get::<_, Option<String>>(6)?,
                    "new_state": row.get::<_, Option<String>>(7)?,
                    "cause": row.get::<_, Option<String>>(8)?,
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let values = rows
            .into_iter()
            .filter(|value| {
                query
                    .event_type
                    .as_deref()
                    .is_none_or(|kind| value["event_type"] == kind)
                    && query.range.channel.as_deref().is_none_or(|channel| {
                        ["channel_key", "channel_id", "short_channel_id", "peer_id"]
                            .iter()
                            .any(|field| value[*field] == channel)
                    })
            })
            .take(query.range.limit as usize)
            .collect::<Vec<_>>();
        Ok(json!({"start": start, "end": end, "events": values}))
    }
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

fn load_samples(
    connection: &Connection,
    query: &RangeQuery,
    htlcs_only: bool,
) -> Result<Vec<Value>> {
    let (start, end) = query.bounds();
    let mut samples = Vec::new();
    let mut baselines = HashMap::<String, (u64, ChannelSample)>::new();
    let mut statement = connection.prepare(
        "SELECT ts, channel_key, channel_id, short_channel_id, peer_id, state,
                connected, reestablished, capacity_msat, to_us_msat,
                spendable_msat, receivable_msat, htlc_in_count, htlc_out_count,
                htlc_in_msat, htlc_out_msat
         FROM channel_samples WHERE ts <= ?1 ORDER BY ts, id",
    )?;
    for row in statement.query_map([to_i64(end)?], |row| {
        Ok((from_i64(row.get(0)?), sample_from_row(row, 1)?))
    })? {
        let (timestamp, sample) = row?;
        if !sample_matches(query.channel.as_deref(), &sample) {
            continue;
        }
        if timestamp < start {
            baselines.insert(sample.channel_key.clone(), (timestamp, sample));
        } else {
            samples.push((timestamp, sample));
        }
    }
    samples.extend(baselines.into_values());
    samples.sort_by_key(|(timestamp, sample)| (sample.channel_key.clone(), *timestamp));
    Ok(samples
        .into_iter()
        .filter(|(_, sample)| !htlcs_only || sample.htlc_in_count > 0 || sample.htlc_out_count > 0)
        .take(query.limit as usize)
        .map(|(timestamp, sample)| sample_json(timestamp, &sample, htlcs_only))
        .collect())
}

fn sample_json(timestamp: u64, sample: &ChannelSample, htlcs_only: bool) -> Value {
    let mut value = json!({
        "timestamp": timestamp,
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
    use super::HistoryDb;
    use crate::model::{PairQuery, RangeQuery};
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
            .channel_samples(&RangeQuery::parse(json!({})).unwrap())
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
}
