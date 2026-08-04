//! Redis backend, on sorted sets.
//!
//! Sorted sets rather than lists or streams: the score carries both priority
//! and due time, so delays and priorities cost nothing extra, and a claim can
//! take a batch. Lists move one job per round trip; streams are strictly FIFO.
//!
//! This backend can lose jobs it has accepted. Redis acknowledges a write
//! before it reaches disk unless `appendfsync always` is set, and that setting
//! costs roughly 26× the throughput. [`Backend::capabilities`] reports what the
//! running server is actually configured for rather than assuming.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gylo_core::{Capabilities, Job};
use redis::{AsyncCommands, Script};

/// Key names, built under a namespace so two deployments can share a server
/// without one flushing the other's jobs.
struct Keys {
    namespace: String,
    leased: String,
    payload: String,
    attempt: String,
}

impl Keys {
    fn new(namespace: &str) -> Self {
        Self {
            namespace: namespace.to_owned(),
            leased: format!("{namespace}:leased"),
            payload: format!("{namespace}:payload"),
            attempt: format!("{namespace}:attempt"),
        }
    }

    fn ready(&self, level: i16) -> String {
        format!("{}:ready:{level}", self.namespace)
    }
}

/// Priority levels, each its own sorted set.
///
/// One set cannot do this job. Folding priority and due time into a single
/// score makes priority dominate, so a range query for "due by now" also
/// matches every delayed job at a lower priority. Splitting by level lets each
/// set be scored purely by due time, which is what the range query needs,
/// while the level itself carries the ordering.
const LEVELS: i16 = 10;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error("job payload for {0} is missing or malformed")]
    Payload(i64),
}

/// A job as stored, since Redis has no columns to spread it across.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Stored {
    id: i64,
    task: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    max_attempts: i16,
}

pub struct Backend {
    conn: redis::aio::MultiplexedConnection,
    keys: Keys,
    durable: bool,
}

/// Claims due jobs and writes their leases in one round trip.
///
/// Redis runs this to completion without interleaving, which is what makes the
/// claim exactly-once — the property `SKIP LOCKED` gives on Postgres, reached
/// by a different route.
const CLAIM: &str = r"
    local claimed = {}
    local remaining = tonumber(ARGV[2])
    for level = 0, tonumber(ARGV[5]) - 1 do
        if remaining <= 0 then break end
        local ready = ARGV[4] .. ':ready:' .. level
        local due = redis.call('ZRANGEBYSCORE', ready, '-inf', ARGV[1], 'LIMIT', 0, remaining)
        if #due > 0 then
            redis.call('ZREM', ready, unpack(due))
            for _, id in ipairs(due) do
                redis.call('ZADD', KEYS[1], ARGV[3], id)
                redis.call('HINCRBY', KEYS[2], id, 1)
                table.insert(claimed, id)
            end
            remaining = remaining - #due
        end
    end
    return claimed
";

/// Returns expired leases to the ready set, so a dead worker's jobs run again.
const RECLAIM: &str = r"
    local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
    if #expired == 0 then return 0 end
    redis.call('ZREM', KEYS[1], unpack(expired))
    for _, id in ipairs(expired) do redis.call('ZADD', KEYS[2], ARGV[1], id) end
    return #expired
";

fn now_millis() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

impl Backend {
    pub async fn connect(url: &str) -> Result<Self, Error> {
        Self::connect_namespaced(url, "gylo").await
    }

    /// Opens a backend whose keys live under `namespace`.
    pub async fn connect_namespaced(url: &str, namespace: &str) -> Result<Self, Error> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let durable = fsync_every_write(&mut conn).await;
        Ok(Self {
            conn,
            keys: Keys::new(namespace),
            durable,
        })
    }

    /// What this backend supports, given how the server is actually running.
    ///
    /// `durable_acknowledgement` is read from the live configuration, because
    /// the difference between keeping accepted jobs and losing a second of
    /// them is a deployment setting rather than a property of the code.
    pub fn capabilities(&self) -> Capabilities {
        Capabilities {
            backend: "redis",
            durable_acknowledgement: self.durable,
            transactional_enqueue: false,
            priorities: true,
            delayed_jobs: true,
            unique_jobs: false,
            keyed_concurrency: false,
            workflows: false,
            durable_steps: false,
            cron: true,
            results: false,
        }
    }

    fn level(priority: i16) -> i16 {
        priority.clamp(0, LEVELS - 1)
    }

    pub async fn enqueue(
        &mut self,
        id: i64,
        task: &str,
        payload: Vec<u8>,
        priority: i16,
        max_attempts: i16,
        delay: Duration,
    ) -> Result<i64, Error> {
        let stored = Stored {
            id,
            task: task.to_owned(),
            payload,
            max_attempts,
        };
        let encoded = rmp_serde::to_vec(&stored).map_err(|_| Error::Payload(id))?;
        let due = now_millis() + delay.as_millis() as f64;

        redis::pipe()
            .atomic()
            .hset(&self.keys.payload, id, encoded)
            .ignore()
            .zadd(self.keys.ready(Self::level(priority)), id, due)
            .ignore()
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(id)
    }

    pub async fn fetch(&mut self, limit: usize, lease: Duration) -> Result<Vec<Job>, Error> {
        let expires = now_millis() + lease.as_millis() as f64;
        let claimed: Vec<i64> = Script::new(CLAIM)
            .key(&self.keys.leased)
            .key(&self.keys.attempt)
            .arg(now_millis())
            .arg(limit)
            .arg(expires)
            .arg(&self.keys.namespace)
            .arg(LEVELS)
            .invoke_async(&mut self.conn)
            .await?;
        if claimed.is_empty() {
            return Ok(Vec::new());
        }

        let mut payloads = redis::cmd("HMGET");
        payloads.arg(&self.keys.payload);
        let mut counters = redis::cmd("HMGET");
        counters.arg(&self.keys.attempt);
        for id in &claimed {
            payloads.arg(id);
            counters.arg(id);
        }
        let raw: Vec<Option<Vec<u8>>> = payloads.query_async(&mut self.conn).await?;
        let attempts: Vec<Option<i16>> = counters.query_async(&mut self.conn).await?;

        claimed
            .iter()
            .zip(raw)
            .zip(attempts)
            .map(|((id, bytes), attempt)| {
                let bytes = bytes.ok_or(Error::Payload(*id))?;
                let stored: Stored =
                    rmp_serde::from_slice(&bytes).map_err(|_| Error::Payload(*id))?;
                Ok(Job {
                    id: stored.id,
                    task: stored.task,
                    payload: stored.payload,
                    attempt: attempt.unwrap_or(1),
                    max_attempts: stored.max_attempts,
                    durable: false,
                })
            })
            .collect()
    }

    pub async fn complete(&mut self, ids: &[i64]) -> Result<u64, Error> {
        if ids.is_empty() {
            return Ok(0);
        }
        let (removed,): (u64,) = redis::pipe()
            .atomic()
            .zrem(&self.keys.leased, ids)
            .hdel(&self.keys.payload, ids)
            .ignore()
            .hdel(&self.keys.attempt, ids)
            .ignore()
            .query_async(&mut self.conn)
            .await?;
        Ok(removed)
    }

    pub async fn retry(&mut self, id: i64, priority: i16, delay: Duration) -> Result<(), Error> {
        let due = now_millis() + delay.as_millis() as f64;
        redis::pipe()
            .atomic()
            .zrem(&self.keys.leased, id)
            .ignore()
            .zadd(self.keys.ready(Self::level(priority)), id, due)
            .ignore()
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn reclaim_expired(&mut self, limit: usize) -> Result<u64, Error> {
        Ok(Script::new(RECLAIM)
            .key(&self.keys.leased)
            .key(self.keys.ready(0))
            .arg(now_millis())
            .arg(limit)
            .invoke_async(&mut self.conn)
            .await?)
    }

    pub async fn depth(&mut self) -> Result<u64, Error> {
        let mut total = 0;
        for level in 0..LEVELS {
            total += self.conn.zcard::<_, u64>(self.keys.ready(level)).await?;
        }
        Ok(total)
    }

    /// Removes this namespace's keys, leaving anything else on the server
    /// alone. `FLUSHDB` would take a co-tenant's jobs with it.
    pub async fn clear(&mut self) -> Result<(), Error> {
        let mut command = redis::cmd("DEL");
        for level in 0..LEVELS {
            command.arg(self.keys.ready(level));
        }
        command
            .arg(&self.keys.leased)
            .arg(&self.keys.payload)
            .arg(&self.keys.attempt)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }
}

/// Whether the server fsyncs every write.
///
/// Asks the running server rather than trusting a setting the operator
/// believes they applied. A managed Redis that quietly ignores it should be
/// reported as lossy, because it is.
async fn fsync_every_write(conn: &mut redis::aio::MultiplexedConnection) -> bool {
    let appendonly: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("appendonly")
        .query_async(conn)
        .await
        .unwrap_or_default();
    let fsync: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("appendfsync")
        .query_async(conn)
        .await
        .unwrap_or_default();

    appendonly.get(1).is_some_and(|value| value == "yes")
        && fsync.get(1).is_some_and(|value| value == "always")
}
