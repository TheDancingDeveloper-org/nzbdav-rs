//! `UsenetArticleProvider` — wraps nzb-nntp connection pools for on-demand
//! article fetching with multi-server failover and yEnc decoding.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use nzb_decode::decode_yenc;
use nzb_nntp::{ConnectionPool, NntpError};
use parking_lot::Mutex;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::error::{Result, StreamError};
use crate::holes::{
    Consult, HOLE_CONFIRM_DELAY, HOLE_CONFIRM_FETCH, HoleLedger, InMemoryHoleLedger, Noted, Outcome,
};
use crate::prioritized_semaphore::{PrioritizedPermit, PrioritizedSemaphore};

/// Fraction of connection capacity reserved for high-priority (WebDAV
/// streaming) work. Ingest pipelines acquire low-priority permits, so they
/// cannot consume the reserved slots — leaving playback headroom.
const HIGH_PRIORITY_RESERVE_FRACTION: f64 = 0.25;

/// Decoded yEnc part header information, used for seek interpolation.
pub struct YencHeaders {
    /// Byte offset where this part begins in the reassembled file.
    pub part_begin: u64,
    /// Byte offset where this part ends in the reassembled file.
    pub part_end: u64,
    /// Total size of the reassembled file.
    pub total_size: u64,
}

/// Backend trait used by [`UsenetArticleProvider`]. Production code uses the
/// pool-backed impl; tests use a fake. The trait is public so integration
/// tests in other crates can provide their own canned responses.
// `#[async_trait]` puts `#[must_use]` on the boxed futures it returns, which
// are already `#[must_use]`; clippy flags the macro's output, not our code.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait ArticleBackend: Send + Sync + 'static {
    async fn fetch_decoded(&self, message_id: &str) -> Result<Vec<u8>>;
    async fn stat(&self, message_id: &str) -> Result<bool>;
    async fn yenc_headers(&self, message_id: &str) -> Result<YencHeaders>;
    /// Total connection capacity across servers. Tests return whatever fits.
    fn total_connections(&self) -> usize;
    /// Number of pools the fake stands in for.
    fn pool_count(&self) -> usize {
        1
    }
    /// Fetch from one pool. `ArticleNotFound` stands for a 430; any other
    /// error is a transport failure.
    async fn fetch_decoded_on_pool(&self, _pool: usize, message_id: &str) -> Result<Vec<u8>> {
        self.fetch_decoded(message_id).await
    }
    /// STAT on one pool. `Ok(false)` or `ArticleNotFound` stands for a 430;
    /// any other error is a transport failure.
    async fn stat_on_pool(&self, _pool: usize, message_id: &str) -> Result<bool> {
        self.stat(message_id).await
    }
}

/// Tunables of the confirming re-ask after a provisional refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HoleConfig {
    /// Wait before re-asking (zurg `holeConfirmDelay`).
    pub confirm_delay: Duration,
    /// Deadline for the re-ask (zurg `holeConfirmFetch`). Missing it is a
    /// transport failure, not a confirmation.
    pub confirm_fetch: Duration,
}

impl Default for HoleConfig {
    fn default() -> Self {
        Self {
            confirm_delay: HOLE_CONFIRM_DELAY,
            confirm_fetch: HOLE_CONFIRM_FETCH,
        }
    }
}

/// Fetches and decodes Usenet articles with multi-server failover.
///
/// Pools are tried in priority order. On `ArticleNotFound`, the connection is
/// released cleanly and the next pool is attempted. On any other NNTP error
/// the connection is discarded (it may be in a bad state) and the next pool
/// is attempted.
///
/// Fetches follow the hole model (see [`crate::holes`]): the ledger is
/// consulted first, a confirmed hole is not fetched, and an article every
/// pool refuses is re-asked once after [`HoleConfig::confirm_delay`] before
/// the hole is confirmed. A pool that fails to answer makes the fetch a
/// transport error, never a miss.
///
/// The pool list can be atomically swapped at runtime via [`replace_pools`]
/// when server configuration changes.
pub struct UsenetArticleProvider {
    pools: ArcSwap<Vec<Arc<ConnectionPool>>>,
    fake: Option<Arc<dyn ArticleBackend>>,
    priority_semaphore: OnceLock<Arc<PrioritizedSemaphore>>,
    ledger: Arc<dyn HoleLedger>,
    hole_config: HoleConfig,
    /// Confirms in flight, by message-id; readers of a provisional article
    /// wait on these.
    confirms: Mutex<HashMap<String, watch::Receiver<Option<Attempt>>>>,
}

impl UsenetArticleProvider {
    /// Create a new provider with pools listed in priority order.
    pub fn new(pools: Vec<Arc<ConnectionPool>>) -> Self {
        Self {
            pools: ArcSwap::from_pointee(pools),
            fake: None,
            priority_semaphore: OnceLock::new(),
            ledger: Arc::new(InMemoryHoleLedger::default()),
            hole_config: HoleConfig::default(),
            confirms: Mutex::new(HashMap::new()),
        }
    }

    /// Use a shared damage ledger instead of the provider's own in-memory
    /// one, so the census and the repair job see the same holes.
    pub fn with_ledger(mut self, ledger: Arc<dyn HoleLedger>) -> Self {
        self.ledger = ledger;
        self
    }

    /// Override the confirming re-ask tunables.
    pub fn with_hole_config(mut self, config: HoleConfig) -> Self {
        self.hole_config = config;
        self
    }

    /// Create a provider backed by a test fake. Production code must not use
    /// this — the constructor exists so integration tests can inject canned
    /// responses without standing up an NNTP server.
    pub fn with_fake(backend: Arc<dyn ArticleBackend>) -> Self {
        Self {
            fake: Some(backend),
            ..Self::new(vec![])
        }
    }

    /// Shared priority semaphore sized to the current connection capacity.
    /// Ingest pipelines should `acquire_low` on it so they never starve out
    /// WebDAV playback, which uses `acquire_high`. Lazily constructed on
    /// first access.
    pub fn priority_semaphore(&self) -> Arc<PrioritizedSemaphore> {
        Arc::clone(self.priority_semaphore.get_or_init(|| {
            let total = self.total_connections().max(1);
            let reserved = ((total as f64) * HIGH_PRIORITY_RESERVE_FRACTION).ceil() as usize;
            // Always leave at least one low-priority slot so ingest can make
            // progress even on tiny pools.
            let reserved = reserved.min(total.saturating_sub(1));
            Arc::new(PrioritizedSemaphore::new(total, reserved))
        }))
    }

    /// Atomically replace the connection pools (e.g. after server config change).
    pub fn replace_pools(&self, pools: Vec<Arc<ConnectionPool>>) {
        self.pools.store(Arc::new(pools));
    }

    fn load_pools(&self) -> arc_swap::Guard<Arc<Vec<Arc<ConnectionPool>>>> {
        self.pools.load()
    }

    /// Total number of connections across all pools.
    /// Useful for limiting concurrency to what the pools can actually serve.
    pub fn total_connections(&self) -> usize {
        if let Some(fake) = &self.fake {
            return fake.total_connections();
        }
        let pools = self.load_pools();
        pools.iter().map(|p| p.config().connections as usize).sum()
    }

    /// Fetch a single article by message-id, decode yEnc, and return the
    /// decoded payload. Tries each server pool in order with failover.
    pub async fn fetch_decoded(&self, message_id: &str) -> Result<Vec<u8>> {
        self.fetch_decoded_high(message_id).await
    }

    /// High-priority article fetch for user-facing WebDAV playback.
    pub async fn fetch_decoded_high(&self, message_id: &str) -> Result<Vec<u8>> {
        self.fetch_decoded_with_priority(message_id, FetchPriority::High)
            .await
    }

    /// Low-priority article fetch for background ingest and metadata probing.
    pub async fn fetch_decoded_low(&self, message_id: &str) -> Result<Vec<u8>> {
        self.fetch_decoded_with_priority(message_id, FetchPriority::Low)
            .await
    }

    async fn fetch_decoded_with_priority(
        &self,
        message_id: &str,
        priority: FetchPriority,
    ) -> Result<Vec<u8>> {
        let servers = self.servers();
        loop {
            match self.ledger.consult(message_id, &servers) {
                Consult::Skip => {
                    debug!(message_id, "confirmed hole, not fetching");
                    return Err(StreamError::ArticleNotFound(message_id.to_string()));
                }
                Consult::Provisional => {
                    if let Some(answer) = self.join_confirm(message_id).await {
                        return answer.into_result(message_id);
                    }
                }
                Consult::Fetch | Consult::Reprobe => {}
            }

            let (attempt, noted) = {
                let _permit = self.permit(priority).await;
                self.fetch_once(message_id, &servers).await
            };
            if !(matches!(attempt, Attempt::Missing) && noted.confirm_needed) {
                return attempt.into_result(message_id);
            }
            match self.confirm(message_id, priority, &servers).await {
                Some(answer) => return answer.into_result(message_id),
                // Another caller's confirm was abandoned; start over.
                None => continue,
            }
        }
    }

    /// Priority permit for one round of attempts. A fake backend takes none.
    async fn permit(&self, priority: FetchPriority) -> Option<PrioritizedPermit> {
        if self.fake.is_some() {
            return None;
        }
        let sem = self.priority_semaphore();
        Some(match priority {
            FetchPriority::High => sem.acquire_high().await,
            FetchPriority::Low => sem.acquire_low().await,
        })
    }

    /// Pool names in priority order; they fingerprint ledger records.
    fn servers(&self) -> Vec<String> {
        (0..self.pool_count()).map(|i| self.pool_name(i)).collect()
    }

    /// Ask every pool in priority order, stopping at the first body, and note
    /// each answer in the ledger. The article is `Missing` only when every
    /// pool answered 430; any pool that failed to answer makes it `Transport`.
    async fn fetch_once(&self, message_id: &str, servers: &[String]) -> (Attempt, Noted) {
        let mut refused = Vec::new();
        let mut failed = Vec::new();
        let mut last_err: Option<StreamError> = None;

        for (i, server) in servers.iter().enumerate() {
            match self.attempt_pool(i, message_id).await {
                PoolAttempt::Body(data) => {
                    debug!(pool = i, message_id, bytes = data.len(), "decoded article");
                    let noted = self.ledger.note(message_id, &Outcome::Present, servers);
                    return (Attempt::Body(data), noted);
                }
                PoolAttempt::Refused => {
                    debug!(pool = i, message_id, "article not found, trying next");
                    refused.push(server.clone());
                }
                PoolAttempt::Failed(e) => {
                    warn!(pool = i, message_id, error = %e, "pool failed to answer");
                    failed.push(server.clone());
                    last_err = Some(e);
                }
            }
        }

        let mut noted = Noted {
            state: None,
            confirm_needed: false,
        };
        if failed.is_empty() {
            for server in refused {
                noted = self
                    .ledger
                    .note(message_id, &Outcome::Refused { server }, servers);
            }
        } else {
            // A round some pool failed to answer is no evidence of a miss,
            // so its refusals are not recorded either.
            for server in failed {
                noted = self
                    .ledger
                    .note(message_id, &Outcome::Transport { server }, servers);
            }
        }

        let attempt = match last_err {
            Some(e) => Attempt::Transport(e),
            None if servers.is_empty() => {
                Attempt::Transport(StreamError::AllServersExhausted(message_id.to_string()))
            }
            None => Attempt::Missing,
        };
        (attempt, noted)
    }

    /// One attempt on one pool. A 430 releases the connection cleanly; any
    /// other failure discards it, since it may be in a bad state.
    async fn attempt_pool(&self, pool: usize, message_id: &str) -> PoolAttempt {
        if let Some(fake) = &self.fake {
            return match fake.fetch_decoded_on_pool(pool, message_id).await {
                Ok(data) => PoolAttempt::Body(data),
                Err(StreamError::ArticleNotFound(_)) => PoolAttempt::Refused,
                Err(e) => PoolAttempt::Failed(e),
            };
        }

        let Some(pool_ref) = self.load_pools().get(pool).cloned() else {
            return PoolAttempt::Failed(StreamError::NntpError(format!(
                "pool {pool} is no longer configured"
            )));
        };
        let mut pooled = match pool_ref.acquire().await {
            Ok(p) => p,
            Err(e) => return PoolAttempt::Failed(StreamError::NntpError(e.to_string())),
        };
        match pooled.conn.fetch_article(message_id).await {
            Ok(resp) => {
                pool_ref.release(pooled);
                let Some(raw) = resp.data else {
                    return PoolAttempt::Failed(StreamError::NntpError(format!(
                        "empty response for article {message_id}"
                    )));
                };
                match decode_yenc(&raw) {
                    Ok(decoded) => PoolAttempt::Body(decoded.data),
                    Err(e) => PoolAttempt::Failed(StreamError::YencDecode(e.to_string())),
                }
            }
            Err(NntpError::ArticleNotFound(_)) => {
                pool_ref.release(pooled);
                PoolAttempt::Refused
            }
            Err(e) => {
                pool_ref.discard(pooled);
                PoolAttempt::Failed(StreamError::NntpError(e.to_string()))
            }
        }
    }

    /// Wait for the confirm in flight for this article, if there is one.
    /// `None` when there is none or it was abandoned.
    async fn join_confirm(&self, message_id: &str) -> Option<Attempt> {
        let mut rx = self.confirms.lock().get(message_id).cloned()?;
        debug!(
            message_id,
            "held provisionally, joining the confirm in flight"
        );
        rx.wait_for(Option::is_some).await.ok()?.clone()
    }

    /// Re-ask once after the confirm delay, bounded by the confirm fetch
    /// deadline; only a second refusal from every pool confirms the hole.
    /// Callers that arrive meanwhile join this confirm instead of asking
    /// again. `None` when another caller's confirm was joined and abandoned.
    async fn confirm(
        &self,
        message_id: &str,
        priority: FetchPriority,
        servers: &[String],
    ) -> Option<Attempt> {
        let registered = {
            let mut confirms = self.confirms.lock();
            match confirms.get(message_id) {
                Some(rx) => Err(rx.clone()),
                None => {
                    let (tx, rx) = watch::channel(None);
                    confirms.insert(message_id.to_string(), rx);
                    Ok(tx)
                }
            }
        };
        let tx = match registered {
            Ok(tx) => tx,
            // Another caller is already confirming this article.
            Err(mut rx) => return rx.wait_for(Option::is_some).await.ok()?.clone(),
        };
        let _registered = ConfirmGuard {
            confirms: &self.confirms,
            message_id,
        };

        debug!(message_id, delay = ?self.hole_config.confirm_delay, "provisional refusal, confirming");
        tokio::time::sleep(self.hole_config.confirm_delay).await;
        let reask = async {
            let _permit = self.permit(priority).await;
            self.fetch_once(message_id, servers).await.0
        };
        let attempt = match tokio::time::timeout(self.hole_config.confirm_fetch, reask).await {
            Ok(attempt) => attempt,
            Err(_) => {
                for server in servers {
                    let outcome = Outcome::Transport {
                        server: server.clone(),
                    };
                    self.ledger.note(message_id, &outcome, servers);
                }
                Attempt::Transport(StreamError::NntpError(format!(
                    "confirming fetch of {message_id} timed out after {:?}",
                    self.hole_config.confirm_fetch
                )))
            }
        };
        tx.send_replace(Some(attempt.clone()));
        Some(attempt)
    }

    /// Whether a reader of this article is blocked on a provisional refusal
    /// while its confirm is in flight. Such a reader is already being
    /// re-asked and must be excluded from any hedging or second-ask logic.
    pub fn held_provisionally(&self, message_id: &str) -> bool {
        self.confirms.lock().contains_key(message_id)
    }

    /// The damage ledger this provider consults and feeds.
    pub fn ledger(&self) -> Arc<dyn HoleLedger> {
        Arc::clone(&self.ledger)
    }

    /// Check article existence via the STAT command, asking each pool in
    /// priority order. `Ok(true)` if any pool has the article, `Ok(false)`
    /// only when every pool answered 430. A pool that failed to answer, with
    /// none having the article, is a transport error, never "not found".
    pub async fn stat(&self, message_id: &str) -> Result<bool> {
        let pools = self.pool_count();
        let mut transport: Option<StreamError> = None;
        for pool in 0..pools {
            match self.stat_pool(pool, message_id).await {
                StatAnswer::Present => return Ok(true),
                StatAnswer::Absent => {
                    debug!(pool, message_id, "article not found via STAT, trying next");
                }
                StatAnswer::Failed(e) => {
                    warn!(pool, message_id, error = %e, "pool failed to answer STAT");
                    transport = Some(e);
                }
            }
        }
        match transport {
            Some(e) => Err(e),
            None if pools == 0 => Err(StreamError::AllServersExhausted(message_id.to_string())),
            None => Ok(false),
        }
    }

    /// Number of server pools, in priority order. A fake-backed provider
    /// reports a single pool.
    pub fn pool_count(&self) -> usize {
        if let Some(fake) = &self.fake {
            return fake.pool_count();
        }
        self.load_pools().len()
    }

    /// Connection limit of one pool, or 0 if the index is out of range.
    pub fn pool_connections(&self, pool: usize) -> usize {
        if let Some(fake) = &self.fake {
            return if pool < fake.pool_count() {
                fake.total_connections()
            } else {
                0
            };
        }
        self.load_pools()
            .get(pool)
            .map_or(0, |p| p.config().connections as usize)
    }

    /// Display name of one pool.
    pub fn pool_name(&self, pool: usize) -> String {
        if self.fake.is_some() {
            return format!("fake-{pool}");
        }
        self.load_pools()
            .get(pool)
            .map_or_else(|| format!("pool-{pool}"), |p| p.config().name.clone())
    }

    /// STAT one article on one pool only, with no failover. Unlike [`stat`],
    /// a 430 and a transport failure stay distinct, so the damage census can
    /// tell a missing article from one no server answered for. Takes no
    /// priority permit; the caller holds one.
    ///
    /// [`stat`]: Self::stat
    pub async fn stat_on_pool(&self, pool: usize, message_id: &str) -> PoolStat {
        match self.stat_pool(pool, message_id).await {
            StatAnswer::Present => PoolStat::Present,
            StatAnswer::Absent => PoolStat::Absent,
            StatAnswer::Failed(StreamError::NntpError(e)) => PoolStat::Failed(e),
            StatAnswer::Failed(e) => PoolStat::Failed(e.to_string()),
        }
    }

    /// STAT one article on one pool. A 430 releases the connection cleanly;
    /// any other failure discards it.
    async fn stat_pool(&self, pool: usize, message_id: &str) -> StatAnswer {
        if let Some(fake) = &self.fake {
            if pool >= fake.pool_count() {
                return StatAnswer::Failed(StreamError::NntpError(format!("no pool {pool}")));
            }
            return match fake.stat_on_pool(pool, message_id).await {
                Ok(true) => StatAnswer::Present,
                Ok(false) | Err(StreamError::ArticleNotFound(_)) => StatAnswer::Absent,
                Err(e) => StatAnswer::Failed(e),
            };
        }

        let failed = |m: String| StatAnswer::Failed(StreamError::NntpError(m));
        let Some(pool_ref) = self.load_pools().get(pool).cloned() else {
            return failed(format!("pool {pool} is no longer configured"));
        };
        let mut pooled = match pool_ref.acquire().await {
            Ok(p) => p,
            Err(e) => return failed(format!("acquire: {e}")),
        };
        match pooled.conn.stat_article(message_id).await {
            Ok(resp) => {
                pool_ref.release(pooled);
                if resp.code == 223 {
                    StatAnswer::Present
                } else {
                    failed(format!("unexpected STAT response {}", resp.code))
                }
            }
            Err(NntpError::ArticleNotFound(_)) => {
                pool_ref.release(pooled);
                StatAnswer::Absent
            }
            Err(e) => {
                pool_ref.discard(pooled);
                failed(e.to_string())
            }
        }
    }

    /// Fetch an article and return its yEnc part headers (begin, end, total).
    /// Useful for seek interpolation to determine which segment contains a
    /// given byte offset.
    pub async fn yenc_headers(&self, message_id: &str) -> Result<YencHeaders> {
        self.yenc_headers_high(message_id).await
    }

    /// High-priority yEnc header fetch for user-facing range seeks.
    pub async fn yenc_headers_high(&self, message_id: &str) -> Result<YencHeaders> {
        if let Some(fake) = &self.fake {
            return fake.yenc_headers(message_id).await;
        }

        let sem = self.priority_semaphore();
        let _permit = sem.acquire_high().await;

        let pools = self.load_pools();
        let mut last_err: Option<StreamError> = None;

        for (i, pool) in pools.iter().enumerate() {
            let mut pooled = match pool.acquire().await {
                Ok(p) => p,
                Err(e) => {
                    warn!(pool = i, error = %e, "failed to acquire connection");
                    last_err = Some(StreamError::NntpError(e.to_string()));
                    continue;
                }
            };

            match pooled.conn.fetch_article(message_id).await {
                Ok(resp) => {
                    pool.release(pooled);
                    let raw = resp.data.ok_or_else(|| {
                        StreamError::NntpError(format!("empty response for article {message_id}"))
                    })?;
                    let decoded =
                        decode_yenc(&raw).map_err(|e| StreamError::YencDecode(e.to_string()))?;
                    return Ok(YencHeaders {
                        part_begin: decoded.part_begin.unwrap_or(0),
                        part_end: decoded.part_end.unwrap_or(0),
                        total_size: decoded.file_size.unwrap_or(0),
                    });
                }
                Err(NntpError::ArticleNotFound(_)) => {
                    debug!(pool = i, message_id, "article not found, trying next");
                    pool.release(pooled);
                    last_err = Some(StreamError::ArticleNotFound(message_id.to_string()));
                }
                Err(e) => {
                    warn!(pool = i, message_id, error = %e, "nntp error, discarding connection");
                    pool.discard(pooled);
                    last_err = Some(StreamError::NntpError(e.to_string()));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| StreamError::AllServersExhausted(message_id.to_string())))
    }
}

/// One pool's answer to a STAT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolStat {
    /// 223: the pool has the article.
    Present,
    /// 430: the pool says it has no such article.
    Absent,
    /// The pool did not answer: connect, auth, protocol error or timeout.
    /// Never evidence that the article is missing.
    Failed(String),
}

/// One pool's answer to a STAT, keeping the transport error.
enum StatAnswer {
    Present,
    /// 430.
    Absent,
    Failed(StreamError),
}

/// One pool's answer to a fetch.
enum PoolAttempt {
    Body(Vec<u8>),
    /// 430.
    Refused,
    /// Did not answer, or answered with something unusable.
    Failed(StreamError),
}

/// The settled result of asking every pool once.
#[derive(Debug)]
enum Attempt {
    Body(Vec<u8>),
    /// Every pool answered 430.
    Missing,
    /// Some pool failed to answer and none had the article.
    Transport(StreamError),
}

impl Clone for Attempt {
    fn clone(&self) -> Self {
        match self {
            Self::Body(data) => Self::Body(data.clone()),
            Self::Missing => Self::Missing,
            Self::Transport(e) => Self::Transport(match e {
                StreamError::YencDecode(m) => StreamError::YencDecode(m.clone()),
                StreamError::AllServersExhausted(m) => StreamError::AllServersExhausted(m.clone()),
                other => StreamError::NntpError(other.to_string()),
            }),
        }
    }
}

impl Attempt {
    fn into_result(self, message_id: &str) -> Result<Vec<u8>> {
        match self {
            Self::Body(data) => Ok(data),
            Self::Missing => Err(StreamError::ArticleNotFound(message_id.to_string())),
            Self::Transport(e) => Err(e),
        }
    }
}

/// Drops a confirm's registration however the confirming fetch ends, so
/// readers waiting on an abandoned confirm fetch for themselves.
struct ConfirmGuard<'a> {
    confirms: &'a Mutex<HashMap<String, watch::Receiver<Option<Attempt>>>>,
    message_id: &'a str,
}

impl Drop for ConfirmGuard<'_> {
    fn drop(&mut self) {
        self.confirms.lock().remove(self.message_id);
    }
}

#[derive(Debug, Clone, Copy)]
enum FetchPriority {
    High,
    Low,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holes::{HOLE_TTL, HoleState};

    #[tokio::test]
    async fn test_empty_provider_fetch_decoded() {
        let provider = UsenetArticleProvider::new(vec![]);
        let result = provider.fetch_decoded("test@example.com").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, StreamError::AllServersExhausted(_)),
            "expected AllServersExhausted, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_empty_provider_stat() {
        let provider = UsenetArticleProvider::new(vec![]);
        let result = provider.stat("test@example.com").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, StreamError::AllServersExhausted(_)),
            "expected AllServersExhausted, got: {err}"
        );
    }

    /// What the scripted fake answers on one pool.
    enum Reply {
        Body,
        Refused,
        Transport,
        Hang,
    }

    type Script = dyn Fn(usize, Duration) -> Reply + Send + Sync;

    /// Multi-pool fake: `script(pool, elapsed since construction)` decides
    /// each answer. Counts calls per pool.
    struct Scripted {
        pools: usize,
        start: tokio::time::Instant,
        latency: Duration,
        script: Box<Script>,
        calls: Mutex<Vec<usize>>,
    }

    impl Scripted {
        fn new(
            pools: usize,
            script: impl Fn(usize, Duration) -> Reply + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                pools,
                start: tokio::time::Instant::now(),
                latency: Duration::ZERO,
                script: Box::new(script),
                calls: Mutex::new(vec![0; pools]),
            })
        }

        fn with_latency(
            pools: usize,
            latency: Duration,
            script: impl Fn(usize, Duration) -> Reply + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                pools,
                start: tokio::time::Instant::now(),
                latency,
                script: Box::new(script),
                calls: Mutex::new(vec![0; pools]),
            })
        }

        fn calls(&self) -> usize {
            self.calls.lock().iter().sum()
        }

        fn calls_on(&self, pool: usize) -> usize {
            self.calls.lock()[pool]
        }
    }

    #[async_trait]
    impl ArticleBackend for Scripted {
        async fn fetch_decoded(&self, message_id: &str) -> Result<Vec<u8>> {
            self.fetch_decoded_on_pool(0, message_id).await
        }
        async fn stat(&self, _message_id: &str) -> Result<bool> {
            Ok(true)
        }
        async fn yenc_headers(&self, message_id: &str) -> Result<YencHeaders> {
            Err(StreamError::Other(format!("no headers for {message_id}")))
        }
        fn total_connections(&self) -> usize {
            self.pools
        }
        fn pool_count(&self) -> usize {
            self.pools
        }
        async fn fetch_decoded_on_pool(&self, pool: usize, message_id: &str) -> Result<Vec<u8>> {
            self.calls.lock()[pool] += 1;
            let reply = (self.script)(pool, self.start.elapsed());
            if !self.latency.is_zero() {
                tokio::time::sleep(self.latency).await;
            }
            match reply {
                Reply::Body => Ok(b"payload".to_vec()),
                Reply::Refused => Err(StreamError::ArticleNotFound(message_id.to_string())),
                Reply::Transport => Err(StreamError::NntpError("connection reset".into())),
                Reply::Hang => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Err(StreamError::NntpError("hung".into()))
                }
            }
        }
        async fn stat_on_pool(&self, pool: usize, message_id: &str) -> Result<bool> {
            match self.fetch_decoded_on_pool(pool, message_id).await {
                Ok(_) => Ok(true),
                Err(StreamError::ArticleNotFound(_)) => Ok(false),
                Err(e) => Err(e),
            }
        }
    }

    fn provider(backend: &Arc<Scripted>) -> Arc<UsenetArticleProvider> {
        Arc::new(UsenetArticleProvider::with_fake(
            Arc::clone(backend) as Arc<dyn ArticleBackend>
        ))
    }

    /// A provider refused twice 350 ms apart, then serving: the confirm waits
    /// a full second, gets the body, and no hole is recorded.
    #[tokio::test(start_paused = true)]
    async fn refused_then_served_within_350ms_is_no_hole() {
        let backend = Scripted::new(1, |_, t| {
            if t < Duration::from_millis(350) {
                Reply::Refused
            } else {
                Reply::Body
            }
        });
        let provider = provider(&backend);

        let started = tokio::time::Instant::now();
        let data = provider.fetch_decoded("a@x").await.unwrap();
        assert_eq!(data, b"payload");
        assert!(started.elapsed() >= HOLE_CONFIRM_DELAY);
        assert_eq!(backend.calls(), 2);
        assert_eq!(provider.ledger().state("a@x"), None);
        assert!(!provider.held_provisionally("a@x"));
    }

    /// Refused, and refused again a second later: a confirmed hole, which a
    /// later fetch skips without asking.
    #[tokio::test(start_paused = true)]
    async fn refused_twice_one_second_apart_is_a_hole() {
        let backend = Scripted::new(2, |_, _| Reply::Refused);
        let provider = provider(&backend);

        let err = provider.fetch_decoded("a@x").await.unwrap_err();
        assert!(matches!(err, StreamError::ArticleNotFound(_)), "{err}");
        assert_eq!(backend.calls_on(0), 2);
        assert_eq!(backend.calls_on(1), 2);
        assert_eq!(provider.ledger().state("a@x"), Some(HoleState::Confirmed));
        let hole = &provider.ledger().holes()[0];
        assert_eq!(
            hole.refused_by,
            vec!["fake-0".to_string(), "fake-1".to_string()]
        );

        let err = provider.fetch_decoded("a@x").await.unwrap_err();
        assert!(matches!(err, StreamError::ArticleNotFound(_)), "{err}");
        assert_eq!(backend.calls(), 4, "a confirmed hole is not fetched");
    }

    /// 430 on the first pool and a transport failure on the last: the error
    /// is a transport error, no hole is recorded and no confirm is run.
    #[tokio::test(start_paused = true)]
    async fn transport_failure_on_last_pool_is_no_hole() {
        let backend = Scripted::new(2, |pool, _| {
            if pool == 0 {
                Reply::Refused
            } else {
                Reply::Transport
            }
        });
        let provider = provider(&backend);

        let err = provider.fetch_decoded("a@x").await.unwrap_err();
        assert!(matches!(err, StreamError::NntpError(_)), "{err}");
        assert_eq!(backend.calls(), 2, "no confirming re-ask");
        assert_eq!(provider.ledger().state("a@x"), None);
        assert!(provider.ledger().holes().is_empty());
    }

    /// A confirmed hole past its TTL is re-probed by one caller while the
    /// others keep the fast answer; a body on the re-probe closes the hole.
    #[tokio::test(start_paused = true)]
    async fn expired_hole_is_reprobed() {
        let backend = Scripted::with_latency(1, Duration::from_millis(100), |_, t| {
            if t < Duration::from_secs(60) {
                Reply::Refused
            } else {
                Reply::Body
            }
        });
        let provider = provider(&backend);
        provider.fetch_decoded("a@x").await.unwrap_err();
        assert_eq!(provider.ledger().state("a@x"), Some(HoleState::Confirmed));
        assert_eq!(backend.calls(), 2);

        // Within the TTL: skipped.
        tokio::time::advance(HOLE_TTL - Duration::from_secs(1)).await;
        provider.fetch_decoded("a@x").await.unwrap_err();
        assert_eq!(backend.calls(), 2);

        tokio::time::advance(Duration::from_secs(1)).await;
        let reprobe = tokio::spawn({
            let provider = Arc::clone(&provider);
            async move { provider.fetch_decoded("a@x").await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let fast = provider.fetch_decoded("a@x").await.unwrap_err();
        assert!(matches!(fast, StreamError::ArticleNotFound(_)), "{fast}");

        assert_eq!(reprobe.await.unwrap().unwrap(), b"payload");
        assert_eq!(
            backend.calls(),
            3,
            "one re-probe, the other took the fast answer"
        );
        assert_eq!(provider.ledger().state("a@x"), None);
    }

    /// A reader arriving while a confirm is in flight joins it instead of
    /// asking again, and is reported held provisionally meanwhile.
    #[tokio::test(start_paused = true)]
    async fn second_reader_joins_the_confirm_in_flight() {
        let backend = Scripted::new(1, |_, t| {
            if t < Duration::from_millis(350) {
                Reply::Refused
            } else {
                Reply::Body
            }
        });
        let provider = provider(&backend);
        let first = tokio::spawn({
            let provider = Arc::clone(&provider);
            async move { provider.fetch_decoded("a@x").await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(provider.held_provisionally("a@x"));
        assert_eq!(provider.ledger().state("a@x"), Some(HoleState::Provisional));

        let second = provider.fetch_decoded("a@x").await.unwrap();
        assert_eq!(second, b"payload");
        assert_eq!(first.await.unwrap().unwrap(), b"payload");
        assert_eq!(backend.calls(), 2, "the second reader did not ask");
        assert!(!provider.held_provisionally("a@x"));
    }

    /// A confirming re-ask that misses its deadline is a transport failure:
    /// the hole stays provisional, never confirmed.
    #[tokio::test(start_paused = true)]
    async fn confirm_timeout_is_transport_not_a_hole() {
        let backend = Scripted::new(1, |_, t| {
            if t < Duration::from_millis(500) {
                Reply::Refused
            } else {
                Reply::Hang
            }
        });
        let provider = provider(&backend);

        let started = tokio::time::Instant::now();
        let err = provider.fetch_decoded("a@x").await.unwrap_err();
        assert!(matches!(err, StreamError::NntpError(_)), "{err}");
        assert!(
            started.elapsed() <= HOLE_CONFIRM_DELAY + HOLE_CONFIRM_FETCH + Duration::from_millis(1)
        );
        assert_eq!(provider.ledger().state("a@x"), Some(HoleState::Provisional));
    }

    /// `stat_on_pool` keeps a 430 and a transport failure apart.
    #[tokio::test]
    async fn stat_on_pool_keeps_430_and_transport_apart() {
        let backend = Scripted::new(3, |pool, _| match pool {
            0 => Reply::Body,
            1 => Reply::Refused,
            _ => Reply::Transport,
        });
        let provider = provider(&backend);
        assert_eq!(provider.stat_on_pool(0, "a@x").await, PoolStat::Present);
        assert_eq!(provider.stat_on_pool(1, "a@x").await, PoolStat::Absent);
        assert!(matches!(
            provider.stat_on_pool(2, "a@x").await,
            PoolStat::Failed(_)
        ));
        assert!(matches!(
            provider.stat_on_pool(3, "a@x").await,
            PoolStat::Failed(_)
        ));
    }

    /// Multi-pool `stat`: a transport failure on any pool, with no pool
    /// holding the article, is `Err`, whichever pool failed.
    #[tokio::test]
    async fn stat_transport_failure_on_any_pool_is_an_error() {
        for failing in 0..2 {
            let backend = Scripted::new(2, move |pool, _| {
                if pool == failing {
                    Reply::Transport
                } else {
                    Reply::Refused
                }
            });
            let provider = provider(&backend);
            let err = provider.stat("a@x").await.unwrap_err();
            assert!(
                matches!(err, StreamError::NntpError(_)),
                "pool {failing} failing: {err}"
            );
            assert_eq!(backend.calls(), 2, "every pool is asked");
        }
    }

    /// Multi-pool `stat`: `Ok(false)` only when every pool answered 430.
    #[tokio::test]
    async fn stat_every_pool_430_is_not_found() {
        let backend = Scripted::new(3, |_, _| Reply::Refused);
        let provider = provider(&backend);
        assert!(!provider.stat("a@x").await.unwrap());
        assert_eq!(backend.calls(), 3);
    }

    /// Multi-pool `stat`: any pool holding the article is `Ok(true)`, even
    /// after other pools refused or failed to answer.
    #[tokio::test]
    async fn stat_any_pool_holding_it_is_found() {
        let backend = Scripted::new(3, |pool, _| match pool {
            0 => Reply::Transport,
            1 => Reply::Refused,
            _ => Reply::Body,
        });
        assert!(provider(&backend).stat("a@x").await.unwrap());

        let backend = Scripted::new(3, |pool, _| match pool {
            0 => Reply::Body,
            _ => Reply::Transport,
        });
        assert!(provider(&backend).stat("a@x").await.unwrap());
        assert_eq!(backend.calls(), 1, "stops at the first pool that has it");
    }

    #[test]
    fn test_replace_pools() {
        let provider = UsenetArticleProvider::new(vec![]);
        assert_eq!(provider.total_connections(), 0);

        // We can't easily create real ConnectionPool instances without a server,
        // but we can verify replace_pools with another empty vec and that
        // total_connections stays consistent.
        provider.replace_pools(vec![]);
        assert_eq!(provider.total_connections(), 0);
    }
}
