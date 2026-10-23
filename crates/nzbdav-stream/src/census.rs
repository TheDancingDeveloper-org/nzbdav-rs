//! Damage census: STAT a release's articles across every server pool and
//! settle each one as present, missing or unestablished.
//!
//! Pools are asked in priority order. Every article goes to pool 0; only the
//! articles pool 0 did not have go to pool 1, and so on. An article is
//! *missing* only when every pool answered 430. If any pool failed to answer
//! (connect error, protocol error, timeout) and none had it, the article is
//! *unestablished*: a transport failure is never a miss.
//!
//! Missing articles are mapped onto PAR2 slices through the release's
//! recovery set, and the census reports `damaged` (slices known lost),
//! `uncertain` (slices that might be lost) and the recovery blocks available.
//! A release is only declared beyond repair when even the best case cannot
//! be repaired, otherwise the verdict stays open and the next damage retries.
//!
//! STATs run on low-priority permits, and every census together holds at
//! most a share of each pool's connections, so a census never starves
//! playback, even on a small pool.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::holes::{HoleLedger, HoleState, Outcome};
use crate::prioritized_semaphore::PrioritizedPermit;
use crate::provider::{PoolStat, UsenetArticleProvider};

/// Census tunables.
#[derive(Debug, Clone)]
pub struct CensusConfig {
    /// Deadline for one STAT on one pool. A STAT that misses it is a pool
    /// failure, not a miss.
    pub stat_timeout: Duration,
    /// Upper bound on concurrent STATs against one pool, across every
    /// census running, further capped by `max_pool_share_percent`.
    pub max_in_flight_per_pool: usize,
    /// Share of a pool's connections all censuses together may hold (at
    /// least one), so the rest stay free for playback.
    pub max_pool_share_percent: usize,
    /// After this many consecutive failures with no answer in between, the
    /// rest of the pass skips the pool (each skipped article counts as a
    /// failure on it) instead of waiting out a dead server article by article.
    pub pool_trip_after: usize,
    /// How long a present result is reused by later censuses.
    pub present_ttl: Duration,
    /// How long a missing (430 everywhere) result is reused.
    pub missing_ttl: Duration,
    /// Pause on a release after a census that reached no clean verdict, or
    /// a failed repair (zurg `repairCooldown`).
    pub cooldown: Duration,
    /// Article results the cache holds. Past it, expired entries are pruned
    /// first, then the entries closest to expiry.
    pub cache_capacity: usize,
}

impl Default for CensusConfig {
    fn default() -> Self {
        Self {
            stat_timeout: Duration::from_secs(20),
            max_in_flight_per_pool: 8,
            max_pool_share_percent: 25,
            pool_trip_after: 16,
            present_ttl: Duration::from_secs(6 * 3600),
            missing_ttl: Duration::from_secs(30 * 60),
            cooldown: Duration::from_secs(10 * 60),
            cache_capacity: 200_000,
        }
    }
}

/// What the census needs from the server pools.
// `#[async_trait]` puts `#[must_use]` on the boxed futures it returns, which
// are already `#[must_use]`; clippy flags the macro's output, not our code.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait CensusBackend: Send + Sync {
    /// Pools in priority order.
    fn pool_count(&self) -> usize;
    fn pool_name(&self, pool: usize) -> String;
    /// Connection limit of one pool; with `max_pool_share_percent` it
    /// bounds census concurrency on the pool.
    fn pool_connections(&self, pool: usize) -> usize;
    /// STAT one article on one pool, no failover.
    async fn stat_on_pool(&self, pool: usize, message_id: &str) -> PoolStat;
    /// Low-priority permit held for the duration of one STAT.
    async fn acquire_low(&self) -> Option<PrioritizedPermit>;
}

#[async_trait]
impl CensusBackend for UsenetArticleProvider {
    fn pool_count(&self) -> usize {
        UsenetArticleProvider::pool_count(self)
    }

    fn pool_name(&self, pool: usize) -> String {
        UsenetArticleProvider::pool_name(self, pool)
    }

    fn pool_connections(&self, pool: usize) -> usize {
        UsenetArticleProvider::pool_connections(self, pool)
    }

    async fn stat_on_pool(&self, pool: usize, message_id: &str) -> PoolStat {
        UsenetArticleProvider::stat_on_pool(self, pool, message_id).await
    }

    async fn acquire_low(&self) -> Option<PrioritizedPermit> {
        Some(self.priority_semaphore().acquire_low().await)
    }
}

/// One article of a file, with the bytes it carries.
#[derive(Debug, Clone)]
pub struct ArticleSpan {
    pub message_id: String,
    /// Offset of the article's first byte in the file.
    pub offset: u64,
    pub len: u64,
}

/// A file of the release as the NZB carries it.
#[derive(Debug, Clone)]
pub struct CensusFile {
    /// Name matched against the PAR2 file entries.
    pub name: String,
    pub articles: Vec<ArticleSpan>,
}

/// A file the PAR2 set protects.
#[derive(Debug, Clone)]
pub struct Par2FileEntry {
    pub name: String,
    pub size: u64,
}

/// A PAR2 recovery volume and the articles that carry it.
#[derive(Debug, Clone)]
pub struct RecoveryVolume {
    /// Recovery blocks the volume holds.
    pub blocks: u32,
    pub segment_ids: Vec<String>,
}

/// The persisted PAR2 recovery set of a release.
#[derive(Debug, Clone)]
pub struct Par2RecoverySet {
    pub slice_size: u64,
    pub files: Vec<Par2FileEntry>,
    pub volumes: Vec<RecoveryVolume>,
}

/// What a census surveys: one release.
#[derive(Debug, Clone)]
pub struct CensusTarget {
    /// Release key; cooldowns and in-progress guards are per key.
    pub key: String,
    pub files: Vec<CensusFile>,
    pub recovery: Option<Par2RecoverySet>,
}

/// What asked for the census.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CensusTrigger {
    /// A read met a confirmed hole.
    HoleRead,
    /// The periodic health check.
    HealthCheck,
    /// An operator asked; bypasses the cooldown.
    Operator,
}

/// Settled state of one article.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArticleState {
    Present,
    /// Every pool answered 430.
    Missing,
    /// No pool had it and at least one failed to answer.
    Unestablished,
}

/// Units the damage figures count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DamageUnit {
    /// PAR2 slices, when the release has a recovery set.
    Slices,
    /// Articles, when it has none.
    Articles,
}

/// Settled outcome of a census.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing missing, nothing uncertain.
    Intact,
    /// Even if every uncertain slice is lost, recovery covers it.
    Repairable,
    /// Even if every uncertain slice survives, recovery cannot cover it.
    BeyondRepair,
}

/// Failures one pool returned during the census.
#[derive(Debug, Clone)]
pub struct PoolFailures {
    pub pool: usize,
    pub name: String,
    pub failed: usize,
    pub last_error: String,
}

/// Census result.
#[derive(Debug, Clone)]
pub struct CensusReport {
    pub key: String,
    pub trigger: CensusTrigger,
    /// Distinct articles surveyed, recovery volumes included.
    pub articles: usize,
    pub present: usize,
    pub missing_ids: Vec<String>,
    pub unestablished_ids: Vec<String>,
    /// Missing articles the ledger holds as provisional holes.
    pub provisional_ids: Vec<String>,
    /// Pools that failed to answer. Their failures are why articles are
    /// unestablished, and are reported as such rather than as misses.
    pub pool_failures: Vec<PoolFailures>,
    pub unit: DamageUnit,
    /// Data known to be lost.
    pub damaged: usize,
    /// Data that might be lost: unanswered articles, provisional holes and
    /// files the PAR2 set names that the NZB does not carry.
    pub uncertain: usize,
    /// Recovery blocks whose volumes are wholly present.
    pub recovery_available: usize,
    /// Recovery blocks in volumes with unanswered or provisional articles.
    pub recovery_uncertain: usize,
    /// Missing articles of files the PAR2 set does not protect. Reported,
    /// not counted in the verdict.
    pub unprotected_missing: usize,
    /// `None` while the census cannot settle; the next damage retries.
    pub verdict: Option<Verdict>,
}

/// Why a census did not run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CensusError {
    #[error("release {key} is cooling down for another {remaining:?}")]
    CoolingDown { key: String, remaining: Duration },
    #[error("a census of release {0} is already running")]
    InProgress(String),
}

/// Runs censuses and keeps the article cache and release cooldowns.
pub struct DamageCensus {
    backend: Arc<dyn CensusBackend>,
    ledger: Arc<dyn HoleLedger>,
    config: CensusConfig,
    cache: Mutex<HashMap<String, (ArticleState, Instant)>>,
    cooldowns: Mutex<HashMap<String, Instant>>,
    running: Mutex<HashSet<String>>,
    /// STAT slots per pool, by pool name, shared by every census: the slot
    /// count and the semaphore holding them.
    pool_slots: Mutex<HashMap<String, (usize, Arc<Semaphore>)>>,
}

struct RunningGuard<'a> {
    running: &'a Mutex<HashSet<String>>,
    key: String,
}

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.running.lock().remove(&self.key);
    }
}

/// Per-article evidence gathered across pools.
#[derive(Default)]
struct Evidence {
    present: bool,
    absent_on: usize,
}

impl DamageCensus {
    pub fn new(
        backend: Arc<dyn CensusBackend>,
        ledger: Arc<dyn HoleLedger>,
        config: CensusConfig,
    ) -> Self {
        Self {
            backend,
            ledger,
            config,
            cache: Mutex::new(HashMap::new()),
            cooldowns: Mutex::new(HashMap::new()),
            running: Mutex::new(HashSet::new()),
            pool_slots: Mutex::new(HashMap::new()),
        }
    }

    /// Article results held in the cache.
    pub fn cached_articles(&self) -> usize {
        self.cache.lock().len()
    }

    /// STATs every census together may run against one pool at once.
    pub fn pool_width(&self, pool: usize) -> usize {
        let connections = self.backend.pool_connections(pool);
        let share = connections.saturating_mul(self.config.max_pool_share_percent) / 100;
        self.config.max_in_flight_per_pool.min(share).max(1)
    }

    /// The slot semaphore of one pool, resized when the pool's width changed.
    fn pool_slots(&self, pool: usize) -> (usize, Arc<Semaphore>) {
        let width = self.pool_width(pool);
        let name = self.backend.pool_name(pool);
        let mut slots = self.pool_slots.lock();
        let entry = slots
            .entry(name)
            .or_insert_with(|| (width, Arc::new(Semaphore::new(width))));
        if entry.0 != width {
            // STATs still holding the old semaphore's slots finish on it.
            *entry = (width, Arc::new(Semaphore::new(width)));
        }
        (entry.0, Arc::clone(&entry.1))
    }

    /// Time left on a release's cooldown, if any.
    pub fn cooldown_remaining(&self, key: &str) -> Option<Duration> {
        let now = Instant::now();
        let mut cooldowns = self.cooldowns.lock();
        match cooldowns.get(key) {
            Some(until) if *until > now => Some(*until - now),
            Some(_) => {
                cooldowns.remove(key);
                None
            }
            None => None,
        }
    }

    /// Start a release's cooldown after a failed repair.
    pub fn note_repair_failed(&self, key: &str) {
        self.start_cooldown(key);
    }

    fn start_cooldown(&self, key: &str) {
        self.cooldowns
            .lock()
            .insert(key.to_string(), Instant::now() + self.config.cooldown);
    }

    /// Survey a release. Refused while the release is cooling down (unless
    /// an operator asked) or already being surveyed.
    pub async fn run(
        &self,
        target: &CensusTarget,
        trigger: CensusTrigger,
    ) -> Result<CensusReport, CensusError> {
        if trigger != CensusTrigger::Operator
            && let Some(remaining) = self.cooldown_remaining(&target.key)
        {
            return Err(CensusError::CoolingDown {
                key: target.key.clone(),
                remaining,
            });
        }
        if !self.running.lock().insert(target.key.clone()) {
            return Err(CensusError::InProgress(target.key.clone()));
        }
        let _guard = RunningGuard {
            running: &self.running,
            key: target.key.clone(),
        };

        let ids = surveyed_ids(target);
        let servers = self.servers();
        let (states, fresh, pool_failures) = self.settle(&ids, &servers).await;

        let mut report = assess(
            target,
            trigger,
            &states,
            &fresh,
            self.ledger.as_ref(),
            &servers,
        );
        report.pool_failures = pool_failures;

        let clean = matches!(
            report.verdict,
            Some(Verdict::Intact) | Some(Verdict::Repairable)
        ) && report.pool_failures.is_empty();
        if !clean {
            self.start_cooldown(&target.key);
        }

        info!(
            key = %report.key,
            trigger = ?trigger,
            articles = report.articles,
            missing = report.missing_ids.len(),
            unestablished = report.unestablished_ids.len(),
            damaged = report.damaged,
            uncertain = report.uncertain,
            recovery_available = report.recovery_available,
            verdict = ?report.verdict,
            "damage census finished",
        );
        Ok(report)
    }

    /// Pool names in priority order; they fingerprint ledger records.
    fn servers(&self) -> Vec<String> {
        (0..self.backend.pool_count())
            .map(|pool| self.backend.pool_name(pool))
            .collect()
    }

    /// Settle every id, from the cache where fresh, else across the pools.
    /// Also returns the ids a STAT pass settled in this run, as opposed to
    /// the cache.
    async fn settle(
        &self,
        ids: &[String],
        servers: &[String],
    ) -> (
        HashMap<String, ArticleState>,
        HashSet<String>,
        Vec<PoolFailures>,
    ) {
        let mut states = HashMap::with_capacity(ids.len());
        let mut pending = Vec::new();
        {
            let now = Instant::now();
            let cache = self.cache.lock();
            for id in ids {
                match cache.get(id) {
                    Some((state, expires)) if *expires > now => {
                        states.insert(id.clone(), *state);
                    }
                    _ => pending.push(id.clone()),
                }
            }
        }
        debug!(
            cached = states.len(),
            pending = pending.len(),
            "census settling articles"
        );

        let pools = self.backend.pool_count();
        let mut evidence: HashMap<String, Evidence> = pending
            .iter()
            .map(|id| (id.clone(), Evidence::default()))
            .collect();
        let mut failures = Vec::new();

        for pool in 0..pools {
            if pending.is_empty() {
                break;
            }
            let answers = self.stat_pass(pool, &pending).await;
            let mut failed = 0usize;
            let mut last_error = String::new();
            let mut next = Vec::new();
            for (id, answer) in pending.into_iter().zip(answers) {
                let ev = evidence.get_mut(&id).expect("evidence for pending id");
                match answer {
                    PoolStat::Present => ev.present = true,
                    PoolStat::Absent => {
                        ev.absent_on += 1;
                        next.push(id);
                    }
                    PoolStat::Failed(e) => {
                        failed += 1;
                        last_error = e;
                        next.push(id);
                    }
                }
            }
            if failed > 0 {
                let name = self.backend.pool_name(pool);
                warn!(pool, %name, failed, error = %last_error, "census: pool failed to answer");
                failures.push(PoolFailures {
                    pool,
                    name,
                    failed,
                    last_error,
                });
            }
            pending = next;
        }

        let now = Instant::now();
        let fresh: HashSet<String> = evidence.keys().cloned().collect();
        let mut cache = self.cache.lock();
        for (id, ev) in evidence {
            let state = if ev.present {
                ArticleState::Present
            } else if pools > 0 && ev.absent_on == pools {
                ArticleState::Missing
            } else {
                ArticleState::Unestablished
            };
            let ttl = match state {
                ArticleState::Present => Some(self.config.present_ttl),
                ArticleState::Missing => Some(self.config.missing_ttl),
                ArticleState::Unestablished => None,
            };
            if let Some(ttl) = ttl {
                cache.insert(id.clone(), (state, now + ttl));
            }
            states.insert(id, state);
        }
        enforce_capacity(&mut cache, self.config.cache_capacity, now);
        drop(cache);

        // Misses go to the ledger in `assess`, which needs the state. An
        // unestablished article is never a miss and leaves the ledger alone.
        for (id, state) in &states {
            if *state == ArticleState::Present {
                self.ledger.note(id, &Outcome::Present, servers);
            }
        }
        (states, fresh, failures)
    }

    /// STAT `ids` on one pool, pipelined; answers come back in `ids` order.
    async fn stat_pass(&self, pool: usize, ids: &[String]) -> Vec<PoolStat> {
        let (width, slots) = self.pool_slots(pool);
        let consecutive = Arc::new(AtomicUsize::new(0));
        let trip = self.config.pool_trip_after.max(1);
        let timeout = self.config.stat_timeout;

        // Each STAT owns what it uses, so the census future stays `Send`
        // for any caller that spawns it.
        futures::stream::iter(ids.to_vec())
            .map(|id| {
                let backend = Arc::clone(&self.backend);
                let consecutive = Arc::clone(&consecutive);
                let slots = Arc::clone(&slots);
                async move {
                    if consecutive.load(Ordering::Relaxed) >= trip {
                        return PoolStat::Failed("pool skipped after repeated failures".into());
                    }
                    let Ok(_slot) = slots.acquire().await else {
                        return PoolStat::Failed("census pool slots closed".into());
                    };
                    let _permit = backend.acquire_low().await;
                    if consecutive.load(Ordering::Relaxed) >= trip {
                        return PoolStat::Failed("pool skipped after repeated failures".into());
                    }
                    let answer = match tokio::time::timeout(
                        timeout,
                        backend.stat_on_pool(pool, &id),
                    )
                    .await
                    {
                        Ok(answer) => answer,
                        Err(_) => PoolStat::Failed(format!("STAT timed out after {timeout:?}")),
                    };
                    match answer {
                        PoolStat::Failed(_) => {
                            consecutive.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => consecutive.store(0, Ordering::Relaxed),
                    }
                    answer
                }
            })
            .buffered(width)
            .collect()
            .await
    }
}

/// Hold the cache to `capacity`: drop expired entries, then, if every entry
/// is still fresh, the ones closest to expiry, down to 90% so the next pass
/// does not have to evict again at once.
fn enforce_capacity(
    cache: &mut HashMap<String, (ArticleState, Instant)>,
    capacity: usize,
    now: Instant,
) {
    if cache.len() <= capacity {
        return;
    }
    cache.retain(|_, (_, expires)| *expires > now);
    if cache.len() <= capacity {
        return;
    }
    let keep = capacity - capacity / 10;
    let mut by_expiry: Vec<(Instant, String)> = cache
        .iter()
        .map(|(id, (_, expires))| (*expires, id.clone()))
        .collect();
    let evict = by_expiry.len() - keep;
    if evict < by_expiry.len() {
        by_expiry.select_nth_unstable(evict);
    }
    for (_, id) in by_expiry.into_iter().take(evict) {
        cache.remove(&id);
    }
    debug!(
        evicted = evict,
        capacity, "census cache full, evicted soonest to expire"
    );
}

/// Every distinct message id the census must settle: data articles, then
/// recovery volume articles.
fn surveyed_ids(target: &CensusTarget) -> Vec<String> {
    let mut seen = HashSet::new();
    let data = target
        .files
        .iter()
        .flat_map(|f| f.articles.iter().map(|a| &a.message_id));
    let recovery = target
        .recovery
        .iter()
        .flat_map(|r| r.volumes.iter().flat_map(|v| v.segment_ids.iter()));
    data.chain(recovery)
        .filter(|id| seen.insert(id.as_str()))
        .cloned()
        .collect()
}

/// How the ledger classes one surveyed article.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Loss {
    None,
    Certain,
    Possible,
}

/// Turn settled article states into damage figures and a verdict.
fn assess(
    target: &CensusTarget,
    trigger: CensusTrigger,
    states: &HashMap<String, ArticleState>,
    fresh: &HashSet<String>,
    ledger: &dyn HoleLedger,
    servers: &[String],
) -> CensusReport {
    let mut missing_ids = Vec::new();
    let mut unestablished_ids = Vec::new();
    let mut provisional_ids = Vec::new();
    let mut loss: HashMap<&str, Loss> = HashMap::with_capacity(states.len());
    let mut present = 0usize;

    let mut ordered: Vec<(&String, &ArticleState)> = states.iter().collect();
    ordered.sort_by(|a, b| a.0.cmp(b.0));
    for (id, state) in ordered {
        let class = match state {
            ArticleState::Present => {
                present += 1;
                Loss::None
            }
            ArticleState::Unestablished => {
                unestablished_ids.push(id.clone());
                Loss::Possible
            }
            ArticleState::Missing => {
                missing_ids.push(id.clone());
                // Only a live STAT pass is evidence: every pool answered 430
                // just now, one refusal per server. A miss replayed from the
                // cache is the same 430 again and must not confirm a hole, so
                // it is reported with whatever the ledger already holds.
                let hole = if fresh.contains(id) {
                    let mut hole = None;
                    for server in servers {
                        let refused = Outcome::Refused {
                            server: server.clone(),
                        };
                        hole = ledger.note(id, &refused, servers).state;
                    }
                    hole
                } else {
                    ledger.state(id)
                };
                match hole.unwrap_or(HoleState::Provisional) {
                    HoleState::Confirmed => Loss::Certain,
                    HoleState::Provisional => {
                        provisional_ids.push(id.clone());
                        Loss::Possible
                    }
                }
            }
        };
        loss.insert(id.as_str(), class);
    }
    let loss_of = |id: &str| loss.get(id).copied().unwrap_or(Loss::Possible);

    let mut report = CensusReport {
        key: target.key.clone(),
        trigger,
        articles: states.len(),
        present,
        missing_ids,
        unestablished_ids,
        provisional_ids,
        pool_failures: Vec::new(),
        unit: DamageUnit::Articles,
        damaged: 0,
        uncertain: 0,
        recovery_available: 0,
        recovery_uncertain: 0,
        unprotected_missing: 0,
        verdict: None,
    };

    match &target.recovery {
        None => {
            let mut damaged = HashSet::new();
            let mut uncertain = HashSet::new();
            for article in target.files.iter().flat_map(|f| &f.articles) {
                match loss_of(&article.message_id) {
                    Loss::Certain => {
                        damaged.insert(article.message_id.as_str());
                    }
                    Loss::Possible => {
                        uncertain.insert(article.message_id.as_str());
                    }
                    Loss::None => {}
                }
            }
            report.damaged = damaged.len();
            report.uncertain = uncertain.difference(&damaged).count();
        }
        Some(set) => {
            report.unit = DamageUnit::Slices;
            let slice = set.slice_size.max(1);
            let carried: HashMap<&str, &CensusFile> =
                target.files.iter().map(|f| (f.name.as_str(), f)).collect();
            let protected: HashSet<&str> = set.files.iter().map(|f| f.name.as_str()).collect();

            let mut damaged: BTreeSet<(usize, u64)> = BTreeSet::new();
            let mut uncertain: BTreeSet<(usize, u64)> = BTreeSet::new();
            for (fi, entry) in set.files.iter().enumerate() {
                let slices = entry.size.div_ceil(slice);
                let Some(file) = carried.get(entry.name.as_str()) else {
                    // The set names a file the NZB does not carry.
                    uncertain.extend((0..slices).map(|s| (fi, s)));
                    continue;
                };
                for article in &file.articles {
                    let class = loss_of(&article.message_id);
                    if class == Loss::None || article.len == 0 {
                        continue;
                    }
                    let first = article.offset / slice;
                    let last = ((article.offset + article.len - 1) / slice).min(slices.max(1) - 1);
                    let bucket = if class == Loss::Certain {
                        &mut damaged
                    } else {
                        &mut uncertain
                    };
                    bucket.extend((first..=last).map(|s| (fi, s)));
                }
            }
            report.damaged = damaged.len();
            report.uncertain = uncertain.difference(&damaged).count();

            report.unprotected_missing = target
                .files
                .iter()
                .filter(|f| !protected.contains(f.name.as_str()))
                .flat_map(|f| &f.articles)
                .filter(|a| loss_of(&a.message_id) == Loss::Certain)
                .count();

            for volume in &set.volumes {
                let classes: Vec<Loss> = volume.segment_ids.iter().map(|id| loss_of(id)).collect();
                if classes.contains(&Loss::Certain) {
                    continue;
                }
                if classes.contains(&Loss::Possible) {
                    report.recovery_uncertain += volume.blocks as usize;
                } else {
                    report.recovery_available += volume.blocks as usize;
                }
            }
        }
    }

    report.verdict = settle_verdict(
        report.damaged,
        report.uncertain,
        report.recovery_available,
        report.recovery_uncertain,
    );
    report
}

/// The settled rule (zurg `unsettled.settled`). With `missing` counting the
/// uncertain slices too, a release is beyond repair only when
/// `missing - uncertain > recovery`, i.e. when the damage already known
/// exceeds every recovery block that might be available. It is repairable
/// only when the worst case fits the recovery known to be available.
/// Anything between stays open.
pub fn settle_verdict(
    damaged: usize,
    uncertain: usize,
    recovery_available: usize,
    recovery_uncertain: usize,
) -> Option<Verdict> {
    if damaged == 0 && uncertain == 0 {
        Some(Verdict::Intact)
    } else if damaged > recovery_available + recovery_uncertain {
        Some(Verdict::BeyondRepair)
    } else if damaged + uncertain <= recovery_available {
        Some(Verdict::Repairable)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holes::InMemoryHoleLedger;

    /// Per-pool canned answers; unknown ids are absent.
    struct FakePools {
        pools: Vec<HashMap<String, PoolStat>>,
        hang: Vec<HashSet<String>>,
        calls: Mutex<Vec<(usize, String)>>,
        connections: usize,
        delay: Duration,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
    }

    impl FakePools {
        fn new(n: usize) -> Self {
            Self {
                pools: vec![HashMap::new(); n],
                hang: vec![HashSet::new(); n],
                calls: Mutex::new(Vec::new()),
                connections: 4,
                delay: Duration::ZERO,
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }
        }

        fn set(&mut self, pool: usize, id: &str, answer: PoolStat) {
            self.pools[pool].insert(id.to_string(), answer);
        }

        fn calls_on(&self, pool: usize) -> Vec<String> {
            let mut v: Vec<String> = self
                .calls
                .lock()
                .iter()
                .filter(|(p, _)| *p == pool)
                .map(|(_, id)| id.clone())
                .collect();
            v.sort();
            v
        }
    }

    #[async_trait]
    impl CensusBackend for FakePools {
        fn pool_count(&self) -> usize {
            self.pools.len()
        }
        fn pool_name(&self, pool: usize) -> String {
            format!("pool-{pool}")
        }
        fn pool_connections(&self, _pool: usize) -> usize {
            self.connections
        }
        async fn stat_on_pool(&self, pool: usize, message_id: &str) -> PoolStat {
            self.calls.lock().push((pool, message_id.to_string()));
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            if self.hang[pool].contains(message_id) {
                std::future::pending::<()>().await;
            }
            self.pools[pool]
                .get(message_id)
                .cloned()
                .unwrap_or(PoolStat::Absent)
        }
        async fn acquire_low(&self) -> Option<PrioritizedPermit> {
            None
        }
    }

    fn file(name: &str, ids: &[&str], article_len: u64) -> CensusFile {
        CensusFile {
            name: name.to_string(),
            articles: ids
                .iter()
                .enumerate()
                .map(|(i, id)| ArticleSpan {
                    message_id: id.to_string(),
                    offset: i as u64 * article_len,
                    len: article_len,
                })
                .collect(),
        }
    }

    fn census(backend: FakePools, ledger: Arc<dyn HoleLedger>) -> (DamageCensus, Arc<FakePools>) {
        let backend = Arc::new(backend);
        let config = CensusConfig {
            stat_timeout: Duration::from_millis(100),
            ..CensusConfig::default()
        };
        (DamageCensus::new(backend.clone(), ledger, config), backend)
    }

    fn confirm_now() -> Arc<dyn HoleLedger> {
        Arc::new(InMemoryHoleLedger::new(Duration::ZERO))
    }

    /// The WI-160 acceptance case: 2 articles missing everywhere, 1 missing
    /// on pool A only, 1 timing out on pool B.
    #[tokio::test]
    async fn acceptance_two_missing_one_partial_one_timeout() {
        let mut pools = FakePools::new(2);
        for id in ["ok1", "ok2", "par1", "par2"] {
            pools.set(0, id, PoolStat::Present);
        }
        // a-only: missing on A, present on B.
        pools.set(1, "a-only", PoolStat::Present);
        // slow: missing on A, times out on B.
        pools.hang[1].insert("slow".into());
        let (census, _) = census(pools, confirm_now());

        let target = CensusTarget {
            key: "rel".into(),
            files: vec![file(
                "movie.mkv",
                &["ok1", "gone1", "a-only", "gone2", "slow", "ok2"],
                100,
            )],
            recovery: Some(Par2RecoverySet {
                slice_size: 100,
                files: vec![Par2FileEntry {
                    name: "movie.mkv".into(),
                    size: 600,
                }],
                volumes: vec![
                    RecoveryVolume {
                        blocks: 1,
                        segment_ids: vec!["par1".into()],
                    },
                    RecoveryVolume {
                        blocks: 1,
                        segment_ids: vec!["par2".into()],
                    },
                ],
            }),
        };
        let report = census.run(&target, CensusTrigger::HoleRead).await.unwrap();
        assert_eq!(report.damaged, 2);
        assert_eq!(report.uncertain, 1);
        assert_eq!(report.recovery_available, 2);
        assert_eq!(report.verdict, None);
        assert_eq!(report.missing_ids, vec!["gone1", "gone2"]);
        assert_eq!(report.unestablished_ids, vec!["slow"]);
        assert_eq!(report.pool_failures.len(), 1);
        assert_eq!(report.pool_failures[0].pool, 1);
        assert!(report.pool_failures[0].last_error.contains("timed out"));
    }

    #[tokio::test]
    async fn only_articles_a_pool_lacked_go_to_the_next() {
        let mut pools = FakePools::new(3);
        pools.set(0, "a", PoolStat::Present);
        pools.set(1, "b", PoolStat::Present);
        pools.set(2, "c", PoolStat::Failed("boom".into()));
        let (census, backend) = census(pools, confirm_now());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["a", "b", "c"], 10)],
            recovery: None,
        };
        let report = census
            .run(&target, CensusTrigger::HealthCheck)
            .await
            .unwrap();
        assert_eq!(backend.calls_on(0), vec!["a", "b", "c"]);
        assert_eq!(backend.calls_on(1), vec!["b", "c"]);
        assert_eq!(backend.calls_on(2), vec!["c"]);
        assert_eq!(report.present, 2);
        assert_eq!(report.unestablished_ids, vec!["c"]);
        assert!(report.missing_ids.is_empty());
        assert_eq!(report.unit, DamageUnit::Articles);
        assert_eq!((report.damaged, report.uncertain), (0, 1));
        assert_eq!(report.verdict, None);
    }

    #[tokio::test]
    async fn no_set_and_known_damage_is_beyond_repair() {
        let (census, _) = census(FakePools::new(2), confirm_now());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["x"], 10)],
            recovery: None,
        };
        let report = census
            .run(&target, CensusTrigger::HealthCheck)
            .await
            .unwrap();
        assert_eq!(report.damaged, 1);
        assert_eq!(report.verdict, Some(Verdict::BeyondRepair));
    }

    #[tokio::test]
    async fn provisional_holes_are_uncertain_until_confirmed() {
        let ledger = Arc::new(InMemoryHoleLedger::new(Duration::from_secs(3600)));
        let (census, _) = census(FakePools::new(1), ledger.clone());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["x"], 10)],
            recovery: None,
        };
        let report = census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(report.provisional_ids, vec!["x"]);
        assert_eq!((report.damaged, report.uncertain), (0, 1));
        assert_eq!(report.verdict, None);
        assert_eq!(ledger.state("x"), Some(HoleState::Provisional));
    }

    /// A miss replayed from the census cache is not a second refusal: two
    /// censuses within the cache window, further apart than the confirm
    /// delay, leave the hole provisional and ask the pools once.
    #[tokio::test]
    async fn cached_misses_do_not_confirm_a_hole() {
        let ledger = Arc::new(InMemoryHoleLedger::new(Duration::from_millis(20)));
        let (census, backend) = census(FakePools::new(2), ledger.clone());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["x"], 10)],
            recovery: None,
        };
        let first = census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(first.provisional_ids, vec!["x"]);
        assert_eq!(ledger.state("x"), Some(HoleState::Provisional));

        tokio::time::sleep(Duration::from_millis(50)).await;
        let calls = backend.calls.lock().len();
        let second = census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(backend.calls.lock().len(), calls, "served from the cache");
        assert_eq!(second.missing_ids, vec!["x"]);
        assert_eq!(second.provisional_ids, vec!["x"]);
        assert_eq!((second.damaged, second.uncertain), (0, 1));
        assert_eq!(ledger.state("x"), Some(HoleState::Provisional));
    }

    #[tokio::test]
    async fn files_the_set_names_but_nzb_lacks_are_uncertain() {
        let mut pools = FakePools::new(1);
        pools.set(0, "a", PoolStat::Present);
        pools.set(0, "p", PoolStat::Present);
        let (census, _) = census(pools, confirm_now());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("one", &["a"], 100)],
            recovery: Some(Par2RecoverySet {
                slice_size: 100,
                files: vec![
                    Par2FileEntry {
                        name: "one".into(),
                        size: 100,
                    },
                    Par2FileEntry {
                        name: "two".into(),
                        size: 250,
                    },
                ],
                volumes: vec![RecoveryVolume {
                    blocks: 3,
                    segment_ids: vec!["p".into()],
                }],
            }),
        };
        let report = census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!((report.damaged, report.uncertain), (0, 3));
        assert_eq!(report.verdict, Some(Verdict::Repairable));
    }

    #[tokio::test]
    async fn article_spanning_slices_damages_each() {
        let mut pools = FakePools::new(1);
        pools.set(0, "p", PoolStat::Present);
        let (census, _) = census(pools, confirm_now());
        let target = CensusTarget {
            key: "k".into(),
            // One 250-byte article over 100-byte slices: slices 0, 1, 2.
            files: vec![file("f", &["x"], 250)],
            recovery: Some(Par2RecoverySet {
                slice_size: 100,
                files: vec![Par2FileEntry {
                    name: "f".into(),
                    size: 250,
                }],
                volumes: vec![RecoveryVolume {
                    blocks: 3,
                    segment_ids: vec!["p".into()],
                }],
            }),
        };
        let report = census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(report.damaged, 3);
        assert_eq!(report.verdict, Some(Verdict::Repairable));
    }

    #[tokio::test]
    async fn cooldown_after_unsettled_census_but_operator_bypasses() {
        let (census, backend) = census(FakePools::new(1), confirm_now());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["x"], 10)],
            recovery: None,
        };
        census.run(&target, CensusTrigger::HoleRead).await.unwrap();
        let err = census
            .run(&target, CensusTrigger::HoleRead)
            .await
            .unwrap_err();
        assert!(matches!(err, CensusError::CoolingDown { .. }));
        // The operator gets through; the miss comes from the cache.
        let calls_before = backend.calls.lock().len();
        census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(backend.calls.lock().len(), calls_before);
    }

    #[tokio::test]
    async fn repair_failure_starts_cooldown() {
        let mut pools = FakePools::new(1);
        pools.set(0, "x", PoolStat::Present);
        let (census, _) = census(pools, confirm_now());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["x"], 10)],
            recovery: None,
        };
        let report = census.run(&target, CensusTrigger::HoleRead).await.unwrap();
        assert_eq!(report.verdict, Some(Verdict::Intact));
        assert!(census.cooldown_remaining("k").is_none());
        census.note_repair_failed("k");
        assert!(census.cooldown_remaining("k").is_some());
    }

    #[tokio::test]
    async fn unestablished_results_are_not_cached() {
        let mut pools = FakePools::new(1);
        pools.set(0, "x", PoolStat::Failed("down".into()));
        let (census, backend) = census(pools, confirm_now());
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["x"], 10)],
            recovery: None,
        };
        census.run(&target, CensusTrigger::Operator).await.unwrap();
        census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(backend.calls_on(0).len(), 2);
    }

    #[tokio::test]
    async fn dead_pool_trips_instead_of_stalling() {
        let mut pools = FakePools::new(2);
        let ids: Vec<String> = (0..50).map(|i| format!("a{i}")).collect();
        for id in &ids {
            pools.set(0, id, PoolStat::Failed("connection refused".into()));
            pools.set(1, id, PoolStat::Present);
        }
        let backend = Arc::new(pools);
        let census = DamageCensus::new(
            backend.clone(),
            confirm_now(),
            CensusConfig {
                pool_trip_after: 4,
                max_in_flight_per_pool: 1,
                ..CensusConfig::default()
            },
        );
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &refs, 10)],
            recovery: None,
        };
        let report = census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(report.present, 50);
        assert_eq!(backend.calls_on(0).len(), 4);
        assert_eq!(report.pool_failures[0].failed, 50);
    }

    /// Cached results are reused within their TTL and asked again after it.
    #[tokio::test]
    async fn cache_entries_expire() {
        let mut pools = FakePools::new(1);
        pools.set(0, "here", PoolStat::Present);
        let backend = Arc::new(pools);
        let census = DamageCensus::new(
            backend.clone(),
            confirm_now(),
            CensusConfig {
                present_ttl: Duration::from_millis(40),
                missing_ttl: Duration::from_millis(40),
                ..CensusConfig::default()
            },
        );
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &["here", "gone"], 10)],
            recovery: None,
        };
        census.run(&target, CensusTrigger::Operator).await.unwrap();
        census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(backend.calls_on(0).len(), 2, "second run served from cache");

        tokio::time::sleep(Duration::from_millis(80)).await;
        census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(backend.calls_on(0).len(), 4, "expired entries asked again");
    }

    /// The cache stays within its capacity even when every entry is fresh.
    #[tokio::test]
    async fn cache_capacity_holds_when_every_entry_is_fresh() {
        let mut pools = FakePools::new(1);
        let ids: Vec<String> = (0..20).map(|i| format!("a{i}")).collect();
        for id in &ids {
            pools.set(0, id, PoolStat::Present);
        }
        let census = DamageCensus::new(
            Arc::new(pools),
            confirm_now(),
            CensusConfig {
                cache_capacity: 10,
                ..CensusConfig::default()
            },
        );
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let target = CensusTarget {
            key: "k".into(),
            files: vec![file("f", &refs, 10)],
            recovery: None,
        };
        let report = census.run(&target, CensusTrigger::Operator).await.unwrap();
        assert_eq!(report.present, 20);
        assert!(
            census.cached_articles() <= 10,
            "{}",
            census.cached_articles()
        );
        assert!(census.cached_articles() > 0);
    }

    /// Censuses of different releases share one pool's slots: with 8
    /// connections and a 25% share, no more than 2 STATs run on the pool at
    /// once, however many censuses are running.
    #[tokio::test]
    async fn concurrent_censuses_share_a_capped_pool() {
        let mut pools = FakePools::new(1);
        pools.connections = 8;
        pools.delay = Duration::from_millis(5);
        let backend = Arc::new(pools);
        let census = Arc::new(DamageCensus::new(
            backend.clone(),
            confirm_now(),
            CensusConfig::default(),
        ));
        assert_eq!(census.pool_width(0), 2);

        let runs = (0..4).map(|r| {
            let census = Arc::clone(&census);
            async move {
                let ids: Vec<String> = (0..10).map(|i| format!("r{r}-{i}")).collect();
                let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
                let target = CensusTarget {
                    key: format!("k{r}"),
                    files: vec![file("f", &refs, 10)],
                    recovery: None,
                };
                census.run(&target, CensusTrigger::Operator).await.unwrap()
            }
        });
        futures::future::join_all(runs).await;
        assert_eq!(backend.calls_on(0).len(), 40);
        assert!(backend.peak.load(Ordering::SeqCst) <= 2);
    }

    #[test]
    fn pool_width_leaves_most_connections_to_playback() {
        let census = |connections: usize| {
            let mut pools = FakePools::new(1);
            pools.connections = connections;
            DamageCensus::new(Arc::new(pools), confirm_now(), CensusConfig::default())
        };
        assert_eq!(census(1).pool_width(0), 1);
        assert_eq!(census(4).pool_width(0), 1);
        assert_eq!(census(8).pool_width(0), 2);
        assert_eq!(census(20).pool_width(0), 5);
        assert_eq!(census(100).pool_width(0), 8, "capped by max_in_flight");
    }

    #[test]
    fn settled_rule() {
        assert_eq!(settle_verdict(0, 0, 0, 0), Some(Verdict::Intact));
        assert_eq!(settle_verdict(3, 0, 2, 0), Some(Verdict::BeyondRepair));
        assert_eq!(settle_verdict(2, 1, 2, 0), None);
        assert_eq!(settle_verdict(2, 1, 3, 0), Some(Verdict::Repairable));
        // Uncertain recovery keeps the verdict open rather than lost.
        assert_eq!(settle_verdict(3, 0, 2, 1), None);
    }
}
