//! The background PAR2 repair job (zurg `internal/nzb/repair.go`): rebuild
//! the byte spans a release lost from its PAR2 recovery set and keep them in
//! the [`PatchStore`], so reads of a confirmed hole get the original bytes.
//!
//! Lifecycle:
//!
//! - A job is scheduled by the damage census (itself started by a read that
//!   met a confirmed hole, or by the health check). There is at most one job
//!   per recovery set and one running job overall; damage found while a job
//!   runs is folded into that job, which goes again once it is done, rather
//!   than starting a second one. Every queued or running job has a completion
//!   signal ([`RepairService::signal`]) so a caller can wait for it.
//! - A job is skipped when repair is switched off, when the set is cooling
//!   down after a failed attempt, or when the release carries a terminal
//!   verdict reached within the last day against the same servers.
//! - A job is bounded by a deadline (two hours by default).
//!
//! One attempt:
//!
//! 1. Load recovery rows from the volumes' articles, lowest exponents first,
//!    until there are as many as the known missing slices plus a margin, or
//!    the volumes run out. Rows are bounded in bytes. Only volumes read to
//!    their end count toward "this is all the release carries".
//! 2. Read every present slice of every carried file at the repair priority
//!    tier, a bounded window at a time, check each against its IFSC checksum
//!    and feed the good ones to the solver in batches. A slice that fails to
//!    read or to verify is unknown, not damage; only a confirmed hole is.
//! 3. If the damage outgrows the rows, load more. When every volume was read
//!    to its end and the known damage still exceeds every row the release
//!    carries, the release is beyond repair: that terminal verdict is
//!    recorded with the shape of the set. Any other shortfall fails the
//!    attempt, and the set cools down.
//! 4. Solve, check each rebuilt slice against its checksum, and store only
//!    the spans of the articles that were actually gone, stamped with the
//!    NZB blob the release came from.
//!
//! Memory is bounded by the rows, the read window plus one feed batch, and
//! the rebuilt slices.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{self, Cursor, Read};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use nzbdav_core::models::Par2SetRecord;
use parking_lot::Mutex;
use rust_par2::recovery::{RecoveryBlock, RecoveryBudget, RecoveryReader, VolumeScan};
use rust_par2::{BlockMap, Par2FileSet, Repair};
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::census::CensusFile;
use crate::error::StreamError;
use crate::holes::{HoleLedger, HoleState, now_ms, server_fingerprint};
use crate::patches::{PatchStamp, PatchStore, ReleaseVerdict};
use crate::provider::UsenetArticleProvider;
use crate::recovery_set::census_file;

const MIB: u64 = 1024 * 1024;

/// Volume articles fetched ahead of the PAR2 packet reader.
const VOLUME_READ_AHEAD: usize = 2;

/// Repair tunables.
#[derive(Debug, Clone)]
pub struct RepairConfig {
    /// Whether repair runs at all (`enable_repair`). A switch passed to
    /// [`RepairService::with_switch`] takes precedence.
    pub enabled: bool,
    /// Pause on a set after a failed attempt (zurg `repairCooldown`).
    pub cooldown: Duration,
    /// How long a terminal verdict stops new attempts.
    pub verdict_ttl: Duration,
    /// Deadline for one job.
    pub timeout: Duration,
    /// Rows loaded beyond the known missing slices, for damage the read
    /// finds on the way.
    pub extra_rows: usize,
    /// Bound on the recovery rows held, in bytes.
    pub max_row_bytes: u64,
    /// Bound on the slice data read ahead of the solver, in bytes; the
    /// read window and the feed batch each stay within it.
    pub prefetch_bytes: u64,
    /// Bound on the articles read ahead and on the slices of a feed batch.
    pub prefetch_slices: usize,
    /// Attempts at loading enough rows within one job.
    pub max_passes: usize,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cooldown: Duration::from_secs(10 * 60),
            verdict_ttl: Duration::from_secs(24 * 60 * 60),
            timeout: Duration::from_secs(2 * 60 * 60),
            extra_rows: 8,
            max_row_bytes: 256 * MIB,
            prefetch_bytes: 48 * MIB,
            prefetch_slices: 8,
            max_passes: 3,
        }
    }
}

/// What the repair job needs from the server pools.
// `#[async_trait]` puts `#[must_use]` on the boxed futures it returns, which
// are already `#[must_use]`; clippy flags the macro's output, not our code.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait RepairBackend: Send + Sync {
    /// Pool names in priority order; they fingerprint verdicts and stamps.
    fn servers(&self) -> Vec<String>;
    /// Fetch and decode one article at the repair priority tier.
    async fn fetch_repair(&self, message_id: &str) -> crate::error::Result<Vec<u8>>;
}

#[async_trait]
impl RepairBackend for UsenetArticleProvider {
    fn servers(&self) -> Vec<String> {
        (0..self.pool_count()).map(|i| self.pool_name(i)).collect()
    }

    async fn fetch_repair(&self, message_id: &str) -> crate::error::Result<Vec<u8>> {
        self.fetch_decoded_repair(message_id).await
    }
}

/// One recovery set of a release to repair.
#[derive(Debug, Clone)]
pub struct RepairTarget {
    /// Set key, as the census names it; one job per key.
    pub key: String,
    /// The release (history item) the patches belong to.
    pub release: Uuid,
    /// The NZB blob the release was imported from; stamps the patches.
    pub nzb_blob_id: Option<Uuid>,
    /// The set as recorded at import.
    pub record: Par2SetRecord,
}

/// How a job ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairOutcome {
    /// Slices rebuilt; `bytes_kept` of lost article data went to the store.
    Repaired { slices: usize, bytes_kept: u64 },
    /// Every slice read and verified: nothing to repair.
    Intact,
    /// Terminal: the release carries too little recovery data. Recorded as
    /// the release's verdict.
    Unrepairable { diagnosis: String },
    /// This attempt failed; the set cools down and a later one may succeed.
    Failed { reason: String },
}

/// Why a job was not scheduled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    Disabled,
    CoolingDown(Duration),
    /// A terminal verdict within its TTL, against the same servers.
    Settled(ReleaseVerdict),
}

/// What [`RepairService::schedule`] did.
#[derive(Debug)]
pub enum Scheduled {
    /// A new job, queued; the receiver yields its outcome.
    Queued(watch::Receiver<Option<RepairOutcome>>),
    /// The set already has a job: the damage was folded into it.
    Folded(watch::Receiver<Option<RepairOutcome>>),
    Skipped(SkipReason),
}

type Switch = Arc<dyn Fn() -> bool + Send + Sync>;

/// Schedules and runs repair jobs, one at a time.
pub struct RepairService {
    backend: Arc<dyn RepairBackend>,
    ledger: Arc<dyn HoleLedger>,
    patches: Arc<PatchStore>,
    config: RepairConfig,
    switch: Option<Switch>,
    schedule: Mutex<Schedule>,
}

#[derive(Default)]
struct Schedule {
    queue: VecDeque<String>,
    /// Queued and running jobs by set key: the `repairs` map.
    jobs: HashMap<String, Job>,
    running: Option<String>,
    worker: bool,
    cooldowns: HashMap<String, Instant>,
}

struct Job {
    target: RepairTarget,
    /// Damage was reported while the job ran.
    folded: bool,
    done: watch::Sender<Option<RepairOutcome>>,
}

impl RepairService {
    pub fn new(
        backend: Arc<dyn RepairBackend>,
        ledger: Arc<dyn HoleLedger>,
        patches: Arc<PatchStore>,
        config: RepairConfig,
    ) -> Self {
        Self {
            backend,
            ledger,
            patches,
            config,
            switch: None,
            schedule: Mutex::new(Schedule::default()),
        }
    }

    /// Ask `switch` whether repair is on each time a job is scheduled, so a
    /// settings change applies without a restart.
    pub fn with_switch(mut self, switch: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        self.switch = Some(Arc::new(switch));
        self
    }

    pub fn enabled(&self) -> bool {
        self.switch.as_ref().map_or(self.config.enabled, |s| s())
    }

    /// The completion signal of a set's queued or running job.
    pub fn signal(&self, key: &str) -> Option<watch::Receiver<Option<RepairOutcome>>> {
        self.schedule
            .lock()
            .jobs
            .get(key)
            .map(|j| j.done.subscribe())
    }

    /// Wait for a set's queued or running job. `None` if it has none.
    pub async fn wait(&self, key: &str) -> Option<RepairOutcome> {
        let mut rx = self.signal(key)?;
        rx.wait_for(Option::is_some).await.ok()?.clone()
    }

    /// Time left on a set's cooldown, if any.
    pub fn cooldown_remaining(&self, key: &str) -> Option<Duration> {
        let now = Instant::now();
        let mut schedule = self.schedule.lock();
        match schedule.cooldowns.get(key) {
            Some(until) if *until > now => Some(*until - now),
            Some(_) => {
                schedule.cooldowns.remove(key);
                None
            }
            None => None,
        }
    }

    /// The release's verdict, if it still stops new attempts: reached
    /// within the TTL, against the same servers, for the same NZB.
    fn settled(&self, target: &RepairTarget) -> Option<ReleaseVerdict> {
        let verdict = self.patches.verdict(target.release)?;
        let same_nzb = self
            .patches
            .stamp(target.release)
            .is_none_or(|s| s.nzb_blob_id == target.nzb_blob_id);
        let fresh =
            now_ms().saturating_sub(verdict.at_ms) < self.config.verdict_ttl.as_millis() as u64;
        (same_nzb && fresh && verdict.applies_to(&self.backend.servers())).then_some(verdict)
    }

    /// Schedule a repair of `target`'s set.
    pub fn schedule(self: &Arc<Self>, target: RepairTarget) -> Scheduled {
        if !self.enabled() {
            return Scheduled::Skipped(SkipReason::Disabled);
        }
        {
            let mut schedule = self.schedule.lock();
            let running = schedule.running.as_deref() == Some(target.key.as_str());
            if let Some(job) = schedule.jobs.get_mut(&target.key) {
                debug!(key = %target.key, running, "repair: damage folded into the set's job");
                job.folded |= running;
                job.target = target;
                return Scheduled::Folded(job.done.subscribe());
            }
        }
        if let Some(remaining) = self.cooldown_remaining(&target.key) {
            return Scheduled::Skipped(SkipReason::CoolingDown(remaining));
        }
        if let Some(verdict) = self.settled(&target) {
            debug!(key = %target.key, diagnosis = %verdict.diagnosis, "repair: release already settled");
            return Scheduled::Skipped(SkipReason::Settled(verdict));
        }

        let mut schedule = self.schedule.lock();
        if let Some(job) = schedule.jobs.get(&target.key) {
            // Scheduled by another caller in between.
            return Scheduled::Folded(job.done.subscribe());
        }
        let (done, rx) = watch::channel(None);
        let key = target.key.clone();
        schedule.jobs.insert(
            key.clone(),
            Job {
                target,
                folded: false,
                done,
            },
        );
        schedule.queue.push_back(key);
        if !schedule.worker {
            schedule.worker = true;
            tokio::spawn(Arc::clone(self).work());
        }
        Scheduled::Queued(rx)
    }

    /// Run queued jobs one at a time until the queue is empty.
    async fn work(self: Arc<Self>) {
        loop {
            let (key, target) = {
                let mut schedule = self.schedule.lock();
                let Some(key) = schedule.queue.pop_front() else {
                    schedule.worker = false;
                    return;
                };
                let target = schedule.jobs[&key].target.clone();
                schedule.running = Some(key.clone());
                (key, target)
            };
            let outcome = Arc::clone(&self).run_job(&key, target).await;
            let mut schedule = self.schedule.lock();
            schedule.running = None;
            if matches!(outcome, RepairOutcome::Failed { .. }) {
                schedule
                    .cooldowns
                    .insert(key.clone(), Instant::now() + self.config.cooldown);
            }
            if let Some(job) = schedule.jobs.remove(&key) {
                job.done.send_replace(Some(outcome));
            }
        }
    }

    /// Attempt a job, again while damage was folded into it and the last
    /// attempt succeeded, all within the job's deadline.
    async fn run_job(self: Arc<Self>, key: &str, mut target: RepairTarget) -> RepairOutcome {
        let timeout = self.config.timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let this = Arc::clone(&self);
            let attempt = async move {
                let outcome = this.attempt(&target).await;
                (outcome, target)
            };
            let mut job = tokio::spawn(attempt);
            let outcome = match tokio::time::timeout_at(deadline, &mut job).await {
                Ok(Ok((outcome, t))) => {
                    target = t;
                    outcome
                }
                Ok(Err(e)) => {
                    return RepairOutcome::Failed {
                        reason: format!("repair job panicked: {e}"),
                    };
                }
                Err(_) => {
                    job.abort();
                    warn!(key, ?timeout, "repair: job timed out");
                    return RepairOutcome::Failed {
                        reason: format!("timed out after {timeout:?}"),
                    };
                }
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(reason) => RepairOutcome::Failed { reason },
            };
            if let RepairOutcome::Failed { reason } = &outcome {
                warn!(key, %reason, "repair: attempt failed, cooling down");
            }
            let again = {
                let mut schedule = self.schedule.lock();
                match schedule.jobs.get_mut(key) {
                    Some(job) if job.folded => {
                        job.folded = false;
                        target = job.target.clone();
                        true
                    }
                    _ => false,
                }
            };
            let succeeded = matches!(
                outcome,
                RepairOutcome::Repaired { .. } | RepairOutcome::Intact
            );
            if !(again && succeeded) {
                return outcome;
            }
            debug!(key, "repair: damage found while running, going again");
        }
    }

    /// One attempt at repairing a set. `Err` fails the attempt.
    async fn attempt(&self, target: &RepairTarget) -> Result<RepairOutcome, String> {
        let started = Instant::now();
        let servers = self.backend.servers();
        let stamp = PatchStamp {
            nzb_blob_id: target.nzb_blob_id,
            server_fingerprint: server_fingerprint(&servers),
        };
        let plan = Plan::new(&target.record)?;
        let mut known = self.known_missing(&plan);
        let mut want = known.len() + self.config.extra_rows;
        let mut announced = false;
        let mut previous_rows = None;

        for pass in 1..=self.config.max_passes.max(1) {
            let rows = self.load_rows(&plan, want).await?;
            let row_count = rows.blocks.len();
            if rows.exhausted && known.len() > row_count {
                return Ok(self.settle_unrepairable(
                    target,
                    &plan,
                    &stamp,
                    &servers,
                    known.len(),
                    row_count,
                ));
            }
            if previous_rows.is_some_and(|p| row_count <= p) {
                // Asking for more rows found none: reading again would not help.
                return Err(format!(
                    "{} damaged slices, and the volumes that could be read hold {row_count} recovery blocks",
                    known.len()
                ));
            }
            previous_rows = Some(row_count);
            if !announced {
                announced = true;
                info!(
                    "repair: {}: {} missing slices, {} recovery blocks loaded",
                    plan.label,
                    known.len(),
                    row_count
                );
            }

            let repair = match row_count {
                0 => None,
                _ => Some(
                    Repair::new_owning(rows.blocks, plan.slice_size(), plan.total_slices())
                        .map_err(|e| format!("recovery rows unusable: {e}"))?,
                ),
            };
            let scan = self.scan(&plan, repair).await?;
            let certain: BTreeSet<usize> = scan.missing.union(&plan.absent).copied().collect();
            let damage: Vec<usize> = certain.union(&scan.unknown).copied().collect();
            debug!(
                key = %target.key,
                pass,
                missing = certain.len(),
                unknown = scan.unknown.len(),
                rows = row_count,
                "repair: slices read"
            );

            if damage.is_empty() {
                self.record_holes(target, &plan, &stamp);
                return Ok(RepairOutcome::Intact);
            }
            if let Some(repair) = scan.repair.filter(|_| damage.len() <= row_count) {
                let (slices, bytes_kept) = self
                    .solve_and_patch(target, &plan, &stamp, repair, &damage, &scan.holes)
                    .await?;
                info!(
                    "repair: {}: rebuilt {} slices in {:.1?}, kept {} bytes",
                    plan.label,
                    slices,
                    started.elapsed(),
                    bytes_kept
                );
                return Ok(RepairOutcome::Repaired { slices, bytes_kept });
            }

            // Fewer rows than damaged slices.
            if rows.exhausted {
                if certain.len() > row_count {
                    return Ok(self.settle_unrepairable(
                        target,
                        &plan,
                        &stamp,
                        &servers,
                        certain.len(),
                        row_count,
                    ));
                }
                return Err(format!(
                    "{} slices lost and {} unknown, the release carries {row_count} recovery blocks",
                    certain.len(),
                    scan.unknown.len()
                ));
            }
            if rows.budget_bound {
                return Err(format!(
                    "{} damaged slices need more recovery rows than the {} MiB row budget holds",
                    damage.len(),
                    self.config.max_row_bytes / MIB
                ));
            }
            known = certain;
            want = damage.len() + self.config.extra_rows;
        }
        Err(format!(
            "no pass loaded enough recovery rows in {} tries",
            self.config.max_passes
        ))
    }

    /// Slices known lost before reading: those of files the release does not
    /// carry, and those under confirmed holes the store has no patch for.
    fn known_missing(&self, plan: &Plan) -> BTreeSet<usize> {
        let mut known = plan.absent.clone();
        for (fi, file) in plan.files.iter().enumerate() {
            let Some(carried) = &file.carried else {
                continue;
            };
            for article in &carried.articles {
                if self.ledger.state(&article.message_id) == Some(HoleState::Confirmed)
                    && !self.patches.covers(&article.message_id)
                {
                    known.extend(plan.slices_of(fi, article.offset, article.len));
                }
            }
        }
        known
    }

    /// Record the release's confirmed holes among the set's articles.
    fn record_holes(&self, target: &RepairTarget, plan: &Plan, stamp: &PatchStamp) {
        let ids: HashSet<&str> = plan
            .files
            .iter()
            .filter_map(|f| f.carried.as_ref())
            .flat_map(|f| f.articles.iter().map(|a| a.message_id.as_str()))
            .collect();
        let holes: Vec<_> = self
            .ledger
            .holes()
            .into_iter()
            .filter(|h| ids.contains(h.message_id.as_str()))
            .collect();
        if let Err(e) = self.patches.record_holes(target.release, stamp, &holes) {
            warn!(key = %target.key, error = %e, "repair: could not record holes");
        }
    }

    /// Record the terminal verdict of a set the release cannot repair.
    fn settle_unrepairable(
        &self,
        target: &RepairTarget,
        plan: &Plan,
        stamp: &PatchStamp,
        servers: &[String],
        lost: usize,
        rows: usize,
    ) -> RepairOutcome {
        let diagnosis = format!(
            "beyond_repair: {lost} slices lost, {rows} recovery blocks; {}",
            plan.shape()
        );
        info!("repair: {}: {}", plan.label, diagnosis);
        self.record_holes(target, plan, stamp);
        let verdict = ReleaseVerdict::new(diagnosis.clone(), servers);
        if let Err(e) = self.patches.record_verdict(target.release, stamp, verdict) {
            warn!(key = %target.key, error = %e, "repair: could not record the verdict");
        }
        RepairOutcome::Unrepairable { diagnosis }
    }

    /// Load up to `want` rows, lowest exponents first.
    async fn load_rows(&self, plan: &Plan, want: usize) -> Result<Rows, String> {
        let budget = RecoveryBudget {
            max_rows: Some(want),
            max_bytes: Some(self.config.max_row_bytes),
        };
        let mut reader =
            RecoveryReader::new(plan.set.recovery_set_id, plan.set.slice_size).with_budget(budget);
        let limit = reader.row_limit();
        let mut all_read = true;
        for volume in &plan.volumes {
            if reader.kept() >= want.min(limit) {
                all_read = false;
                break;
            }
            let (back, scan) = self.read_volume(reader, &volume.segment_ids).await?;
            reader = back;
            if !scan.read_to_end() {
                all_read = false;
                warn!(
                    volume = %volume.name,
                    end = %scan.end,
                    "repair: recovery volume not read to the end"
                );
            }
            debug!(
                volume = %volume.name,
                seen = scan.seen,
                kept = scan.kept,
                corrupt = scan.corrupt,
                "repair: recovery volume read"
            );
        }
        let kept = reader.kept();
        Ok(Rows {
            // Nothing was dropped for the budget only while under its limit.
            exhausted: all_read && kept < limit,
            budget_bound: kept >= limit && limit < want,
            blocks: reader.into_blocks(),
        })
    }

    /// Stream one volume's articles through the PAR2 packet reader, which
    /// runs on a blocking thread so a large volume streams in constant
    /// memory. An article that fails to fetch ends the volume there.
    async fn read_volume(
        &self,
        mut reader: RecoveryReader,
        ids: &[String],
    ) -> Result<(RecoveryReader, VolumeScan), String> {
        let (tx, rx) = mpsc::channel(VOLUME_READ_AHEAD);
        let parse = tokio::task::spawn_blocking(move || {
            let scan = reader.read_volume(ChannelReader::new(rx));
            (reader, scan)
        });
        // Each fetch owns what it uses, so the job future stays `Send`.
        let backend = Arc::clone(&self.backend);
        let mut fetches = futures::stream::iter(ids.to_vec())
            .map(move |id| {
                let backend = Arc::clone(&backend);
                async move { backend.fetch_repair(&id).await }
            })
            .buffered(VOLUME_READ_AHEAD);
        while let Some(fetched) = fetches.next().await {
            let item = fetched.map_err(|e| io::Error::other(e.to_string()));
            let failed = item.is_err();
            // The reader hangs up when it stops early (the volume cap).
            if tx.send(item).await.is_err() || failed {
                break;
            }
        }
        drop(tx);
        parse
            .await
            .map_err(|e| format!("recovery volume reader failed: {e}"))
    }

    /// Read and verify every slice of every carried file, feeding the good
    /// ones to `repair`. Once the damage outgrows its rows the solver is
    /// dropped and the read only counts.
    async fn scan(&self, plan: &Plan, repair: Option<Repair>) -> Result<Scan, String> {
        let rows = repair.as_ref().map_or(0, Repair::rows);
        let slice_size = plan.set.slice_size;
        let batch_size = ((self.config.prefetch_bytes / slice_size.max(1)) as usize)
            .clamp(1, self.config.prefetch_slices.max(1));
        let mut scan = Scan {
            repair,
            missing: BTreeSet::new(),
            unknown: BTreeSet::new(),
            holes: Vec::new(),
            batch: Vec::with_capacity(batch_size),
            batch_size,
            rows,
            absent: plan.absent.len(),
        };
        scan.check_overflow();

        for (fi, file) in plan.files.iter().enumerate() {
            let Some(carried) = &file.carried else {
                continue;
            };
            let block = &plan.map.files[file.block];
            let par2 = &plan.set.files[&block.file_id];
            let part = carried
                .articles
                .iter()
                .map(|a| a.len)
                .max()
                .unwrap_or(1)
                .max(1);
            let window = ((self.config.prefetch_bytes / part) as usize)
                .clamp(1, self.config.prefetch_slices.max(1));
            let mut assembler = Assembler::new(slice_size, block.file_size);

            let ids: Vec<String> = carried
                .articles
                .iter()
                .map(|a| a.message_id.clone())
                .collect();
            let reader = ArticleReader {
                backend: Arc::clone(&self.backend),
                ledger: Arc::clone(&self.ledger),
                patches: Arc::clone(&self.patches),
            };
            let mut reads = futures::stream::iter(ids.into_iter().enumerate())
                .map(move |(ai, id)| {
                    let reader = reader.clone();
                    async move { (ai, reader.read(&id).await) }
                })
                .buffered(window);
            while let Some((ai, read)) = reads.next().await {
                let article = &carried.articles[ai];
                if matches!(read, ArticleRead::Hole) {
                    scan.holes.push((fi, ai));
                }
                for (local, slice) in assembler.push(article.offset, article.len, &read) {
                    let global = block.start_block as usize + local as usize;
                    match slice {
                        Slice::Hole => {
                            scan.missing.insert(global);
                        }
                        Slice::Unknown => {
                            scan.unknown.insert(global);
                        }
                        Slice::Data(data) => {
                            if par2.slice_matches(local as usize, &data, slice_size) {
                                scan.batch.push((global, data));
                            } else {
                                debug!(file = %block.filename, local, "repair: slice failed its checksum");
                                scan.unknown.insert(global);
                            }
                        }
                    }
                    scan.check_overflow();
                    if scan.batch.len() >= scan.batch_size {
                        scan.feed().await?;
                    }
                }
            }
            // Slices the articles never reached.
            for local in assembler.next..block.block_count {
                scan.unknown
                    .insert(block.start_block as usize + local as usize);
            }
            scan.check_overflow();
        }
        scan.feed().await?;
        Ok(scan)
    }

    /// Solve for the damaged slices, check them, and store the spans of the
    /// hole articles. Returns the slices rebuilt and the bytes stored.
    async fn solve_and_patch(
        &self,
        target: &RepairTarget,
        plan: &Plan,
        stamp: &PatchStamp,
        repair: Repair,
        damage: &[usize],
        holes: &[(usize, usize)],
    ) -> Result<(usize, u64), String> {
        let missing = damage.to_vec();
        let solved = tokio::task::spawn_blocking(move || repair.solve(&missing))
            .await
            .map_err(|e| format!("solver failed: {e}"))?
            .map_err(|e| format!("solve failed: {e}"))?;

        for &global in damage {
            let loc = plan
                .map
                .global_to_file(global)
                .ok_or_else(|| format!("slice {global} outside the set"))?;
            let file = &plan.set.files[&loc.file.file_id];
            let data = &solved[&global][..loc.len];
            if !file.slice_matches(loc.local_index as usize, data, plan.set.slice_size) {
                return Err(format!(
                    "rebuilt slice {} of {} does not match its checksum",
                    loc.local_index, loc.file.filename
                ));
            }
        }

        let slice_size = plan.set.slice_size;
        let mut bytes_kept = 0u64;
        for &(fi, ai) in holes {
            let file = &plan.files[fi];
            let carried = file.carried.as_ref().expect("holes are in carried files");
            let article = &carried.articles[ai];
            if article.len == 0 {
                continue;
            }
            let start = plan.map.files[file.block].start_block as usize;
            let mut bytes = Vec::with_capacity(article.len as usize);
            let mut pos = article.offset;
            let end = article.offset + article.len;
            while pos < end {
                let local = pos / slice_size;
                let slice_end = ((local + 1) * slice_size).min(end);
                let data = solved.get(&(start + local as usize)).ok_or_else(|| {
                    format!("hole slice {local} of {} was not solved", carried.name)
                })?;
                let from = (pos - local * slice_size) as usize;
                let to = (slice_end - local * slice_size) as usize;
                bytes.extend_from_slice(&data[from..to]);
                pos = slice_end;
            }
            let len = bytes.len() as u64;
            self.patches
                .insert(target.release, stamp, carried, article.offset, bytes)
                .await
                .map_err(|e| format!("patch of {} not stored: {e}", article.message_id))?;
            bytes_kept += len;
        }
        self.record_holes(target, plan, stamp);
        Ok((damage.len(), bytes_kept))
    }
}

/// Reads data articles for a scan; owns what it uses, so the job future
/// stays `Send`.
#[derive(Clone)]
struct ArticleReader {
    backend: Arc<dyn RepairBackend>,
    ledger: Arc<dyn HoleLedger>,
    patches: Arc<PatchStore>,
}

impl ArticleReader {
    /// Fetch one data article at the repair tier. A confirmed hole the store
    /// already patched is read from the patch (the checksum still decides).
    async fn read(&self, message_id: &str) -> ArticleRead {
        match self.backend.fetch_repair(message_id).await {
            Ok(bytes) => ArticleRead::Data(bytes),
            Err(StreamError::ArticleNotFound(_)) => {
                if let Some(bytes) = self.patches.read_article(message_id).await {
                    return ArticleRead::Data(bytes);
                }
                if self.ledger.state(message_id) == Some(HoleState::Confirmed) {
                    ArticleRead::Hole
                } else {
                    ArticleRead::Unknown
                }
            }
            Err(e) => {
                debug!(message_id, error = %e, "repair: article unreadable, slices unknown");
                ArticleRead::Unknown
            }
        }
    }
}

/// What one pass read.
struct Scan {
    /// `None` once the damage outgrew the rows.
    repair: Option<Repair>,
    /// Slices under confirmed holes.
    missing: BTreeSet<usize>,
    /// Slices that failed to read or to verify.
    unknown: BTreeSet<usize>,
    /// Hole articles, as (plan file, article) indices.
    holes: Vec<(usize, usize)>,
    batch: Vec<(usize, Vec<u8>)>,
    batch_size: usize,
    rows: usize,
    /// Slices of files the release does not carry.
    absent: usize,
}

impl Scan {
    /// Drop the solver once the damage outgrows its rows: the read goes on
    /// to count, so the next pass knows how many rows to load.
    fn check_overflow(&mut self) {
        let damage = self.missing.len() + self.unknown.len() + self.absent;
        if self.repair.is_some() && damage > self.rows {
            debug!(damage, rows = self.rows, "repair: damage outgrew the rows");
            self.repair = None;
            self.batch.clear();
        }
    }

    /// Feed the batch to the solver on a blocking thread.
    async fn feed(&mut self) -> Result<(), String> {
        let batch = std::mem::take(&mut self.batch);
        let Some(mut repair) = self.repair.take() else {
            return Ok(());
        };
        if batch.is_empty() {
            self.repair = Some(repair);
            return Ok(());
        }
        let (repair, fed) = tokio::task::spawn_blocking(move || {
            let pairs: Vec<(usize, &[u8])> =
                batch.iter().map(|(i, d)| (*i, d.as_slice())).collect();
            let fed = repair.feed_batch(&pairs);
            (repair, fed)
        })
        .await
        .map_err(|e| format!("feeding slices failed: {e}"))?;
        fed.map_err(|e| format!("feeding slices failed: {e}"))?;
        self.repair = Some(repair);
        self.batch = Vec::with_capacity(self.batch_size);
        Ok(())
    }
}

/// Recovery rows a pass loaded.
struct Rows {
    blocks: Vec<RecoveryBlock>,
    /// Every volume was read to its end and no row was dropped: these are
    /// all the rows the release carries.
    exhausted: bool,
    /// The byte budget, not the want, capped the rows.
    budget_bound: bool,
}

/// A set ready to repair: the parsed index and where the release carries
/// each file.
struct Plan {
    set: Par2FileSet,
    map: BlockMap,
    /// In the set's global slice order.
    files: Vec<PlanFile>,
    /// Volumes, lowest first exponent first.
    volumes: Vec<PlanVolume>,
    /// Slices of files the release does not carry.
    absent: BTreeSet<usize>,
    label: String,
}

struct PlanFile {
    /// Index into the block map's files.
    block: usize,
    carried: Option<CensusFile>,
}

struct PlanVolume {
    name: String,
    segment_ids: Vec<String>,
}

impl Plan {
    fn new(record: &Par2SetRecord) -> Result<Self, String> {
        let mut cursor = Cursor::new(&record.index_packets);
        let set = rust_par2::parse_par2_reader(&mut cursor, record.index_packets.len() as u64)
            .map_err(|e| format!("PAR2 index does not parse: {e}"))?;
        if set.slice_size == 0 || set.files.is_empty() {
            return Err("PAR2 index has no files".into());
        }
        let map = BlockMap::new(&set);
        let mut files = Vec::with_capacity(map.files.len());
        let mut absent = BTreeSet::new();
        for (block, entry) in map.files.iter().enumerate() {
            let carried = record
                .files
                .iter()
                .find(|f| f.file_id == entry.file_id)
                .and_then(|f| census_file(f, record.part_size));
            if carried.is_none() {
                let start = entry.start_block as usize;
                absent.extend(start..start + entry.block_count as usize);
            }
            files.push(PlanFile { block, carried });
        }
        let mut volumes: Vec<_> = record.volumes.iter().collect();
        volumes.sort_by(|a, b| {
            (a.first_exponent.unwrap_or(u32::MAX), &a.name)
                .cmp(&(b.first_exponent.unwrap_or(u32::MAX), &b.name))
        });
        let volumes = volumes
            .into_iter()
            .map(|v| PlanVolume {
                name: v.name.clone(),
                segment_ids: v.segment_ids.clone(),
            })
            .collect();
        let label = map
            .files
            .first()
            .map_or_else(|| "PAR2 set".to_string(), |f| f.filename.clone());
        Ok(Self {
            set,
            map,
            files,
            volumes,
            absent,
            label,
        })
    }

    fn slice_size(&self) -> usize {
        self.set.slice_size as usize
    }

    fn total_slices(&self) -> usize {
        self.map.total_blocks as usize
    }

    /// Global indices of the slices a byte span of a plan file touches.
    fn slices_of(&self, file: usize, offset: u64, len: u64) -> impl Iterator<Item = usize> {
        let block = &self.map.files[self.files[file].block];
        let slice = self.set.slice_size;
        let count = u64::from(block.block_count);
        let range = if len == 0 || count == 0 {
            0..0
        } else {
            let first = (offset / slice).min(count - 1);
            let last = ((offset + len - 1) / slice).min(count - 1);
            first..last + 1
        };
        let start = block.start_block as usize;
        range.map(move |s| start + s as usize)
    }

    /// The shape diagnosis: how much the set covers and how much of it this
    /// release carries.
    fn shape(&self) -> String {
        let total: u64 = self.map.files.iter().map(|f| f.file_size).sum();
        let carried = self.files.iter().filter(|f| f.carried.is_some()).count();
        format!(
            "the recovery set covers {} files of {} MiB and this document carries {carried} of them",
            self.map.files.len(),
            total.div_ceil(MIB)
        )
    }
}

/// One data article as the repair job read it.
enum ArticleRead {
    Data(Vec<u8>),
    /// A confirmed hole: lost.
    Hole,
    /// Failed for any other reason: may be there.
    Unknown,
}

/// A slice the assembler finished.
enum Slice {
    Data(Vec<u8>),
    Hole,
    Unknown,
}

/// Cuts a file's articles, in order, into its PAR2 slices.
struct Assembler {
    slice_size: u64,
    file_size: u64,
    /// Next slice to finish, and the file position reached in it.
    next: u32,
    pos: u64,
    buf: Vec<u8>,
    hole: bool,
    unknown: bool,
}

impl Assembler {
    fn new(slice_size: u64, file_size: u64) -> Self {
        Self {
            slice_size,
            file_size,
            next: 0,
            pos: 0,
            buf: Vec::new(),
            hole: false,
            unknown: false,
        }
    }

    /// Take the next article; returns the slices it finished, by local index.
    fn push(&mut self, offset: u64, len: u64, read: &ArticleRead) -> Vec<(u32, Slice)> {
        let mut done = Vec::new();
        let end = offset.saturating_add(len).min(self.file_size);
        if end <= self.pos || offset >= end {
            return done;
        }
        if offset > self.pos {
            // The spans skip bytes: whatever lies between is unknown.
            self.unknown = true;
            while self.slice_end() <= offset {
                self.finish(&mut done);
            }
            self.pos = offset;
        }
        let data = match read {
            ArticleRead::Data(bytes) if bytes.len() as u64 == len => Some(bytes.as_slice()),
            _ => None,
        };
        while self.pos < end {
            let take_end = end.min(self.slice_end());
            match (data, read) {
                (Some(bytes), _) => self.buf.extend_from_slice(
                    &bytes[(self.pos - offset) as usize..(take_end - offset) as usize],
                ),
                (None, ArticleRead::Hole) => self.hole = true,
                (None, _) => self.unknown = true,
            }
            self.pos = take_end;
            if self.pos == self.slice_end() {
                self.finish(&mut done);
            }
        }
        done
    }

    /// End of the slice being assembled.
    fn slice_end(&self) -> u64 {
        ((u64::from(self.next) + 1) * self.slice_size).min(self.file_size)
    }

    fn finish(&mut self, done: &mut Vec<(u32, Slice)>) {
        let slice = if self.hole {
            Slice::Hole
        } else if self.unknown {
            Slice::Unknown
        } else {
            Slice::Data(std::mem::take(&mut self.buf))
        };
        done.push((self.next, slice));
        self.next += 1;
        self.buf.clear();
        self.hole = false;
        self.unknown = false;
    }
}

/// Blocking [`Read`] over article bodies arriving on a channel. An error
/// item fails the read; the channel closing is the end of the volume.
struct ChannelReader {
    rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    buf: Vec<u8>,
    pos: usize,
}

impl ChannelReader {
    fn new(rx: mpsc::Receiver<io::Result<Vec<u8>>>) -> Self {
        Self {
            rx,
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        while self.pos == self.buf.len() {
            match self.rx.blocking_recv() {
                Some(Ok(bytes)) => {
                    self.buf = bytes;
                    self.pos = 0;
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(0),
            }
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(n: usize) -> ArticleRead {
        ArticleRead::Data(vec![7; n])
    }

    fn kinds(slices: Vec<(u32, Slice)>) -> Vec<(u32, &'static str)> {
        slices
            .into_iter()
            .map(|(i, s)| {
                let kind = match s {
                    Slice::Data(_) => "data",
                    Slice::Hole => "hole",
                    Slice::Unknown => "unknown",
                };
                (i, kind)
            })
            .collect()
    }

    #[test]
    fn assembler_cuts_articles_into_slices() {
        // Slices of 100 over a 250-byte file; articles of 60.
        let mut a = Assembler::new(100, 250);
        assert!(a.push(0, 60, &data(60)).is_empty());
        assert_eq!(kinds(a.push(60, 60, &ArticleRead::Hole)), vec![(0, "hole")]);
        assert_eq!(kinds(a.push(120, 60, &data(60))), vec![]);
        assert_eq!(
            kinds(a.push(180, 60, &ArticleRead::Unknown)),
            vec![(1, "hole")]
        );
        assert_eq!(kinds(a.push(240, 10, &data(10))), vec![(2, "unknown")]);
        assert_eq!(a.next, 3);
    }

    #[test]
    fn assembler_returns_slice_bytes_and_flags_short_articles() {
        let mut a = Assembler::new(4, 8);
        let out = a.push(0, 8, &ArticleRead::Data((0..8).collect()));
        let bytes: Vec<Vec<u8>> = out
            .into_iter()
            .map(|(_, s)| match s {
                Slice::Data(d) => d,
                _ => panic!("expected data"),
            })
            .collect();
        assert_eq!(bytes, vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]]);

        let mut a = Assembler::new(4, 8);
        assert_eq!(
            kinds(a.push(0, 8, &data(5))),
            vec![(0, "unknown"), (1, "unknown")]
        );
    }

    #[test]
    fn channel_reader_ends_on_close_and_fails_on_error() {
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(Ok(vec![1, 2])).unwrap();
        tx.try_send(Ok(vec![3])).unwrap();
        drop(tx);
        let mut out = Vec::new();
        ChannelReader::new(rx).read_to_end(&mut out).unwrap();
        assert_eq!(out, vec![1, 2, 3]);

        let (tx, rx) = mpsc::channel(4);
        tx.try_send(Ok(vec![1])).unwrap();
        tx.try_send(Err(io::Error::other("gone"))).unwrap();
        let mut out = Vec::new();
        assert!(ChannelReader::new(rx).read_to_end(&mut out).is_err());
    }
}
