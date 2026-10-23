//! The patch store: byte spans the repair job rebuilt from PAR2, served in
//! place of a confirmed hole, with the release's hole records and terminal
//! verdict.
//!
//! Semantics follow zurg's `patchdisk.go`:
//!
//! - A patch is keyed by release (the history item id), file (the name the
//!   PAR2 set gives it) and a byte range in that file. Spans of one file
//!   never overlap; re-inserting the exact same range replaces it.
//! - Every release is stamped with the NZB blob it was rebuilt from and the
//!   server fingerprint at the time. A patch inserted under a different NZB
//!   blob drops the release's older patches: they were rebuilt from another
//!   NZB and may not match the articles any more.
//! - The store has a byte budget. Past it, whole releases are evicted, least
//!   recently read first, so a release's patches go together. The budget
//!   counts the span bytes and what the store holds in memory to find them:
//!   the per-file article index, hole records and verdicts.
//! - The read path asks by article: [`PatchStore::read_article`] returns the
//!   article's decoded bytes when patches cover its whole span in the file,
//!   and nothing otherwise, so a reader never sees a torn article.
//! - A release also keeps its confirmed holes ([`StoredHole`]) and its
//!   terminal [`ReleaseVerdict`], so the next process knows them without
//!   asking the servers again (zurg measured 934 hopeless repair attempts
//!   over 508 releases in three hours while verdicts lived in memory only).
//!
//! Patches are typically megabytes and must survive a restart (the repair
//! that produced them costs a PAR2 volume download and a rebuild), so the
//! store keeps them as files under the data dir rather than in SQLite rows.
//! A span is written to the incoming directory first and renamed into the
//! release under the store's lock, so a crash never leaves a partial span and
//! a span is indexed only once its file is in place. A release is deleted by
//! renaming its directory into the trash under the lock and removing it
//! after, so a write cannot land in a directory that is about to go, and a
//! reader with a span already open is unaffected. An in-memory store with
//! the same rules exists for `par2_patch_cache_mb = -1` and for tests.
//!
//! On-disk layout under the store's directory:
//!
//! ```text
//! <release uuid>/stamp.json
//! <release uuid>/holes.json                 confirmed holes
//! <release uuid>/verdict.json               terminal verdict
//! <release uuid>/<file key>/layout.json     file name and article spans
//! <release uuid>/<file key>/<offset>-<len>.bin
//! .incoming/                                spans being written
//! .trash/                                   releases being deleted
//! ```
//!
//! The file key is a UUIDv5 of the file name, so any name is safe on disk.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::census::{ArticleSpan, CensusFile};
use crate::holes::{HoleRecord, StoredHole, now_ms, server_fingerprint};

/// Default budget, in MiB (`par2_patch_cache_mb`).
pub const DEFAULT_PATCH_CACHE_MB: i64 = 512;

const STAMP_FILE: &str = "stamp.json";
const HOLES_FILE: &str = "holes.json";
const VERDICT_FILE: &str = "verdict.json";
const LAYOUT_FILE: &str = "layout.json";
const SPAN_EXT: &str = "bin";
const TMP_EXT: &str = "tmp";
const INCOMING_DIR: &str = ".incoming";
const TRASH_DIR: &str = ".trash";

/// Budget charged per article of a patched file, on top of its message-id
/// (held twice: in the layout and as the index key) and the file name (held
/// by its index entry).
const ARTICLE_INDEX_COST: u64 = 96;
/// Budget charged per patched file, on top of its name.
const FILE_INDEX_COST: u64 = 128;
/// Budget charged per hole record or verdict, on top of its strings.
const RECORD_COST: u64 = 64;

/// What a release's patches were rebuilt from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchStamp {
    /// The NZB blob the release was imported from. A patch under another
    /// blob replaces the release's patches.
    pub nzb_blob_id: Option<Uuid>,
    /// The servers asked when the hole was confirmed
    /// ([`crate::holes::server_fingerprint`]).
    pub server_fingerprint: String,
}

/// The settled outcome of repairing a release that is not worth trying
/// again: what was found, which servers were asked, and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseVerdict {
    /// What the repair concluded, e.g. `beyond_repair: 12 slices lost, 4
    /// recovery blocks`.
    pub diagnosis: String,
    /// The servers that were asked.
    pub servers: Vec<String>,
    /// When, in milliseconds since the Unix epoch.
    pub at_ms: u64,
}

impl ReleaseVerdict {
    /// A verdict reached now against `servers`.
    pub fn new(diagnosis: impl Into<String>, servers: &[String]) -> Self {
        Self {
            diagnosis: diagnosis.into(),
            servers: servers.to_vec(),
            at_ms: now_ms(),
        }
    }

    /// Whether the verdict was reached against the same set of servers. A
    /// server added since may hold what the others lost.
    pub fn applies_to(&self, servers: &[String]) -> bool {
        server_fingerprint(&self.servers) == server_fingerprint(servers)
    }
}

/// Why an insert was refused.
#[derive(Debug, thiserror::Error)]
pub enum PatchError {
    #[error("patch is empty")]
    Empty,
    #[error("patch {offset}+{len} runs past the end of {file} ({size} bytes)")]
    OutOfFile {
        file: String,
        offset: u64,
        len: u64,
        size: u64,
    },
    #[error("patch {offset}+{len} overlaps an existing span of {file}")]
    Overlap { file: String, offset: u64, len: u64 },
    #[error("release patches would need {needed} bytes, budget is {budget}")]
    OverBudget { needed: u64, budget: u64 },
    #[error("release patches were replaced from another NZB blob meanwhile")]
    Stale,
    #[error("patch store io: {0}")]
    Io(#[from] io::Error),
}

/// Bytes held, for the status surface.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PatchTotals {
    pub releases: usize,
    pub spans: usize,
    /// Span bytes.
    pub patch_bytes: u64,
    /// Article index, hole records and verdicts.
    pub index_bytes: u64,
    /// Held in memory: the index, and the spans of an in-memory store.
    pub memory_bytes: u64,
    /// Span bytes on disk.
    pub disk_bytes: u64,
    pub budget_bytes: u64,
}

#[derive(Debug, Clone)]
enum SpanData {
    Memory(Arc<[u8]>),
    Disk(PathBuf),
}

/// Part of an article: span data, offset into the span, and byte count.
type Piece = (SpanData, u64, u64);

#[derive(Debug, Clone)]
struct Span {
    len: u64,
    data: SpanData,
}

#[derive(Debug)]
struct FilePatches {
    name: String,
    articles: Vec<ArticleSpan>,
    size: u64,
    /// Spans by start offset; never overlapping.
    spans: BTreeMap<u64, Span>,
}

#[derive(Debug)]
struct Release {
    stamp: PatchStamp,
    /// By file name.
    files: HashMap<String, FilePatches>,
    /// Confirmed holes, by message-id order of arrival.
    holes: Vec<StoredHole>,
    verdict: Option<ReleaseVerdict>,
    /// Span bytes.
    data_bytes: u64,
    /// Index, hole and verdict bytes charged to the budget.
    meta_bytes: u64,
    /// Recency on the store's clock; bumped under the read lock.
    last_read: AtomicU64,
}

impl Release {
    fn new(stamp: PatchStamp, now: u64) -> Self {
        Self {
            stamp,
            files: HashMap::new(),
            holes: Vec::new(),
            verdict: None,
            data_bytes: 0,
            meta_bytes: 0,
            last_read: AtomicU64::new(now),
        }
    }

    fn bytes(&self) -> u64 {
        self.data_bytes + self.meta_bytes
    }

    fn files_cost(&self) -> u64 {
        self.files
            .values()
            .map(|f| file_cost(&f.name, &f.articles))
            .sum()
    }

    fn is_empty(&self) -> bool {
        self.files.is_empty() && self.holes.is_empty() && self.verdict.is_none()
    }
}

/// Where an article sits in a patched file.
#[derive(Debug, Clone)]
struct ArticleRef {
    release: Uuid,
    file: String,
    offset: u64,
    len: u64,
}

#[derive(Debug, Default)]
struct Inner {
    releases: HashMap<Uuid, Release>,
    /// Message-id to the patched files it is an article of.
    articles: HashMap<String, Vec<ArticleRef>>,
    data_bytes: u64,
    meta_bytes: u64,
    spans: usize,
}

impl Inner {
    fn bytes(&self) -> u64 {
        self.data_bytes + self.meta_bytes
    }

    fn index_file(&mut self, release: Uuid, name: &str, articles: &[ArticleSpan]) {
        for a in articles {
            let refs = self.articles.entry(a.message_id.clone()).or_default();
            if !refs.iter().any(|r| r.release == release && r.file == name) {
                refs.push(ArticleRef {
                    release,
                    file: name.to_string(),
                    offset: a.offset,
                    len: a.len,
                });
            }
        }
    }

    /// Add a loaded release to the index.
    fn add_release(&mut self, id: Uuid, release: Release) {
        for file in release.files.values() {
            self.index_file(id, &file.name, &file.articles);
            self.spans += file.spans.len();
        }
        self.data_bytes += release.data_bytes;
        self.meta_bytes += release.meta_bytes;
        self.releases.insert(id, release);
    }

    /// Remove a release from the index.
    fn remove_release(&mut self, id: Uuid) -> Option<Release> {
        let release = self.releases.remove(&id)?;
        for file in release.files.values() {
            for a in &file.articles {
                if let Some(refs) = self.articles.get_mut(&a.message_id) {
                    refs.retain(|r| r.release != id);
                    if refs.is_empty() {
                        self.articles.remove(&a.message_id);
                    }
                }
            }
            self.spans -= file.spans.len();
        }
        self.data_bytes -= release.data_bytes;
        self.meta_bytes -= release.meta_bytes;
        Some(release)
    }
}

/// A span's files written to the incoming directory, waiting to be renamed
/// into the release.
#[derive(Debug)]
struct Staged {
    stamp: PathBuf,
    layout: PathBuf,
    span: PathBuf,
}

impl Staged {
    fn discard(&self) {
        for path in [&self.stamp, &self.layout, &self.span] {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// A span's bytes on their way into the index.
enum Payload {
    Memory(Arc<[u8]>),
    Disk(Staged),
}

impl Payload {
    fn discard(&self) {
        if let Payload::Disk(staged) = self {
            staged.discard();
        }
    }
}

/// Patches for repaired spans, with a byte budget and LRU eviction by
/// release. Cheap to share: wrap it in an `Arc`.
pub struct PatchStore {
    /// `None` keeps patches in memory only.
    dir: Option<PathBuf>,
    budget: u64,
    clock: AtomicU64,
    inner: RwLock<Inner>,
}

impl std::fmt::Debug for PatchStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PatchStore")
            .field("dir", &self.dir)
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

impl PatchStore {
    /// A store that keeps patches in memory, up to `budget` bytes.
    pub fn in_memory(budget: u64) -> Self {
        Self {
            dir: None,
            budget,
            clock: AtomicU64::new(0),
            inner: RwLock::new(Inner::default()),
        }
    }

    /// Open (or create) a store under `dir`, loading the patches, holes and
    /// verdicts a previous process left there. Partial writes are removed,
    /// and if the budget shrank, the least recently written releases are
    /// evicted.
    pub fn open(dir: impl Into<PathBuf>, budget: u64) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        for scratch in [INCOMING_DIR, TRASH_DIR] {
            match std::fs::remove_dir_all(dir.join(scratch)) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
        }
        std::fs::create_dir_all(dir.join(INCOMING_DIR))?;
        let store = Self {
            dir: Some(dir.clone()),
            budget,
            clock: AtomicU64::new(0),
            inner: RwLock::new(Inner::default()),
        };
        let mut loaded: Vec<(SystemTime, Uuid, Release)> = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(id) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| Uuid::parse_str(n).ok())
            else {
                continue;
            };
            match load_release(&path) {
                Ok(Some((newest, release))) => loaded.push((newest, id, release)),
                Ok(None) => {
                    let _ = std::fs::remove_dir_all(&path);
                }
                Err(e) => {
                    warn!(release = %id, error = %e, "unreadable patch release, dropping it");
                    let _ = std::fs::remove_dir_all(&path);
                }
            }
        }
        // Oldest first, so the clock orders releases by when they were last
        // written; recency of reads does not survive a restart.
        loaded.sort_by_key(|(t, _, _)| *t);
        let mut trash = Vec::new();
        {
            let mut inner = store.inner.write();
            for (_, id, release) in loaded {
                release.last_read.store(store.tick(), Ordering::Relaxed);
                inner.add_release(id, release);
            }
            store.evict_to_fit(&mut inner, 0, None, &mut trash);
            info!(
                releases = inner.releases.len(),
                bytes = inner.bytes(),
                "patch store loaded"
            );
        }
        empty_trash(trash);
        Ok(store)
    }

    /// Build the store the `par2_patch_cache_mb` setting asks for: `-1`
    /// keeps patches in memory with the default budget, `0` turns the store
    /// off, and a positive value is a disk budget in MiB under `dir`.
    pub fn from_setting(cache_mb: i64, dir: impl Into<PathBuf>) -> io::Result<Option<Self>> {
        const MIB: u64 = 1024 * 1024;
        match cache_mb {
            0 => Ok(None),
            n if n < 0 => Ok(Some(Self::in_memory(DEFAULT_PATCH_CACHE_MB as u64 * MIB))),
            n => Self::open(dir, n as u64 * MIB).map(Some),
        }
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The byte budget.
    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// Store a repaired span of `file` in `release`, starting `offset` bytes
    /// into the file. `file` carries the file's article layout, which is how
    /// a reader that knows only a message-id finds the span.
    ///
    /// Older patches of the release are dropped first when `stamp` names a
    /// different NZB blob. Releases are evicted, least recently read first,
    /// until the budget holds the new span; a release that cannot fit on its
    /// own is refused.
    pub async fn insert(
        &self,
        release: Uuid,
        stamp: &PatchStamp,
        file: &CensusFile,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), PatchError> {
        let len = bytes.len() as u64;
        if len == 0 {
            return Err(PatchError::Empty);
        }
        let size = file_size(file);
        if offset.checked_add(len).is_none_or(|end| end > size) {
            return Err(PatchError::OutOfFile {
                file: file.name.clone(),
                offset,
                len,
                size,
            });
        }

        // Check first, so a refused insert writes nothing.
        let stale = {
            let inner = self.inner.read();
            let existing = inner.releases.get(&release);
            let stale = existing.is_some_and(|r| r.stamp.nzb_blob_id != stamp.nzb_blob_id);
            let current = existing.filter(|_| !stale);
            self.fits(current, file, offset, len)?;
            stale
        };
        if stale {
            info!(%release, "NZB blob changed, dropping the release's patches");
            self.drop_release(release);
        }

        let payload = match &self.dir {
            None => Payload::Memory(bytes.into()),
            Some(dir) => Payload::Disk(stage(dir, stamp, file, &bytes).await?),
        };
        self.commit(release, stamp, file, offset, len, payload)
    }

    /// Whether a span fits `release` (its current state): no overlap, and
    /// the release alone within the budget. Returns the growth in bytes.
    fn fits(
        &self,
        release: Option<&Release>,
        file: &CensusFile,
        offset: u64,
        len: u64,
    ) -> Result<u64, PatchError> {
        let patches = release.and_then(|r| r.files.get(&file.name));
        let replaced = patches.map_or(Ok(0), |f| check_overlap(f, offset, len))?;
        let index = if patches.is_some() {
            0
        } else {
            file_cost(&file.name, &file.articles)
        };
        let growth = len - replaced + index;
        let needed = release.map_or(0, Release::bytes) + growth;
        if needed > self.budget {
            return Err(PatchError::OverBudget {
                needed,
                budget: self.budget,
            });
        }
        Ok(growth)
    }

    /// Index a span: under the write lock, re-check what
    /// [`insert`](Self::insert) checked (another insert may have run in
    /// between), make room, move a staged span into the release, and only
    /// then index it. A refused span's staged files are removed; they never
    /// share a path with a live span.
    fn commit(
        &self,
        release: Uuid,
        stamp: &PatchStamp,
        file: &CensusFile,
        offset: u64,
        len: u64,
        payload: Payload,
    ) -> Result<(), PatchError> {
        let mut trash = Vec::new();
        let result = (|| {
            let mut inner = self.inner.write();
            let existing = inner.releases.get(&release);
            if existing.is_some_and(|r| r.stamp.nzb_blob_id != stamp.nzb_blob_id) {
                return Err(PatchError::Stale);
            }
            let growth = self.fits(existing, file, offset, len)?;
            let new_file = !existing.is_some_and(|r| r.files.contains_key(&file.name));
            self.evict_to_fit(&mut inner, growth, Some(release), &mut trash);

            let data = match &payload {
                Payload::Memory(bytes) => SpanData::Memory(Arc::clone(bytes)),
                Payload::Disk(staged) => {
                    let dir = self.dir.as_deref().expect("disk payload needs a dir");
                    SpanData::Disk(place(dir, release, file, offset, len, staged)?)
                }
            };

            let now = self.tick();
            let entry = inner
                .releases
                .entry(release)
                .or_insert_with(|| Release::new(stamp.clone(), now));
            entry.stamp.server_fingerprint = stamp.server_fingerprint.clone();
            entry.last_read.store(now, Ordering::Relaxed);
            let patches = entry
                .files
                .entry(file.name.clone())
                .or_insert_with(|| FilePatches {
                    name: file.name.clone(),
                    articles: file.articles.clone(),
                    size: file_size(file),
                    spans: BTreeMap::new(),
                });
            let replaced = patches.spans.insert(offset, Span { len, data });
            let data_growth = len - replaced.as_ref().map_or(0, |s| s.len);
            let meta_growth = growth - data_growth;
            entry.data_bytes += data_growth;
            entry.meta_bytes += meta_growth;
            inner.data_bytes += data_growth;
            inner.meta_bytes += meta_growth;
            if replaced.is_none() {
                inner.spans += 1;
            }
            if new_file {
                inner.index_file(release, &file.name, &file.articles);
            }
            debug!(%release, file = %file.name, offset, len, "patch stored");
            Ok(())
        })();
        if result.is_err() {
            payload.discard();
        }
        empty_trash(trash);
        result
    }

    /// Record a release's confirmed holes, so the next process finds them
    /// ([`stored_holes`](Self::stored_holes)). Provisional holes are skipped;
    /// a hole already on record for the release is replaced. Returns how
    /// many were recorded.
    pub fn record_holes(
        &self,
        release: Uuid,
        stamp: &PatchStamp,
        holes: &[HoleRecord],
    ) -> Result<usize, PatchError> {
        let stored: Vec<StoredHole> = holes.iter().filter_map(StoredHole::from_record).collect();
        if stored.is_empty() {
            return Ok(0);
        }
        let n = stored.len();
        self.update_records(release, stamp, |holes, _| {
            for hole in stored {
                match holes.iter_mut().find(|h| h.message_id == hole.message_id) {
                    Some(h) => *h = hole,
                    None => holes.push(hole),
                }
            }
        })?;
        Ok(n)
    }

    /// Record a release's terminal verdict, replacing any earlier one.
    pub fn record_verdict(
        &self,
        release: Uuid,
        stamp: &PatchStamp,
        verdict: ReleaseVerdict,
    ) -> Result<(), PatchError> {
        self.update_records(release, stamp, |_, v| *v = Some(verdict))
    }

    /// Change a release's hole records or verdict, write them out, and
    /// charge them to the budget. Records are small, so they are written
    /// under the lock.
    fn update_records(
        &self,
        release: Uuid,
        stamp: &PatchStamp,
        change: impl FnOnce(&mut Vec<StoredHole>, &mut Option<ReleaseVerdict>),
    ) -> Result<(), PatchError> {
        let stale = self
            .inner
            .read()
            .releases
            .get(&release)
            .is_some_and(|r| r.stamp.nzb_blob_id != stamp.nzb_blob_id);
        if stale {
            info!(%release, "NZB blob changed, dropping the release's patches");
            self.drop_release(release);
        }
        let mut trash = Vec::new();
        let result = (|| {
            let mut inner = self.inner.write();
            let existing = inner.releases.get(&release);
            if existing.is_some_and(|r| r.stamp.nzb_blob_id != stamp.nzb_blob_id) {
                return Err(PatchError::Stale);
            }
            let mut holes = existing.map(|r| r.holes.clone()).unwrap_or_default();
            let mut verdict = existing.and_then(|r| r.verdict.clone());
            change(&mut holes, &mut verdict);
            let meta = existing.map_or(0, Release::files_cost)
                + holes.iter().map(hole_cost).sum::<u64>()
                + verdict.as_ref().map_or(0, verdict_cost);
            let (old_meta, data) = existing.map_or((0, 0), |r| (r.meta_bytes, r.data_bytes));
            if data + meta > self.budget {
                return Err(PatchError::OverBudget {
                    needed: data + meta,
                    budget: self.budget,
                });
            }
            self.evict_to_fit(
                &mut inner,
                meta.saturating_sub(old_meta),
                Some(release),
                &mut trash,
            );
            if let Some(dir) = &self.dir {
                let rel_dir = dir.join(release.to_string());
                std::fs::create_dir_all(&rel_dir)?;
                write_json(&rel_dir.join(STAMP_FILE), stamp)?;
                write_json(&rel_dir.join(HOLES_FILE), &holes)?;
                match &verdict {
                    Some(v) => write_json(&rel_dir.join(VERDICT_FILE), v)?,
                    None => remove_file(&rel_dir.join(VERDICT_FILE))?,
                }
            }
            let now = self.tick();
            let entry = inner
                .releases
                .entry(release)
                .or_insert_with(|| Release::new(stamp.clone(), now));
            entry.stamp.server_fingerprint = stamp.server_fingerprint.clone();
            entry.holes = holes;
            entry.verdict = verdict;
            entry.meta_bytes = meta;
            inner.meta_bytes = inner.meta_bytes - old_meta + meta;
            Ok(())
        })();
        empty_trash(trash);
        result
    }

    /// Evict least recently read releases other than `keep` until `growth`
    /// more bytes fit the budget. Evicted releases go to the trash.
    fn evict_to_fit(
        &self,
        inner: &mut Inner,
        growth: u64,
        keep: Option<Uuid>,
        trash: &mut Vec<PathBuf>,
    ) {
        while inner.bytes() + growth > self.budget {
            let Some(victim) = inner
                .releases
                .iter()
                .filter(|(id, _)| Some(**id) != keep)
                .min_by_key(|(_, r)| r.last_read.load(Ordering::Relaxed))
                .map(|(id, _)| *id)
            else {
                break;
            };
            if let Some(release) = inner.remove_release(victim) {
                info!(release = %victim, bytes = release.bytes(), "patch budget full, evicted release");
                trash.extend(self.trash_release(victim));
            }
        }
    }

    /// Move a release's directory into the trash. Called under the write
    /// lock, so no span can be placed in it after; the caller removes the
    /// returned path once the lock is released.
    fn trash_release(&self, release: Uuid) -> Option<PathBuf> {
        let dir = self.dir.as_ref()?;
        let src = dir.join(release.to_string());
        let trash = dir.join(TRASH_DIR);
        let dst = trash.join(format!("{release}-{}", Uuid::new_v4().simple()));
        let moved = std::fs::create_dir_all(&trash).and_then(|()| std::fs::rename(&src, &dst));
        match moved {
            Ok(()) => Some(dst),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => {
                warn!(%release, error = %e, "could not trash patch files, deleting in place");
                if let Err(e) = std::fs::remove_dir_all(&src) {
                    warn!(%release, error = %e, "could not delete patch files");
                }
                None
            }
        }
    }

    /// The patched bytes of an article, if patches cover its whole span in
    /// a file. Counts as a read of the release for eviction.
    pub async fn read_article(&self, message_id: &str) -> Option<Vec<u8>> {
        let (len, pieces) = self.locate(message_id)?;
        let mut out = Vec::with_capacity(len as usize);
        for (data, start, n) in pieces {
            match data {
                SpanData::Memory(bytes) => {
                    out.extend_from_slice(&bytes[start as usize..(start + n) as usize]);
                }
                SpanData::Disk(path) => match read_range(&path, start, n).await {
                    Ok(bytes) => out.extend_from_slice(&bytes),
                    // Evicted or deleted since it was located.
                    Err(e) => {
                        debug!(message_id, error = %e, "patch span vanished");
                        return None;
                    }
                },
            }
        }
        Some(out)
    }

    /// Whether patches cover the article's whole span.
    pub fn covers(&self, message_id: &str) -> bool {
        let inner = self.inner.read();
        inner
            .articles
            .get(message_id)
            .is_some_and(|refs| refs.iter().any(|r| pieces(&inner, r).is_some()))
    }

    /// The pieces covering an article: span data, offset into the span, and
    /// byte count, in file order.
    fn locate(&self, message_id: &str) -> Option<(u64, Vec<Piece>)> {
        let inner = self.inner.read();
        let refs = inner.articles.get(message_id)?;
        refs.iter().find_map(|r| {
            let pieces = pieces(&inner, r)?;
            if let Some(release) = inner.releases.get(&r.release) {
                release.last_read.store(self.tick(), Ordering::Relaxed);
            }
            Some((r.len, pieces))
        })
    }

    /// Drop every patch, hole record and verdict of a release: its item or
    /// history entry was deleted, or its NZB was replaced.
    pub fn drop_release(&self, release: Uuid) -> bool {
        let (found, trash) = {
            let mut inner = self.inner.write();
            let found = inner.remove_release(release).is_some();
            // Even with nothing indexed, clear anything a failed write left.
            (found, self.trash_release(release))
        };
        empty_trash(trash.into_iter().collect());
        found
    }

    /// Drop a release's patches if they were rebuilt from an NZB blob other
    /// than `nzb_blob_id`.
    pub fn drop_if_stale(&self, release: Uuid, nzb_blob_id: Option<Uuid>) -> bool {
        let stale = self
            .inner
            .read()
            .releases
            .get(&release)
            .is_some_and(|r| r.stamp.nzb_blob_id != nzb_blob_id);
        stale && self.drop_release(release)
    }

    /// Drop every patch (history cleared).
    pub fn clear(&self) {
        let ids: Vec<Uuid> = self.inner.read().releases.keys().copied().collect();
        for id in ids {
            self.drop_release(id);
        }
    }

    /// Releases with patches, hole records or a verdict.
    pub fn releases(&self) -> Vec<Uuid> {
        self.inner.read().releases.keys().copied().collect()
    }

    /// The stamp a release's patches carry.
    pub fn stamp(&self, release: Uuid) -> Option<PatchStamp> {
        self.inner
            .read()
            .releases
            .get(&release)
            .map(|r| r.stamp.clone())
    }

    /// A release's recorded confirmed holes.
    pub fn holes(&self, release: Uuid) -> Vec<StoredHole> {
        self.inner
            .read()
            .releases
            .get(&release)
            .map(|r| r.holes.clone())
            .unwrap_or_default()
    }

    /// Every recorded confirmed hole, for a new process's hole ledger
    /// ([`crate::HoleLedger::restore`]).
    pub fn stored_holes(&self) -> Vec<StoredHole> {
        self.inner
            .read()
            .releases
            .values()
            .flat_map(|r| r.holes.iter().cloned())
            .collect()
    }

    /// A release's terminal verdict.
    pub fn verdict(&self, release: Uuid) -> Option<ReleaseVerdict> {
        self.inner
            .read()
            .releases
            .get(&release)
            .and_then(|r| r.verdict.clone())
    }

    /// Totals for the status surface.
    pub fn totals(&self) -> PatchTotals {
        let inner = self.inner.read();
        let (memory_bytes, disk_bytes) = if self.dir.is_some() {
            (inner.meta_bytes, inner.data_bytes)
        } else {
            (inner.bytes(), 0)
        };
        PatchTotals {
            releases: inner.releases.len(),
            spans: inner.spans,
            patch_bytes: inner.data_bytes,
            index_bytes: inner.meta_bytes,
            memory_bytes,
            disk_bytes,
            budget_bytes: self.budget,
        }
    }
}

/// Spans covering `r`'s whole range, contiguously.
fn pieces(inner: &Inner, r: &ArticleRef) -> Option<Vec<Piece>> {
    let file = inner.releases.get(&r.release)?.files.get(&r.file)?;
    let end = r.offset + r.len;
    let mut at = r.offset;
    let mut out = Vec::new();
    let first = file.spans.range(..=at).next_back().map(|(o, _)| *o)?;
    for (&start, span) in file.spans.range(first..end) {
        let span_end = start + span.len;
        if span_end <= at {
            continue;
        }
        if start > at {
            return None;
        }
        let n = span_end.min(end) - at;
        out.push((span.data.clone(), at - start, n));
        at += n;
        if at >= end {
            break;
        }
    }
    (at >= end && r.len > 0).then_some(out)
}

/// Bytes an insert of `offset..offset+len` replaces: the length of an
/// existing span with exactly that range, or 0. Any other overlap is
/// refused.
fn check_overlap(file: &FilePatches, offset: u64, len: u64) -> Result<u64, PatchError> {
    let end = offset + len;
    if let Some(span) = file.spans.get(&offset)
        && span.len == len
    {
        return Ok(len);
    }
    let before = file
        .spans
        .range(..=offset)
        .next_back()
        .is_some_and(|(&s, span)| s + span.len > offset);
    let after = file.spans.range(offset..end).next().is_some();
    if before || after {
        return Err(PatchError::Overlap {
            file: file.name.clone(),
            offset,
            len,
        });
    }
    Ok(0)
}

fn file_size(file: &CensusFile) -> u64 {
    file.articles
        .iter()
        .map(|a| a.offset + a.len)
        .max()
        .unwrap_or(0)
}

/// Budget a patched file's layout and article index take.
fn file_cost(name: &str, articles: &[ArticleSpan]) -> u64 {
    let name = name.len() as u64;
    FILE_INDEX_COST
        + name
        + articles
            .iter()
            .map(|a| ARTICLE_INDEX_COST + 2 * a.message_id.len() as u64 + name)
            .sum::<u64>()
}

fn hole_cost(hole: &StoredHole) -> u64 {
    RECORD_COST
        + (hole.message_id.len()
            + hole.server_fingerprint.len()
            + hole.refused_by.iter().map(String::len).sum::<usize>()
            + hole.unreachable.iter().map(String::len).sum::<usize>()) as u64
}

fn verdict_cost(verdict: &ReleaseVerdict) -> u64 {
    RECORD_COST
        + (verdict.diagnosis.len() + verdict.servers.iter().map(String::len).sum::<usize>()) as u64
}

fn file_key(name: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes()).to_string()
}

#[derive(Serialize, Deserialize)]
struct Layout {
    name: String,
    articles: Vec<LayoutArticle>,
}

#[derive(Serialize, Deserialize)]
struct LayoutArticle {
    message_id: String,
    offset: u64,
    len: u64,
}

/// Write a span's stamp, layout and bytes to the incoming directory.
async fn stage(
    dir: &Path,
    stamp: &PatchStamp,
    file: &CensusFile,
    bytes: &[u8],
) -> io::Result<Staged> {
    let incoming = dir.join(INCOMING_DIR);
    tokio::fs::create_dir_all(&incoming).await?;
    let layout = Layout {
        name: file.name.clone(),
        articles: file
            .articles
            .iter()
            .map(|a| LayoutArticle {
                message_id: a.message_id.clone(),
                offset: a.offset,
                len: a.len,
            })
            .collect(),
    };
    let stamp = serde_json::to_vec(stamp)?;
    let layout = serde_json::to_vec(&layout)?;
    let mut written = Vec::with_capacity(3);
    for body in [&stamp[..], &layout[..], bytes] {
        let path = incoming.join(format!("{}.{TMP_EXT}", Uuid::new_v4().simple()));
        if let Err(e) = tokio::fs::write(&path, body).await {
            let _ = tokio::fs::remove_file(&path).await;
            for p in &written {
                let _ = tokio::fs::remove_file(p).await;
            }
            return Err(e);
        }
        written.push(path);
    }
    let [stamp, layout, span]: [PathBuf; 3] = written.try_into().expect("three files staged");
    Ok(Staged {
        stamp,
        layout,
        span,
    })
}

/// Rename a staged span into its release, creating the release's
/// directory, stamp and layout as needed. Runs under the write lock.
fn place(
    dir: &Path,
    release: Uuid,
    file: &CensusFile,
    offset: u64,
    len: u64,
    staged: &Staged,
) -> io::Result<PathBuf> {
    let rel_dir = dir.join(release.to_string());
    let file_dir = rel_dir.join(file_key(&file.name));
    std::fs::create_dir_all(&file_dir)?;
    std::fs::rename(&staged.stamp, rel_dir.join(STAMP_FILE))?;
    std::fs::rename(&staged.layout, file_dir.join(LAYOUT_FILE))?;
    let path = file_dir.join(format!("{offset}-{len}.{SPAN_EXT}"));
    std::fs::rename(&staged.span, &path)?;
    Ok(path)
}

/// Write `value` as JSON through a temporary file and a rename.
fn write_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let tmp = path.with_extension(format!("{}.{TMP_EXT}", Uuid::new_v4().simple()));
    std::fs::write(&tmp, serde_json::to_vec(value)?)?;
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

fn remove_file(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

fn empty_trash(trash: Vec<PathBuf>) {
    for path in trash {
        if let Err(e) = std::fs::remove_dir_all(&path)
            && e.kind() != io::ErrorKind::NotFound
        {
            warn!(path = %path.display(), error = %e, "could not delete patch files");
        }
    }
}

async fn read_range(path: &Path, start: u64, n: u64) -> io::Result<Vec<u8>> {
    let mut f = tokio::fs::File::open(path).await?;
    f.seek(io::SeekFrom::Start(start)).await?;
    let mut buf = vec![0u8; n as usize];
    f.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Read an optional JSON file of a release, with its write time.
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<Option<(T, SystemTime)>> {
    match std::fs::read(path) {
        Ok(b) => {
            let at = std::fs::metadata(path)?
                .modified()
                .unwrap_or(SystemTime::UNIX_EPOCH);
            Ok(Some((serde_json::from_slice(&b)?, at)))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Load one release directory: its stamp, hole records, verdict, file
/// layouts and complete spans, with the newest write time. `None` when
/// nothing usable is left.
fn load_release(path: &Path) -> io::Result<Option<(SystemTime, Release)>> {
    let Some((stamp, _)) = read_json::<PatchStamp>(&path.join(STAMP_FILE))? else {
        return Ok(None);
    };
    let mut release = Release::new(stamp, 0);
    let mut newest = SystemTime::UNIX_EPOCH;
    if let Some((holes, at)) = read_json::<Vec<StoredHole>>(&path.join(HOLES_FILE))? {
        release.holes = holes;
        newest = newest.max(at);
    }
    if let Some((verdict, at)) = read_json::<ReleaseVerdict>(&path.join(VERDICT_FILE))? {
        release.verdict = Some(verdict);
        newest = newest.max(at);
    }
    for entry in std::fs::read_dir(path)? {
        let file_dir = entry?.path();
        if !file_dir.is_dir() {
            remove_if_tmp(&file_dir);
            continue;
        }
        let Some((layout, _)) = read_json::<Layout>(&file_dir.join(LAYOUT_FILE))? else {
            let _ = std::fs::remove_dir_all(&file_dir);
            continue;
        };
        let articles: Vec<ArticleSpan> = layout
            .articles
            .into_iter()
            .map(|a| ArticleSpan {
                message_id: a.message_id,
                offset: a.offset,
                len: a.len,
            })
            .collect();
        let mut patches = FilePatches {
            size: 0,
            name: layout.name,
            articles,
            spans: BTreeMap::new(),
        };
        patches.size = patches
            .articles
            .iter()
            .map(|a| a.offset + a.len)
            .max()
            .unwrap_or(0);
        for span in std::fs::read_dir(&file_dir)? {
            let span_path = span?.path();
            if remove_if_tmp(&span_path) {
                continue;
            }
            if span_path.extension().and_then(|e| e.to_str()) != Some(SPAN_EXT) {
                continue;
            }
            let Some((offset, len)) = span_path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.split_once('-'))
                .and_then(|(o, l)| Some((o.parse::<u64>().ok()?, l.parse::<u64>().ok()?)))
            else {
                continue;
            };
            let meta = std::fs::metadata(&span_path)?;
            let fits = offset.checked_add(len).is_some_and(|e| e <= patches.size);
            if meta.len() != len
                || len == 0
                || !fits
                || check_overlap(&patches, offset, len).is_err()
            {
                warn!(path = %span_path.display(), "dropping inconsistent patch span");
                let _ = std::fs::remove_file(&span_path);
                continue;
            }
            if let Ok(t) = meta.modified() {
                newest = newest.max(t);
            }
            release.data_bytes += len;
            patches.spans.insert(
                offset,
                Span {
                    len,
                    data: SpanData::Disk(span_path),
                },
            );
        }
        if patches.spans.is_empty() {
            let _ = std::fs::remove_dir_all(&file_dir);
        } else {
            release.files.insert(patches.name.clone(), patches);
        }
    }
    if release.is_empty() {
        return Ok(None);
    }
    release.meta_bytes = release.files_cost()
        + release.holes.iter().map(hole_cost).sum::<u64>()
        + release.verdict.as_ref().map_or(0, verdict_cost);
    Ok(Some((newest, release)))
}

fn remove_if_tmp(path: &Path) -> bool {
    let tmp = path.extension().and_then(|e| e.to_str()) == Some(TMP_EXT);
    if tmp {
        let _ = std::fs::remove_file(path);
    }
    tmp
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(blob: u128) -> PatchStamp {
        PatchStamp {
            nzb_blob_id: Some(Uuid::from_u128(blob)),
            server_fingerprint: "fake-0".into(),
        }
    }

    /// A file of `n` articles of `part` bytes, message-ids `<prefix><i>`.
    fn file(name: &str, prefix: &str, n: u64, part: u64) -> CensusFile {
        CensusFile {
            name: name.into(),
            articles: (0..n)
                .map(|i| ArticleSpan {
                    message_id: format!("{prefix}{i}"),
                    offset: i * part,
                    len: part,
                })
                .collect(),
        }
    }

    fn release(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[tokio::test]
    async fn article_covered_by_one_span_is_served() {
        let store = PatchStore::in_memory(1 << 20);
        let f = file("a.rar", "a", 4, 100);
        let bytes: Vec<u8> = (0..200).map(|i| i as u8).collect();
        store
            .insert(release(1), &stamp(1), &f, 100, bytes.clone())
            .await
            .unwrap();
        assert_eq!(store.read_article("a1").await.unwrap(), bytes[..100]);
        assert_eq!(store.read_article("a2").await.unwrap(), bytes[100..]);
        assert!(store.read_article("a0").await.is_none());
        assert!(store.read_article("a3").await.is_none());
        assert!(store.read_article("unknown").await.is_none());
    }

    #[tokio::test]
    async fn article_needs_full_contiguous_coverage() {
        let store = PatchStore::in_memory(1 << 20);
        let f = file("a.rar", "a", 2, 100);
        // Covers half of a0 only.
        store
            .insert(release(1), &stamp(1), &f, 0, vec![1; 50])
            .await
            .unwrap();
        assert!(!store.covers("a0"));
        assert!(store.read_article("a0").await.is_none());
        // A gap at 50..60, then the rest: still not covered.
        store
            .insert(release(1), &stamp(1), &f, 60, vec![3; 40])
            .await
            .unwrap();
        assert!(store.read_article("a0").await.is_none());
        // Fill the gap: two spans and the gap stitch into one article.
        store
            .insert(release(1), &stamp(1), &f, 50, vec![2; 10])
            .await
            .unwrap();
        let mut want = vec![1u8; 50];
        want.extend([2; 10]);
        want.extend([3; 40]);
        assert_eq!(store.read_article("a0").await.unwrap(), want);
    }

    #[tokio::test]
    async fn overlap_is_refused_and_exact_range_replaces() {
        let store = PatchStore::in_memory(1 << 20);
        let f = file("a.rar", "a", 4, 100);
        store
            .insert(release(1), &stamp(1), &f, 100, vec![1; 100])
            .await
            .unwrap();
        for (offset, len) in [(50, 100), (150, 100), (120, 10), (0, 400)] {
            let err = store
                .insert(release(1), &stamp(1), &f, offset, vec![9; len])
                .await
                .unwrap_err();
            assert!(
                matches!(err, PatchError::Overlap { .. }),
                "{offset}+{len}: {err}"
            );
        }
        // Adjacent spans are fine.
        store
            .insert(release(1), &stamp(1), &f, 0, vec![0; 100])
            .await
            .unwrap();
        store
            .insert(release(1), &stamp(1), &f, 100, vec![7; 100])
            .await
            .unwrap();
        assert_eq!(store.read_article("a1").await.unwrap(), vec![7; 100]);
        let totals = store.totals();
        assert_eq!((totals.spans, totals.patch_bytes), (2, 200));
        assert_eq!(totals.index_bytes, file_cost(&f.name, &f.articles));
        assert_eq!(totals.memory_bytes, 200 + totals.index_bytes);
        // Past the end of the file.
        let err = store
            .insert(release(1), &stamp(1), &f, 350, vec![0; 100])
            .await
            .unwrap_err();
        assert!(matches!(err, PatchError::OutOfFile { .. }), "{err}");
    }

    #[tokio::test]
    async fn budget_evicts_least_recently_read_release_whole() {
        let fa = file("a.rar", "a", 4, 100);
        // Room for 400 span bytes and the index of two files.
        let index = file_cost(&fa.name, &fa.articles);
        let store = PatchStore::in_memory(400 + 2 * index);
        let fb = file("b.rar", "b", 4, 100);
        let fc = file("c.rar", "c", 4, 100);
        // Release 1 has two spans, release 2 one.
        store
            .insert(release(1), &stamp(1), &fa, 0, vec![1; 100])
            .await
            .unwrap();
        store
            .insert(release(1), &stamp(1), &fa, 200, vec![1; 100])
            .await
            .unwrap();
        store
            .insert(release(2), &stamp(2), &fb, 0, vec![2; 100])
            .await
            .unwrap();
        // Release 1 is read after release 2 was written, so 2 is oldest.
        assert!(store.read_article("a0").await.is_some());

        store
            .insert(release(3), &stamp(3), &fc, 0, vec![3; 200])
            .await
            .unwrap();
        assert_eq!(store.releases().len(), 2);
        assert!(
            store.read_article("b0").await.is_none(),
            "release 2 evicted"
        );
        assert!(store.read_article("a2").await.is_some());

        // Release 3 grows: release 1 goes, both its spans with it.
        store
            .insert(release(3), &stamp(3), &fc, 200, vec![3; 200])
            .await
            .unwrap();
        assert!(store.read_article("a0").await.is_none());
        assert!(store.read_article("a2").await.is_none());
        assert_eq!(store.releases(), vec![release(3)]);
        assert_eq!(store.totals().patch_bytes, 400);
        assert_eq!(store.totals().index_bytes, index);

        // A release that cannot fit alone is refused, and nothing is evicted.
        let err = store
            .insert(
                release(4),
                &stamp(4),
                &file("d.rar", "d", 16, 100),
                0,
                vec![4; 1000],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PatchError::OverBudget { .. }), "{err}");
        assert_eq!(store.releases(), vec![release(3)]);
    }

    #[tokio::test]
    async fn new_nzb_blob_drops_old_patches() {
        let store = PatchStore::in_memory(1 << 20);
        let f = file("a.rar", "a", 4, 100);
        store
            .insert(release(1), &stamp(1), &f, 0, vec![1; 100])
            .await
            .unwrap();
        store
            .insert(release(1), &stamp(2), &f, 100, vec![2; 100])
            .await
            .unwrap();
        assert!(store.read_article("a0").await.is_none());
        assert_eq!(store.read_article("a1").await.unwrap(), vec![2; 100]);
        assert_eq!(store.stamp(release(1)), Some(stamp(2)));

        assert!(!store.drop_if_stale(release(1), stamp(2).nzb_blob_id));
        assert!(store.drop_if_stale(release(1), stamp(3).nzb_blob_id));
        assert!(store.releases().is_empty());
        assert_eq!(
            store.totals(),
            PatchTotals {
                budget_bytes: 1 << 20,
                ..PatchTotals::default()
            }
        );
    }

    #[tokio::test]
    async fn disk_patches_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let f = file("odd name/../a.rar", "a", 4, 100);
        let bytes: Vec<u8> = (0..100).map(|i| (i * 3) as u8).collect();
        {
            let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
            store
                .insert(release(1), &stamp(1), &f, 200, bytes.clone())
                .await
                .unwrap();
            assert_eq!(store.totals().disk_bytes, 100);
        }
        let index = file_cost(&f.name, &f.articles);
        // A partial write left by a crash is cleaned up on open.
        let rel_dir = dir.path().join(release(1).to_string());
        let file_dir = rel_dir.join(file_key(&f.name));
        std::fs::write(file_dir.join("0-100.abc.tmp"), [0u8; 10]).unwrap();

        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(store.read_article("a2").await.unwrap(), bytes);
        assert_eq!(store.stamp(release(1)), Some(stamp(1)));
        assert!(!file_dir.join("0-100.abc.tmp").exists());
        let totals = store.totals();
        assert_eq!(
            (totals.releases, totals.spans, totals.disk_bytes),
            (1, 1, 100)
        );
        assert_eq!(
            totals.memory_bytes, index,
            "the index is reloaded and counted"
        );

        // Deleting the release removes its files.
        assert!(store.drop_release(release(1)));
        assert!(!rel_dir.exists());
        assert!(store.read_article("a2").await.is_none());
        drop(store);
        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        assert!(store.releases().is_empty());
    }

    #[tokio::test]
    async fn disk_eviction_deletes_files_and_reopen_honours_budget() {
        let dir = tempfile::tempdir().unwrap();
        let fa = file("a", "a", 2, 100);
        let index = file_cost(&fa.name, &fa.articles);
        let store = PatchStore::open(dir.path(), 300 + index).unwrap();
        store
            .insert(release(1), &stamp(1), &fa, 0, vec![1; 200])
            .await
            .unwrap();
        store
            .insert(
                release(2),
                &stamp(2),
                &file("b", "b", 2, 100),
                0,
                vec![2; 200],
            )
            .await
            .unwrap();
        assert_eq!(store.releases(), vec![release(2)]);
        assert!(!dir.path().join(release(1).to_string()).exists());
        drop(store);

        // A smaller budget on reopen evicts.
        let store = PatchStore::open(dir.path(), 100 + index).unwrap();
        assert!(store.releases().is_empty());
        assert!(!dir.path().join(release(2).to_string()).exists());
    }

    #[test]
    fn setting_selects_the_store() {
        let dir = tempfile::tempdir().unwrap();
        assert!(PatchStore::from_setting(0, dir.path()).unwrap().is_none());
        let mem = PatchStore::from_setting(-1, dir.path()).unwrap().unwrap();
        assert!(mem.dir.is_none());
        assert_eq!(mem.budget(), 512 * 1024 * 1024);
        let disk = PatchStore::from_setting(2, dir.path().join("p"))
            .unwrap()
            .unwrap();
        assert_eq!(disk.budget(), 2 * 1024 * 1024);
        assert!(dir.path().join("p").is_dir());
    }

    /// Readers racing a writer that keeps replacing a span (and adding and
    /// evicting others) always see one whole version of the article.
    async fn no_tearing(store: Arc<PatchStore>) {
        let f = file(NO_TEARING_A, "a", 64, 4096);
        store
            .insert(release(1), &stamp(1), &f, 0, vec![0; 8192])
            .await
            .unwrap();
        let writer = {
            let store = Arc::clone(&store);
            let f = f.clone();
            tokio::spawn(async move {
                for round in 0..200u32 {
                    let fill = (round % 250) as u8 + 1;
                    store
                        .insert(release(1), &stamp(1), &f, 0, vec![fill; 8192])
                        .await
                        .unwrap();
                    // Churn another release through the budget.
                    let other = file(NO_TEARING_B, "b", 4, 4096);
                    let _ = store
                        .insert(
                            release(2 + u128::from(round)),
                            &stamp(2),
                            &other,
                            0,
                            vec![fill; 4096],
                        )
                        .await;
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let store = Arc::clone(&store);
                tokio::spawn(async move {
                    let mut seen = 0;
                    for _ in 0..400 {
                        for id in ["a0", "a1"] {
                            let bytes =
                                store.read_article(id).await.expect("span 0 always present");
                            assert_eq!(bytes.len(), 4096);
                            assert!(bytes.iter().all(|&b| b == bytes[0]), "torn read of {id}");
                            seen += 1;
                        }
                        tokio::task::yield_now().await;
                    }
                    seen
                })
            })
            .collect();
        writer.await.unwrap();
        for r in readers {
            assert_eq!(r.await.unwrap(), 800);
        }
    }

    const NO_TEARING_A: &str = "a.rar";
    const NO_TEARING_B: &str = "b.rar";

    /// Room for release 1 and one other release.
    fn no_tearing_budget() -> u64 {
        let a = file(NO_TEARING_A, "a", 64, 4096);
        let b = file(NO_TEARING_B, "b", 4, 4096);
        8192 + 4096 + file_cost(&a.name, &a.articles) + file_cost(&b.name, &b.articles)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_and_writes_in_memory() {
        no_tearing(Arc::new(PatchStore::in_memory(no_tearing_budget()))).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_and_writes_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = PatchStore::open(dir.path(), no_tearing_budget()).unwrap();
        no_tearing(Arc::new(store)).await;
    }

    fn confirmed_hole(message_id: &str) -> HoleRecord {
        let now = tokio::time::Instant::now();
        HoleRecord {
            message_id: message_id.into(),
            state: crate::holes::HoleState::Confirmed,
            refused_by: vec!["fake-0".into()],
            unreachable: vec![],
            first_refused_at: now,
            confirmed_at: Some(now),
            expires_at: Some(now + crate::holes::HOLE_TTL),
            server_fingerprint: "fake-0".into(),
        }
    }

    /// Hole records and the terminal verdict outlive the process, with or
    /// without patches, count against the budget, and go with the release.
    #[tokio::test]
    async fn holes_and_verdict_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let f = file("a.rar", "a", 4, 100);
        let servers = vec!["fake-0".to_string()];
        {
            let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
            // Release 1: a patch and its hole. Release 2: hopeless, no patch.
            store
                .insert(release(1), &stamp(1), &f, 100, vec![5; 100])
                .await
                .unwrap();
            let mut provisional = confirmed_hole("a3");
            provisional.state = crate::holes::HoleState::Provisional;
            let recorded = store
                .record_holes(release(1), &stamp(1), &[confirmed_hole("a1"), provisional])
                .unwrap();
            assert_eq!(recorded, 1, "provisional holes are not kept");
            // Recording the same hole again replaces it.
            store
                .record_holes(release(1), &stamp(1), &[confirmed_hole("a1")])
                .unwrap();
            store
                .record_holes(release(2), &stamp(2), &[confirmed_hole("b0")])
                .unwrap();
            store
                .record_verdict(
                    release(2),
                    &stamp(2),
                    ReleaseVerdict::new("beyond_repair: 3 slices lost, 1 block", &servers),
                )
                .unwrap();
        }

        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        let mut releases = store.releases();
        releases.sort();
        assert_eq!(releases, vec![release(1), release(2)]);
        let holes = store.holes(release(1));
        assert_eq!(holes.len(), 1);
        assert_eq!(holes[0].message_id, "a1");
        assert!(!holes[0].expired());
        assert_eq!(store.read_article("a1").await.unwrap(), vec![5; 100]);
        let verdict = store.verdict(release(2)).unwrap();
        assert!(verdict.diagnosis.starts_with("beyond_repair"));
        assert!(verdict.applies_to(&servers));
        assert!(!verdict.applies_to(&["fake-0".into(), "fake-1".into()]));
        assert_eq!(store.verdict(release(1)), None);
        let mut ids: Vec<String> = store
            .stored_holes()
            .into_iter()
            .map(|h| h.message_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["a1", "b0"]);

        let totals = store.totals();
        let records = 2 * hole_cost(&holes[0]) + verdict_cost(&verdict);
        assert_eq!(
            totals.index_bytes,
            file_cost(&f.name, &f.articles) + records
        );

        // A new NZB blob drops the release's holes with its patches.
        store
            .record_holes(release(1), &stamp(9), &[confirmed_hole("a2")])
            .unwrap();
        assert_eq!(store.holes(release(1))[0].message_id, "a2");
        assert!(store.read_article("a1").await.is_none());

        // Dropping a release drops its records, on disk too.
        assert!(store.drop_release(release(2)));
        assert_eq!(store.verdict(release(2)), None);
        drop(store);
        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(store.releases(), vec![release(1)]);
        assert!(store.holes(release(2)).is_empty());
    }

    /// A span of a few bytes still pays for its file's article index: a
    /// file with many articles cannot hide in the budget.
    #[tokio::test]
    async fn article_index_counts_against_the_budget() {
        let f = file("big.rar", "article-with-a-long-message-id-", 10_000, 100);
        let index = file_cost(&f.name, &f.articles);
        assert!(index > 1_000_000, "{index}");
        let store = PatchStore::in_memory(1 << 20);
        let err = store
            .insert(release(1), &stamp(1), &f, 0, vec![1; 10])
            .await
            .unwrap_err();
        assert!(matches!(err, PatchError::OverBudget { .. }), "{err}");

        // A second span of a file already indexed pays only its bytes, and
        // a file's index makes room by evicting others.
        let small = file("s.rar", "s", 4, 100);
        let small_index = file_cost(&small.name, &small.articles);
        let store = PatchStore::in_memory(200 + small_index + 50);
        store
            .insert(release(1), &stamp(1), &small, 0, vec![1; 100])
            .await
            .unwrap();
        store
            .insert(release(1), &stamp(1), &small, 100, vec![1; 100])
            .await
            .unwrap();
        assert_eq!(store.totals().index_bytes, small_index);
        let other = file("t.rar", "t", 4, 100);
        store
            .insert(release(2), &stamp(2), &other, 0, vec![2; 10])
            .await
            .unwrap();
        assert_eq!(store.releases(), vec![release(2)]);
        assert_eq!(store.totals().patch_bytes, 10);
    }

    /// A span refused at commit (here: the release went to another NZB blob
    /// between the check and the commit) removes only its staged files, not
    /// the live span at the same range, and its bytes are never counted.
    #[tokio::test]
    async fn refused_commit_keeps_the_live_span() {
        let dir = tempfile::tempdir().unwrap();
        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        let f = file("a.rar", "a", 4, 100);
        store
            .insert(release(1), &stamp(1), &f, 100, vec![1; 100])
            .await
            .unwrap();
        let staged = stage(dir.path(), &stamp(2), &f, &[2; 100]).await.unwrap();
        let err = store
            .commit(release(1), &stamp(2), &f, 100, 100, Payload::Disk(staged))
            .unwrap_err();
        assert!(matches!(err, PatchError::Stale), "{err}");
        assert_eq!(store.read_article("a1").await.unwrap(), vec![1; 100]);
        assert_eq!(store.totals().disk_bytes, 100);
        assert_eq!(
            std::fs::read_dir(dir.path().join(INCOMING_DIR))
                .unwrap()
                .count(),
            0,
            "staged files removed"
        );
        drop(store);
        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(store.read_article("a1").await.unwrap(), vec![1; 100]);
    }

    /// A release dropped while a span of it is being written: the commit
    /// recreates the release whole (stamp, layout and span), so what is
    /// counted is on disk and survives a restart.
    #[tokio::test]
    async fn drop_between_write_and_commit_leaves_a_whole_release() {
        let dir = tempfile::tempdir().unwrap();
        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        let f = file("a.rar", "a", 4, 100);
        store
            .insert(release(1), &stamp(1), &f, 0, vec![1; 100])
            .await
            .unwrap();
        let staged = stage(dir.path(), &stamp(1), &f, &[2; 100]).await.unwrap();
        assert!(store.drop_release(release(1)));
        store
            .commit(release(1), &stamp(1), &f, 100, 100, Payload::Disk(staged))
            .unwrap();
        let totals = store.totals();
        assert_eq!((totals.spans, totals.disk_bytes), (1, 100));
        assert!(store.read_article("a0").await.is_none());
        assert_eq!(store.read_article("a1").await.unwrap(), vec![2; 100]);
        drop(store);
        let store = PatchStore::open(dir.path(), 1 << 20).unwrap();
        assert_eq!(store.read_article("a1").await.unwrap(), vec![2; 100]);
        assert_eq!(store.totals().disk_bytes, 100);
    }
}
