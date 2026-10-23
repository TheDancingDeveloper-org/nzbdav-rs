//! The patch store: byte spans the repair job rebuilt from PAR2, served in
//! place of a confirmed hole.
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
//!   recently read first, so a release's patches go together.
//! - The read path asks by article: [`PatchStore::read_article`] returns the
//!   article's decoded bytes when patches cover its whole span in the file,
//!   and nothing otherwise, so a reader never sees a torn article.
//!
//! Patches are typically megabytes and must survive a restart (the repair
//! that produced them costs a PAR2 volume download and a rebuild), so the
//! store keeps them as files under the data dir rather than in SQLite rows:
//! a span is written to a temporary file and renamed into place, so a crash
//! never leaves a partial span, and eviction or deletion is an `unlink` that
//! a reader with the span already open is unaffected by. An in-memory store
//! with the same rules exists for `par2_patch_cache_mb = -1` and for tests.
//!
//! On-disk layout under the store's directory:
//!
//! ```text
//! <release uuid>/stamp.json
//! <release uuid>/<file key>/layout.json      file name and article spans
//! <release uuid>/<file key>/<offset>-<len>.bin
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

/// Default budget, in MiB (`par2_patch_cache_mb`).
pub const DEFAULT_PATCH_CACHE_MB: i64 = 512;

const STAMP_FILE: &str = "stamp.json";
const LAYOUT_FILE: &str = "layout.json";
const SPAN_EXT: &str = "bin";
const TMP_EXT: &str = "tmp";

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
    pub memory_bytes: u64,
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
    bytes: u64,
    /// Recency on the store's clock; bumped under the read lock.
    last_read: AtomicU64,
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
    bytes: u64,
    spans: usize,
}

impl Inner {
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

    /// Remove a release from the index; returns it so the caller can delete
    /// its files outside the lock.
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
        self.bytes -= release.bytes;
        Some(release)
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

    /// Open (or create) a store under `dir`, loading the patches a previous
    /// process left there. Partial writes are removed, and if the budget
    /// shrank, the least recently written releases are evicted.
    pub fn open(dir: impl Into<PathBuf>, budget: u64) -> io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
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
        let mut evict = Vec::new();
        {
            let mut inner = store.inner.write();
            for (_, id, release) in loaded {
                release.last_read.store(store.tick(), Ordering::Relaxed);
                for file in release.files.values() {
                    inner.index_file(id, &file.name, &file.articles);
                    inner.spans += file.spans.len();
                }
                inner.bytes += release.bytes;
                inner.releases.insert(id, release);
            }
            store.evict_to_fit(&mut inner, 0, None, &mut evict);
            info!(
                releases = inner.releases.len(),
                bytes = inner.bytes,
                "patch store loaded"
            );
        }
        store.delete_files(evict);
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
            let replaced = current
                .and_then(|r| r.files.get(&file.name))
                .map_or(Ok(0), |f| check_overlap(f, offset, len))?;
            let needed = current.map_or(0, |r| r.bytes) - replaced + len;
            if needed > self.budget {
                return Err(PatchError::OverBudget {
                    needed,
                    budget: self.budget,
                });
            }
            stale
        };
        if stale {
            info!(%release, "NZB blob changed, dropping the release's patches");
            self.drop_release(release);
        }

        let data = match &self.dir {
            None => SpanData::Memory(bytes.into()),
            Some(dir) => {
                let path = self
                    .write_span(dir, release, stamp, file, offset, &bytes)
                    .await?;
                SpanData::Disk(path)
            }
        };

        let mut evict = Vec::new();
        let result = {
            let mut inner = self.inner.write();
            self.commit(
                &mut inner,
                release,
                stamp,
                file,
                offset,
                Span { len, data },
                &mut evict,
            )
        };
        self.delete_files(evict);
        result
    }

    /// Index a written span. Runs under the write lock and re-checks what
    /// [`insert`](Self::insert) checked, since another insert may have run
    /// in between.
    #[allow(clippy::too_many_arguments)]
    fn commit(
        &self,
        inner: &mut Inner,
        release: Uuid,
        stamp: &PatchStamp,
        file: &CensusFile,
        offset: u64,
        span: Span,
        evict: &mut Vec<(Uuid, Release)>,
    ) -> Result<(), PatchError> {
        let len = span.len;
        let refuse = |e: PatchError, span: &Span| {
            if let SpanData::Disk(path) = &span.data {
                let _ = std::fs::remove_file(path);
            }
            Err(e)
        };
        if inner
            .releases
            .get(&release)
            .is_some_and(|r| r.stamp.nzb_blob_id != stamp.nzb_blob_id)
        {
            return refuse(PatchError::Stale, &span);
        }
        let replaced = match inner
            .releases
            .get(&release)
            .and_then(|r| r.files.get(&file.name))
            .map_or(Ok(0), |f| check_overlap(f, offset, len))
        {
            Ok(r) => r,
            Err(e) => return refuse(e, &span),
        };
        let growth = len - replaced;
        let own = inner.releases.get(&release).map_or(0, |r| r.bytes) + growth;
        if own > self.budget {
            return refuse(
                PatchError::OverBudget {
                    needed: own,
                    budget: self.budget,
                },
                &span,
            );
        }
        self.evict_to_fit(inner, growth, Some(release), evict);

        let now = self.tick();
        let entry = inner.releases.entry(release).or_insert_with(|| Release {
            stamp: stamp.clone(),
            files: HashMap::new(),
            bytes: 0,
            last_read: AtomicU64::new(now),
        });
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
        let added = patches.spans.insert(offset, span).is_none();
        entry.bytes += growth;
        inner.bytes += growth;
        if added {
            inner.spans += 1;
        }
        inner.index_file(release, &file.name, &file.articles);
        debug!(%release, file = %file.name, offset, len, "patch stored");
        Ok(())
    }

    /// Evict least recently read releases other than `keep` until `growth`
    /// more bytes fit the budget.
    fn evict_to_fit(
        &self,
        inner: &mut Inner,
        growth: u64,
        keep: Option<Uuid>,
        evict: &mut Vec<(Uuid, Release)>,
    ) {
        while inner.bytes + growth > self.budget {
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
                info!(release = %victim, bytes = release.bytes, "patch budget full, evicted release");
                evict.push((victim, release));
            }
        }
    }

    async fn write_span(
        &self,
        dir: &Path,
        release: Uuid,
        stamp: &PatchStamp,
        file: &CensusFile,
        offset: u64,
        bytes: &[u8],
    ) -> io::Result<PathBuf> {
        let file_dir = dir.join(release.to_string()).join(file_key(&file.name));
        tokio::fs::create_dir_all(&file_dir).await?;
        write_atomic(
            &dir.join(release.to_string()).join(STAMP_FILE),
            &serde_json::to_vec(stamp)?,
        )
        .await?;
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
        write_atomic(&file_dir.join(LAYOUT_FILE), &serde_json::to_vec(&layout)?).await?;
        let path = file_dir.join(format!("{offset}-{}.{SPAN_EXT}", bytes.len()));
        write_atomic(&path, bytes).await?;
        Ok(path)
    }

    fn delete_files(&self, evicted: Vec<(Uuid, Release)>) {
        let Some(dir) = &self.dir else { return };
        for (id, _) in evicted {
            let path = dir.join(id.to_string());
            if let Err(e) = std::fs::remove_dir_all(&path)
                && e.kind() != io::ErrorKind::NotFound
            {
                warn!(release = %id, error = %e, "could not delete patch files");
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

    /// Drop every patch of a release: its history entry was deleted, or its
    /// NZB was replaced.
    pub fn drop_release(&self, release: Uuid) -> bool {
        let removed = self.inner.write().remove_release(release);
        let found = removed.is_some();
        if let Some(r) = removed {
            self.delete_files(vec![(release, r)]);
        } else if let Some(dir) = &self.dir {
            // Nothing indexed, but clear anything a failed write left.
            let _ = std::fs::remove_dir_all(dir.join(release.to_string()));
        }
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

    /// Releases with patches.
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

    /// Totals for the status surface.
    pub fn totals(&self) -> PatchTotals {
        let inner = self.inner.read();
        let (memory_bytes, disk_bytes) = if self.dir.is_some() {
            (0, inner.bytes)
        } else {
            (inner.bytes, 0)
        };
        PatchTotals {
            releases: inner.releases.len(),
            spans: inner.spans,
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

async fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension(format!("{}.{TMP_EXT}", Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, bytes).await?;
    if let Err(e) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    Ok(())
}

async fn read_range(path: &Path, start: u64, n: u64) -> io::Result<Vec<u8>> {
    let mut f = tokio::fs::File::open(path).await?;
    f.seek(io::SeekFrom::Start(start)).await?;
    let mut buf = vec![0u8; n as usize];
    f.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Load one release directory: its stamp, file layouts and complete spans,
/// with the newest span's write time. `None` when nothing usable is left.
fn load_release(path: &Path) -> io::Result<Option<(SystemTime, Release)>> {
    let stamp: PatchStamp = match std::fs::read(path.join(STAMP_FILE)) {
        Ok(b) => serde_json::from_slice(&b)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut files = HashMap::new();
    let mut bytes = 0u64;
    let mut newest = SystemTime::UNIX_EPOCH;
    for entry in std::fs::read_dir(path)? {
        let file_dir = entry?.path();
        if !file_dir.is_dir() {
            remove_if_tmp(&file_dir);
            continue;
        }
        let layout: Layout = match std::fs::read(file_dir.join(LAYOUT_FILE)) {
            Ok(b) => serde_json::from_slice(&b)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let _ = std::fs::remove_dir_all(&file_dir);
                continue;
            }
            Err(e) => return Err(e),
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
            bytes += len;
            patches.spans.insert(
                offset,
                Span {
                    len,
                    data: SpanData::Disk(span_path),
                },
            );
        }
        if !patches.spans.is_empty() {
            files.insert(patches.name.clone(), patches);
        }
    }
    if files.is_empty() {
        return Ok(None);
    }
    Ok(Some((
        newest,
        Release {
            stamp,
            files,
            bytes,
            last_read: AtomicU64::new(0),
        },
    )))
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
        assert_eq!((totals.spans, totals.memory_bytes), (2, 200));
        // Past the end of the file.
        let err = store
            .insert(release(1), &stamp(1), &f, 350, vec![0; 100])
            .await
            .unwrap_err();
        assert!(matches!(err, PatchError::OutOfFile { .. }), "{err}");
    }

    #[tokio::test]
    async fn budget_evicts_least_recently_read_release_whole() {
        let store = PatchStore::in_memory(400);
        let fa = file("a.rar", "a", 4, 100);
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
        assert_eq!(store.totals().memory_bytes, 400);

        // A release that cannot fit alone is refused, and nothing is evicted.
        let err = store
            .insert(
                release(4),
                &stamp(4),
                &file("d.rar", "d", 8, 100),
                0,
                vec![4; 500],
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
        let store = PatchStore::open(dir.path(), 300).unwrap();
        store
            .insert(
                release(1),
                &stamp(1),
                &file("a", "a", 2, 100),
                0,
                vec![1; 200],
            )
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
        let store = PatchStore::open(dir.path(), 100).unwrap();
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
        let f = file("a.rar", "a", 64, 4096);
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
                    let other = file("b.rar", "b", 4, 4096);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_and_writes_in_memory() {
        // Room for release 1 and one other release.
        no_tearing(Arc::new(PatchStore::in_memory(8192 + 4096))).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_and_writes_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        no_tearing(Arc::new(PatchStore::open(dir.path(), 8192 + 4096).unwrap())).await;
    }
}
