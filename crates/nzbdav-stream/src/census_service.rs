//! Where the damage census meets the rest of the system: it builds census
//! targets from a DAV item's real articles and the PAR2 sets recorded at
//! import, and is what the three triggers call:
//!
//! - a read that met a missing article ([`CensusService::hole_read`]),
//! - the health check ([`CensusTrigger::HealthCheck`]),
//! - an operator asking ([`CensusTrigger::Operator`]).
//!
//! A DAV item protected by a PAR2 set is surveyed as that set: every file
//! the set protects that the release carries, plus its recovery volumes, so
//! the verdict says whether the set can repair the damage. An item no set
//! protects is surveyed on its own articles, counted in articles.
//!
//! A census that finds lost slices the set may still cover schedules a
//! repair of the set ([`CensusService::with_repair`]).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use nzbdav_core::database::DavDatabase;
use nzbdav_core::error::{DavError, Result};
use nzbdav_core::models::{DavItem, DavMultipartFile, DavNzbFile, ItemSubType};
use parking_lot::Mutex;
use tokio::time::Instant;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::census::{
    ArticleSpan, CensusError, CensusFile, CensusReport, CensusTarget, CensusTrigger, DamageCensus,
    Par2RecoverySet, Verdict,
};
use crate::error::StreamError;
use crate::recovery_set::census_files;
use crate::repair::{RepairService, RepairTarget, Scheduled};

/// A second hole read of the same item within this window does not start
/// another census; the census's own cooldown takes over after that.
pub const HOLE_READ_DEBOUNCE: Duration = Duration::from_secs(60);

/// Runs censuses of DAV items for the triggers.
pub struct CensusService {
    census: Arc<DamageCensus>,
    db: Arc<dyn DavDatabase>,
    /// When each item last started a census from a hole read.
    hole_reads: Mutex<HashMap<Uuid, Instant>>,
    /// Repairs damage a census finds.
    repair: Option<Arc<RepairService>>,
}

impl CensusService {
    pub fn new(census: Arc<DamageCensus>, db: Arc<dyn DavDatabase>) -> Self {
        Self {
            census,
            db,
            hole_reads: Mutex::new(HashMap::new()),
            repair: None,
        }
    }

    /// Schedule a repair whenever a census finds lost slices that the set
    /// may still cover.
    pub fn with_repair(mut self, repair: Arc<RepairService>) -> Self {
        self.repair = Some(repair);
        self
    }

    /// The repair service, if one is attached.
    pub fn repair(&self) -> Option<&Arc<RepairService>> {
        self.repair.as_ref()
    }

    /// The census this service runs.
    pub fn census(&self) -> &Arc<DamageCensus> {
        &self.census
    }

    /// The releases a DAV item belongs to, as census targets: one per PAR2
    /// set protecting it, or the item alone when none does.
    pub async fn targets_for_item(&self, item: &DavItem) -> Result<Vec<CensusTarget>> {
        Ok(self
            .releases_for_item(item)
            .await?
            .into_iter()
            .map(|(target, _)| target)
            .collect())
    }

    /// Census targets of an item, each with the repair target of its set.
    async fn releases_for_item(
        &self,
        item: &DavItem,
    ) -> Result<Vec<(CensusTarget, Option<RepairTarget>)>> {
        let refs = self.db.get_par2_files_for_dav_item(item.id).await?;
        let mut wanted: BTreeMap<Uuid, Vec<[u8; 16]>> = BTreeMap::new();
        for r in refs {
            let sets = wanted.entry(r.history_item_id).or_default();
            if !sets.contains(&r.recovery_set_id) {
                sets.push(r.recovery_set_id);
            }
        }

        let mut targets = Vec::new();
        for (history_item_id, set_ids) in wanted {
            for record in self.db.get_par2_sets(history_item_id).await? {
                if !set_ids.contains(&record.recovery_set_id) {
                    continue;
                }
                let files = census_files(&record);
                if files.is_empty() {
                    continue;
                }
                let key = set_key(history_item_id, &record.recovery_set_id);
                let census = CensusTarget {
                    key: key.clone(),
                    files,
                    recovery: Some(Par2RecoverySet::from(&record)),
                };
                let repair = RepairTarget {
                    key,
                    release: history_item_id,
                    nzb_blob_id: item.nzb_blob_id,
                    record,
                };
                targets.push((census, Some(repair)));
            }
        }
        if targets.is_empty() {
            targets.push((self.item_target(item).await?, None));
        }
        Ok(targets)
    }

    /// The item surveyed on its own articles, with no recovery set.
    async fn item_target(&self, item: &DavItem) -> Result<CensusTarget> {
        let blob_id = item
            .file_blob_id
            .ok_or_else(|| DavError::ItemNotFound(format!("item {} has no file blob", item.id)))?;
        let blob = self.db.get_file_blob(blob_id).await?;
        let segment_ids = segment_ids_of(item.sub_type, &blob)?;
        let size = item.file_size.unwrap_or(0).max(0) as u64;
        Ok(CensusTarget {
            key: format!("item:{}", item.id),
            files: vec![CensusFile {
                name: item.name.clone(),
                articles: spread(&segment_ids, size),
            }],
            recovery: None,
        })
    }

    /// Survey every release an item belongs to. Each target's census may
    /// be refused (cooling down, already running) without failing the rest.
    pub async fn survey_item(
        &self,
        item: &DavItem,
        trigger: CensusTrigger,
    ) -> Result<Vec<std::result::Result<CensusReport, CensusError>>> {
        let targets = self.releases_for_item(item).await?;
        let mut results = Vec::with_capacity(targets.len());
        for (target, repair) in targets {
            let result = self.census.run(&target, trigger).await;
            if let (Ok(report), Some(repair)) = (&result, repair) {
                self.maybe_repair(report, repair);
            }
            results.push(result);
        }
        Ok(results)
    }

    /// Schedule a repair when the census found lost slices and did not
    /// settle the set as beyond repair.
    fn maybe_repair(&self, report: &CensusReport, target: RepairTarget) {
        let Some(repair) = &self.repair else { return };
        if !wants_repair(report) {
            return;
        }
        match repair.schedule(target) {
            Scheduled::Queued(_) => {
                info!(key = %report.key, damaged = report.damaged, "repair scheduled")
            }
            Scheduled::Folded(_) => {
                debug!(key = %report.key, "damage folded into the running repair")
            }
            Scheduled::Skipped(reason) => debug!(key = %report.key, ?reason, "repair skipped"),
        }
    }

    /// Survey the item by id, for the operator.
    pub async fn survey_item_id(
        &self,
        item_id: Uuid,
        trigger: CensusTrigger,
    ) -> Result<Vec<std::result::Result<CensusReport, CensusError>>> {
        let item = self
            .db
            .get_dav_item_by_id(item_id)
            .await?
            .ok_or_else(|| DavError::ItemNotFound(format!("item {item_id}")))?;
        self.survey_item(&item, trigger).await
    }

    /// A read of `item` met a missing article: survey it in the background.
    /// Repeated hole reads of one item within [`HOLE_READ_DEBOUNCE`] start
    /// one census. Returns whether a census was started.
    pub fn hole_read(self: &Arc<Self>, item: &DavItem) -> bool {
        let now = Instant::now();
        {
            let mut recent = self.hole_reads.lock();
            recent.retain(|_, at| now.duration_since(*at) < HOLE_READ_DEBOUNCE);
            if recent.contains_key(&item.id) {
                debug!(item = %item.id, "hole read: census already started recently");
                return false;
            }
            recent.insert(item.id, now);
        }
        let this = Arc::clone(self);
        let item = item.clone();
        tokio::spawn(async move {
            match this.survey_item(&item, CensusTrigger::HoleRead).await {
                Ok(results) => {
                    for result in results {
                        match result {
                            Ok(report) => info!(
                                path = %item.path,
                                key = %report.key,
                                verdict = ?report.verdict,
                                "hole read census finished"
                            ),
                            Err(e) => {
                                debug!(path = %item.path, error = %e, "hole read census skipped")
                            }
                        }
                    }
                }
                Err(e) => warn!(path = %item.path, error = %e, "hole read census failed"),
            }
        });
        true
    }
}

/// Whether a read error means an article is missing (every server refused
/// it, or the ledger holds it as a confirmed hole), as opposed to a
/// transport failure.
pub fn is_missing_article(err: &std::io::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = err.get_ref().map(|e| e as _);
    while let Some(e) = source {
        if let Some(StreamError::ArticleNotFound(_)) = e.downcast_ref::<StreamError>() {
            return true;
        }
        source = e.source();
    }
    false
}

/// Whether a census report calls for a repair: slices are known lost and
/// the census did not find them beyond what the set carries.
fn wants_repair(report: &CensusReport) -> bool {
    report.damaged > 0 && report.verdict != Some(Verdict::BeyondRepair)
}

fn set_key(history_item_id: Uuid, recovery_set_id: &[u8; 16]) -> String {
    let hex: String = recovery_set_id.iter().map(|b| format!("{b:02x}")).collect();
    format!("{history_item_id}:{hex}")
}

/// Article ids of a DAV item's file blob, in read order.
fn segment_ids_of(sub_type: ItemSubType, blob: &[u8]) -> Result<Vec<String>> {
    let decode_err = |e: bincode::Error| DavError::Serialization(e.to_string());
    match sub_type {
        ItemSubType::NzbFile | ItemSubType::RarFile => {
            let file: DavNzbFile = bincode::deserialize(blob).map_err(decode_err)?;
            Ok(file.segment_ids)
        }
        ItemSubType::MultipartFile => {
            let file: DavMultipartFile = bincode::deserialize(blob).map_err(decode_err)?;
            Ok(file
                .file_parts
                .into_iter()
                .flat_map(|p| p.segment_ids)
                .collect())
        }
        other => Err(DavError::Other(format!(
            "no articles to survey for a {other:?} item"
        ))),
    }
}

/// Spread `size` bytes evenly over the articles. Without a recovery set the
/// census counts articles, so the spans only need to cover the file.
fn spread(ids: &[String], size: u64) -> Vec<ArticleSpan> {
    let n = ids.len() as u64;
    if n == 0 {
        return Vec::new();
    }
    let part = size.div_ceil(n).max(1);
    ids.iter()
        .enumerate()
        .map(|(i, id)| {
            let offset = (i as u64 * part).min(size);
            let len = part.min(size - offset);
            ArticleSpan {
                message_id: id.clone(),
                offset,
                len,
            }
        })
        .collect()
}
