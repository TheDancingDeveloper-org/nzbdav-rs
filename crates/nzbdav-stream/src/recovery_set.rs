//! Load the PAR2 recovery sets recorded at import in the shape the damage
//! census takes.

use nzbdav_core::database::DavDatabase;
use nzbdav_core::models::{Par2ProtectedFile, Par2SetRecord};
use uuid::Uuid;

use crate::census::{ArticleSpan, CensusFile, Par2FileEntry, Par2RecoverySet, RecoveryVolume};

impl From<&Par2SetRecord> for Par2RecoverySet {
    fn from(record: &Par2SetRecord) -> Self {
        Self {
            slice_size: record.slice_size,
            files: record
                .files
                .iter()
                .map(|f| Par2FileEntry {
                    name: f.name.clone(),
                    size: f.size,
                })
                .collect(),
            volumes: record
                .volumes
                .iter()
                .map(|v| RecoveryVolume {
                    blocks: v.blocks,
                    segment_ids: v.segment_ids.clone(),
                })
                .collect(),
        }
    }
}

/// Recovery sets recorded for a history item, one per PAR2 set the release
/// carries. Empty when the release has no PAR2 or the backend does not
/// record sets.
pub async fn load_recovery_sets(
    db: &dyn DavDatabase,
    history_item_id: Uuid,
) -> nzbdav_core::error::Result<Vec<Par2RecoverySet>> {
    Ok(db
        .get_par2_sets(history_item_id)
        .await?
        .iter()
        .map(Par2RecoverySet::from)
        .collect())
}

/// The files a set protects that the release carries, named as the set
/// names them, with each article's span sized from the recorded part size.
pub fn census_files(record: &Par2SetRecord) -> Vec<CensusFile> {
    record
        .files
        .iter()
        .filter_map(|f| census_file(f, record.part_size))
        .collect()
}

fn census_file(file: &Par2ProtectedFile, release_part_size: Option<u64>) -> Option<CensusFile> {
    let carried = file.carried.as_ref()?;
    let n = carried.segment_ids.len() as u64;
    if n == 0 {
        return None;
    }
    // Without a learned part size, spread the file evenly over its articles.
    let part = carried
        .part_size
        .or(release_part_size)
        .filter(|&p| p > 0)
        .unwrap_or_else(|| file.size.div_ceil(n).max(1));
    let articles = carried
        .segment_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let offset = (i as u64).saturating_mul(part).min(file.size);
            let remaining = file.size - offset;
            let len = if i as u64 == n - 1 {
                remaining
            } else {
                part.min(remaining)
            };
            ArticleSpan {
                message_id: id.clone(),
                offset,
                len,
            }
        })
        .collect();
    Some(CensusFile {
        name: file.name.clone(),
        articles,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbdav_core::models::{
        Par2BlockSource, Par2CarriedFile, Par2ProtectedFile, Par2VolumeRecord,
    };

    fn record() -> Par2SetRecord {
        Par2SetRecord {
            recovery_set_id: [1; 16],
            slice_size: 1000,
            index_packets: Vec::new(),
            index_segment_ids: Vec::new(),
            files: vec![
                Par2ProtectedFile {
                    file_id: [1; 16],
                    name: "a.rar".into(),
                    size: 2500,
                    carried: Some(Par2CarriedFile {
                        subject_name: "x".into(),
                        segment_ids: vec!["a1".into(), "a2".into(), "a3".into()],
                        part_size: Some(1000),
                    }),
                    dav_item_ids: Vec::new(),
                },
                Par2ProtectedFile {
                    file_id: [2; 16],
                    name: "b.rar".into(),
                    size: 900,
                    carried: None,
                    dav_item_ids: Vec::new(),
                },
            ],
            volumes: vec![Par2VolumeRecord {
                name: "a.vol0+2.par2".into(),
                first_exponent: Some(0),
                blocks: 2,
                blocks_source: Par2BlockSource::FileName,
                size: 2200,
                segment_ids: vec!["v1".into()],
            }],
            part_size: Some(1000),
        }
    }

    #[test]
    fn record_converts_to_census_set() {
        let set = Par2RecoverySet::from(&record());
        assert_eq!(set.slice_size, 1000);
        let names: Vec<_> = set
            .files
            .iter()
            .map(|f| (f.name.as_str(), f.size))
            .collect();
        assert_eq!(names, vec![("a.rar", 2500), ("b.rar", 900)]);
        assert_eq!(set.volumes.len(), 1);
        assert_eq!(set.volumes[0].blocks, 2);
        assert_eq!(set.volumes[0].segment_ids, vec!["v1"]);
    }

    #[test]
    fn census_files_size_articles_from_part_size() {
        let files = census_files(&record());
        assert_eq!(files.len(), 1, "files the release lacks are left out");
        let spans: Vec<_> = files[0]
            .articles
            .iter()
            .map(|a| (a.message_id.as_str(), a.offset, a.len))
            .collect();
        assert_eq!(
            spans,
            vec![("a1", 0, 1000), ("a2", 1000, 1000), ("a3", 2000, 500)]
        );
    }

    #[test]
    fn census_files_without_part_size_spread_evenly() {
        let mut r = record();
        r.part_size = None;
        r.files[0].carried.as_mut().unwrap().part_size = None;
        let spans: Vec<_> = census_files(&r)[0]
            .articles
            .iter()
            .map(|a| (a.offset, a.len))
            .collect();
        assert_eq!(spans, vec![(0, 834), (834, 834), (1668, 832)]);
    }
}
