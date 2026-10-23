//! The census service against a real (in-memory) database: targets built
//! from a DAV item's articles and the PAR2 sets recorded at import, holes
//! recorded in the shared ledger, and hole reads debounced.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nzbdav_core::database::DavDatabase;
use nzbdav_core::models::{
    DavItem, DavNzbFile, ItemSubType, ItemType, Par2BlockSource, Par2CarriedFile,
    Par2ProtectedFile, Par2SetRecord, Par2VolumeRecord,
};
use nzbdav_core::sqlite_db::SqliteDavDatabase;
use nzbdav_stream::census::{CensusBackend, DamageUnit};
use nzbdav_stream::{
    CensusConfig, CensusService, CensusTrigger, DamageCensus, HoleLedger, HoleState,
    InMemoryHoleLedger, PoolStat, PrioritizedPermit, StreamError, Verdict, is_missing_article,
};
use parking_lot::Mutex;
use uuid::Uuid;

/// One pool holding every article except `missing`.
struct OnePool {
    missing: HashSet<String>,
    calls: Mutex<usize>,
}

#[async_trait]
impl CensusBackend for OnePool {
    fn pool_count(&self) -> usize {
        1
    }
    fn pool_name(&self, _pool: usize) -> String {
        "only".into()
    }
    fn pool_connections(&self, _pool: usize) -> usize {
        4
    }
    async fn stat_on_pool(&self, _pool: usize, message_id: &str) -> PoolStat {
        *self.calls.lock() += 1;
        if self.missing.contains(message_id) {
            PoolStat::Absent
        } else {
            PoolStat::Present
        }
    }
    async fn acquire_low(&self) -> Option<PrioritizedPermit> {
        None
    }
}

struct Fixture {
    db: Arc<SqliteDavDatabase>,
    history: Uuid,
    protected: DavItem,
    loose: DavItem,
}

fn file_item(name: &str, history: Uuid, size: i64, blob: Uuid) -> DavItem {
    DavItem {
        id: Uuid::new_v4(),
        id_prefix: "x".into(),
        created_at: chrono::Utc::now().naive_utc(),
        parent_id: None,
        name: name.into(),
        file_size: Some(size),
        item_type: ItemType::UsenetFile,
        sub_type: ItemSubType::NzbFile,
        path: format!("/content/{name}"),
        release_date: None,
        last_health_check: None,
        next_health_check: None,
        history_item_id: Some(history),
        file_blob_id: Some(blob),
        nzb_blob_id: None,
    }
}

async fn put_item(db: &SqliteDavDatabase, item: &DavItem, segment_ids: &[&str]) {
    let blob = DavNzbFile {
        segment_ids: segment_ids.iter().map(|s| s.to_string()).collect(),
    };
    db.put_file_blob(
        item.file_blob_id.unwrap(),
        &bincode::serialize(&blob).unwrap(),
    )
    .await
    .unwrap();
    db.insert_dav_item(item).await.unwrap();
}

async fn fixture() -> Fixture {
    let conn = nzbdav_core::db::open(":memory:").unwrap();
    let db = Arc::new(SqliteDavDatabase::new(Arc::new(Mutex::new(conn))));
    let history = Uuid::new_v4();

    let protected = file_item("movie.mkv", history, 3000, Uuid::new_v4());
    put_item(&db, &protected, &["m1", "m2", "m3"]).await;
    let loose = file_item("extra.nfo", history, 200, Uuid::new_v4());
    put_item(&db, &loose, &["n1", "n2"]).await;

    let set = Par2SetRecord {
        recovery_set_id: [9; 16],
        slice_size: 1000,
        index_packets: Vec::new(),
        index_segment_ids: vec!["idx".into()],
        files: vec![Par2ProtectedFile {
            file_id: [1; 16],
            name: "movie.mkv".into(),
            size: 3000,
            carried: Some(Par2CarriedFile {
                subject_name: "movie.mkv".into(),
                segment_ids: vec!["m1".into(), "m2".into(), "m3".into()],
                part_size: Some(1000),
            }),
            dav_item_ids: vec![protected.id],
        }],
        volumes: vec![Par2VolumeRecord {
            name: "movie.vol00+02.par2".into(),
            first_exponent: Some(0),
            blocks: 2,
            blocks_source: Par2BlockSource::FileName,
            size: 2200,
            segment_ids: vec!["v1".into()],
        }],
        part_size: Some(1000),
    };
    db.put_par2_sets(history, protected.id, &[set])
        .await
        .unwrap();

    Fixture {
        db,
        history,
        protected,
        loose,
    }
}

fn service(
    db: Arc<SqliteDavDatabase>,
    missing: &[&str],
) -> (Arc<CensusService>, Arc<OnePool>, Arc<InMemoryHoleLedger>) {
    let backend = Arc::new(OnePool {
        missing: missing.iter().map(|s| s.to_string()).collect(),
        calls: Mutex::new(0),
    });
    let ledger = Arc::new(InMemoryHoleLedger::new(Duration::ZERO));
    let census = Arc::new(DamageCensus::new(
        backend.clone(),
        ledger.clone(),
        CensusConfig::default(),
    ));
    (Arc::new(CensusService::new(census, db)), backend, ledger)
}

#[tokio::test]
async fn protected_item_is_surveyed_as_its_recorded_set() {
    let f = fixture().await;
    let (service, _, _) = service(f.db.clone(), &[]);
    let targets = service.targets_for_item(&f.protected).await.unwrap();
    assert_eq!(targets.len(), 1);
    let target = &targets[0];
    assert!(target.key.starts_with(&f.history.to_string()));
    let recovery = target.recovery.as_ref().expect("recorded set loaded");
    assert_eq!(recovery.slice_size, 1000);
    assert_eq!(recovery.volumes[0].segment_ids, vec!["v1"]);
    let spans: Vec<_> = target.files[0]
        .articles
        .iter()
        .map(|a| (a.message_id.as_str(), a.offset, a.len))
        .collect();
    assert_eq!(
        spans,
        vec![("m1", 0, 1000), ("m2", 1000, 1000), ("m3", 2000, 1000)]
    );
}

#[tokio::test]
async fn unprotected_item_is_surveyed_on_its_own_articles() {
    let f = fixture().await;
    let (service, _, _) = service(f.db.clone(), &[]);
    let targets = service.targets_for_item(&f.loose).await.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].key, format!("item:{}", f.loose.id));
    assert!(targets[0].recovery.is_none());
    let ids: Vec<_> = targets[0].files[0]
        .articles
        .iter()
        .map(|a| (a.message_id.as_str(), a.offset, a.len))
        .collect();
    assert_eq!(ids, vec![("n1", 0, 100), ("n2", 100, 100)]);
}

/// The operator trigger surveys the set, records the miss in the shared
/// ledger and finds it repairable from the recorded volume.
#[tokio::test]
async fn operator_survey_records_holes_in_the_ledger() {
    let f = fixture().await;
    let (service, _, ledger) = service(f.db.clone(), &["m2"]);
    let results = service
        .survey_item_id(f.protected.id, CensusTrigger::Operator)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    let report = results[0].as_ref().unwrap();
    assert_eq!(report.trigger, CensusTrigger::Operator);
    assert_eq!(report.unit, DamageUnit::Slices);
    assert_eq!(report.missing_ids, vec!["m2"]);
    assert_eq!((report.damaged, report.recovery_available), (1, 2));
    assert_eq!(report.verdict, Some(Verdict::Repairable));
    assert_eq!(ledger.state("m2"), Some(HoleState::Confirmed));
    assert_eq!(ledger.state("m1"), None);
}

#[tokio::test]
async fn unknown_item_is_an_error() {
    let f = fixture().await;
    let (service, _, _) = service(f.db.clone(), &[]);
    assert!(
        service
            .survey_item_id(Uuid::new_v4(), CensusTrigger::Operator)
            .await
            .is_err()
    );
}

/// A hole read starts one background census per item, however many reads
/// hit the hole.
#[tokio::test]
async fn hole_reads_are_debounced_per_item() {
    let f = fixture().await;
    let (service, backend, ledger) = service(f.db.clone(), &["n1"]);
    assert!(service.hole_read(&f.loose));
    assert!(!service.hole_read(&f.loose));
    assert!(service.hole_read(&f.protected), "another item is surveyed");

    for _ in 0..200 {
        if ledger.state("n1").is_some() && *backend.calls.lock() >= 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(ledger.state("n1"), Some(HoleState::Confirmed));
    // n1, n2 for the loose item; m1..m3 and v1 for the set.
    assert_eq!(*backend.calls.lock(), 6);
}

#[test]
fn missing_article_errors_are_told_from_transport_errors() {
    let missing = std::io::Error::other(StreamError::ArticleNotFound("a".into()));
    let transport = std::io::Error::other(StreamError::NntpError("reset".into()));
    assert!(is_missing_article(&missing));
    assert!(!is_missing_article(&transport));
    assert!(!is_missing_article(&std::io::Error::other("plain")));
}
