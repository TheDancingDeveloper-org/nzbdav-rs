//! A read that meets a missing article starts a damage census of the item.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use common::{MockDavDatabase, make_root_item};
use http_body_util::BodyExt;
use nzbdav_core::database::DavDatabase;
use nzbdav_core::models::{DavItem, DavNzbFile, ItemSubType, ItemType};
use nzbdav_dav::DatabaseStore;
use nzbdav_stream::census::CensusBackend;
use nzbdav_stream::testing::{MockArticleBackend, build_yenc_articles};
use nzbdav_stream::{
    CensusConfig, CensusService, DamageCensus, HoleLedger, HoleState, InMemoryHoleLedger, PoolStat,
    PrioritizedPermit,
};
use uuid::Uuid;

/// Counts STATs; every article but `seg-1` is there.
#[derive(Default)]
struct Counting {
    stats: AtomicUsize,
}

#[async_trait]
impl CensusBackend for Counting {
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
        self.stats.fetch_add(1, Ordering::SeqCst);
        if message_id == "seg-1" {
            PoolStat::Absent
        } else {
            PoolStat::Present
        }
    }
    async fn acquire_low(&self) -> Option<PrioritizedPermit> {
        None
    }
}

#[tokio::test]
async fn read_meeting_a_missing_article_starts_a_census() {
    let data: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
    let articles = build_yenc_articles(
        "movie.mkv",
        &data,
        &[(1, 0, 1000), (2, 1000, 1000), (3, 2000, 1000)],
    );
    // seg-1 is gone from the server.
    let mut backend = MockArticleBackend::new();
    for (i, article) in articles.into_iter().enumerate() {
        if i != 1 {
            backend.add(format!("seg-{i}"), article);
        }
    }
    let provider = backend.into_provider();

    let db = Arc::new(MockDavDatabase::new());
    let root = make_root_item();
    let item = DavItem {
        id: Uuid::new_v4(),
        id_prefix: "m".into(),
        created_at: chrono::Utc::now().naive_utc(),
        parent_id: Some(root.id),
        name: "movie.mkv".into(),
        file_size: Some(3000),
        item_type: ItemType::UsenetFile,
        sub_type: ItemSubType::NzbFile,
        path: "/content/movie.mkv".into(),
        release_date: None,
        last_health_check: None,
        next_health_check: None,
        history_item_id: None,
        file_blob_id: Some(Uuid::new_v4()),
        nzb_blob_id: None,
    };
    let blob = DavNzbFile {
        segment_ids: (0..3).map(|i| format!("seg-{i}")).collect(),
    };
    db.seed_file_blob(
        item.file_blob_id.unwrap(),
        bincode::serialize(&blob).unwrap(),
    )
    .await;
    db.seed_item(root).await;
    db.seed_item(item.clone()).await;

    let counting = Arc::new(Counting::default());
    let ledger = Arc::new(InMemoryHoleLedger::new(Duration::ZERO));
    let census = Arc::new(DamageCensus::new(
        counting.clone(),
        ledger.clone(),
        CensusConfig::default(),
    ));
    let dyn_db: Arc<dyn DavDatabase> = db.clone();
    let service = Arc::new(CensusService::new(census, dyn_db.clone()));
    let store = DatabaseStore::new(dyn_db, provider, 2).with_census(service);

    let body = store.get_body(&item).await.unwrap();
    assert!(body.collect().await.is_err(), "the read fails at seg-1");

    for _ in 0..200 {
        if ledger.state("seg-1").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(ledger.state("seg-1"), Some(HoleState::Confirmed));
    assert_eq!(counting.stats.load(Ordering::SeqCst), 3);
}
