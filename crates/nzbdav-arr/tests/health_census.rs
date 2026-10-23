//! The health check runs the damage census when one is attached.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use nzbdav_arr::HealthCheckService;
use nzbdav_arr::health_check::HealthCheckOutcome;
use nzbdav_core::database::DavDatabase;
use nzbdav_core::models::{DavItem, DavNzbFile, ItemSubType, ItemType};
use nzbdav_core::sqlite_db::SqliteDavDatabase;
use nzbdav_stream::census::CensusBackend;
use nzbdav_stream::{
    CensusConfig, CensusService, CensusTrigger, DamageCensus, HoleLedger, HoleState,
    InMemoryHoleLedger, PoolStat, PrioritizedPermit, UsenetArticleProvider,
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

struct Pool {
    missing: HashSet<&'static str>,
    failing: HashSet<&'static str>,
}

#[async_trait]
impl CensusBackend for Pool {
    fn pool_count(&self) -> usize {
        1
    }
    fn pool_name(&self, _pool: usize) -> String {
        "only".into()
    }
    fn pool_connections(&self, _pool: usize) -> usize {
        4
    }
    async fn stat_on_pool(&self, _pool: usize, id: &str) -> PoolStat {
        if self.failing.contains(id) {
            PoolStat::Failed("reset".into())
        } else if self.missing.contains(id) {
            PoolStat::Absent
        } else {
            PoolStat::Present
        }
    }
    async fn acquire_low(&self) -> Option<PrioritizedPermit> {
        None
    }
}

struct Setup {
    service: HealthCheckService,
    census: Arc<CensusService>,
    ledger: Arc<InMemoryHoleLedger>,
    item: DavItem,
}

async fn setup(missing: &[&'static str], failing: &[&'static str]) -> Setup {
    let conn = Arc::new(Mutex::new(nzbdav_core::db::open(":memory:").unwrap()));
    let db = Arc::new(SqliteDavDatabase::new(conn.clone()));
    let item = DavItem {
        id: Uuid::new_v4(),
        id_prefix: "m".into(),
        created_at: chrono::Utc::now().naive_utc(),
        parent_id: None,
        name: "movie.mkv".into(),
        file_size: Some(300),
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
        segment_ids: vec!["a".into(), "b".into(), "c".into()],
    };
    db.put_file_blob(
        item.file_blob_id.unwrap(),
        &bincode::serialize(&blob).unwrap(),
    )
    .await
    .unwrap();
    db.insert_dav_item(&item).await.unwrap();

    let ledger = Arc::new(InMemoryHoleLedger::new(Duration::ZERO));
    let census = Arc::new(CensusService::new(
        Arc::new(DamageCensus::new(
            Arc::new(Pool {
                missing: missing.iter().copied().collect(),
                failing: failing.iter().copied().collect(),
            }),
            ledger.clone(),
            CensusConfig::default(),
        )),
        db,
    ));
    let service = HealthCheckService::new(
        conn,
        Arc::new(UsenetArticleProvider::new(vec![])),
        CancellationToken::new(),
    )
    .with_census(census.clone());
    Setup {
        service,
        census,
        ledger,
        item,
    }
}

#[tokio::test]
async fn census_finds_an_intact_item_healthy() {
    let s = setup(&[], &[]).await;
    assert!(matches!(
        s.service.check_item(&s.item).await,
        HealthCheckOutcome::Healthy
    ));
}

#[tokio::test]
async fn census_misses_make_the_item_unhealthy_and_reach_the_ledger() {
    let s = setup(&["b"], &[]).await;
    match s.service.check_item(&s.item).await {
        HealthCheckOutcome::Unhealthy {
            missing_articles,
            total_articles,
            census,
        } => {
            assert_eq!((missing_articles, total_articles), (1, 3));
            assert_eq!(census.as_deref(), Some("census: beyond repair"));
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(s.ledger.state("b"), Some(HoleState::Confirmed));
}

#[tokio::test]
async fn unanswered_articles_are_an_error_not_a_miss() {
    let s = setup(&[], &["c"]).await;
    match s.service.check_item(&s.item).await {
        HealthCheckOutcome::Error(msg) => assert!(msg.contains("unestablished"), "{msg}"),
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(s.ledger.state("c"), None);
}

/// A release still cooling down from an earlier census falls back to STAT
/// (here: no servers, so the STAT path reports an error).
#[tokio::test]
async fn cooling_down_release_falls_back_to_stat() {
    let s = setup(&["b"], &[]).await;
    // An unsettled operator census leaves the release cooling down.
    s.census
        .survey_item(&s.item, CensusTrigger::Operator)
        .await
        .unwrap();
    assert!(matches!(
        s.service.check_item(&s.item).await,
        HealthCheckOutcome::Error(msg) if msg.contains("STAT failed")
    ));
}
