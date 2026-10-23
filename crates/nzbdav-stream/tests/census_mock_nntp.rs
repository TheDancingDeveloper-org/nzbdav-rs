//! Damage census against in-process mock NNTP servers, through real
//! connection pools: mixed pools, partial outages and timeouts.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nzb_nntp::ConnectionPool;
use nzb_nntp::testutil::{MockConfig, MockNntpServer, test_config};
use nzbdav_stream::census::{ArticleSpan, CensusFile};
use nzbdav_stream::{
    CensusConfig, CensusTarget, CensusTrigger, DamageCensus, InMemoryHoleLedger,
    UsenetArticleProvider, Verdict,
};

fn mock(has: &[&str]) -> MockConfig {
    MockConfig {
        articles: has
            .iter()
            .map(|id| (id.to_string(), b"body".to_vec()))
            .collect(),
        ..MockConfig::default()
    }
}

fn pool(name: &str, port: u16) -> Arc<ConnectionPool> {
    let mut config = test_config(port);
    config.id = name.into();
    config.name = name.into();
    Arc::new(ConnectionPool::new(Arc::new(config)))
}

fn target(ids: &[&str]) -> CensusTarget {
    CensusTarget {
        key: "release".into(),
        files: vec![CensusFile {
            name: "movie.mkv".into(),
            articles: ids
                .iter()
                .enumerate()
                .map(|(i, id)| ArticleSpan {
                    message_id: id.to_string(),
                    offset: i as u64 * 100,
                    len: 100,
                })
                .collect(),
        }],
        recovery: None,
    }
}

fn census(pools: Vec<Arc<ConnectionPool>>, stat_timeout: Duration) -> DamageCensus {
    DamageCensus::new(
        Arc::new(UsenetArticleProvider::new(pools)),
        Arc::new(InMemoryHoleLedger::new(Duration::ZERO)),
        CensusConfig {
            stat_timeout,
            ..CensusConfig::default()
        },
    )
}

/// A port nothing listens on.
async fn dead_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn mixed_pools_settle_present_missing_and_unestablished() {
    let a = MockNntpServer::start(mock(&["ok1", "ok2"])).await;
    let mut b_config = mock(&["b-only"]);
    b_config.article_response_overrides = HashMap::from([("flaky".to_string(), 503)]);
    let b = MockNntpServer::start(b_config).await;

    let census = census(
        vec![pool("A", a.port()), pool("B", b.port())],
        Duration::from_secs(5),
    );
    let report = census
        .run(
            &target(&["ok1", "gone", "b-only", "flaky", "ok2"]),
            CensusTrigger::HealthCheck,
        )
        .await
        .unwrap();

    assert_eq!(report.present, 3);
    assert_eq!(report.missing_ids, vec!["gone"]);
    assert_eq!(report.unestablished_ids, vec!["flaky"]);
    assert_eq!((report.damaged, report.uncertain), (1, 1));
    assert_eq!(report.verdict, Some(Verdict::BeyondRepair));
    assert_eq!(report.pool_failures.len(), 1);
    assert_eq!(report.pool_failures[0].name, "B");
    assert_eq!(report.pool_failures[0].failed, 1);
}

#[tokio::test]
async fn hanging_pool_times_out_as_unestablished_not_missing() {
    let a = MockNntpServer::start(mock(&["ok"])).await;
    let mut b_config = mock(&[]);
    b_config.hang_after_command = Some("STAT".into());
    let b = MockNntpServer::start(b_config).await;

    let census = census(
        vec![pool("A", a.port()), pool("B", b.port())],
        Duration::from_millis(300),
    );
    let report = census
        .run(&target(&["ok", "x", "y"]), CensusTrigger::Operator)
        .await
        .unwrap();

    assert_eq!(report.present, 1);
    assert!(report.missing_ids.is_empty());
    assert_eq!(report.unestablished_ids, vec!["x", "y"]);
    assert_eq!(report.verdict, None);
    assert!(report.pool_failures[0].last_error.contains("timed out"));
}

#[tokio::test]
async fn pool_outage_falls_through_to_the_next_pool() {
    let b = MockNntpServer::start(mock(&["p", "q"])).await;
    let census = census(
        vec![pool("down", dead_port().await), pool("B", b.port())],
        Duration::from_secs(5),
    );
    let report = census
        .run(&target(&["p", "q", "r"]), CensusTrigger::Operator)
        .await
        .unwrap();

    assert_eq!(report.present, 2);
    // `r` is 430 on B, but the dead pool never answered: not a miss.
    assert!(report.missing_ids.is_empty());
    assert_eq!(report.unestablished_ids, vec!["r"]);
    assert_eq!(report.pool_failures[0].name, "down");
    assert_eq!(report.pool_failures[0].failed, 3);
}

#[tokio::test]
async fn every_pool_saying_430_is_a_miss() {
    let a = MockNntpServer::start(mock(&[])).await;
    let b = MockNntpServer::start(mock(&[])).await;
    let census = census(
        vec![pool("A", a.port()), pool("B", b.port())],
        Duration::from_secs(5),
    );
    let report = census
        .run(&target(&["gone"]), CensusTrigger::Operator)
        .await
        .unwrap();
    assert_eq!(report.missing_ids, vec!["gone"]);
    assert!(report.pool_failures.is_empty());
    assert_eq!(report.verdict, Some(Verdict::BeyondRepair));
}

/// A server that does not know STAT answers `500`: that is the server
/// failing to answer, never a 430, so the article stays unestablished.
#[tokio::test]
async fn server_without_stat_is_a_failure_not_a_miss() {
    let a = MockNntpServer::start(mock(&[])).await;
    let mut b_config = mock(&[]);
    b_config.article_response_overrides =
        HashMap::from([("x".to_string(), 500), ("y".to_string(), 500)]);
    let b = MockNntpServer::start(b_config).await;
    let census = census(
        vec![pool("A", a.port()), pool("B", b.port())],
        Duration::from_secs(5),
    );
    let report = census
        .run(&target(&["x", "y"]), CensusTrigger::Operator)
        .await
        .unwrap();
    assert!(report.missing_ids.is_empty());
    assert_eq!(report.unestablished_ids, vec!["x", "y"]);
    assert_eq!(report.pool_failures.len(), 1);
    assert_eq!(report.pool_failures[0].name, "B");
    assert_eq!(report.verdict, None);
}

/// A STAT that times out gives its connection back: on a one-connection
/// pool, the next acquire succeeds rather than waiting forever.
#[tokio::test]
async fn timed_out_stat_returns_its_connection() {
    let mut config = mock(&[]);
    config.hang_after_command = Some("STAT".into());
    let server = MockNntpServer::start(config).await;
    let mut server_config = test_config(server.port());
    server_config.name = "slow".into();
    server_config.connections = 1;
    let slow = Arc::new(ConnectionPool::new(Arc::new(server_config)));

    let census = census(vec![slow.clone()], Duration::from_millis(200));
    let report = census
        .run(&target(&["x", "y"]), CensusTrigger::Operator)
        .await
        .unwrap();
    assert_eq!(report.unestablished_ids, vec!["x", "y"]);
    assert!(report.pool_failures[0].last_error.contains("timed out"));

    let conn = tokio::time::timeout(Duration::from_secs(5), slow.acquire())
        .await
        .expect("the timed-out STAT's connection came back")
        .expect("a fresh connection opens");
    slow.discard(conn);
}
