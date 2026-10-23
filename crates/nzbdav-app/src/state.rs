use std::sync::Arc;

use nzbdav_arr::health_check::HealthCheckService;
use nzbdav_core::config::ConfigManager;
use nzbdav_core::database::DavDatabase;
use nzbdav_stream::provider::UsenetArticleProvider;
use nzbdav_stream::{CensusConfig, CensusService, DamageCensus};
use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::queue_manager::QueueStatus;

/// Shared application state.
#[derive(Clone)]
#[allow(dead_code)]
pub struct AppState {
    pub db: Arc<Mutex<Connection>>,
    pub config: ConfigManager,
    pub provider: Arc<UsenetArticleProvider>,
    pub version: &'static str,
    pub queue_status: watch::Receiver<QueueStatus>,
    /// Damage census of releases, for the operator trigger.
    pub census: Arc<CensusService>,
}

/// The damage census over `provider`'s pools, recording into the hole
/// ledger the provider consults, so reads and censuses share one ledger.
pub fn census_service(
    provider: &Arc<UsenetArticleProvider>,
    db: Arc<dyn DavDatabase>,
) -> Arc<CensusService> {
    let census = DamageCensus::new(provider.clone(), provider.ledger(), CensusConfig::default());
    Arc::new(CensusService::new(Arc::new(census), db))
}

/// The background health check, checking items by a damage census so the
/// health-check census trigger runs in the app.
pub fn health_check_service(
    db: Arc<Mutex<Connection>>,
    provider: Arc<UsenetArticleProvider>,
    census: Arc<CensusService>,
    cancel: CancellationToken,
) -> HealthCheckService {
    HealthCheckService::new(db, provider, cancel).with_census(census)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbdav_core::sqlite_db::SqliteDavDatabase;

    #[test]
    fn health_check_service_carries_the_census() {
        let conn = nzbdav_core::db::open(":memory:").expect("db");
        let db = Arc::new(Mutex::new(conn));
        let dav_db = Arc::new(SqliteDavDatabase::new(db.clone()));
        let provider = Arc::new(UsenetArticleProvider::new(vec![]));
        let census = census_service(&provider, dav_db);
        let service = health_check_service(db, provider, census, CancellationToken::new());
        assert!(service.has_census());
    }
}
