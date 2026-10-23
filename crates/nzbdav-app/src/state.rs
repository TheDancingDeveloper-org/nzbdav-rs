use std::sync::Arc;

use nzbdav_arr::health_check::HealthCheckService;
use nzbdav_core::config::ConfigManager;
use nzbdav_core::database::DavDatabase;
use nzbdav_stream::provider::UsenetArticleProvider;
use nzbdav_stream::{CensusConfig, CensusService, DamageCensus, RepairConfig, RepairService};
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
#[cfg(test)]
pub fn census_service(
    provider: &Arc<UsenetArticleProvider>,
    db: Arc<dyn DavDatabase>,
) -> Arc<CensusService> {
    census_service_with_repair(provider, db, None)
}

/// The census service, scheduling repairs on `repair` when given.
pub fn census_service_with_repair(
    provider: &Arc<UsenetArticleProvider>,
    db: Arc<dyn DavDatabase>,
    repair: Option<Arc<RepairService>>,
) -> Arc<CensusService> {
    let census = DamageCensus::new(provider.clone(), provider.ledger(), CensusConfig::default());
    let service = CensusService::new(Arc::new(census), db);
    Arc::new(match repair {
        Some(repair) => service.with_repair(repair),
        None => service,
    })
}

/// The PAR2 repair job over `provider`'s pools, its hole ledger and patch
/// store, switched by `enable_repair`. `None` without a patch store: repairs
/// would have nowhere to go.
pub fn repair_service(
    provider: &Arc<UsenetArticleProvider>,
    config: &ConfigManager,
) -> Option<Arc<RepairService>> {
    let patches = provider.patches()?;
    let config = config.clone();
    let service = RepairService::new(
        provider.clone(),
        provider.ledger(),
        patches,
        RepairConfig::default(),
    )
    .with_switch(move || config.get_enable_repair());
    Some(Arc::new(service))
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
