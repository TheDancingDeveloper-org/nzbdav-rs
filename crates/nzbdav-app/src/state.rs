use std::sync::Arc;

use nzbdav_core::config::ConfigManager;
use nzbdav_core::database::DavDatabase;
use nzbdav_stream::provider::UsenetArticleProvider;
use nzbdav_stream::{CensusConfig, CensusService, DamageCensus};
use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::sync::watch;

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
