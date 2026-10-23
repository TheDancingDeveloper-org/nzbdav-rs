//! Operator trigger for the damage census.
//!
//! `POST /api/census/{item_id}` surveys every release the DAV item belongs
//! to (bypassing the release cooldown) and returns one result per release.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use nzbdav_core::error::DavError;
use nzbdav_stream::census::{CensusReport, DamageUnit};
use nzbdav_stream::{CensusTrigger, Verdict};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::state::AppState;

/// POST /api/census/{item_id}
pub async fn run_census(
    State(state): State<AppState>,
    Path(item_id): Path<Uuid>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let results = state
        .census
        .survey_item_id(item_id, CensusTrigger::Operator)
        .await
        .map_err(|e| {
            let status = match e {
                DavError::ItemNotFound(_) => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({ "error": e.to_string() })))
        })?;
    let releases: Vec<Value> = results
        .into_iter()
        .map(|result| match result {
            Ok(report) => report_json(&report),
            Err(e) => json!({ "error": e.to_string() }),
        })
        .collect();
    Ok(Json(json!({ "item_id": item_id, "releases": releases })))
}

fn report_json(report: &CensusReport) -> Value {
    let pool_failures: Vec<Value> = report
        .pool_failures
        .iter()
        .map(|f| json!({ "pool": f.name, "failed": f.failed, "last_error": f.last_error }))
        .collect();
    json!({
        "key": report.key,
        "articles": report.articles,
        "present": report.present,
        "missing": report.missing_ids,
        "unestablished": report.unestablished_ids,
        "provisional": report.provisional_ids,
        "pool_failures": pool_failures,
        "unit": match report.unit {
            DamageUnit::Slices => "slices",
            DamageUnit::Articles => "articles",
        },
        "damaged": report.damaged,
        "uncertain": report.uncertain,
        "recovery_available": report.recovery_available,
        "recovery_uncertain": report.recovery_uncertain,
        "unprotected_missing": report.unprotected_missing,
        "verdict": match report.verdict {
            Some(Verdict::Intact) => "intact",
            Some(Verdict::Repairable) => "repairable",
            Some(Verdict::BeyondRepair) => "beyond_repair",
            None => "unsettled",
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::post;
    use std::sync::Arc;
    use tower::ServiceExt;

    async fn test_router() -> Router {
        let conn = nzbdav_core::db::open(":memory:").unwrap();
        let db = Arc::new(parking_lot::Mutex::new(conn));
        let sqlite_db = nzbdav_core::sqlite_db::SqliteDavDatabase::new(Arc::clone(&db));
        let provider = Arc::new(nzbdav_stream::UsenetArticleProvider::new(vec![]));
        let (_, queue_status) =
            tokio::sync::watch::channel(crate::queue_manager::QueueStatus::default());
        let census = crate::state::census_service(&provider, Arc::new(sqlite_db));
        let state = AppState {
            db,
            config: nzbdav_core::config::ConfigManager::new(),
            provider,
            version: "0.1.0-test",
            queue_status,
            census,
        };
        Router::new()
            .route("/api/census/{item_id}", post(run_census))
            .with_state(state)
    }

    #[tokio::test]
    async fn unknown_item_is_404() {
        let resp = test_router()
            .await
            .oneshot(
                Request::post(format!("/api/census/{}", Uuid::new_v4()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
