use axum::extract::{Path, Query, State};
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;

use crate::db::Database;

pub struct AppState {
    pub db: Mutex<Database>,
    pub started_at: std::time::Instant,
}

/// Stats returned by `Database::api_stats`.
pub struct ApiStats {
    pub vehicle_count: i64,
    pub car_count: i64,
    pub active_count: i64,
    pub alarm_count: i64,
    pub grouped_count: i64,
    pub pending_count: i64,
    pub last_packet_ts: Option<String>,
}

/// Vehicle row returned by `Database::api_vehicles`.
#[derive(Serialize)]
pub struct VehicleRow {
    pub vehicle_id: String,
    pub car_id: Option<String>,
    pub protocol: String,
    pub rtl433_id: i64,
    pub vehicle_class: String,
    pub pressure_kpa: f64,
    pub alarm: bool,
    pub battery_ok: bool,
    pub first_seen: String,
    pub last_seen: String,
    pub sighting_count: i64,
    pub tx_interval_median_ms: Option<i64>,
    pub fingerprint_id: Option<String>,
    pub active: bool,
}

/// Fingerprint row returned by `Database::api_fingerprints`.
#[derive(Serialize)]
pub struct FingerprintRow {
    pub fingerprint_id: String,
    pub rtl433_id: i64,
    pub vehicle_class: String,
    pub pressure_median_kpa: f64,
    pub tx_interval_median_ms: Option<i64>,
    pub first_seen: String,
    pub last_seen: String,
    pub total_sighting_count: i64,
    pub session_count: i64,
    pub alarm_rate: f64,
    pub vehicle_ids: Vec<String>,
}

/// Car detail returned by `Database::api_car_detail`.
#[derive(Serialize)]
pub struct CarDetailResponse {
    pub car_id: String,
    pub first_seen: Option<String>,
    pub last_seen: Option<String>,
    pub vehicle_class: Option<String>,
    pub members: Vec<String>,
    pub sightings: Vec<SightingRow>,
}

/// A single sighting inside the car detail response.
#[derive(Serialize)]
pub struct SightingRow {
    pub ts: String,
    pub pressure_kpa: f64,
    pub alarm: bool,
    pub sensor_id: String,
    pub receiver_id: String,
}

/// Temporal fingerprint row for the API and report.
#[derive(Serialize)]
pub struct TemporalFingerprintRow {
    pub fingerprint_id: String,
    pub vehicle_class: String,
    pub pressure_kpa: f64,
    pub session_count: i64,
    pub observation_days: i64,
    pub arrival_gmm_json: String,
    pub arrival_peak_hours_json: String,
    pub dwell_median_secs: i64,
    pub dwell_class: String,
    pub dominant_period_hrs: Option<f64>,
    pub periodicity_class: Option<String>,
    pub acf_peak_value: Option<f64>,
    pub presence_map_json: String,
    pub classification: String,
}

/// Temporal fingerprint API response for a single fingerprint.
#[derive(Serialize)]
pub struct TemporalApiResponse {
    pub fingerprint_id: String,
    pub classification: String,
    pub arrival_peaks: Vec<f32>,
    pub dwell_class: String,
    pub dwell_median_secs: i64,
    pub dominant_period_hrs: Option<f64>,
    pub periodicity_strength: Option<f64>,
    pub observation_days: i64,
    pub sessions_used: u32,
    pub presence_map: Vec<Vec<f32>>,
}

/// Start the HTTP dashboard server.
///
/// Opens its own read-only database connection so the ingestion loop and the
/// web server do not contend on the same `Connection` handle. WAL mode
/// (enabled by `Database::open`) allows concurrent reads.
pub async fn serve(db_path: &str, addr: &str) -> anyhow::Result<()> {
    let db = Database::open(db_path)?;
    let state = Arc::new(AppState {
        db: Mutex::new(db),
        started_at: std::time::Instant::now(),
    });

    let app = Router::new()
        .route("/", get(handle_index))
        .route("/api/stats", get(handle_stats))
        .route("/api/cars", get(handle_cars))
        .route("/api/cars/{car_id}", get(handle_car_detail))
        .route("/api/fingerprints", get(handle_fingerprints))
        .route(
            "/api/fingerprints/{fp_id}/temporal",
            get(handle_temporal_fingerprint),
        )
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = TcpListener::bind(addr).await?;
    eprintln!("Dashboard: http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_index() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

// ---------------------------------------------------------------------------
// JSON response types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StatsResponse {
    vehicle_count: i64,
    car_count: i64,
    active_count: i64,
    alarm_count: i64,
    grouped_count: i64,
    pending_count: i64,
    uptime_secs: u64,
    last_packet_ts: Option<String>,
}

#[derive(Deserialize)]
struct CarsQuery {
    filter: Option<String>,
    q: Option<String>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn handle_stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<StatsResponse>, axum::http::StatusCode> {
    let db = state
        .db
        .lock()
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let uptime = state.started_at.elapsed().as_secs();

    let stats = db
        .api_stats()
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(StatsResponse {
        vehicle_count: stats.vehicle_count,
        car_count: stats.car_count,
        active_count: stats.active_count,
        alarm_count: stats.alarm_count,
        grouped_count: stats.grouped_count,
        pending_count: stats.pending_count,
        uptime_secs: uptime,
        last_packet_ts: stats.last_packet_ts,
    }))
}

async fn handle_cars(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CarsQuery>,
) -> Result<Json<Vec<VehicleRow>>, axum::http::StatusCode> {
    let db = state
        .db
        .lock()
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let rows = db
        .api_vehicles(params.filter.as_deref(), params.q.as_deref())
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(rows))
}

async fn handle_car_detail(
    State(state): State<Arc<AppState>>,
    Path(car_id): Path<String>,
) -> Result<Json<CarDetailResponse>, axum::http::StatusCode> {
    let db = state
        .db
        .lock()
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let detail = db
        .api_car_detail(&car_id)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    match detail {
        Some(d) => Ok(Json(d)),
        None => Err(axum::http::StatusCode::NOT_FOUND),
    }
}

async fn handle_fingerprints(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<FingerprintRow>>, axum::http::StatusCode> {
    let db = state
        .db
        .lock()
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = db
        .api_fingerprints()
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rows))
}

async fn handle_temporal_fingerprint(
    State(state): State<Arc<AppState>>,
    Path(fp_id): Path<String>,
) -> Result<Json<TemporalApiResponse>, axum::http::StatusCode> {
    let db = state
        .db
        .lock()
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let tbf = db
        .get_temporal_fingerprint(&fp_id)
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    match tbf {
        Some(t) => {
            let arrival_peaks: Vec<f32> = t.arrival_gmm.iter().map(|c| c.mean_hour).collect();
            let presence_map: Vec<Vec<f32>> =
                t.presence_map.iter().map(|row| row.to_vec()).collect();
            let (dominant_period_hrs, periodicity_strength) = match &t.periodicity {
                Some(p) => (Some(p.dominant_period_hrs as f64), Some(p.strength as f64)),
                None => (None, None),
            };

            Ok(Json(TemporalApiResponse {
                fingerprint_id: t.fingerprint_id,
                classification: t.classification.as_str().to_string(),
                arrival_peaks,
                dwell_class: t.dwell_class.as_str().to_string(),
                dwell_median_secs: t.dwell_median_secs,
                dominant_period_hrs,
                periodicity_strength,
                observation_days: t.observation_days,
                sessions_used: t.sessions_used,
                presence_map,
            }))
        }
        None => Err(axum::http::StatusCode::NOT_FOUND),
    }
}
