//! Temporal Behavioural Fingerprinting (TBF) for long-term vehicle
//! re-identification.
//!
//! A temporal fingerprint captures four distributions computed from a
//! vehicle's sighting history:
//!
//! 1. **Arrival distribution** — hour-of-day GMM
//! 2. **Dwell time distribution** — log-normal fit
//! 3. **Periodicity** — autocorrelation-derived dominant recurrence period
//! 4. **Presence map** — 24×7 matrix of presence probability

use chrono::{DateTime, Datelike, Timelike, Utc};
use rustfft::num_complex::Complex;
use rustfft::FftPlanner;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum number of sessions before a TBF is computed.
pub const MIN_TBF_SESSIONS: u32 = 7;

/// Default gap threshold (seconds) for session segmentation.
pub const SESSION_GAP_THRESHOLD_SECS: i64 = 600;

/// TBF recompute interval (seconds).
pub const TBF_UPDATE_INTERVAL_SECS: u64 = 3600;

/// Similarity threshold for cross-receiver matching.
pub const TBF_SIMILARITY_THRESHOLD: f32 = 0.85;

/// Minimum ACF peak strength to claim periodicity.
pub const MIN_PERIODICITY_STRENGTH: f32 = 0.30;

/// Presence map dimensions.
pub const PRESENCE_MAP_HOURS: usize = 24;
pub const PRESENCE_MAP_DAYS: usize = 7;

/// Maximum GMM components for arrival distribution.
pub const MAX_GMM_COMPONENTS: usize = 4;

/// Minimum peak height (fraction of max bin) for GMM peak detection.
pub const MIN_GMM_PEAK_HEIGHT: f32 = 0.10;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A contiguous session of sightings for a fingerprint.
#[derive(Debug, Clone)]
pub struct Session {
    pub fingerprint_id: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub sighting_count: u32,
}

impl Session {
    /// Dwell time in seconds.
    pub fn dwell_secs(&self) -> i64 {
        (self.end - self.start).num_seconds()
    }

    /// Fractional arrival hour (e.g. 8.5 = 08:30).
    pub fn arrival_hour(&self) -> f32 {
        self.start.hour() as f32 + self.start.minute() as f32 / 60.0
    }

    /// Day of week (0 = Monday, 6 = Sunday).
    pub fn day_of_week(&self) -> u32 {
        self.start.weekday().num_days_from_monday()
    }
}

/// A single Gaussian component in the arrival-time mixture model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GaussianComponent {
    pub mean_hour: f32,
    pub sigma_hours: f32,
    pub weight: f32,
}

/// Periodicity classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum PeriodClass {
    #[serde(rename = "twice_daily")]
    TwiceDaily,
    #[serde(rename = "daily")]
    Daily,
    #[serde(rename = "weekly")]
    Weekly,
    #[serde(rename = "irregular")]
    Irregular,
    #[serde(rename = "stationary")]
    Stationary,
}

impl PeriodClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            PeriodClass::TwiceDaily => "twice_daily",
            PeriodClass::Daily => "daily",
            PeriodClass::Weekly => "weekly",
            PeriodClass::Irregular => "irregular",
            PeriodClass::Stationary => "stationary",
        }
    }
}

/// Result of periodicity analysis.
#[derive(Debug, Clone, Serialize)]
pub struct PeriodResult {
    pub dominant_period_hrs: f32,
    pub strength: f32,
    pub class: PeriodClass,
}

/// Dwell-time classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum DwellClass {
    #[serde(rename = "drive_by")]
    DriveBy,
    #[serde(rename = "brief_stop")]
    BriefStop,
    #[serde(rename = "long_term_parked")]
    LongTermParked,
}

impl DwellClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            DwellClass::DriveBy => "drive_by",
            DwellClass::BriefStop => "brief_stop",
            DwellClass::LongTermParked => "long_term_parked",
        }
    }
}

/// High-level vehicle classification based on TBF.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum VehicleClassification {
    #[serde(rename = "stationary")]
    Stationary,
    #[serde(rename = "daily_commuter")]
    DailyCommuter,
    #[serde(rename = "twice_daily_commuter")]
    TwiceDailyCommuter,
    #[serde(rename = "weekly_visitor")]
    WeeklyVisitor,
    #[serde(rename = "regular_visitor")]
    RegularVisitor,
    #[serde(rename = "transient")]
    Transient,
}

impl VehicleClassification {
    pub fn as_str(&self) -> &'static str {
        match self {
            VehicleClassification::Stationary => "stationary",
            VehicleClassification::DailyCommuter => "daily_commuter",
            VehicleClassification::TwiceDailyCommuter => "twice_daily_commuter",
            VehicleClassification::WeeklyVisitor => "weekly_visitor",
            VehicleClassification::RegularVisitor => "regular_visitor",
            VehicleClassification::Transient => "transient",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            VehicleClassification::Stationary => "STATIONARY VEHICLE",
            VehicleClassification::DailyCommuter => "DAILY COMMUTER",
            VehicleClassification::TwiceDailyCommuter => "TWICE-DAILY COMMUTER",
            VehicleClassification::WeeklyVisitor => "WEEKLY VISITOR",
            VehicleClassification::RegularVisitor => "REGULAR VISITOR",
            VehicleClassification::Transient => "TRANSIENT",
        }
    }
}

/// Full temporal behavioural fingerprint for a vehicle.
#[derive(Debug, Clone, Serialize)]
pub struct TemporalFingerprint {
    pub fingerprint_id: String,
    pub arrival_gmm: Vec<GaussianComponent>,
    pub arrival_peak_hours: Vec<u32>,
    pub dwell_lognormal_mu: f64,
    pub dwell_lognormal_sigma: f64,
    pub dwell_median_secs: i64,
    pub dwell_class: DwellClass,
    pub periodicity: Option<PeriodResult>,
    pub presence_map: [[f32; PRESENCE_MAP_DAYS]; PRESENCE_MAP_HOURS],
    pub observation_days: i64,
    pub sessions_used: u32,
    pub classification: VehicleClassification,
    pub computed_at: String,
}

// ---------------------------------------------------------------------------
// Session extraction
// ---------------------------------------------------------------------------

/// A timestamped sighting for session extraction.
#[derive(Debug, Clone)]
pub struct SightingTimestamp {
    pub fingerprint_id: String,
    pub ts: DateTime<Utc>,
}

/// Extract sessions from time-ordered sightings for a single fingerprint.
///
/// A session is a contiguous block where the gap between consecutive
/// sightings is less than `gap_threshold_secs`.
pub fn extract_sessions(
    fingerprint_id: &str,
    timestamps: &[DateTime<Utc>],
    gap_threshold_secs: i64,
) -> Vec<Session> {
    if timestamps.is_empty() {
        return Vec::new();
    }

    let mut sessions = Vec::new();
    let mut session_start = timestamps[0];
    let mut session_end = timestamps[0];
    let mut count: u32 = 1;

    for ts in &timestamps[1..] {
        let gap = (*ts - session_end).num_seconds();
        if gap > gap_threshold_secs {
            sessions.push(Session {
                fingerprint_id: fingerprint_id.to_string(),
                start: session_start,
                end: session_end,
                sighting_count: count,
            });
            session_start = *ts;
            session_end = *ts;
            count = 1;
        } else {
            session_end = *ts;
            count += 1;
        }
    }

    // Push the final session.
    sessions.push(Session {
        fingerprint_id: fingerprint_id.to_string(),
        start: session_start,
        end: session_end,
        sighting_count: count,
    });

    sessions
}

// ---------------------------------------------------------------------------
// GMM fitting for arrival distribution
// ---------------------------------------------------------------------------

/// Fit a simplified Gaussian mixture model to arrival hours.
///
/// Uses histogram peak detection followed by single-pass Gaussian parameter
/// estimation around each peak.
pub fn fit_arrival_gmm(sessions: &[Session], max_components: usize) -> Vec<GaussianComponent> {
    if sessions.is_empty() {
        return Vec::new();
    }

    // Build 24-bin hourly histogram.
    let mut histogram = [0u32; 24];
    for s in sessions {
        let hour = s.arrival_hour() as usize;
        if hour < 24 {
            histogram[hour] += 1;
        }
    }

    let max_count = *histogram.iter().max().unwrap_or(&1) as f32;
    if max_count == 0.0 {
        return Vec::new();
    }

    // Find peaks: bins that are local maxima and above the minimum height.
    let threshold = max_count * MIN_GMM_PEAK_HEIGHT;
    let mut peaks: Vec<usize> = Vec::new();
    for h in 0..24 {
        let val = histogram[h] as f32;
        if val < threshold {
            continue;
        }
        let left = histogram[(h + 23) % 24] as f32;
        let right = histogram[(h + 1) % 24] as f32;
        if val >= left && val >= right {
            peaks.push(h);
        }
    }

    // Limit to max_components.
    peaks.sort_by(|a, b| histogram[*b].cmp(&histogram[*a]));
    peaks.truncate(max_components);
    peaks.sort();

    if peaks.is_empty() {
        // Fallback: use the overall mean.
        let total: f32 = sessions.iter().map(|s| s.arrival_hour()).sum();
        let mean = total / sessions.len() as f32;
        let variance: f32 = sessions
            .iter()
            .map(|s| {
                let d = s.arrival_hour() - mean;
                d * d
            })
            .sum::<f32>()
            / sessions.len() as f32;
        return vec![GaussianComponent {
            mean_hour: mean,
            sigma_hours: variance.sqrt().max(0.5),
            weight: 1.0,
        }];
    }

    // Assign each session to the nearest peak and compute per-peak stats.
    let total_sessions = sessions.len() as f32;
    let mut components = Vec::new();

    for &peak_hour in &peaks {
        let _peak_f = peak_hour as f32 + 0.5; // centre of the hour bin

        // Collect arrival hours assigned to this peak (nearest-peak rule).
        let assigned: Vec<f32> = sessions
            .iter()
            .map(|s| s.arrival_hour())
            .filter(|&ah| {
                let nearest = peaks
                    .iter()
                    .min_by(|&&a, &&b| {
                        let da = circular_dist(ah, a as f32 + 0.5, 24.0);
                        let db = circular_dist(ah, b as f32 + 0.5, 24.0);
                        da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .copied()
                    .unwrap_or(peak_hour);
                nearest == peak_hour
            })
            .collect();

        if assigned.is_empty() {
            continue;
        }

        let n = assigned.len() as f32;
        let mean = assigned.iter().sum::<f32>() / n;
        let variance = assigned.iter().map(|&x| (x - mean) * (x - mean)).sum::<f32>() / n;
        let sigma = variance.sqrt().max(0.5);

        components.push(GaussianComponent {
            mean_hour: mean,
            sigma_hours: sigma,
            weight: n / total_sessions,
        });
    }

    components
}

/// Circular distance on a [0, period) domain.
fn circular_dist(a: f32, b: f32, period: f32) -> f32 {
    let d = (a - b).abs();
    d.min(period - d)
}

// ---------------------------------------------------------------------------
// Dwell-time analysis
// ---------------------------------------------------------------------------

/// Compute log-normal parameters and classification from session dwell times.
pub fn compute_dwell(sessions: &[Session]) -> (f64, f64, i64, DwellClass) {
    let dwell_secs: Vec<i64> = sessions.iter().map(|s| s.dwell_secs().max(1)).collect();

    if dwell_secs.is_empty() {
        return (0.0, 0.0, 0, DwellClass::DriveBy);
    }

    // Log-normal parameters.
    let log_vals: Vec<f64> = dwell_secs.iter().map(|&s| (s as f64).ln()).collect();
    let n = log_vals.len() as f64;
    let mu = log_vals.iter().sum::<f64>() / n;
    let sigma = if n > 1.0 {
        (log_vals.iter().map(|&x| (x - mu) * (x - mu)).sum::<f64>() / (n - 1.0)).sqrt()
    } else {
        0.0
    };

    // Median dwell.
    let mut sorted = dwell_secs.clone();
    sorted.sort_unstable();
    let median = sorted[sorted.len() / 2];

    // Classification.
    let dwell_class = if median < 30 {
        DwellClass::DriveBy
    } else if median < 600 {
        DwellClass::BriefStop
    } else {
        DwellClass::LongTermParked
    };

    (mu, sigma, median, dwell_class)
}

// ---------------------------------------------------------------------------
// Periodicity via autocorrelation (FFT)
// ---------------------------------------------------------------------------

/// Compute periodicity from session data using FFT-based autocorrelation.
///
/// Bins sessions into 1-hour slots over a 7-day window and finds the
/// dominant recurrence period.
pub fn compute_periodicity(sessions: &[Session], window_days: u32) -> Option<PeriodResult> {
    if sessions.is_empty() {
        return None;
    }

    let n_hours = (window_days as usize) * 24;
    if n_hours < 2 {
        return None;
    }

    // Find the earliest session to establish the time origin.
    let t0 = sessions.iter().map(|s| s.start).min()?;

    // Build presence series: 1 if any session overlaps this hour, else 0.
    let mut presence = vec![0.0f32; n_hours];
    for s in sessions {
        let start_offset = (s.start - t0).num_hours().max(0) as usize;
        let end_offset = (s.end - t0).num_hours().max(0) as usize;
        for h in start_offset..=end_offset.min(n_hours - 1) {
            if h < n_hours {
                presence[h] = 1.0;
            }
        }
    }

    // Check if there's any variation.
    let mean: f32 = presence.iter().sum::<f32>() / n_hours as f32;
    if mean < f32::EPSILON || (1.0 - mean) < f32::EPSILON {
        // No variation — stationary or never present.
        return None;
    }

    // Compute autocorrelation via FFT.
    let acf = autocorrelation_fft(&presence);

    // Find peak lag excluding lag-0, for lags in [2, n_hours/2].
    let max_lag = (n_hours / 2).min(168); // at most 7 days
    if max_lag < 2 {
        return None;
    }

    let mut best_lag = 1usize;
    let mut best_val = f32::NEG_INFINITY;
    for lag in 2..=max_lag.min(acf.len() - 1) {
        if acf[lag] > best_val {
            best_val = acf[lag];
            best_lag = lag;
        }
    }

    if best_val < MIN_PERIODICITY_STRENGTH {
        return None;
    }

    let period_hrs = best_lag as f32;
    let class = classify_period(period_hrs);

    Some(PeriodResult {
        dominant_period_hrs: period_hrs,
        strength: best_val,
        class,
    })
}

/// Classify a dominant period into a named category.
fn classify_period(period_hrs: f32) -> PeriodClass {
    if period_hrs < 14.0 {
        PeriodClass::TwiceDaily
    } else if period_hrs <= 30.0 {
        PeriodClass::Daily
    } else if period_hrs >= 150.0 && period_hrs <= 186.0 {
        PeriodClass::Weekly
    } else {
        PeriodClass::Irregular
    }
}

/// Compute the normalised autocorrelation function via FFT.
///
/// Returns a vector where `acf[k]` is the normalised autocorrelation at lag `k`.
fn autocorrelation_fft(signal: &[f32]) -> Vec<f32> {
    let n = signal.len();
    // Pad to at least 2*n for linear (non-circular) autocorrelation.
    let padded_len = (2 * n).next_power_of_two();

    let mut planner = FftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(padded_len);
    let ifft = planner.plan_fft_inverse(padded_len);

    // Mean-subtract the signal.
    let mean: f32 = signal.iter().sum::<f32>() / n as f32;
    let mut buffer: Vec<Complex<f32>> = signal
        .iter()
        .map(|&x| Complex::new(x - mean, 0.0))
        .collect();
    buffer.resize(padded_len, Complex::new(0.0, 0.0));

    // Forward FFT.
    fft.process(&mut buffer);

    // Power spectrum (|X(f)|²).
    for c in buffer.iter_mut() {
        *c = Complex::new(c.norm_sqr(), 0.0);
    }

    // Inverse FFT.
    ifft.process(&mut buffer);

    // Normalise.
    let scale = padded_len as f32;
    let acf0 = buffer[0].re / scale;
    if acf0.abs() < f32::EPSILON {
        return vec![0.0; n];
    }

    buffer
        .iter()
        .take(n)
        .map(|c| (c.re / scale) / acf0)
        .collect()
}

// ---------------------------------------------------------------------------
// Presence map (24×7)
// ---------------------------------------------------------------------------

/// Compute a 24×7 presence probability map.
///
/// `presence_map[hour][day_of_week]` is the fraction of weeks in which
/// the vehicle was observed during that (hour, day) cell.
pub fn compute_presence_map(
    sessions: &[Session],
    observation_days: i64,
) -> [[f32; PRESENCE_MAP_DAYS]; PRESENCE_MAP_HOURS] {
    let mut map = [[0.0f32; PRESENCE_MAP_DAYS]; PRESENCE_MAP_HOURS];

    if sessions.is_empty() || observation_days <= 0 {
        return map;
    }

    let weeks = (observation_days as f32 / 7.0).max(1.0);

    // Count distinct weeks in which each (hour, day) cell had a session.
    // Use a set of (hour, day, week_number) to avoid double-counting.
    let mut seen: std::collections::HashSet<(u32, u32, i64)> = std::collections::HashSet::new();
    let t0 = sessions.iter().map(|s| s.start).min().unwrap();

    for s in sessions {
        let hour = s.start.hour();
        let dow = s.start.weekday().num_days_from_monday();
        let week_num = (s.start - t0).num_weeks();
        seen.insert((hour, dow, week_num));

        // Also mark hours covered by dwell time.
        if s.dwell_secs() > 0 {
            let end_hour = s.end.hour();
            let end_dow = s.end.weekday().num_days_from_monday();
            if end_dow == dow {
                for h in hour..=end_hour {
                    seen.insert((h, dow, week_num));
                }
            }
        }
    }

    for (hour, dow, _) in seen {
        if (hour as usize) < PRESENCE_MAP_HOURS && (dow as usize) < PRESENCE_MAP_DAYS {
            map[hour as usize][dow as usize] += 1.0;
        }
    }

    // Normalise by number of weeks.
    for row in map.iter_mut() {
        for cell in row.iter_mut() {
            *cell = (*cell / weeks).min(1.0);
        }
    }

    map
}

// ---------------------------------------------------------------------------
// TBF computation
// ---------------------------------------------------------------------------

/// Compute a full temporal fingerprint from sessions.
pub fn compute_tbf(fingerprint_id: &str, sessions: &[Session]) -> Option<TemporalFingerprint> {
    if (sessions.len() as u32) < MIN_TBF_SESSIONS {
        return None;
    }

    let arrival_gmm = fit_arrival_gmm(sessions, MAX_GMM_COMPONENTS);
    let arrival_peak_hours: Vec<u32> = arrival_gmm
        .iter()
        .map(|c| c.mean_hour.round() as u32 % 24)
        .collect();

    let (dwell_mu, dwell_sigma, dwell_median, dwell_class) = compute_dwell(sessions);

    // Observation span in days.
    let first = sessions.iter().map(|s| s.start).min().unwrap();
    let last = sessions.iter().map(|s| s.end).max().unwrap();
    let observation_days = (last - first).num_days().max(1);

    let periodicity = compute_periodicity(sessions, observation_days.min(7) as u32);
    let presence_map = compute_presence_map(sessions, observation_days);

    let classification = classify_vehicle(&dwell_class, &periodicity, &arrival_gmm);

    Some(TemporalFingerprint {
        fingerprint_id: fingerprint_id.to_string(),
        arrival_gmm,
        arrival_peak_hours,
        dwell_lognormal_mu: dwell_mu,
        dwell_lognormal_sigma: dwell_sigma,
        dwell_median_secs: dwell_median,
        dwell_class,
        periodicity,
        presence_map,
        observation_days,
        sessions_used: sessions.len() as u32,
        classification,
        computed_at: Utc::now().to_rfc3339(),
    })
}

/// Derive a high-level vehicle classification from TBF components.
fn classify_vehicle(
    dwell_class: &DwellClass,
    periodicity: &Option<PeriodResult>,
    arrival_gmm: &[GaussianComponent],
) -> VehicleClassification {
    // Stationary: long-term parked with no clear periodicity.
    if *dwell_class == DwellClass::LongTermParked {
        if periodicity.is_none() {
            return VehicleClassification::Stationary;
        }
    }

    // Check periodicity class.
    if let Some(period) = periodicity {
        match period.class {
            PeriodClass::TwiceDaily => return VehicleClassification::TwiceDailyCommuter,
            PeriodClass::Daily => {
                if *dwell_class == DwellClass::DriveBy {
                    return VehicleClassification::DailyCommuter;
                }
                // Bimodal arrival with daily period → daily commuter.
                if arrival_gmm.len() >= 2 {
                    return VehicleClassification::DailyCommuter;
                }
                return VehicleClassification::RegularVisitor;
            }
            PeriodClass::Weekly => return VehicleClassification::WeeklyVisitor,
            _ => {}
        }
    }

    // Fallback: drive-by with no periodicity → transient.
    if *dwell_class == DwellClass::DriveBy && periodicity.is_none() {
        return VehicleClassification::Transient;
    }

    // Brief stops with some periodicity → regular visitor.
    if *dwell_class == DwellClass::BriefStop {
        return VehicleClassification::RegularVisitor;
    }

    VehicleClassification::Transient
}

// ---------------------------------------------------------------------------
// TBF similarity
// ---------------------------------------------------------------------------

/// Compute similarity between two temporal fingerprints (0.0–1.0).
pub fn tbf_similarity(a: &TemporalFingerprint, b: &TemporalFingerprint) -> f32 {
    let arrival_sim = gmm_similarity(&a.arrival_gmm, &b.arrival_gmm);
    let dwell_sim = lognormal_overlap(
        a.dwell_lognormal_mu,
        a.dwell_lognormal_sigma,
        b.dwell_lognormal_mu,
        b.dwell_lognormal_sigma,
    );
    let period_sim = period_similarity(&a.periodicity, &b.periodicity);
    let presence_sim = presence_map_cosine(&a.presence_map, &b.presence_map);

    0.35 * arrival_sim + 0.20 * dwell_sim + 0.20 * period_sim + 0.25 * presence_sim
}

/// Simplified GMM similarity using overlap of peak positions.
fn gmm_similarity(a: &[GaussianComponent], b: &[GaussianComponent]) -> f32 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    // For each component in A, find the best-matching component in B.
    let mut total_sim = 0.0f32;
    let mut total_weight = 0.0f32;
    for ca in a {
        let mut best = 0.0f32;
        for cb in b {
            let dist = circular_dist(ca.mean_hour, cb.mean_hour, 24.0);
            let sigma_avg = (ca.sigma_hours + cb.sigma_hours) / 2.0;
            let sim = (-dist * dist / (2.0 * sigma_avg * sigma_avg)).exp();
            if sim > best {
                best = sim;
            }
        }
        total_sim += ca.weight * best;
        total_weight += ca.weight;
    }

    if total_weight > 0.0 {
        total_sim / total_weight
    } else {
        0.0
    }
}

/// Log-normal distribution overlap using simplified Bhattacharyya coefficient.
fn lognormal_overlap(mu1: f64, sigma1: f64, mu2: f64, sigma2: f64) -> f32 {
    if sigma1.abs() < f64::EPSILON && sigma2.abs() < f64::EPSILON {
        return if (mu1 - mu2).abs() < 0.1 { 1.0 } else { 0.0 };
    }
    let sigma_avg = (sigma1 * sigma1 + sigma2 * sigma2) / 2.0;
    if sigma_avg.abs() < f64::EPSILON {
        return 1.0;
    }
    let dm = mu1 - mu2;
    let bc = (-dm * dm / (8.0 * sigma_avg)).exp();
    bc as f32
}

/// Period similarity: 1.0 if both have similar periods, 0.0 otherwise.
fn period_similarity(a: &Option<PeriodResult>, b: &Option<PeriodResult>) -> f32 {
    match (a, b) {
        (Some(pa), Some(pb)) => {
            if pa.class == pb.class {
                // Refine by actual period proximity.
                let diff = (pa.dominant_period_hrs - pb.dominant_period_hrs).abs();
                let max_p = pa.dominant_period_hrs.max(pb.dominant_period_hrs);
                if max_p > 0.0 {
                    1.0 - (diff / max_p).min(1.0)
                } else {
                    1.0
                }
            } else {
                0.2 // Different period class → low similarity.
            }
        }
        (None, None) => 1.0, // Both irregular/stationary.
        _ => 0.0,
    }
}

/// Cosine similarity of two 24×7 presence maps.
fn presence_map_cosine(
    a: &[[f32; PRESENCE_MAP_DAYS]; PRESENCE_MAP_HOURS],
    b: &[[f32; PRESENCE_MAP_DAYS]; PRESENCE_MAP_HOURS],
) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for h in 0..PRESENCE_MAP_HOURS {
        for d in 0..PRESENCE_MAP_DAYS {
            dot += a[h][d] * b[h][d];
            norm_a += a[h][d] * a[h][d];
            norm_b += b[h][d] * b[h][d];
        }
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom > f32::EPSILON {
        dot / denom
    } else {
        0.0
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Format a dwell time in seconds as a human-readable string.
pub fn format_dwell(secs: i64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        let m = secs / 60;
        let s = secs % 60;
        if s > 0 {
            format!("{m}min {s}s")
        } else {
            format!("{m}min")
        }
    } else {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        if m > 0 {
            format!("{h}h {m}min")
        } else {
            format!("{h}h")
        }
    }
}

/// Format arrival peaks as a human-readable string.
pub fn format_arrival(gmm: &[GaussianComponent]) -> String {
    if gmm.is_empty() {
        return "unknown".to_string();
    }

    // Check if the distribution is nearly flat (all weights similar, high sigma).
    let max_sigma = gmm.iter().map(|c| c.sigma_hours).fold(0.0f32, f32::max);
    if max_sigma > 6.0 {
        return "flat (always present)".to_string();
    }

    if gmm.len() == 1 {
        let c = &gmm[0];
        let h = c.mean_hour as u32;
        let m = ((c.mean_hour - h as f32) * 60.0) as u32;
        return format!("unimodal — peak at {h:02}:{m:02}");
    }

    let peaks: Vec<String> = gmm
        .iter()
        .map(|c| {
            let h = c.mean_hour as u32;
            let m = ((c.mean_hour - h as f32) * 60.0) as u32;
            format!("{h:02}:{m:02}")
        })
        .collect();

    format!("bimodal — peaks at {}", peaks.join(", "))
}

/// Render an ASCII 24×7 presence heatmap.
pub fn ascii_presence_map(
    map: &[[f32; PRESENCE_MAP_DAYS]; PRESENCE_MAP_HOURS],
) -> String {
    let days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    let mut out = String::new();
    out.push_str(&format!(
        "       {}",
        days.iter()
            .map(|d| format!("{d:>4}"))
            .collect::<Vec<_>>()
            .join(" ")
    ));
    out.push('\n');

    for h in 0..24 {
        out.push_str(&format!("{h:02}:00  "));
        for d in 0..7 {
            let val = map[h][d];
            let cell = if val > 0.5 {
                "███"
            } else if val > 0.2 {
                " ░░"
            } else {
                "  ."
            };
            out.push_str(&format!(" {cell}"));
        }
        out.push('\n');
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_sessions_commuter() -> Vec<Session> {
        // Simulate a daily commuter: 22 sessions over 11 days
        // Morning pass ~10:15, afternoon pass ~17:35
        let mut sessions = Vec::new();
        for day in 0..11 {
            let date = 7 + day; // April 7-17
            let dow = (date - 7) % 7; // 0=Mon
            if dow >= 5 {
                continue; // skip weekends
            }
            // Morning pass
            let morning = Utc
                .with_ymd_and_hms(2026, 4, date, 10, 15, 0)
                .unwrap();
            sessions.push(Session {
                fingerprint_id: "fp-test".to_string(),
                start: morning,
                end: morning + chrono::Duration::seconds(8),
                sighting_count: 3,
            });
            // Afternoon pass
            let afternoon = Utc
                .with_ymd_and_hms(2026, 4, date, 17, 35, 0)
                .unwrap();
            sessions.push(Session {
                fingerprint_id: "fp-test".to_string(),
                start: afternoon,
                end: afternoon + chrono::Duration::seconds(8),
                sighting_count: 3,
            });
        }
        sessions
    }

    #[test]
    fn session_extraction_single_long_session() {
        // 221 sightings spaced 30s apart → 1 session.
        let fp = "fp-7102d681";
        let t0 = Utc.with_ymd_and_hms(2026, 4, 20, 8, 0, 0).unwrap();
        let timestamps: Vec<DateTime<Utc>> = (0..221)
            .map(|i| t0 + chrono::Duration::seconds(i * 30))
            .collect();

        let sessions = extract_sessions(fp, &timestamps, SESSION_GAP_THRESHOLD_SECS);
        assert_eq!(sessions.len(), 1, "221 sightings 30s apart → 1 session");
        assert_eq!(sessions[0].sighting_count, 221);
    }

    #[test]
    fn session_extraction_multiple_sessions() {
        // Two groups with a 2-hour gap.
        let fp = "fp-8edc0e7a";
        let t0 = Utc.with_ymd_and_hms(2026, 4, 20, 10, 0, 0).unwrap();
        let mut timestamps = Vec::new();
        // Group 1: 5 sightings, 10s apart.
        for i in 0..5 {
            timestamps.push(t0 + chrono::Duration::seconds(i * 10));
        }
        // Group 2: 3 sightings starting 2 hours later.
        let t1 = t0 + chrono::Duration::hours(2);
        for i in 0..3 {
            timestamps.push(t1 + chrono::Duration::seconds(i * 10));
        }

        let sessions = extract_sessions(fp, &timestamps, SESSION_GAP_THRESHOLD_SECS);
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].sighting_count, 5);
        assert_eq!(sessions[1].sighting_count, 3);
    }

    #[test]
    fn bimodal_gmm_from_commuter_sessions() {
        let sessions = make_sessions_commuter();
        assert!(sessions.len() >= 14, "need enough sessions");

        let gmm = fit_arrival_gmm(&sessions, MAX_GMM_COMPONENTS);
        assert!(gmm.len() >= 2, "should detect at least 2 peaks");

        // Check that peaks are near 10 and 17.
        let mut peak_hours: Vec<f32> = gmm.iter().map(|c| c.mean_hour).collect();
        peak_hours.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!(
            peak_hours[0] >= 9.0 && peak_hours[0] <= 11.0,
            "morning peak {:.1} should be in [9,11]",
            peak_hours[0]
        );
        let last = *peak_hours.last().unwrap();
        assert!(
            last >= 16.0 && last <= 18.0,
            "afternoon peak {:.1} should be in [16,18]",
            last
        );

        // Sigma should be < 1.0 for each component.
        for c in &gmm {
            assert!(
                c.sigma_hours <= 1.0,
                "sigma {:.2} should be ≤ 1.0",
                c.sigma_hours
            );
        }
    }

    #[test]
    fn presence_map_weekday_commuter() {
        // Vehicle seen Mon–Fri 08:00–09:00 for 2 weeks.
        let mut sessions = Vec::new();
        for week in 0..2 {
            for dow in 0..5u32 {
                // Mon-Fri
                let day = 6 + week * 7 + dow; // April 6 is Monday
                let start = Utc.with_ymd_and_hms(2026, 4, day, 8, 0, 0).unwrap();
                sessions.push(Session {
                    fingerprint_id: "fp-test".to_string(),
                    start,
                    end: start + chrono::Duration::minutes(50),
                    sighting_count: 10,
                });
            }
        }

        let map = compute_presence_map(&sessions, 14);

        // Hour 8, Mon–Fri should have high values.
        for d in 0..5 {
            assert!(
                map[8][d] > 0.5,
                "map[8][{d}] = {:.2} should be > 0.5",
                map[8][d]
            );
        }
        // Weekends should be near zero.
        assert!(
            map[8][5] < 0.01,
            "map[8][5] (Sat) = {:.2} should be near 0",
            map[8][5]
        );
        assert!(
            map[8][6] < 0.01,
            "map[8][6] (Sun) = {:.2} should be near 0",
            map[8][6]
        );
        // Other hours should be near zero.
        for h in [0, 1, 2, 3, 4, 5, 12, 18, 23] {
            for d in 0..7 {
                assert!(
                    map[h][d] < 0.01,
                    "map[{h}][{d}] = {:.2} should be near 0",
                    map[h][d]
                );
            }
        }
    }

    #[test]
    fn autocorrelation_daily_commuter() {
        // Create sessions with daily periodicity over 14 days.
        let mut sessions = Vec::new();
        for day in 0..14u32 {
            let date = 6 + day;
            let start = Utc.with_ymd_and_hms(2026, 4, date, 10, 0, 0).unwrap();
            sessions.push(Session {
                fingerprint_id: "fp-test".to_string(),
                start,
                end: start + chrono::Duration::seconds(30),
                sighting_count: 5,
            });
        }

        let result = compute_periodicity(&sessions, 14);
        assert!(result.is_some(), "should find periodicity");
        let period = result.unwrap();
        // Dominant period should be approximately 24 hours.
        assert!(
            period.dominant_period_hrs >= 22.0 && period.dominant_period_hrs <= 26.0,
            "dominant period {:.1}h should be ~24h",
            period.dominant_period_hrs
        );
        assert!(
            period.strength > 0.5,
            "strength {:.2} should be > 0.5",
            period.strength
        );
        assert_eq!(period.class, PeriodClass::Daily);
    }

    #[test]
    fn dwell_classification_drive_by() {
        let sessions: Vec<Session> = (0..10)
            .map(|i| {
                let start = Utc.with_ymd_and_hms(2026, 4, 10 + i, 10, 0, 0).unwrap();
                Session {
                    fingerprint_id: "fp-test".to_string(),
                    start,
                    end: start + chrono::Duration::seconds(8),
                    sighting_count: 3,
                }
            })
            .collect();

        let (_, _, median, class) = compute_dwell(&sessions);
        assert!(median < 30, "median {median} should be < 30s for drive-by");
        assert_eq!(class, DwellClass::DriveBy);
    }

    #[test]
    fn dwell_classification_long_term_parked() {
        let sessions: Vec<Session> = (0..10)
            .map(|i| {
                let start = Utc.with_ymd_and_hms(2026, 4, 10 + i, 8, 0, 0).unwrap();
                Session {
                    fingerprint_id: "fp-test".to_string(),
                    start,
                    end: start + chrono::Duration::hours(5),
                    sighting_count: 100,
                }
            })
            .collect();

        let (_, _, median, class) = compute_dwell(&sessions);
        assert!(
            median > 3600,
            "median {median} should be > 3600s for long-term"
        );
        assert_eq!(class, DwellClass::LongTermParked);
    }

    #[test]
    fn full_tbf_commuter() {
        let sessions = make_sessions_commuter();
        let tbf = compute_tbf("fp-test", &sessions);
        assert!(tbf.is_some(), "should compute TBF with enough sessions");
        let tbf = tbf.unwrap();
        assert_eq!(tbf.dwell_class, DwellClass::DriveBy);
        assert!(tbf.arrival_gmm.len() >= 2);
        assert!(
            tbf.classification == VehicleClassification::DailyCommuter
                || tbf.classification == VehicleClassification::TwiceDailyCommuter,
            "expected commuter classification, got {:?}",
            tbf.classification
        );
    }

    #[test]
    fn full_tbf_stationary() {
        // Long-duration single session (always present).
        let start = Utc.with_ymd_and_hms(2026, 4, 10, 0, 0, 0).unwrap();
        let sessions: Vec<Session> = (0..10)
            .map(|i| Session {
                fingerprint_id: "fp-test".to_string(),
                start: start + chrono::Duration::hours(i * 5),
                end: start + chrono::Duration::hours(i * 5 + 4),
                sighting_count: 50,
            })
            .collect();

        let tbf = compute_tbf("fp-test", &sessions);
        assert!(tbf.is_some());
        let tbf = tbf.unwrap();
        assert_eq!(tbf.dwell_class, DwellClass::LongTermParked);
    }

    #[test]
    fn tbf_similarity_identical() {
        let sessions = make_sessions_commuter();
        let tbf = compute_tbf("fp-a", &sessions).unwrap();
        let sim = tbf_similarity(&tbf, &tbf);
        assert!(
            sim > 0.99,
            "identical TBFs should have similarity > 0.99, got {sim:.3}"
        );
    }

    #[test]
    fn tbf_similarity_different() {
        let commuter = make_sessions_commuter();
        let tbf_commuter = compute_tbf("fp-a", &commuter).unwrap();

        // Stationary vehicle.
        let start = Utc.with_ymd_and_hms(2026, 4, 10, 0, 0, 0).unwrap();
        let stationary: Vec<Session> = (0..10)
            .map(|i| Session {
                fingerprint_id: "fp-b".to_string(),
                start: start + chrono::Duration::hours(i * 5),
                end: start + chrono::Duration::hours(i * 5 + 4),
                sighting_count: 50,
            })
            .collect();
        let tbf_stationary = compute_tbf("fp-b", &stationary).unwrap();

        let sim = tbf_similarity(&tbf_commuter, &tbf_stationary);
        assert!(
            sim < 0.5,
            "different TBFs should have similarity < 0.5, got {sim:.3}"
        );
    }

    #[test]
    fn min_sessions_not_met() {
        let sessions: Vec<Session> = (0..5)
            .map(|i| {
                let start = Utc.with_ymd_and_hms(2026, 4, 10 + i, 10, 0, 0).unwrap();
                Session {
                    fingerprint_id: "fp-test".to_string(),
                    start,
                    end: start + chrono::Duration::seconds(8),
                    sighting_count: 3,
                }
            })
            .collect();

        let tbf = compute_tbf("fp-test", &sessions);
        assert!(
            tbf.is_none(),
            "should not compute TBF with fewer than MIN_TBF_SESSIONS"
        );
    }
}
