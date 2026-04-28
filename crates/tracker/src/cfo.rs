// ============================================================
//  cfo.rs — Carrier Frequency Offset (CFO) fingerprinting for
//  the tracker side.
//
//  Issue #45: CFO measured from raw I/Q preambles is an
//  ID-independent oscillator fingerprint.  This module owns:
//
//    • the CFO statistics aggregation (running mean / σ over
//      multiple per-packet estimates retained in `cfo_samples`),
//    • the `cfo_similarity` function used by the resolver,
//    • the bucketing helper used to key rolling-ID interval
//      tracking by CFO cluster instead of Hamming distance.
//
//  The CFO estimator itself (`estimate_cfo`) lives in
//  `crates/sniffer/src/cfo.rs` so it can run on raw IQ samples
//  before demodulation.
// ============================================================

use chrono::{DateTime, Utc};
use serde::Serialize;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum number of CFO samples retained per fingerprint before
/// the running mean is considered stable.
pub const MIN_CFO_SAMPLES: usize = 20;

/// Maximum CFO samples retained per fingerprint (ring buffer).
pub const MAX_CFO_SAMPLES: usize = 10_000;

/// How often (seconds) the CFO running statistics are recomputed.
pub const CFO_RECOMPUTE_INTERVAL_SECS: u64 = 300;

/// Expected inter-sensor CFO spread (Hz).  Crystals on TPMS sensors
/// have ±20 ppm tolerance; at 433.920 MHz this is 8.7 kHz.
pub const CFO_DISCRIMINATION_RANGE_HZ: f32 = 8_700.0;

/// Minimum CFO difference (Hz) below which two estimates cannot be
/// reliably distinguished from RTL-SDR V3 measurement noise.
pub const CFO_MIN_DISCRIMINABLE_HZ: f32 = 1_000.0;

/// Quantisation step (Hz) for CFO-based clustering.  Sensors within
/// ±BUCKET/2 share a CFO bucket key.
pub const CFO_BUCKET_SIZE_HZ: f32 = 500.0;

/// Weight assigned to the CFO score when blending pressure / interval /
/// CFO into a combined fingerprint match score.  Pressure dominates
/// (0.45) because it is most reliably measured; CFO is the next most
/// discriminative dimension because it is ID-independent.
pub const CFO_MATCH_WEIGHT: f32 = 0.30;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Aggregated CFO statistics for a single fingerprint.
#[derive(Debug, Clone, Serialize)]
pub struct CfoProfile {
    /// Mean CFO across retained samples (Hz).
    pub mean_hz: f32,
    /// Standard deviation of CFO samples (Hz).  Small values
    /// (< ~500 Hz) indicate a single, stable physical oscillator.
    pub sigma_hz: f32,
    /// Number of CFO samples used to compute the statistics.
    pub samples: usize,
    /// Timestamp of the most recent recomputation.
    pub updated_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

/// Compute aggregate CFO statistics from a slice of per-packet
/// estimates (Hz).  Returns `None` if fewer than `MIN_CFO_SAMPLES`
/// estimates are provided.
///
/// Performs simple z-score outlier rejection (drop |z| > 3) so that
/// a single garbled measurement cannot poison the running mean.
pub fn compute_cfo_profile(estimates: &[f32]) -> Option<CfoProfile> {
    if estimates.len() < MIN_CFO_SAMPLES {
        return None;
    }

    let n = estimates.len() as f32;
    let mean = estimates.iter().sum::<f32>() / n;
    let variance = estimates.iter().map(|&x| (x - mean).powi(2)).sum::<f32>() / n;
    let sigma = variance.sqrt();

    // z-score filter (3σ).  If sigma is degenerate, skip filtering.
    let filtered: Vec<f32> = if sigma > f32::EPSILON {
        estimates
            .iter()
            .copied()
            .filter(|x| (x - mean).abs() <= 3.0 * sigma)
            .collect()
    } else {
        estimates.to_vec()
    };

    if filtered.len() < MIN_CFO_SAMPLES {
        // Too many estimates rejected — surface a profile from the
        // unfiltered set rather than refusing to summarise at all.
        return Some(CfoProfile {
            mean_hz: mean,
            sigma_hz: sigma,
            samples: estimates.len(),
            updated_at: Utc::now(),
        });
    }

    let nf = filtered.len() as f32;
    let mean_f = filtered.iter().sum::<f32>() / nf;
    let var_f = filtered
        .iter()
        .map(|&x| (x - mean_f).powi(2))
        .sum::<f32>()
        / nf;
    let sigma_f = var_f.sqrt();

    Some(CfoProfile {
        mean_hz: mean_f,
        sigma_hz: sigma_f,
        samples: filtered.len(),
        updated_at: Utc::now(),
    })
}

// ---------------------------------------------------------------------------
// Similarity & bucketing
// ---------------------------------------------------------------------------

/// CFO-based fingerprint similarity score in `[0, 1]`.
///
/// Two CFO estimates whose absolute difference is much smaller than
/// `CFO_DISCRIMINATION_RANGE_HZ` come back near 1.0 (likely the same
/// physical oscillator); estimates differing by the full range come
/// back at 0.0 (definitely different sensors).
pub fn cfo_similarity(cfo_a: f32, cfo_b: f32) -> f32 {
    let diff = (cfo_a - cfo_b).abs();
    1.0 - (diff / CFO_DISCRIMINATION_RANGE_HZ).min(1.0)
}

/// Round a CFO estimate to its bucket key.  Two estimates that
/// quantise to the same bucket are within ±`CFO_BUCKET_SIZE_HZ / 2`.
///
/// Used to key rolling-ID interval tracking by CFO cluster instead
/// of Hamming distance, which fails in dense urban environments.
pub fn cfo_bucket(cfo_hz: f32) -> i32 {
    (cfo_hz / CFO_BUCKET_SIZE_HZ).round() as i32
}

/// Decide whether a CFO comparison is meaningful.  When either
/// fingerprint has fewer than `MIN_CFO_SAMPLES` retained estimates,
/// or when the two means differ by less than
/// `CFO_MIN_DISCRIMINABLE_HZ` (RTL-SDR noise floor), the resolver
/// should fall back to a neutral score rather than treating CFO as
/// authoritative.
pub fn cfo_comparison_is_reliable(
    a_samples: usize,
    b_samples: usize,
    a_mean_hz: f32,
    b_mean_hz: f32,
) -> bool {
    a_samples >= MIN_CFO_SAMPLES
        && b_samples >= MIN_CFO_SAMPLES
        && (a_mean_hz - b_mean_hz).abs() > CFO_MIN_DISCRIMINABLE_HZ
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn similarity_identical_is_one() {
        assert!((cfo_similarity(3_000.0, 3_000.0) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn similarity_close_estimates_above_0_9() {
        // 500 Hz / 8700 Hz ≈ 0.057 distance → similarity ≈ 0.94
        let s = cfo_similarity(3_000.0, 3_500.0);
        assert!(s > 0.9, "expected > 0.9, got {s}");
    }

    #[test]
    fn similarity_far_estimates_below_0_1() {
        // 8000 Hz delta → very near full discrimination range → similarity ≈ 0.08
        let s = cfo_similarity(-4_000.0, 4_000.0);
        assert!(s < 0.1, "expected < 0.1, got {s}");
    }

    #[test]
    fn similarity_clamps_at_zero() {
        let s = cfo_similarity(-50_000.0, 50_000.0);
        assert!(s >= 0.0, "must clamp to [0,1], got {s}");
    }

    #[test]
    fn bucket_quantises_to_500_hz() {
        assert_eq!(cfo_bucket(0.0), 0);
        assert_eq!(cfo_bucket(249.0), 0);
        assert_eq!(cfo_bucket(251.0), 1);
        assert_eq!(cfo_bucket(-251.0), -1);
        assert_eq!(cfo_bucket(1_000.0), 2);
    }

    #[test]
    fn comparison_unreliable_when_too_few_samples() {
        assert!(!cfo_comparison_is_reliable(5, 30, 3_000.0, 6_000.0));
        assert!(!cfo_comparison_is_reliable(30, 5, 3_000.0, 6_000.0));
    }

    #[test]
    fn comparison_unreliable_when_below_noise_floor() {
        // 200 Hz delta — below the 1 kHz RTL-SDR floor.
        assert!(!cfo_comparison_is_reliable(50, 50, 3_000.0, 3_200.0));
    }

    #[test]
    fn comparison_reliable_above_noise_floor() {
        assert!(cfo_comparison_is_reliable(50, 50, 3_000.0, 6_000.0));
    }

    #[test]
    fn profile_mean_and_sigma() {
        // 30 samples around 3000 Hz, σ ≈ 50 Hz.
        let mut estimates = Vec::with_capacity(30);
        let mut state: u64 = 17;
        for _ in 0..30 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let off = ((state >> 33) as i64 % 100) - 50;
            estimates.push(3_000.0 + off as f32);
        }
        let p = compute_cfo_profile(&estimates).unwrap();
        assert!((p.mean_hz - 3_000.0).abs() < 50.0, "mean={}", p.mean_hz);
        assert!(p.sigma_hz < 200.0, "sigma={}", p.sigma_hz);
        assert!(p.samples >= MIN_CFO_SAMPLES);
    }

    #[test]
    fn profile_rejects_outliers() {
        let mut estimates: Vec<f32> = (0..MIN_CFO_SAMPLES).map(|_| 3_000.0).collect();
        // Add a single 50 kHz outlier; it must be filtered out so the
        // reported mean stays at 3 kHz.
        estimates.push(50_000.0);
        let p = compute_cfo_profile(&estimates).unwrap();
        assert!(
            (p.mean_hz - 3_000.0).abs() < 100.0,
            "outlier not rejected: mean={}",
            p.mean_hz
        );
    }

    #[test]
    fn profile_insufficient_samples_returns_none() {
        let estimates = vec![3_000.0; MIN_CFO_SAMPLES - 1];
        assert!(compute_cfo_profile(&estimates).is_none());
    }
}
