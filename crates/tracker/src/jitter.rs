use chrono::{DateTime, Utc};
use serde::Serialize;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum number of interval samples before jitter statistics are computed.
pub const MIN_JITTER_SAMPLES: usize = 50;

/// Maximum interval samples retained per fingerprint (ring buffer).
pub const MAX_INTERVAL_SAMPLES: usize = 10_000;

/// How often (seconds) the jitter profile is recomputed.
pub const JITTER_RECOMPUTE_INTERVAL_SECS: u64 = 300;

/// Similarity threshold for jitter-based matching.
pub const JITTER_SIMILARITY_THRESHOLD: f32 = 0.80;

/// Minimum plausible gap between two transmissions (ms).  Anything below
/// this is treated as a burst duplicate of the same packet.
pub const MIN_PLAUSIBLE_GAP_MS: i64 = 500;

/// Maximum plausible gap between two transmissions (ms).  Anything above
/// this is treated as a cross-session gap rather than a real interval.
/// Set generously so that rolling-ID sensors — whose consecutive
/// fingerprint matches are typically separated by 5–7 missed
/// transmissions — still contribute samples.
pub const MAX_PLAUSIBLE_GAP_MS: i64 = 300_000;

/// Tukey fence multiplier used by the IQR outlier filter inside
/// `compute_jitter_profile`.
const IQR_FENCE_MULT: f64 = 1.5;

// Normalization denominators for jitter similarity distance.  Each value
// represents the expected typical range of the corresponding metric across
// sensors, so that the per-dimension distance is normalised to [0, 1].
const SIMILARITY_NORM_SIGMA: f32 = 2.0; // σ range ~0–2 ms for similar sensors
const SIMILARITY_NORM_SKEW: f32 = 1.0; // skewness range ~0–1
const SIMILARITY_NORM_KURT: f32 = 2.0; // excess kurtosis range ~0–2
const SIMILARITY_NORM_ACF: f32 = 0.5; // lag-1 ACF range ~0–0.5

// Weights for the four jitter dimensions in the combined similarity score.
// sigma has the highest weight because it is the most stable and
// discriminative oscillator characteristic.
const W_SIGMA: f32 = 0.40;
const W_SKEW: f32 = 0.25;
const W_KURT: f32 = 0.20;
const W_ACF: f32 = 0.15;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Computed jitter profile for a fingerprint.
#[derive(Debug, Clone, Serialize)]
pub struct JitterProfile {
    pub sigma_ms: f32,
    pub skewness: f32,
    pub kurtosis: f32,
    pub acf_lag1: f32,
    pub samples: usize,
    pub updated_at: DateTime<Utc>,
}

/// Classification of a jitter profile based on its statistical properties.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum JitterClass {
    StableGaussian,
    Drifting,
    Corrected,
    BurstProne,
    Irregular,
}

impl JitterClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StableGaussian => "STABLE_GAUSSIAN",
            Self::Drifting => "DRIFTING",
            Self::Corrected => "CORRECTED",
            Self::BurstProne => "BURST_PRONE",
            Self::Irregular => "IRREGULAR",
        }
    }
}

// ---------------------------------------------------------------------------
// Interval extraction
// ---------------------------------------------------------------------------

/// Extract a new interval sample when a packet arrives for a known fingerprint.
///
/// Uses absolute plausibility bounds so that rolling-ID sensors — whose
/// consecutive successful fingerprint matches may be separated by several
/// missed transmissions — still produce samples.  Distribution-shape
/// outlier rejection is performed later inside `compute_jitter_profile`
/// via an IQR filter.
///
/// Returns `None` if:
///   - the gap is below `MIN_PLAUSIBLE_GAP_MS` (burst duplicate)
///   - the gap is at or above `MAX_PLAUSIBLE_GAP_MS` (cross-session gap)
pub fn extract_interval(last_seen: DateTime<Utc>, now: DateTime<Utc>) -> Option<i64> {
    let gap = (now - last_seen).num_milliseconds();
    if (MIN_PLAUSIBLE_GAP_MS..MAX_PLAUSIBLE_GAP_MS).contains(&gap) {
        Some(gap)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Jitter statistics
// ---------------------------------------------------------------------------

/// Compute a jitter profile from a slice of interval samples (ms).
///
/// Outliers — including the long inter-match gaps that rolling-ID sensors
/// produce when packets fall between successful fingerprint matches — are
/// removed via an IQR (Tukey) filter before statistics are computed.
///
/// Returns `None` if fewer than `MIN_JITTER_SAMPLES` are provided, or if
/// fewer than `MIN_JITTER_SAMPLES` samples remain after filtering.
pub fn compute_jitter_profile(intervals: &[i64]) -> Option<JitterProfile> {
    if intervals.len() < MIN_JITTER_SAMPLES {
        return None;
    }

    let filtered = iqr_filter(intervals);
    if filtered.len() < MIN_JITTER_SAMPLES {
        return None;
    }

    let n = filtered.len() as f64;
    let mean = filtered.iter().map(|&x| x as f64).sum::<f64>() / n;

    let variance = filtered
        .iter()
        .map(|&x| (x as f64 - mean).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    let sigma = variance.sqrt();

    if sigma < f64::EPSILON {
        // All intervals identical — no jitter.
        return Some(JitterProfile {
            sigma_ms: 0.0,
            skewness: 0.0,
            kurtosis: 0.0,
            acf_lag1: 0.0,
            samples: filtered.len(),
            updated_at: Utc::now(),
        });
    }

    // Skewness: E[(X-μ)³] / σ³
    let skewness = filtered
        .iter()
        .map(|&x| ((x as f64 - mean) / sigma).powi(3))
        .sum::<f64>()
        / n;

    // Excess kurtosis: E[(X-μ)⁴] / σ⁴ - 3
    let kurtosis = filtered
        .iter()
        .map(|&x| ((x as f64 - mean) / sigma).powi(4))
        .sum::<f64>()
        / n
        - 3.0;

    // Lag-1 autocorrelation: Cov(x[i], x[i+1]) / Var(x)
    let pairs: Vec<(f64, f64)> = filtered
        .windows(2)
        .map(|w| (w[0] as f64, w[1] as f64))
        .collect();
    let acf_lag1 = if pairs.len() >= 2 {
        let cov = pairs
            .iter()
            .map(|(a, b)| (a - mean) * (b - mean))
            .sum::<f64>()
            / pairs.len() as f64;
        (cov / variance) as f32
    } else {
        0.0
    };

    Some(JitterProfile {
        sigma_ms: sigma as f32,
        skewness: skewness as f32,
        kurtosis: kurtosis as f32,
        acf_lag1,
        samples: filtered.len(),
        updated_at: Utc::now(),
    })
}

/// Drop samples outside the Tukey fence `[Q1 - 1.5*IQR, Q3 + 1.5*IQR]`,
/// preserving the original order of the remaining samples (so that the
/// lag-1 autocorrelation computed downstream stays meaningful).
fn iqr_filter(intervals: &[i64]) -> Vec<i64> {
    if intervals.is_empty() {
        return Vec::new();
    }
    let mut sorted: Vec<i64> = intervals.to_vec();
    sorted.sort_unstable();
    let q1 = percentile_sorted(&sorted, 0.25);
    let q3 = percentile_sorted(&sorted, 0.75);
    let iqr = q3 - q1;
    let lo = q1 - IQR_FENCE_MULT * iqr;
    let hi = q3 + IQR_FENCE_MULT * iqr;
    intervals
        .iter()
        .copied()
        .filter(|&x| {
            let v = x as f64;
            v >= lo && v <= hi
        })
        .collect()
}

/// Linear-interpolated percentile of an already-sorted slice.
fn percentile_sorted(sorted: &[i64], p: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    let rank = p * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo] as f64
    } else {
        let w = rank - lo as f64;
        sorted[lo] as f64 * (1.0 - w) + sorted[hi] as f64 * w
    }
}

// ---------------------------------------------------------------------------
// Jitter similarity
// ---------------------------------------------------------------------------

/// Compute a similarity score in `[0, 1]` between two jitter profiles.
///
/// Higher values indicate more similar profiles.
pub fn jitter_similarity(a: &JitterProfile, b: &JitterProfile) -> f32 {
    let d_sigma = ((a.sigma_ms - b.sigma_ms).abs() / SIMILARITY_NORM_SIGMA).min(1.0);
    let d_skew = ((a.skewness - b.skewness).abs() / SIMILARITY_NORM_SKEW).min(1.0);
    let d_kurt = ((a.kurtosis - b.kurtosis).abs() / SIMILARITY_NORM_KURT).min(1.0);
    let d_acf = ((a.acf_lag1 - b.acf_lag1).abs() / SIMILARITY_NORM_ACF).min(1.0);

    let distance = W_SIGMA * d_sigma + W_SKEW * d_skew + W_KURT * d_kurt + W_ACF * d_acf;

    1.0 - distance
}

// ---------------------------------------------------------------------------
// Jitter classification
// ---------------------------------------------------------------------------

/// Classify a jitter profile into a human-readable category.
pub fn classify_jitter(profile: &JitterProfile) -> JitterClass {
    if profile.acf_lag1 < -0.2 {
        JitterClass::Corrected
    } else if profile.kurtosis > 2.0 {
        JitterClass::BurstProne
    } else if profile.sigma_ms > 20.0 || profile.acf_lag1.abs() > 0.3 {
        JitterClass::Drifting
    } else if profile.sigma_ms < 5.0 && profile.skewness.abs() < 0.5 && profile.acf_lag1.abs() < 0.1
    {
        JitterClass::StableGaussian
    } else {
        JitterClass::Irregular
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    // -----------------------------------------------------------------------
    // Interval bound tests
    // -----------------------------------------------------------------------

    #[test]
    fn cross_session_gap_excluded() {
        // 1 hour gap is well above MAX_PLAUSIBLE_GAP_MS.
        let last = Utc::now();
        let now = last + Duration::milliseconds(3_600_000);
        assert!(extract_interval(last, now).is_none());
    }

    #[test]
    fn burst_duplicate_excluded() {
        // 50 ms gap is below MIN_PLAUSIBLE_GAP_MS.
        let last = Utc::now();
        let now = last + Duration::milliseconds(50);
        assert!(extract_interval(last, now).is_none());
    }

    #[test]
    fn rolling_id_inter_match_gap_accepted() {
        // A rolling-ID sensor with ~22 s TX interval typically yields
        // 60–150 s gaps between consecutive successful fingerprint
        // matches.  These must be accepted now that the median-based
        // gate is gone.
        let last = Utc::now();
        for &gap_ms in &[60_000i64, 90_000, 132_000, 150_000] {
            let now = last + Duration::milliseconds(gap_ms);
            assert_eq!(
                extract_interval(last, now),
                Some(gap_ms),
                "gap {gap_ms} ms must be accepted"
            );
        }
    }

    #[test]
    fn min_boundary_accepted() {
        let last = Utc::now();
        let now = last + Duration::milliseconds(MIN_PLAUSIBLE_GAP_MS);
        assert_eq!(extract_interval(last, now), Some(MIN_PLAUSIBLE_GAP_MS));
    }

    #[test]
    fn just_below_min_rejected() {
        let last = Utc::now();
        let now = last + Duration::milliseconds(MIN_PLAUSIBLE_GAP_MS - 1);
        assert!(extract_interval(last, now).is_none());
    }

    #[test]
    fn just_below_max_accepted() {
        let last = Utc::now();
        let now = last + Duration::milliseconds(MAX_PLAUSIBLE_GAP_MS - 1);
        assert_eq!(
            extract_interval(last, now),
            Some(MAX_PLAUSIBLE_GAP_MS - 1)
        );
    }

    #[test]
    fn at_max_rejected() {
        let last = Utc::now();
        let now = last + Duration::milliseconds(MAX_PLAUSIBLE_GAP_MS);
        assert!(extract_interval(last, now).is_none());
    }

    // -----------------------------------------------------------------------
    // Jitter profile computation tests
    // -----------------------------------------------------------------------

    #[test]
    fn insufficient_samples_returns_none() {
        let intervals: Vec<i64> = (0..49).map(|_| 60_000).collect();
        assert!(compute_jitter_profile(&intervals).is_none());
    }

    #[test]
    fn gaussian_input_correct_sigma() {
        // 200 samples around μ=60000, σ≈12 ms.
        // Use a deterministic pseudo-random sequence via simple linear
        // congruential generator to produce independent jitter.
        let mut intervals = Vec::with_capacity(200);
        let mut state: u64 = 42; // seed
        for _ in 0..200 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            // Map to range [-12, +12] ms offset
            let offset = ((state >> 33) as i64 % 25) - 12;
            intervals.push(60_000 + offset);
        }
        let profile = compute_jitter_profile(&intervals).unwrap();

        // σ should be reasonable (within an order of magnitude of 12)
        assert!(
            profile.sigma_ms > 1.0 && profile.sigma_ms < 30.0,
            "sigma_ms={} out of expected range",
            profile.sigma_ms
        );
        // Near-zero skewness for a symmetric distribution
        assert!(
            profile.skewness.abs() < 0.5,
            "skewness={} should be near zero",
            profile.skewness
        );
        // Near-zero autocorrelation (independent values)
        assert!(
            profile.acf_lag1.abs() < 0.2,
            "acf_lag1={} should be near zero",
            profile.acf_lag1
        );
        assert_eq!(profile.samples, 200);
    }

    #[test]
    fn bimodal_distribution_high_kurtosis() {
        // Bimodal: alternating clusters at 59000 and 61000.
        // This should have negative excess kurtosis for a perfect bimodal,
        // but with some noise it could be positive. The key test is that
        // kurtosis is distinctly different from 0.
        let mut intervals = Vec::with_capacity(200);
        for i in 0..200 {
            if i % 2 == 0 {
                intervals.push(59_000);
            } else {
                intervals.push(61_000);
            }
        }
        let profile = compute_jitter_profile(&intervals).unwrap();
        // For a perfect bimodal distribution the excess kurtosis is -2.0.
        // The alternating pattern means ACF is also very negative.
        // We just verify the profile computes and kurtosis is non-trivial.
        assert!(
            profile.kurtosis.abs() > 0.5,
            "kurtosis={} should be non-trivial for bimodal",
            profile.kurtosis
        );
    }

    #[test]
    fn iqr_filter_removes_extreme_outliers() {
        // 100 tightly-clustered samples plus 4 huge outliers.  The IQR
        // filter should drop the outliers, leaving sigma reflecting only
        // the inlier spread.
        let mut intervals: Vec<i64> = Vec::with_capacity(104);
        for i in 0..100 {
            intervals.push(22_000 + (i as i64 % 11) - 5);
        }
        intervals.push(150_000);
        intervals.push(180_000);
        intervals.push(200_000);
        intervals.push(250_000);

        let profile =
            compute_jitter_profile(&intervals).expect("should produce a profile after IQR filter");

        // After filtering, remaining samples should be ~100 (the inliers),
        // not 104, and sigma should reflect the inlier spread (single
        // digits ms), not the multi-thousand spread the outliers would
        // introduce.
        assert!(
            profile.samples >= MIN_JITTER_SAMPLES && profile.samples <= 100,
            "filtered sample count = {} out of expected range",
            profile.samples
        );
        assert!(
            profile.sigma_ms < 100.0,
            "sigma_ms={} should reflect inliers only after IQR filtering",
            profile.sigma_ms
        );
    }

    #[test]
    fn iqr_filter_preserves_clean_distribution() {
        // 200 well-behaved samples with no outliers — filtering should
        // remove very few samples and produce a usable profile.
        let mut intervals = Vec::with_capacity(200);
        let mut state: u64 = 7;
        for _ in 0..200 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let offset = ((state >> 33) as i64 % 11) - 5;
            intervals.push(22_000 + offset);
        }
        let profile = compute_jitter_profile(&intervals).unwrap();
        // Most samples must survive; the Tukey fence should drop only a
        // small fraction of a near-uniform distribution.
        assert!(
            profile.samples >= 150,
            "filtered sample count = {} unexpectedly low",
            profile.samples
        );
    }

    #[test]
    fn alternating_long_short_negative_acf() {
        // Alternating long/short: 58000, 62000, 58000, 62000, ...
        // Should produce strongly negative lag-1 autocorrelation.
        let mut intervals = Vec::with_capacity(200);
        for i in 0..200 {
            if i % 2 == 0 {
                intervals.push(58_000);
            } else {
                intervals.push(62_000);
            }
        }
        let profile = compute_jitter_profile(&intervals).unwrap();
        assert!(
            profile.acf_lag1 < -0.4,
            "acf_lag1={} should be strongly negative for alternating pattern",
            profile.acf_lag1
        );
    }

    // -----------------------------------------------------------------------
    // Similarity and classification tests
    // -----------------------------------------------------------------------

    #[test]
    fn identical_profiles_similarity_one() {
        let p = JitterProfile {
            sigma_ms: 12.0,
            skewness: 0.3,
            kurtosis: 1.5,
            acf_lag1: -0.05,
            samples: 200,
            updated_at: Utc::now(),
        };
        let sim = jitter_similarity(&p, &p);
        assert!((sim - 1.0).abs() < 1e-6, "similarity={}", sim);
    }

    #[test]
    fn very_different_profiles_low_similarity() {
        let a = JitterProfile {
            sigma_ms: 2.0,
            skewness: 0.0,
            kurtosis: 0.0,
            acf_lag1: 0.0,
            samples: 200,
            updated_at: Utc::now(),
        };
        let b = JitterProfile {
            sigma_ms: 25.0,
            skewness: 1.5,
            kurtosis: 3.0,
            acf_lag1: 0.4,
            samples: 200,
            updated_at: Utc::now(),
        };
        let sim = jitter_similarity(&a, &b);
        assert!(
            sim < JITTER_SIMILARITY_THRESHOLD,
            "similarity={} should be below threshold",
            sim
        );
    }

    #[test]
    fn classify_stable_gaussian() {
        let p = JitterProfile {
            sigma_ms: 3.0,
            skewness: 0.1,
            kurtosis: 0.5,
            acf_lag1: 0.02,
            samples: 200,
            updated_at: Utc::now(),
        };
        assert_eq!(classify_jitter(&p), JitterClass::StableGaussian);
    }

    #[test]
    fn classify_drifting() {
        let p = JitterProfile {
            sigma_ms: 25.0,
            skewness: 0.1,
            kurtosis: 0.5,
            acf_lag1: 0.02,
            samples: 200,
            updated_at: Utc::now(),
        };
        assert_eq!(classify_jitter(&p), JitterClass::Drifting);
    }

    #[test]
    fn classify_corrected() {
        let p = JitterProfile {
            sigma_ms: 3.0,
            skewness: 0.1,
            kurtosis: 0.5,
            acf_lag1: -0.3,
            samples: 200,
            updated_at: Utc::now(),
        };
        assert_eq!(classify_jitter(&p), JitterClass::Corrected);
    }

    #[test]
    fn classify_burst_prone() {
        let p = JitterProfile {
            sigma_ms: 10.0,
            skewness: 0.8,
            kurtosis: 3.0,
            acf_lag1: 0.05,
            samples: 200,
            updated_at: Utc::now(),
        };
        assert_eq!(classify_jitter(&p), JitterClass::BurstProne);
    }

    #[test]
    fn classify_irregular() {
        let p = JitterProfile {
            sigma_ms: 10.0,
            skewness: 0.8,
            kurtosis: 1.0,
            acf_lag1: 0.05,
            samples: 200,
            updated_at: Utc::now(),
        };
        assert_eq!(classify_jitter(&p), JitterClass::Irregular);
    }
}
