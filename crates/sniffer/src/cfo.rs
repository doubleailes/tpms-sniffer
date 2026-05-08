// ============================================================
//  cfo.rs — Carrier Frequency Offset (CFO) estimation
//
//  Issue #45 — physical-layer oscillator fingerprinting via CFO
//  measurement of the packet preamble.  The estimator is the
//  Moose autocorrelation method, which is robust to AWGN and
//  well-suited to short preamble windows.
//
//  CFO is extracted from raw interleaved I/Q samples (f32) before
//  any demodulation, so it is independent of sensor ID rotation —
//  a critical property for rolling-ID protocols whose ID continuity
//  cannot be assumed across packets.
//
//  At 433.920 MHz the inter-sensor CFO spread for typical TPMS
//  crystals (±20 ppm) is ≈ 8.7 kHz, while an RTL-SDR V3 with TCXO
//  reference has a measurement noise floor of ≈ 0.4 kHz.  CFO
//  differences of ≥ 1 kHz are reliably discriminated.
// ============================================================

use chrono::{DateTime, Utc};

// ─── Constants ──────────────────────────────────────────────

/// IQ samples per CFO estimate.  At 250 kHz sample rate this is
/// 256 µs — enough to cover the alternating-bit preamble used by
/// the Schrader / TRW / Continental TPMS family.
pub const PREAMBLE_SAMPLES: usize = 64;

/// Minimum number of CFO estimates retained per fingerprint before
/// the running mean is considered stable.
pub const MIN_CFO_SAMPLES: usize = 20;

/// Expected inter-sensor CFO spread (Hz).  Based on a ±20 ppm
/// crystal tolerance at 433.920 MHz: 20 × 433.92 ≈ 8.7 kHz.
pub const CFO_DISCRIMINATION_RANGE_HZ: f32 = 8_700.0;

/// Minimum CFO difference (Hz) below which two estimates are
/// indistinguishable from RTL-SDR V3 measurement noise (~1 ppm
/// TCXO at 433 MHz ≈ 434 Hz; we round up to give margin).
pub const CFO_MIN_DISCRIMINABLE_HZ: f32 = 1_000.0;

/// Quantisation step (Hz) for CFO-based clustering / keying of
/// rolling-ID packets.  Sensors within ±BUCKET/2 share a key.
pub const CFO_BUCKET_SIZE_HZ: f32 = 500.0;

/// Outlier rejection threshold for `refine_cfo`: estimates more
/// than this many standard deviations from the running mean are
/// dropped before averaging.
pub const CFO_OUTLIER_SIGMA: f32 = 2.0;

// ─── IQ window record ───────────────────────────────────────

/// A short window of raw interleaved I/Q samples captured around
/// a detected packet preamble, plus the metadata needed to extract
/// a CFO estimate downstream.
#[derive(Debug, Clone)]
pub struct IqWindow {
    /// Raw IQ samples covering the packet preamble.
    /// Length: `PREAMBLE_SAMPLES × 2` (interleaved I, Q as f32).
    pub samples: Vec<f32>,
    /// Centre frequency at time of capture (Hz).
    pub centre_freq_hz: u64,
    /// Sample rate (samples/sec).
    pub sample_rate_hz: u32,
    /// Timestamp of first sample.
    pub ts: DateTime<Utc>,
    /// rtl_433 protocol id of the detected packet.
    pub rtl433_id: u16,
    /// Raw sensor_id from the decoded packet.  For rolling-ID
    /// protocols this is the (rotating) ID and should not be used
    /// to key the CFO sample on its own.
    pub sensor_id: u32,
}

impl IqWindow {
    /// Build an IqWindow from a u8 slice of offset-binary IQ samples
    /// (the format produced by the RTL-SDR driver, interleaved
    /// I0,Q0,I1,Q1,…, DC = 127.5).  Truncates to `PREAMBLE_SAMPLES`.
    pub fn from_u8(
        bytes: &[u8],
        centre_freq_hz: u64,
        sample_rate_hz: u32,
        ts: DateTime<Utc>,
        rtl433_id: u16,
        sensor_id: u32,
    ) -> Self {
        let max_pairs = PREAMBLE_SAMPLES;
        let mut samples = Vec::with_capacity(max_pairs * 2);
        for pair in bytes.chunks_exact(2).take(max_pairs) {
            samples.push(pair[0] as f32 - 127.5);
            samples.push(pair[1] as f32 - 127.5);
        }
        Self {
            samples,
            centre_freq_hz,
            sample_rate_hz,
            ts,
            rtl433_id,
            sensor_id,
        }
    }
}

// ─── CFO estimation (autocorrelation / Moose) ───────────────

/// Estimate the carrier frequency offset (Hz) from a window of
/// interleaved I/Q samples.
///
/// Uses the Moose autocorrelation estimator at lag 1:
///
/// ```text
///     r1   = Σ z[i]* · z[i+1]
///     CFO  = arg(r1) · Fs / (2π)
/// ```
///
/// Returns `None` when fewer than two complex samples are provided.
pub fn estimate_cfo(samples: &[f32], sample_rate_hz: u32) -> Option<f32> {
    if samples.len() < 4 {
        return None;
    }

    // Autocorrelation at lag 1: Σ conj(z[i]) · z[i+1]
    //   conj(a + jb) · (c + jd) = (ac + bd) + j(ad - bc)
    let mut acc_re = 0.0f32;
    let mut acc_im = 0.0f32;

    let pairs = samples.chunks_exact(2);
    let n = pairs.len();
    if n < 2 {
        return None;
    }

    let mut prev_i = 0.0f32;
    let mut prev_q = 0.0f32;
    let mut have_prev = false;

    for pair in pairs {
        let i = pair[0];
        let q = pair[1];
        if have_prev {
            // conj(prev) · current
            acc_re += prev_i * i + prev_q * q;
            acc_im += prev_i * q - prev_q * i;
        }
        prev_i = i;
        prev_q = q;
        have_prev = true;
    }

    let arg = acc_im.atan2(acc_re);
    let cfo = arg * sample_rate_hz as f32 / (2.0 * std::f32::consts::PI);
    Some(cfo)
}

/// Refine a CFO estimate by averaging over multiple per-packet
/// measurements.  Estimates more than `CFO_OUTLIER_SIGMA` standard
/// deviations from the unfiltered mean are dropped before averaging
/// is repeated on the inliers.
///
/// Returns `None` if fewer than three samples are supplied or if the
/// outlier filter removes every sample.
pub fn refine_cfo(estimates: &[f32]) -> Option<f32> {
    if estimates.len() < 3 {
        return None;
    }

    let n = estimates.len() as f32;
    let mean = estimates.iter().sum::<f32>() / n;
    let variance = estimates.iter().map(|&x| (x - mean).powi(2)).sum::<f32>() / n;
    let sigma = variance.sqrt();

    let cutoff = CFO_OUTLIER_SIGMA * sigma;
    let filtered: Vec<f32> = estimates
        .iter()
        .copied()
        .filter(|x| (x - mean).abs() <= cutoff)
        .collect();

    if filtered.is_empty() {
        return None;
    }

    Some(filtered.iter().sum::<f32>() / filtered.len() as f32)
}

// ─── Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::f32::consts::PI;

    /// Synthesise a complex tone of the given offset frequency at
    /// the given sample rate and return interleaved I,Q samples.
    fn synth_tone(offset_hz: f32, fs: u32, n: usize) -> Vec<f32> {
        let mut out = Vec::with_capacity(n * 2);
        let dphi = 2.0 * PI * offset_hz / fs as f32;
        for k in 0..n {
            let phi = dphi * k as f32;
            out.push(phi.cos());
            out.push(phi.sin());
        }
        out
    }

    #[test]
    fn estimate_cfo_recovers_known_offset() {
        // Pure tone at +3,000 Hz, sampled at 250 kHz.  The autocorrelation
        // estimator should recover the offset to within ~100 Hz on a
        // 64-sample window.
        let fs = 250_000u32;
        let samples = synth_tone(3_000.0, fs, PREAMBLE_SAMPLES);
        let cfo = estimate_cfo(&samples, fs).expect("estimate");
        assert!(
            (cfo - 3_000.0).abs() < 100.0,
            "expected ≈3000 Hz, got {cfo}"
        );
    }

    #[test]
    fn estimate_cfo_negative_offset() {
        let fs = 250_000u32;
        let samples = synth_tone(-2_500.0, fs, PREAMBLE_SAMPLES);
        let cfo = estimate_cfo(&samples, fs).expect("estimate");
        assert!(
            (cfo - (-2_500.0)).abs() < 100.0,
            "expected ≈-2500 Hz, got {cfo}"
        );
    }

    #[test]
    fn estimate_cfo_zero_offset() {
        let fs = 250_000u32;
        let samples = synth_tone(0.0, fs, PREAMBLE_SAMPLES);
        let cfo = estimate_cfo(&samples, fs).expect("estimate");
        assert!(cfo.abs() < 50.0, "expected ≈0 Hz, got {cfo}");
    }

    #[test]
    fn estimate_cfo_too_short_returns_none() {
        // Only one complex sample — not enough for a lag-1 product.
        let samples = vec![1.0, 0.0];
        assert!(estimate_cfo(&samples, 250_000).is_none());
    }

    #[test]
    fn refine_cfo_averages_clean_estimates() {
        let estimates = [3_000.0, 3_010.0, 2_990.0, 3_005.0, 2_995.0];
        let mean = refine_cfo(&estimates).expect("refine");
        assert!((mean - 3_000.0).abs() < 10.0);
    }

    #[test]
    fn refine_cfo_rejects_outliers() {
        // Five tight samples around 3000 Hz plus one wild outlier at 50 kHz.
        let estimates = [3_000.0, 3_010.0, 2_990.0, 3_005.0, 2_995.0, 50_000.0];
        let mean = refine_cfo(&estimates).expect("refine");
        // The outlier must be filtered out — refined mean must still
        // be near 3 kHz, not pulled toward the 50 kHz contamination.
        assert!(
            (mean - 3_000.0).abs() < 500.0,
            "outlier not rejected: mean={mean}"
        );
    }

    #[test]
    fn refine_cfo_too_few_samples_returns_none() {
        assert!(refine_cfo(&[1.0, 2.0]).is_none());
    }

    #[test]
    fn iq_window_from_u8_centres_at_127_5() {
        let bytes: Vec<u8> = (0..PREAMBLE_SAMPLES * 2).map(|i| (i % 256) as u8).collect();
        let win = IqWindow::from_u8(&bytes, 433_920_000, 250_000, Utc::now(), 241, 0xDEADBEEF);
        assert_eq!(win.samples.len(), PREAMBLE_SAMPLES * 2);
        assert_eq!(win.centre_freq_hz, 433_920_000);
        assert_eq!(win.sample_rate_hz, 250_000);
        assert_eq!(win.rtl433_id, 241);
        assert_eq!(win.sensor_id, 0xDEADBEEF);
        // First byte (0) should map to -127.5; second byte (1) to -126.5
        assert!((win.samples[0] - (-127.5)).abs() < 1e-3);
        assert!((win.samples[1] - (-126.5)).abs() < 1e-3);
    }
}
