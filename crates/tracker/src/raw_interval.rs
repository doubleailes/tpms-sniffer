//! Raw inter-packet interval tracking, upstream of the fingerprint correlator.
//!
//! The fingerprint correlator only matches a fraction of received packets
//! (rolling/bit-flipping IDs miss most matches), so the gap between two
//! consecutive correlator matches is much larger than the sensor's true TX
//! cadence.  Recording jitter from inter-match gaps measures correlator
//! behaviour, not the crystal oscillator.
//!
//! [`RawIntervalTracker`] records the gap between consecutive packets keyed
//! by `(sensor_id, rtl433_id)` *before* fingerprint resolution.  The interval
//! corresponds to the sensor's TX cadence (typically 15–60 s) and reflects
//! the true oscillator jitter.
//!
//! Because the fingerprint that owns these intervals is unknown until the
//! correlator runs, [`RawIntervalBuffer`] queues them per
//! `(sensor_id, rtl433_id)` until the resolver flushes them after
//! fingerprint assignment.

use chrono::{DateTime, Utc};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Minimum plausible inter-packet gap (ms).  Anything below this is treated
/// as a burst duplicate of the same packet (e.g. multipath echo).
pub const RAW_INTERVAL_MIN_MS: i64 = 5_000;

/// Maximum plausible inter-packet gap (ms).  Anything above this is treated
/// as a cross-session gap (vehicle absent, then returns).  Set to ~3× the
/// longest known TPMS TX interval (EezTire ~90s when parked).
pub const RAW_INTERVAL_MAX_MS: i64 = 300_000;

/// TTL (seconds) for buffered intervals awaiting fingerprint association.
/// Buffered entries older than this are dropped — they belong to transient
/// drive-bys that never resolved to a fingerprint.
pub const RAW_INTERVAL_BUFFER_TTL_SECS: i64 = 600;

/// Maximum Hamming distance between two consecutive sensor IDs to be
/// attributed to the same physical sensor by [`RollingIntervalTracker`].
///
/// EezTire / TRW-OOK / TRW-FSK flip 1–3 bits of the sensor ID per
/// transmission.  Two random 32-bit IDs differ in ~16 bits on average, so a
/// threshold of 3 cleanly separates same-sensor consecutive packets from
/// different-sensor packets.
pub const MAX_HAMMING_DISTANCE: u32 = 3;

/// Maximum pressure delta (kPa) between two consecutive packets attributed
/// to the same physical sensor in [`RollingIntervalTracker::observe_with_pressure`].
///
/// A real tyre's pressure changes by less than 0.1 kPa over a 22 s interval,
/// so any delta above this threshold indicates two distinct sensors that
/// happen to be within `MAX_HAMMING_DISTANCE` of each other.
pub const MAX_PRESSURE_DELTA_KPA: f32 = 5.0;

/// `rtl433_id`s of protocols whose sensor ID rotates on every transmission
/// via a bit-flip rolling scheme.  Jitter measurement for these protocols
/// requires Hamming-distance grouping rather than exact sensor-ID equality.
pub const ROLLING_ID_PROTOCOLS: &[u16] = &[
    241, // EezTire / Carchet / TST-507
    298, // TRW-OOK
    299, // TRW-FSK
];

/// Whether the given protocol uses a bit-flip rolling sensor ID and so must
/// be tracked by [`RollingIntervalTracker`] rather than [`RawIntervalTracker`].
pub fn is_rolling_id_protocol(rtl433_id: u16) -> bool {
    ROLLING_ID_PROTOCOLS.contains(&rtl433_id)
}

// ---------------------------------------------------------------------------
// RawIntervalTracker
// ---------------------------------------------------------------------------

/// Tracks the last-seen timestamp per `(sensor_id, rtl433_id)` pair so that
/// inter-packet intervals can be computed before fingerprint resolution.
///
/// This is the input side of the deferred-association pipeline.  Successful
/// observations (within the plausibility gate) are pushed into a
/// [`RawIntervalBuffer`] that is later flushed to `interval_samples` once
/// the correlator has resolved a fingerprint for the sensor.
#[derive(Debug, Default)]
pub struct RawIntervalTracker {
    last_seen: HashMap<(u32, u16), DateTime<Utc>>,
}

impl RawIntervalTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a packet observation.  Returns the interval (ms) since the
    /// previous packet from the same `(sensor_id, rtl433_id)` pair, if it
    /// falls inside `[RAW_INTERVAL_MIN_MS, RAW_INTERVAL_MAX_MS]`.
    ///
    /// Returns `None` for the first observation of a sensor, for burst
    /// duplicates, and for cross-session gaps.
    pub fn observe(&mut self, sensor_id: u32, rtl433_id: u16, now: DateTime<Utc>) -> Option<i64> {
        let key = (sensor_id, rtl433_id);
        let prev = self.last_seen.insert(key, now);
        prev.and_then(|t| {
            let gap = (now - t).num_milliseconds();
            if (RAW_INTERVAL_MIN_MS..=RAW_INTERVAL_MAX_MS).contains(&gap) {
                Some(gap)
            } else {
                None
            }
        })
    }

    /// Drop entries whose last-seen timestamp is older than `max_age_secs`.
    /// Call periodically to release memory for sensors that have departed.
    pub fn evict_stale(&mut self, now: DateTime<Utc>, max_age_secs: i64) {
        self.last_seen
            .retain(|_, t| (now - *t).num_seconds() < max_age_secs);
    }

    /// Number of tracked sensor keys.  Exposed for tests and diagnostics.
    pub fn len(&self) -> usize {
        self.last_seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.last_seen.is_empty()
    }
}

// ---------------------------------------------------------------------------
// RollingIntervalTracker
// ---------------------------------------------------------------------------

/// Tracks raw TX intervals for protocols whose sensor ID rotates on every
/// transmission (EezTire, TRW-OOK, TRW-FSK).
///
/// Unlike [`RawIntervalTracker`], which keys on `(sensor_id, rtl433_id)`,
/// this tracker keys on `rtl433_id` only and uses Hamming distance between
/// consecutive sensor IDs to decide whether two packets came from the same
/// physical sensor.  Two consecutive packets are attributed to the same
/// sensor when `hamming_distance(prev_id, new_id) <= MAX_HAMMING_DISTANCE`.
///
/// In dense environments where two sensors of the same protocol may be in
/// range simultaneously, [`Self::observe_with_pressure`] adds pressure
/// continuity as a second matching criterion.
#[derive(Debug, Default)]
pub struct RollingIntervalTracker {
    /// `rtl433_id → (last_sensor_id, last_pressure_kpa, last_timestamp)`.
    /// `last_pressure_kpa` is `None` when the previous observation was
    /// recorded via [`Self::observe`] without a pressure value.
    last_seen: HashMap<u16, (u32, Option<f32>, DateTime<Utc>)>,
}

impl RollingIntervalTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a packet observation and return the interval (ms) since the
    /// previous packet of the same protocol if:
    ///   * Hamming distance between the previous and new sensor IDs is
    ///     `<= MAX_HAMMING_DISTANCE`, **and**
    ///   * the gap is inside `[RAW_INTERVAL_MIN_MS, RAW_INTERVAL_MAX_MS]`.
    ///
    /// The stored `(sensor_id, timestamp)` for this protocol is always
    /// updated, regardless of whether an interval is returned.
    pub fn observe(&mut self, sensor_id: u32, rtl433_id: u16, now: DateTime<Utc>) -> Option<i64> {
        let prev = self.last_seen.insert(rtl433_id, (sensor_id, None, now));
        prev.and_then(|(last_id, _, last_ts)| {
            Self::interval_if_same_sensor(sensor_id, last_id, now, last_ts)
        })
    }

    /// Like [`Self::observe`] but additionally requires the pressure delta
    /// between the two packets to stay within `MAX_PRESSURE_DELTA_KPA`.
    /// This disambiguates two sensors of the same protocol that happen to
    /// transmit IDs within Hamming distance 3 of each other.
    ///
    /// If the previous observation was recorded via [`Self::observe`]
    /// (no pressure available), pressure is not used to gate the match —
    /// the current call falls back to the pure Hamming-distance check.
    pub fn observe_with_pressure(
        &mut self,
        sensor_id: u32,
        rtl433_id: u16,
        pressure_kpa: f32,
        now: DateTime<Utc>,
    ) -> Option<i64> {
        let prev = self
            .last_seen
            .insert(rtl433_id, (sensor_id, Some(pressure_kpa), now));
        prev.and_then(|(last_id, last_pressure, last_ts)| {
            let interval = Self::interval_if_same_sensor(sensor_id, last_id, now, last_ts)?;
            // If we have a previous pressure to compare against, gate on it.
            // Otherwise accept the Hamming-only match.
            if let Some(prev_p) = last_pressure
                && (pressure_kpa - prev_p).abs() > MAX_PRESSURE_DELTA_KPA
            {
                return None;
            }
            Some(interval)
        })
    }

    /// Combined Hamming + interval gate.  Pulled out so [`Self::observe`]
    /// and [`Self::observe_with_pressure`] share the same logic.
    fn interval_if_same_sensor(
        new_id: u32,
        last_id: u32,
        now: DateTime<Utc>,
        last_ts: DateTime<Utc>,
    ) -> Option<i64> {
        let hamming = (new_id ^ last_id).count_ones();
        if hamming > MAX_HAMMING_DISTANCE {
            return None;
        }
        let gap = (now - last_ts).num_milliseconds();
        if (RAW_INTERVAL_MIN_MS..=RAW_INTERVAL_MAX_MS).contains(&gap) {
            Some(gap)
        } else {
            None
        }
    }

    /// Drop entries whose last-seen timestamp is older than `max_age_secs`.
    /// Call periodically to release memory for protocols whose sensors have
    /// departed.
    pub fn evict_stale(&mut self, now: DateTime<Utc>, max_age_secs: i64) {
        self.last_seen
            .retain(|_, (_, _, ts)| (now - *ts).num_seconds() < max_age_secs);
    }

    /// Number of tracked protocol keys.  Exposed for tests and diagnostics.
    pub fn len(&self) -> usize {
        self.last_seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.last_seen.is_empty()
    }
}

// ---------------------------------------------------------------------------
// RawIntervalBuffer
// ---------------------------------------------------------------------------

/// One buffered observation: `(interval_ms, observed_at)`.
type BufferedInterval = (i64, DateTime<Utc>);

/// Short-lived buffer of raw intervals awaiting fingerprint association.
///
/// Entries are keyed on `(sensor_id, rtl433_id)`.  The resolver pushes
/// intervals here as packets arrive and drains them with [`Self::drain`]
/// once the correlator has resolved a fingerprint for that sensor.  Stale
/// entries (those whose owning sensor never resolves) are dropped by
/// [`Self::evict_stale`].
#[derive(Debug, Default)]
pub struct RawIntervalBuffer {
    inner: HashMap<(u32, u16), Vec<BufferedInterval>>,
}

impl RawIntervalBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an interval observation for the given sensor.
    pub fn push(&mut self, sensor_id: u32, rtl433_id: u16, interval_ms: i64, ts: DateTime<Utc>) {
        self.inner
            .entry((sensor_id, rtl433_id))
            .or_default()
            .push((interval_ms, ts));
    }

    /// Remove and return all buffered intervals for the given sensor key.
    pub fn drain(&mut self, sensor_id: u32, rtl433_id: u16) -> Vec<BufferedInterval> {
        self.inner
            .remove(&(sensor_id, rtl433_id))
            .unwrap_or_default()
    }

    /// Drop intervals older than `ttl_secs` and any sensor key that has no
    /// remaining intervals afterwards.
    pub fn evict_stale(&mut self, now: DateTime<Utc>, ttl_secs: i64) {
        self.inner.retain(|_, intervals| {
            intervals.retain(|(_, ts)| (now - *ts).num_seconds() < ttl_secs);
            !intervals.is_empty()
        });
    }

    /// Total number of buffered interval observations across all sensor keys.
    pub fn len(&self) -> usize {
        self.inner.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.values().all(|v| v.is_empty())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    // -----------------------------------------------------------------------
    // RawIntervalTracker::observe
    // -----------------------------------------------------------------------

    #[test]
    fn first_observation_returns_none() {
        let mut tracker = RawIntervalTracker::new();
        assert_eq!(tracker.observe(0x1234, 89, t0()), None);
    }

    #[test]
    fn normal_gap_returns_interval() {
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1234, 89, t0());
        let gap = tracker.observe(0x1234, 89, t0() + Duration::milliseconds(22_000));
        assert_eq!(gap, Some(22_000));
    }

    #[test]
    fn burst_duplicate_below_min_rejected() {
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1234, 89, t0());
        // 200 ms gap is well below RAW_INTERVAL_MIN_MS (5_000).
        let gap = tracker.observe(0x1234, 89, t0() + Duration::milliseconds(200));
        assert_eq!(
            gap, None,
            "burst duplicate gaps below RAW_INTERVAL_MIN_MS must be rejected"
        );
    }

    #[test]
    fn cross_session_gap_above_max_rejected() {
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1234, 89, t0());
        // 10 minutes is well above RAW_INTERVAL_MAX_MS (300_000 ms).
        let gap = tracker.observe(0x1234, 89, t0() + Duration::minutes(10));
        assert_eq!(
            gap, None,
            "cross-session gaps above RAW_INTERVAL_MAX_MS must be rejected"
        );
    }

    #[test]
    fn min_boundary_accepted() {
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1234, 89, t0());
        let gap = tracker.observe(
            0x1234,
            89,
            t0() + Duration::milliseconds(RAW_INTERVAL_MIN_MS),
        );
        assert_eq!(gap, Some(RAW_INTERVAL_MIN_MS));
    }

    #[test]
    fn max_boundary_accepted() {
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1234, 89, t0());
        let gap = tracker.observe(
            0x1234,
            89,
            t0() + Duration::milliseconds(RAW_INTERVAL_MAX_MS),
        );
        assert_eq!(gap, Some(RAW_INTERVAL_MAX_MS));
    }

    #[test]
    fn different_protocols_kept_separate() {
        // Same sensor_id under two different rtl433 IDs must not produce an
        // interval — they are independent sensors as far as the tracker is
        // concerned.
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1234, 89, t0());
        let gap = tracker.observe(0x1234, 95, t0() + Duration::milliseconds(22_000));
        assert_eq!(gap, None);
    }

    #[test]
    fn last_seen_advances_each_observation() {
        // Three packets at 22 s spacing produce two intervals, both 22 s.
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1234, 89, t0());
        let g1 = tracker.observe(0x1234, 89, t0() + Duration::milliseconds(22_000));
        let g2 = tracker.observe(0x1234, 89, t0() + Duration::milliseconds(44_000));
        assert_eq!(g1, Some(22_000));
        assert_eq!(g2, Some(22_000));
    }

    // -----------------------------------------------------------------------
    // RawIntervalTracker::evict_stale
    // -----------------------------------------------------------------------

    #[test]
    fn evict_stale_removes_old_entries() {
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1111, 89, t0());
        tracker.observe(0x2222, 89, t0() + Duration::seconds(150));
        // Now is t0 + 200 s.  0x1111 is 200 s old (stale at 60 s max age);
        // 0x2222 is 50 s old (still fresh).
        tracker.evict_stale(t0() + Duration::seconds(200), 60);
        assert_eq!(
            tracker.len(),
            1,
            "only the recent entry should survive eviction"
        );
    }

    #[test]
    fn evict_stale_keeps_recent_entries() {
        let mut tracker = RawIntervalTracker::new();
        tracker.observe(0x1111, 89, t0());
        tracker.evict_stale(t0() + Duration::seconds(30), 60);
        assert_eq!(tracker.len(), 1);
    }

    // -----------------------------------------------------------------------
    // RawIntervalBuffer
    // -----------------------------------------------------------------------

    #[test]
    fn buffer_drain_returns_buffered_intervals() {
        let mut buf = RawIntervalBuffer::new();
        buf.push(0x1234, 89, 22_000, t0());
        buf.push(0x1234, 89, 22_050, t0() + Duration::milliseconds(22_050));

        let drained = buf.drain(0x1234, 89);
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].0, 22_000);
        assert_eq!(drained[1].0, 22_050);

        // A second drain returns nothing — the buffer was emptied.
        assert!(buf.drain(0x1234, 89).is_empty());
    }

    #[test]
    fn buffer_drain_isolates_keys() {
        let mut buf = RawIntervalBuffer::new();
        buf.push(0x1111, 89, 22_000, t0());
        buf.push(0x2222, 89, 30_000, t0());

        let drained = buf.drain(0x1111, 89);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, 22_000);

        // The other key is untouched.
        let other = buf.drain(0x2222, 89);
        assert_eq!(other.len(), 1);
        assert_eq!(other[0].0, 30_000);
    }

    #[test]
    fn buffer_evict_stale_removes_old_intervals() {
        let mut buf = RawIntervalBuffer::new();
        buf.push(0x1234, 89, 22_000, t0());
        buf.push(0x1234, 89, 22_000, t0() + Duration::seconds(900));

        // Evict anything older than 600 s relative to t0 + 900 s.
        buf.evict_stale(t0() + Duration::seconds(900), 600);

        let drained = buf.drain(0x1234, 89);
        assert_eq!(
            drained.len(),
            1,
            "only the recent interval should survive eviction"
        );
    }

    #[test]
    fn buffer_evict_stale_drops_empty_keys() {
        let mut buf = RawIntervalBuffer::new();
        buf.push(0x1234, 89, 22_000, t0());
        // Force every interval out by evicting with a very large now.
        buf.evict_stale(t0() + Duration::seconds(10_000), 600);
        assert!(buf.is_empty());
    }

    // -----------------------------------------------------------------------
    // RollingIntervalTracker
    // -----------------------------------------------------------------------

    #[test]
    fn rolling_first_observation_returns_none() {
        let mut tracker = RollingIntervalTracker::new();
        assert_eq!(tracker.observe(0xFFFFFFFF, 241, t0()), None);
    }

    #[test]
    fn rolling_same_sensor_within_hamming_threshold_returns_interval() {
        // 0xFFFFFFFF → 0xFBFFFFDF: 2 bits flipped (within threshold).
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        let gap = tracker.observe(0xFBFFFFDF, 241, t0() + Duration::milliseconds(22_000));
        assert_eq!(gap, Some(22_000));
    }

    #[test]
    fn rolling_three_bit_flip_accepted() {
        // 0xFFFFFFFF → 0xFDFFFDFB: 3 bits flipped — at the threshold.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        let gap = tracker.observe(0xFDFFFDFB, 241, t0() + Duration::milliseconds(22_000));
        assert_eq!(
            gap,
            Some(22_000),
            "Hamming distance == threshold must match"
        );
    }

    #[test]
    fn rolling_above_hamming_threshold_returns_none() {
        // 0xFFFFFFFF → 0xFFFF0FFF: 4 bits differ — different sensor.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        let gap = tracker.observe(0xFFFF0FFF, 241, t0() + Duration::milliseconds(22_000));
        assert_eq!(
            gap, None,
            "consecutive packets > MAX_HAMMING_DISTANCE bits apart must not produce an interval"
        );
    }

    #[test]
    fn rolling_burst_duplicate_below_min_rejected() {
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        // 200 ms gap, 1 bit flipped — gap below RAW_INTERVAL_MIN_MS.
        let gap = tracker.observe(0xFBFFFFFF, 241, t0() + Duration::milliseconds(200));
        assert_eq!(gap, None);
    }

    #[test]
    fn rolling_cross_session_gap_above_max_rejected() {
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        // 10 minutes — well above RAW_INTERVAL_MAX_MS, even though the IDs
        // are within Hamming threshold.
        let gap = tracker.observe(0xFBFFFFFF, 241, t0() + Duration::minutes(10));
        assert_eq!(gap, None);
    }

    #[test]
    fn rolling_state_advances_even_on_rejected_match() {
        // After a rejected match (different sensor), the stored state must
        // still advance to the rejecting packet so the *next* packet is
        // measured against the most recent observation.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        // 0xFFFF0FFF: 4 bits flipped from 0xFFFFFFFF → rejected.
        let _ = tracker.observe(0xFFFF0FFF, 241, t0() + Duration::milliseconds(22_000));
        // 0xFFFF0FFF → 0xFFFE0FFF: 1 bit flipped from the rejected packet → match.
        let gap = tracker.observe(0xFFFE0FFF, 241, t0() + Duration::milliseconds(44_000));
        assert_eq!(gap, Some(22_000));
    }

    #[test]
    fn rolling_protocols_kept_separate() {
        // Two protocols sharing a sensor_id must not produce an interval —
        // the tracker is keyed per-protocol.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        let gap = tracker.observe(0xFFFFFFFF, 298, t0() + Duration::milliseconds(22_000));
        assert_eq!(gap, None);
    }

    #[test]
    fn rolling_observe_with_pressure_accepts_close_pressure() {
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe_with_pressure(0xFFFFFFFF, 241, 250.0, t0());
        let gap = tracker.observe_with_pressure(
            0xFBFFFFDF,
            241,
            250.5,
            t0() + Duration::milliseconds(22_000),
        );
        assert_eq!(gap, Some(22_000));
    }

    #[test]
    fn rolling_observe_with_pressure_rejects_large_pressure_delta() {
        // Two sensors at very different pressures whose IDs happen to be
        // within Hamming distance 3 — the pressure delta must reject the
        // match.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe_with_pressure(0xFFFFFFFF, 241, 250.0, t0());
        let gap = tracker.observe_with_pressure(
            0xFBFFFFDF,
            241,
            300.0, // Δ = 50 kPa, well above MAX_PRESSURE_DELTA_KPA
            t0() + Duration::milliseconds(22_000),
        );
        assert_eq!(
            gap, None,
            "pressure delta > MAX_PRESSURE_DELTA_KPA must reject the match"
        );
    }

    #[test]
    fn rolling_observe_with_pressure_pressure_does_not_rescue_far_hamming() {
        // Even if the pressures match exactly, a Hamming distance > threshold
        // still rejects the interval.  0xFFFF0FFF differs from 0xFFFFFFFF
        // by 4 bits — above MAX_HAMMING_DISTANCE.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe_with_pressure(0xFFFFFFFF, 241, 250.0, t0());
        let gap = tracker.observe_with_pressure(
            0xFFFF0FFF,
            241,
            250.0,
            t0() + Duration::milliseconds(22_000),
        );
        assert_eq!(gap, None);
    }

    #[test]
    fn rolling_evict_stale_removes_old_protocol_entries() {
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0xFFFFFFFF, 241, t0());
        tracker.observe(0xFFFFFFFF, 298, t0() + Duration::seconds(150));
        tracker.evict_stale(t0() + Duration::seconds(200), 60);
        assert_eq!(
            tracker.len(),
            1,
            "only the recent protocol entry should survive eviction"
        );
    }

    #[test]
    fn is_rolling_id_protocol_recognises_known_protocols() {
        assert!(is_rolling_id_protocol(241));
        assert!(is_rolling_id_protocol(298));
        assert!(is_rolling_id_protocol(299));
        assert!(!is_rolling_id_protocol(89));
        assert!(!is_rolling_id_protocol(208));
    }

    #[test]
    fn rolling_synthetic_eeztire_stream_yields_per_transmission_intervals() {
        // Issue #43 acceptance: 100 synthetic EezTire packets at 22 s
        // intervals, each with 1–2 random bit flips relative to the previous,
        // must produce 99 interval samples whose mean is ~22_000 ms.
        let mut tracker = RollingIntervalTracker::new();
        let mut id: u32 = 0xFFFFFFFF;
        let mut intervals = Vec::new();
        // Pseudo-random bit-flip pattern (deterministic for reproducibility).
        let bit_seq = [3u32, 17, 28, 9, 14, 22, 5, 30, 1, 11, 26, 19];
        for i in 0..100 {
            let ts = t0() + Duration::milliseconds((i as i64) * 22_000);
            if let Some(g) = tracker.observe(id, 241, ts) {
                intervals.push(g);
            }
            // Flip 1–2 bits for the next packet.
            let bit = bit_seq[i % bit_seq.len()];
            id ^= 1 << bit;
            if i % 3 == 0 {
                id ^= 1 << bit_seq[(i + 1) % bit_seq.len()];
            }
        }
        assert_eq!(intervals.len(), 99);
        // All gaps were exactly 22_000 ms by construction.
        for g in &intervals {
            assert_eq!(*g, 22_000);
        }
    }
}
