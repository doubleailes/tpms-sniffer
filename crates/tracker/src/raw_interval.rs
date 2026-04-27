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
use std::collections::{HashMap, VecDeque};

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

/// Number of slots in the per-protocol rolling buffer.
///
/// Must exceed the maximum number of simultaneous sensors of the same
/// protocol expected in the capture environment.  In a 10-sensor EezTire
/// environment with a 22 s TX cadence and ~2.2 s mean inter-packet gap,
/// each sensor's previous packet is followed by ~10 competing entries
/// before its own next packet arrives, so a buffer of 20 provides 2×
/// headroom.  Scales linearly with `density × tx_interval / mean_gap`.
pub const ROLLING_BUFFER_SIZE: usize = 20;

/// Suppress repeat matches of the same `(sensor_id_a, sensor_id_b)` pair
/// within this window.  With a multi-slot buffer the same buffered entry
/// can match several incoming packets, which would inject duplicate
/// near-identical intervals.  Set to half of the minimum plausible TX
/// interval so that genuine same-sensor cadence (≥ 5 s) is unaffected.
pub const DEDUP_WINDOW_MS: i64 = 8_000;

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

/// One slot in the rolling buffer of recent packets for a protocol.
#[derive(Debug, Clone)]
struct BufferEntry {
    sensor_id: u32,
    /// `None` when the entry was recorded via [`RollingIntervalTracker::observe`]
    /// without a pressure value.  Pressure gating is skipped in that case.
    pressure_kpa: Option<f32>,
    ts: DateTime<Utc>,
}

/// Tracks raw TX intervals for protocols whose sensor ID rotates on every
/// transmission (EezTire, TRW-OOK, TRW-FSK).
///
/// Unlike [`RawIntervalTracker`], which keys on `(sensor_id, rtl433_id)`,
/// this tracker keys on `rtl433_id` only and uses Hamming distance between
/// consecutive sensor IDs to decide whether two packets came from the same
/// physical sensor.
///
/// Each protocol has a circular buffer of the last `ROLLING_BUFFER_SIZE`
/// observations.  A new packet is matched against **every** entry in the
/// buffer; the match with the lowest Hamming distance (tiebroken by recency)
/// is selected as the previous packet from the same physical sensor.  This
/// is necessary in dense environments where many sensors of the same
/// protocol transmit concurrently — a single-slot tracker is overwritten
/// by other sensors faster than the same sensor's next packet arrives, so
/// every match is a cross-sensor pair and the resulting interval histogram
/// is uniform noise.
///
/// In addition to Hamming distance, [`Self::observe_with_pressure`] gates
/// matches on pressure continuity (`MAX_PRESSURE_DELTA_KPA`) to disambiguate
/// sensors whose IDs happen to be within 3 bits of each other.  A
/// deduplication window (`DEDUP_WINDOW_MS`) prevents the same `(id_a, id_b)`
/// pair from producing duplicate intervals when an entry is matched again
/// before being evicted.
#[derive(Debug)]
pub struct RollingIntervalTracker {
    /// `rtl433_id → circular buffer of recent observations`.
    buffers: HashMap<u16, VecDeque<BufferEntry>>,
    /// Last match time per order-independent sensor-ID pair.  Drives the
    /// dedup gate so that successive packets cannot pull the same anchor
    /// twice within `DEDUP_WINDOW_MS`.
    recent_matches: HashMap<(u32, u32), DateTime<Utc>>,
    /// Maximum slots per protocol buffer.  Defaults to `ROLLING_BUFFER_SIZE`;
    /// callers (tests, density-sweep experiments) can override via
    /// [`Self::with_buffer_size`].
    buffer_size: usize,
}

impl Default for RollingIntervalTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RollingIntervalTracker {
    pub fn new() -> Self {
        Self::with_buffer_size(ROLLING_BUFFER_SIZE)
    }

    /// Construct a tracker with a custom per-protocol buffer size.  Used by
    /// tests and by white-paper buffer-size-vs-SNR sweeps.
    pub fn with_buffer_size(buffer_size: usize) -> Self {
        Self {
            buffers: HashMap::new(),
            recent_matches: HashMap::new(),
            buffer_size: buffer_size.max(1),
        }
    }

    /// Record a packet observation without pressure gating.  Returns the
    /// interval (ms) since the best-matching buffered packet of the same
    /// protocol if all gates pass, otherwise `None`.  The new packet is
    /// always pushed to the buffer regardless of whether a match was found.
    pub fn observe(&mut self, sensor_id: u32, rtl433_id: u16, now: DateTime<Utc>) -> Option<i64> {
        self.observe_inner(sensor_id, rtl433_id, None, now)
    }

    /// Like [`Self::observe`] but additionally gates matches on pressure
    /// continuity: a buffered entry is eligible only if its pressure differs
    /// from the new packet's by at most `MAX_PRESSURE_DELTA_KPA`.  Buffered
    /// entries recorded without pressure (via [`Self::observe`]) are not
    /// pressure-gated.
    pub fn observe_with_pressure(
        &mut self,
        sensor_id: u32,
        rtl433_id: u16,
        pressure_kpa: f32,
        now: DateTime<Utc>,
    ) -> Option<i64> {
        self.observe_inner(sensor_id, rtl433_id, Some(pressure_kpa), now)
    }

    fn observe_inner(
        &mut self,
        sensor_id: u32,
        rtl433_id: u16,
        pressure_kpa: Option<f32>,
        now: DateTime<Utc>,
    ) -> Option<i64> {
        let buffer_size = self.buffer_size;

        // Phase 1: read-only search of the buffer.  Lower Hamming is preferred;
        // ties break toward the most recent entry (largest ts).
        let best = self.buffers.get(&rtl433_id).and_then(|buf| {
            buf.iter()
                .filter_map(|entry| {
                    let hamming = (sensor_id ^ entry.sensor_id).count_ones();
                    if hamming > MAX_HAMMING_DISTANCE {
                        return None;
                    }
                    let gap = (now - entry.ts).num_milliseconds();
                    if !(RAW_INTERVAL_MIN_MS..=RAW_INTERVAL_MAX_MS).contains(&gap) {
                        return None;
                    }
                    if let (Some(p_new), Some(p_prev)) = (pressure_kpa, entry.pressure_kpa)
                        && (p_new - p_prev).abs() > MAX_PRESSURE_DELTA_KPA
                    {
                        return None;
                    }
                    Some((hamming, entry.ts, gap, entry.sensor_id))
                })
                .min_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)))
        });

        // Phase 2: deduplication gate on the (min, max) pair key.  We update
        // `recent_matches` only when an interval is actually emitted, so that
        // a dedup-suppressed call does not extend the suppression window.
        let mut interval = best.map(|(_, _, gap, _)| gap);
        if let Some((_, _, _, prev_id)) = best {
            let pair_key = (sensor_id.min(prev_id), sensor_id.max(prev_id));
            let suppress = self
                .recent_matches
                .get(&pair_key)
                .is_some_and(|last| (now - *last).num_milliseconds() < DEDUP_WINDOW_MS);
            if suppress {
                interval = None;
            } else if interval.is_some() {
                self.recent_matches.insert(pair_key, now);
            }
        }

        // Phase 3: push the new packet, evicting by age first, then by size.
        let buffer = self.buffers.entry(rtl433_id).or_default();
        if buffer.len() >= buffer_size {
            let cutoff = now - chrono::Duration::milliseconds(RAW_INTERVAL_MAX_MS);
            buffer.retain(|e| e.ts > cutoff);
            while buffer.len() >= buffer_size {
                buffer.pop_front();
            }
        }
        buffer.push_back(BufferEntry {
            sensor_id,
            pressure_kpa,
            ts: now,
        });

        interval
    }

    /// Drop buffer entries (and dedup records) older than `max_age_secs`.
    /// Empty protocol buffers are removed entirely.
    pub fn evict_stale(&mut self, now: DateTime<Utc>, max_age_secs: i64) {
        let cutoff = now - chrono::Duration::seconds(max_age_secs);
        for buffer in self.buffers.values_mut() {
            buffer.retain(|e| e.ts > cutoff);
        }
        self.buffers.retain(|_, buf| !buf.is_empty());
        self.recent_matches.retain(|_, ts| *ts > cutoff);
    }

    /// Number of tracked protocol keys with at least one buffered entry.
    /// Exposed for tests and diagnostics.
    pub fn len(&self) -> usize {
        self.buffers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
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

    // -----------------------------------------------------------------------
    // Issue #44 — multi-slot rolling buffer in dense environments
    // -----------------------------------------------------------------------

    /// Generate one packet's `(sensor_id, ts)` for sensor `idx` at transmission
    /// `n` in the dense-environment simulation.  Each sensor uses a "colour":
    /// nibble pattern `i*0x11111111`.  Cross-sensor Hamming distance is at
    /// least 8 bits, while the within-sensor walk flips bits 0..2 only — so
    /// intra-sensor pairs stay within `MAX_HAMMING_DISTANCE`.
    fn dense_packet(idx: usize, n: usize, base: DateTime<Utc>) -> (u32, DateTime<Utc>) {
        let lane: u32 = (idx as u32 & 0xF).wrapping_mul(0x1111_1111);
        let walk: u32 = match n % 4 {
            0 => 0b000,
            1 => 0b001,
            2 => 0b011,
            3 => 0b010,
            _ => unreachable!(),
        };
        let id = lane ^ walk;
        // 22 s cadence per sensor, staggered by 2.2 s between sensors so the
        // interleaved rate is 10 packets / 22 s.
        let ts = base
            + Duration::milliseconds(idx as i64 * 2_200)
            + Duration::milliseconds(n as i64 * 22_000);
        (id, ts)
    }

    #[test]
    fn rolling_multi_slot_buffer_recovers_signal_in_dense_environment() {
        // 10 simultaneous EezTire sensors at 22 s cadence, time-staggered by
        // 2.2 s.  With ROLLING_BUFFER_SIZE = 20 the per-sensor previous
        // packet is always reachable, so each new packet should recover its
        // own ~22 s interval.
        let mut tracker = RollingIntervalTracker::with_buffer_size(20);
        let mut intervals = Vec::new();
        // 10 sensors × 10 transmissions each = 100 packets.  We sort by
        // timestamp so the tracker sees them in real-world arrival order.
        let mut packets = Vec::new();
        for sensor in 0..10 {
            for n in 0..10 {
                let (id, ts) = dense_packet(sensor, n, t0());
                packets.push((ts, id));
            }
        }
        packets.sort_by_key(|p| p.0);
        for (ts, id) in packets {
            if let Some(g) = tracker.observe(id, 241, ts) {
                intervals.push(g);
            }
        }
        // Histogram: peak should be at the 21–23 s bin.
        let in_peak = intervals.iter().filter(|&&g| (21_000..=23_000).contains(&g)).count();
        let off_peak = intervals.len() - in_peak;
        assert!(
            in_peak >= 80,
            "expected ≥ 80 same-sensor intervals near 22 s, got {in_peak} of {} total",
            intervals.len()
        );
        // Signal-to-noise: the tight peak must dominate by > 3×.
        assert!(
            in_peak as f64 / off_peak.max(1) as f64 > 3.0,
            "SNR too low: {in_peak} peak vs {off_peak} background"
        );
    }

    #[test]
    fn rolling_single_slot_regression_yields_no_signal_in_dense_environment() {
        // Same dense scenario as above with `buffer_size = 1` — reproduces
        // the issue #43 single-slot pathology.  The slot is overwritten by
        // a different sensor before the same sensor's next packet arrives,
        // and inter-lane Hamming distance is far above the threshold, so
        // no intervals (or near none) are recorded.
        let mut tracker = RollingIntervalTracker::with_buffer_size(1);
        let mut packets = Vec::new();
        for sensor in 0..10 {
            for n in 0..10 {
                let (id, ts) = dense_packet(sensor, n, t0());
                packets.push((ts, id));
            }
        }
        packets.sort_by_key(|p| p.0);
        let mut intervals = Vec::new();
        for (ts, id) in packets {
            if let Some(g) = tracker.observe(id, 241, ts) {
                intervals.push(g);
            }
        }
        // With distinct nibble lanes, cross-sensor Hamming is always 4+, so
        // a single-slot tracker yields zero same-sensor matches.  This is
        // the regression guard: any future change that quietly reverts to
        // a single-slot design would push this number above zero.
        assert_eq!(
            intervals.len(),
            0,
            "buffer_size=1 must not recover same-sensor pairs in this dense \
             environment (got {} intervals)",
            intervals.len()
        );
    }

    #[test]
    fn rolling_dedup_suppresses_repeat_pair_within_window() {
        // Multipath echo scenario: a sensor transmits id=0x3 once at t=22 s
        // (matching a prior id=0x1 at t=0), and a near-duplicate of the same
        // packet arrives 500 ms later (multipath echo, same sensor_id).  The
        // echo would re-pair (0x1, 0x3) and inject a near-identical interval,
        // contaminating the histogram.  The dedup guard must suppress it.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0x1, 241, t0());
        let g1 = tracker.observe(0x3, 241, t0() + Duration::milliseconds(22_000));
        assert_eq!(g1, Some(22_000), "first match must emit a fresh interval");
        // Echo at t = 22.5 s.  Best buffer match is still 0x1@t=0 (gap to
        // 0x3@22 s is 500 ms, gap-gated below MIN).  Pair (0x1, 0x3) was
        // recorded at t=22 s, only 500 ms ago < DEDUP_WINDOW_MS = 8 s.
        let g2 = tracker.observe(0x3, 241, t0() + Duration::milliseconds(22_500));
        assert_eq!(
            g2, None,
            "repeat (0x1, 0x3) match within DEDUP_WINDOW_MS must be suppressed"
        );
    }

    #[test]
    fn rolling_dedup_releases_pair_after_window() {
        // Same multipath scenario, but the echo arrives long after
        // DEDUP_WINDOW_MS — the pair must be matchable again.  Use 31 s as
        // a "second TX" of the rolling-ID echo so the gap is well above
        // the dedup window and the gap gate.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0x1, 241, t0());
        let g1 = tracker.observe(0x3, 241, t0() + Duration::milliseconds(22_000));
        assert_eq!(g1, Some(22_000));
        // 31 s later: pair recorded 9 s ago > DEDUP_WINDOW_MS = 8 s, allowed.
        let g2 = tracker.observe(0x3, 241, t0() + Duration::milliseconds(31_000));
        assert!(
            g2.is_some(),
            "pair must be re-matchable after DEDUP_WINDOW_MS elapses"
        );
    }

    #[test]
    fn rolling_buffer_skips_stale_entries_and_matches_recent() {
        // Mixed buffer: a stale entry (> RAW_INTERVAL_MAX_MS old) and a
        // recent one.  The new packet must skip the stale entry by gap-gate
        // and match the recent one.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0x1, 241, t0());
        let t_recent = t0() + Duration::milliseconds(RAW_INTERVAL_MAX_MS + 1_000);
        tracker.observe(0x4, 241, t_recent);
        // 22 s after the recent entry: 0x5 at Hamming 1 from both 0x1 and
        // 0x4.  0x1 is now > MAX_MS old → gap-gated; 0x4 matches.
        let gap = tracker.observe(0x5, 241, t_recent + Duration::milliseconds(22_000));
        assert_eq!(gap, Some(22_000));
    }

    #[test]
    fn rolling_buffer_evicts_by_age_when_full() {
        // Fill a small buffer with stale entries, then push a fresh packet.
        // Age-eviction must reclaim every slot so the next packet matches
        // only the fresh one.
        let mut tracker = RollingIntervalTracker::with_buffer_size(3);
        // Three stale entries spaced 1 s apart at the start.
        tracker.observe(0x1, 241, t0());
        tracker.observe(0x2, 241, t0() + Duration::milliseconds(1_000));
        tracker.observe(0x3, 241, t0() + Duration::milliseconds(2_000));
        assert_eq!(tracker.len(), 1, "all three packets share rtl433_id=241");
        // Push a fresh packet 5 s past RAW_INTERVAL_MAX_MS — buffer is full,
        // so age-eviction runs and removes all three stale entries.
        let t_fresh = t0() + Duration::milliseconds(RAW_INTERVAL_MAX_MS + 5_000);
        tracker.observe(0x10, 241, t_fresh);
        // The next packet, 22 s later, must match 0x10 — proving none of
        // the original three is still in the buffer.  0x10 (binary 10000)
        // and 0x12 (binary 10010) differ by 1 bit; 0x10 vs 0x1 also differs
        // by exactly 1 bit, but 0x1's gap is now > MAX_MS, so even without
        // age-eviction the gap-gate would reject it.  We instead test that
        // a second observation 22 s after `t_fresh` yields exactly one
        // candidate (0x10), with a clean 22 s interval.
        let gap = tracker.observe(0x12, 241, t_fresh + Duration::milliseconds(22_000));
        assert_eq!(gap, Some(22_000));
    }

    #[test]
    fn rolling_evict_stale_clears_dedup_state() {
        // After `evict_stale` removes records older than `max_age_secs`, the
        // dedup state for the same period must also be cleared so a freshly
        // arriving pair is not silently suppressed.
        let mut tracker = RollingIntervalTracker::new();
        tracker.observe(0x1, 241, t0());
        let _ = tracker.observe(0x3, 241, t0() + Duration::milliseconds(6_000));
        // Evict everything before t0 + 1000 s.
        tracker.evict_stale(t0() + Duration::seconds(1_000), 60);
        assert!(
            tracker.is_empty(),
            "buffer must be empty after stale eviction"
        );
        // A new pair arriving long after eviction must produce an interval —
        // dedup state must not have lingered.
        tracker.observe(0x1, 241, t0() + Duration::seconds(1_500));
        let gap = tracker.observe(
            0x3,
            241,
            t0() + Duration::seconds(1_500) + Duration::milliseconds(6_000),
        );
        assert_eq!(gap, Some(6_000));
    }
}
