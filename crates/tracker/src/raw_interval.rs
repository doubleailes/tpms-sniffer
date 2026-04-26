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
    pub fn observe(
        &mut self,
        sensor_id: u32,
        rtl433_id: u16,
        now: DateTime<Utc>,
    ) -> Option<i64> {
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
    pub fn push(
        &mut self,
        sensor_id: u32,
        rtl433_id: u16,
        interval_ms: i64,
        ts: DateTime<Utc>,
    ) {
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
}
