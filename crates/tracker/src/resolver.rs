use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use log::{debug, trace};
use std::collections::VecDeque;
use uuid::Uuid;

use crate::classification::{compensate_pressure, infer_vehicle_class};
use crate::db::Database;
use crate::jaccard::{
    self, CoOccurrenceMatrix, VehicleMeta, WINDOW_SIZE_S, group_vehicles_into_cars_with_meta,
    infer_wheel_positions,
};
use crate::jitter;
use crate::raw_interval::{
    RAW_INTERVAL_BUFFER_TTL_SECS, RAW_INTERVAL_MAX_MS, RawIntervalBuffer, RawIntervalTracker,
};
use crate::{
    CROSS_RECEIVER_WINDOW_MS, FINGERPRINT_MAX_GAP_DAYS, MAX_PLAUSIBLE_PRESSURE_KPA,
    MIN_PLAUSIBLE_PRESSURE_KPA, Sighting, TX_INTERVAL_MAX_MS, TX_INTERVAL_MIN_SAMPLES,
    TX_INTERVAL_TOLERANCE_MS, TX_INTERVAL_WINDOW, TpmsPacket, VehicleTrack, compute_median,
    make_model_hint,
};

/// rtl_433 protocol IDs that transmit rolling (non-stable) sensor IDs.
/// For these we cluster packets into time-window bursts and fingerprint by
/// the resulting pressure tuple.
const ROLLING_ID_PROTOCOLS: &[u16] = &[
    208, // AVE
];

/// rtl_433 protocol IDs whose sensor ID field is unreliable because individual
/// bits flip between transmissions (so every packet looks like a different
/// sensor).  For these we ignore the sensor ID entirely and correlate each
/// packet against active vehicles by protocol + pressure fingerprint.
const BIT_FLIP_ID_PROTOCOLS: &[u16] = &[
    241, // EezTire
];

/// Rejects sensor IDs that are decode artifacts rather than real identifiers.
///
/// When a protocol decoder cannot extract all bits of the sensor ID field
/// reliably, unresolved bits default to `1`, producing values close to
/// `0xFFFFFFFF` (e.g. `0xFFFFFFFD`, `0xFFFFFFF7`, `0xFFFFFFFB`, `0xFFFFFFBF`).
/// These near-sentinel IDs appear across multiple protocols and, if treated
/// as legitimate fixed IDs, cause cross-protocol vehicle merging in the
/// fixed-ID map.
///
/// The check rejects all-zeros and any ID with fewer than 3 bits cleared from
/// the all-ones state. IDs with 0–2 bits cleared are near-sentinel decode
/// artifacts.
fn is_valid_sensor_id(id: u32) -> bool {
    // Reject 0x00000000 and any ID with ≤ 2 bits cleared from 0xFFFFFFFF.
    // IDs with 0–2 bits cleared are decode artifacts (unresolved bit fields).
    id != 0x00000000 && id.count_zeros() >= 3
}

/// Maximum gap (ms) between consecutive packets belonging to the same burst.
const BURST_GAP_MS: i64 = 200;

/// Maximum wheel slots per burst.
const BURST_MAX_WHEELS: usize = 4;

/// L1 distance threshold (kPa, per-wheel average) for matching a new burst
/// against a known vehicle's pressure signature.  Retained as a fallback
/// constant; the per-vehicle dynamic tolerance from
/// `VehicleClass::pressure_tolerance_kpa()` is used in active matching.
#[allow(dead_code)]
const PRESSURE_MATCH_TOLERANCE_KPA: f32 = 5.0;

/// How long a vehicle remains eligible for pressure-fingerprint correlation
/// after its most recent sighting.  After this gap we assume the sensor has
/// gone out of range and any new match is a different vehicle.
///
/// Some protocols transmit infrequently when the vehicle is stationary and
/// need a longer expiry than the default.  See `crates/tracker/README.md`
/// for the documented per-protocol values and rationale.
fn vehicle_expiry_for(rtl433_id: u16) -> Duration {
    match rtl433_id {
        208 => Duration::seconds(600), // AVE-TPMS: low TX rate when parked
        241 => Duration::seconds(480), // EezTire: may be slow when low battery
        298 => Duration::seconds(480), // TRW-OOK: similar
        _ => Duration::seconds(300),   // default: 5 minutes
    }
}

/// Compute the effective expiry for a vehicle, extending the base per-protocol
/// window when the sensor has reported low battery.  A low-battery sensor may
/// transmit less frequently but is still physically present; the extension
/// avoids spurious track restarts.
fn effective_expiry(vehicle: &VehicleTrack) -> Duration {
    let base = vehicle_expiry_for(vehicle.rtl433_id);
    if !vehicle.battery_ok {
        base + Duration::seconds(300) // extend by 5 min for low-battery sensors
    } else {
        base
    }
}

/// EMA weight for updating the pressure signature (higher = faster adaptation).
const EMA_ALPHA: f32 = 0.2;

// ---------------------------------------------------------------------------
// Internal accumulator for rolling-ID bursts
// ---------------------------------------------------------------------------

struct BurstAccumulator {
    protocol: String,
    rtl433_id: u16,
    sensor_id: u32,
    pressures: Vec<f32>,
    last_ts: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

pub struct Resolver {
    /// `(sensor_id, rtl433_id) → vehicle_id` for fixed-ID protocols.
    ///
    /// The decoder ID is part of the key so that two sensors from different
    /// protocols that happen to share a `sensor_id` value cannot be absorbed
    /// into the same vehicle UUID. See the issue "Fixed-ID lookup does not
    /// verify protocol, allows near-sentinel cross-protocol absorption" for
    /// background.
    fixed_map: HashMap<(u32, u16), Uuid>,
    /// All tracked vehicles keyed by vehicle_id.
    vehicles: HashMap<Uuid, VehicleTrack>,
    /// Accumulator for an in-progress rolling-ID burst.
    pending_burst: Option<BurstAccumulator>,
    /// Tracks the timestamp of the last packet seen per `rtl433_id`, used to
    /// compute `tx_interval_hint_ms` on incoming sightings.
    last_seen_by_protocol: HashMap<u16, DateTime<Utc>>,
    db: Database,
    /// Jaccard co-occurrence matrix for wheel grouping.
    cooccurrence: CoOccurrenceMatrix,
    /// Timestamp of the last window advance.
    last_window_advance: Option<DateTime<Utc>>,
    /// Receiver ID assigned to all locally-created sightings.
    receiver_id: String,
    /// Fast inverse index: `vehicle_id → car_id`.  Persists for the lifetime
    /// of the session so that a vehicle retains its `car_id` across grouping
    /// evaluations.
    vehicle_to_car: HashMap<Uuid, Uuid>,
    /// Mapping from `vehicle_id` to its persistent fingerprint_id.
    vehicle_to_fingerprint: HashMap<Uuid, String>,
    /// Car-IDs that have already been merged (absorbed) into another car.
    /// Prevents duplicate merge log events across grouping passes.
    merged_car_ids: HashSet<(Uuid, Uuid)>,
    /// Tracks last-seen timestamp per `(sensor_id, rtl433_id)` to compute
    /// raw inter-packet intervals upstream of the fingerprint correlator.
    /// These intervals reflect the true TX cadence (and hence oscillator
    /// jitter) rather than the inter-correlator-match latency.
    raw_interval_tracker: RawIntervalTracker,
    /// Buffers raw intervals per sensor key until the correlator resolves a
    /// fingerprint for the sensor.  Drained into `interval_samples` after
    /// resolution; stale entries are dropped after `RAW_INTERVAL_BUFFER_TTL_SECS`.
    raw_interval_buffer: RawIntervalBuffer,
    /// Last time stale entries were evicted from the raw-interval tracker
    /// and buffer.  Used to throttle eviction to roughly once per minute.
    last_raw_interval_evict: Option<DateTime<Utc>>,
}

impl Resolver {
    /// Open (or create) the database and restore in-memory state from it.
    pub fn new(db: Database) -> Result<Self> {
        Self::with_receiver_id(db, "default".to_string())
    }

    /// Open (or create) the database with a specific receiver ID.
    pub fn with_receiver_id(db: Database, receiver_id: String) -> Result<Self> {
        let mut r = Self {
            fixed_map: HashMap::new(),
            vehicles: HashMap::new(),
            pending_burst: None,
            last_seen_by_protocol: HashMap::new(),
            db,
            cooccurrence: CoOccurrenceMatrix::new(),
            last_window_advance: None,
            receiver_id,
            vehicle_to_car: HashMap::new(),
            vehicle_to_fingerprint: HashMap::new(),
            merged_car_ids: HashSet::new(),
            raw_interval_tracker: RawIntervalTracker::new(),
            raw_interval_buffer: RawIntervalBuffer::new(),
            last_raw_interval_evict: None,
        };
        r.load_from_db()?;
        Ok(r)
    }

    fn load_from_db(&mut self) -> Result<()> {
        for vehicle in self.db.all_vehicles()? {
            if let Some(sid) = vehicle.fixed_sensor_id {
                // Only restore a fixed-ID mapping when the sensor ID passes
                // the sentinel check.  Legacy rows persisted before the
                // popcount filter was added may contain near-sentinel IDs
                // that would pollute the fixed_map and cause cross-protocol
                // merges on restart.
                if is_valid_sensor_id(sid) {
                    self.fixed_map
                        .insert((sid, vehicle.rtl433_id), vehicle.vehicle_id);
                }
            }
            // Restore vehicle_to_car from persisted car_id assignments.
            if let Some(car_id) = vehicle.car_id {
                self.vehicle_to_car.insert(vehicle.vehicle_id, car_id);
            }
            // Restore vehicle_to_fingerprint from persisted fingerprint_id.
            if let Some(ref fp_id) = vehicle.fingerprint_id {
                self.vehicle_to_fingerprint
                    .insert(vehicle.vehicle_id, fp_id.clone());
            }
            self.vehicles.insert(vehicle.vehicle_id, vehicle);
        }
        Ok(())
    }

    /// Process one decoded TPMS packet.
    ///
    /// Returns the vehicle UUID if it could be resolved immediately.  For
    /// rolling-ID protocols the UUID is returned only when a complete burst
    /// has been correlated; returns `None` while the burst is still building.
    pub fn process(&mut self, packet: &TpmsPacket) -> Result<Option<Uuid>> {
        // Guard: discard physically impossible pressure readings before they
        // reach the resolver and corrupt vehicle state.
        if packet.pressure_kpa < MIN_PLAUSIBLE_PRESSURE_KPA
            || packet.pressure_kpa > MAX_PLAUSIBLE_PRESSURE_KPA
        {
            debug!(
                "{} | DISCARD | sensor={:#010x} | reason=implausible_pressure \
                 | pressure={:.1} | protocol={} | rtl433={}",
                packet.timestamp,
                packet.sensor_id_u32().unwrap_or(0),
                packet.pressure_kpa,
                packet.protocol,
                packet.rtl433_id,
            );
            return Ok(None);
        }

        let ts = packet.parsed_ts().unwrap_or_else(Utc::now);
        let Some(sensor_id) = packet.sensor_id_u32() else {
            return Ok(None);
        };

        // Record raw inter-packet interval BEFORE the correlator runs.  The
        // correlator only matches a fraction of received packets (rolling and
        // bit-flip IDs miss most matches), so any interval taken downstream
        // measures inter-match latency rather than the sensor's true TX
        // cadence.  Successful observations are buffered per-sensor and
        // flushed to interval_samples after fingerprint resolution.
        if let Some(interval_ms) =
            self.raw_interval_tracker
                .observe(sensor_id, packet.rtl433_id, ts)
        {
            self.raw_interval_buffer
                .push(sensor_id, packet.rtl433_id, interval_ms, ts);
        }

        // Discard temperature sentinel values (≥ 200 °C means "not available").
        let temp_c = packet.temp_c.filter(|&t| t < 200.0);

        // Compute tx_interval_hint from the last packet of the same protocol.
        let hint = self
            .last_seen_by_protocol
            .get(&packet.rtl433_id)
            .map(|&prev| (ts - prev).num_milliseconds() as u32)
            .filter(|&ms| ms < TX_INTERVAL_MAX_MS);
        self.last_seen_by_protocol.insert(packet.rtl433_id, ts);

        let sighting = Sighting {
            ts,
            protocol: packet.protocol.clone(),
            rtl433_id: packet.rtl433_id,
            sensor_id,
            pressure_kpa: packet.pressure_kpa,
            temp_c,
            alarm: packet.alarm.unwrap_or(false),
            battery_ok: packet.battery_ok.unwrap_or(true),
            pressure_reliable: packet.pressure_kpa_reliable,
            tx_interval_hint_ms: hint,
            receiver_id: if packet.receiver_id == "default" {
                self.receiver_id.clone()
            } else {
                packet.receiver_id.clone()
            },
        };

        let result = if BIT_FLIP_ID_PROTOCOLS.contains(&packet.rtl433_id) {
            self.process_fingerprint(sighting)
        } else if ROLLING_ID_PROTOCOLS.contains(&packet.rtl433_id) || !is_valid_sensor_id(sensor_id)
        {
            self.process_rolling(sighting)
        } else {
            self.process_fixed(sighting, packet.rtl433_id)
        };

        // After the correlator has resolved (or failed to resolve) a vehicle
        // for this packet, flush any buffered raw intervals for the same
        // (sensor_id, rtl433_id) into interval_samples.  This deferred-
        // association strategy lets us record the upstream interval even
        // though the fingerprint identity is only known post-correlation.
        if let Ok(Some(vehicle_id)) = result.as_ref()
            && let Some(fp_id) = self.vehicle_to_fingerprint.get(vehicle_id).cloned()
        {
            self.flush_raw_intervals_for_sensor(sensor_id, packet.rtl433_id, *vehicle_id, &fp_id);
        }

        // Throttled eviction of stale tracker entries and unresolved buffer
        // entries.  Runs at most once a minute to bound memory.
        self.maybe_evict_raw_intervals(ts);

        result
    }

    /// Flush all buffered raw intervals for a given sensor key into
    /// `interval_samples` under the resolved `fingerprint_id`.
    fn flush_raw_intervals_for_sensor(
        &mut self,
        sensor_id: u32,
        rtl433_id: u16,
        vehicle_id: Uuid,
        fingerprint_id: &str,
    ) {
        let intervals = self.raw_interval_buffer.drain(sensor_id, rtl433_id);
        if intervals.is_empty() {
            return;
        }
        for (interval_ms, ts) in intervals {
            if let Err(e) = self.db.insert_interval_sample(
                fingerprint_id,
                vehicle_id,
                &ts.to_rfc3339(),
                interval_ms,
                None,
            ) {
                eprintln!("warn: raw interval sample insert failed: {e}");
                continue;
            }
            if let Err(e) = self
                .db
                .enforce_interval_ring_buffer(fingerprint_id, jitter::MAX_INTERVAL_SAMPLES)
            {
                eprintln!("warn: ring buffer enforce failed: {e}");
            }
        }
    }

    /// Periodically prune the raw-interval tracker and buffer.  Runs at most
    /// once per minute; older sensor entries (older than `RAW_INTERVAL_MAX_MS`)
    /// are dropped from the tracker, and unresolved buffered intervals older
    /// than `RAW_INTERVAL_BUFFER_TTL_SECS` are discarded.
    fn maybe_evict_raw_intervals(&mut self, now: DateTime<Utc>) {
        let due = match self.last_raw_interval_evict {
            Some(prev) => (now - prev).num_seconds() >= 60,
            None => true,
        };
        if !due {
            return;
        }
        self.last_raw_interval_evict = Some(now);
        self.raw_interval_tracker
            .evict_stale(now, RAW_INTERVAL_MAX_MS / 1000);
        self.raw_interval_buffer
            .evict_stale(now, RAW_INTERVAL_BUFFER_TTL_SECS);
    }

    /// Flush any pending rolling-ID burst.  Call this when the input stream ends.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(burst) = self.pending_burst.take() {
            if burst.pressures.len() >= 2 {
                self.resolve_burst(&burst)?;
            }
        }
        Ok(())
    }

    /// Check whether a sighting from a different receiver within the
    /// cross-receiver deduplication window should be treated as a duplicate
    /// (same physical event).
    fn is_cross_receiver_duplicate(vehicle: &VehicleTrack, sighting: &Sighting) -> bool {
        if vehicle.receiver_sightings.is_empty() {
            return false;
        }
        let last_receiver = vehicle
            .receiver_sightings
            .iter()
            .filter_map(|(rid, times)| times.last().map(|t| (rid, t)))
            .max_by_key(|&(_, t)| *t);
        if let Some((last_rid, last_ts)) = last_receiver {
            if *last_rid != sighting.receiver_id {
                let delta_ms = sighting
                    .ts
                    .signed_duration_since(*last_ts)
                    .num_milliseconds()
                    .unsigned_abs();
                return delta_ms < CROSS_RECEIVER_WINDOW_MS;
            }
        }
        false
    }

    /// Record a receiver sighting timestamp on the vehicle track.
    fn record_receiver_sighting(vehicle: &mut VehicleTrack, receiver_id: &str, ts: DateTime<Utc>) {
        vehicle
            .receiver_sightings
            .entry(receiver_id.to_string())
            .or_default()
            .push(ts);
    }

    // -----------------------------------------------------------------------
    // Fixed-ID path
    // -----------------------------------------------------------------------

    fn process_fixed(&mut self, sighting: Sighting, rtl433_id: u16) -> Result<Option<Uuid>> {
        let sensor_id = sighting.sensor_id;
        let zeros = sensor_id.count_zeros();

        // Defense-in-depth: reject near-sentinel IDs at the insertion point,
        // not just at the routing level in `process()`.  Without this guard a
        // future refactor that changes the routing logic could silently allow
        // near-sentinel IDs to create fixed-ID vehicles.
        if !is_valid_sensor_id(sensor_id) {
            debug!(
                "{} | RESOLVE | sensor={:#010X} | zeros={} | valid=false | \
                 path=fixed_id | result=rejected_sentinel",
                sighting.ts.format("%Y-%m-%d %H:%M:%S%.3f"),
                sensor_id,
                zeros,
            );
            return self.process_rolling(sighting);
        }

        // Key the fixed-ID map on `(sensor_id, rtl433_id)` so that two
        // sensors from different decoders sharing a `sensor_id` value produce
        // distinct vehicle UUIDs instead of merging.
        let key = (sensor_id, rtl433_id);

        // Look up or create the vehicle.
        let mut is_new_vehicle = false;
        let vehicle_id = if let Some(&vid) = self.fixed_map.get(&key) {
            vid
        } else if rtl433_id != 0 {
            // Legacy backward-compat bridge: a migrated row may have
            // `rtl433_id=0` because the column did not exist at insert time.
            // Check whether `(sensor_id, 0)` already maps to a vehicle whose
            // protocol matches, and if so adopt it rather than creating a
            // duplicate.
            let legacy_key = (sensor_id, 0u16);
            if let Some(&legacy_vid) = self.fixed_map.get(&legacy_key) {
                let protocol_matches = self
                    .vehicles
                    .get(&legacy_vid)
                    .map_or(false, |v| v.protocol == sighting.protocol);
                if protocol_matches {
                    // Migrate the in-memory key from (sensor_id, 0) to the
                    // real (sensor_id, rtl433_id) and correct the stored value.
                    self.fixed_map.remove(&legacy_key);
                    self.fixed_map.insert(key, legacy_vid);
                    if let Some(v) = self.vehicles.get_mut(&legacy_vid) {
                        v.rtl433_id = rtl433_id;
                    }
                    self.db.update_rtl433_id(legacy_vid, rtl433_id)?;
                    legacy_vid
                } else {
                    // Different protocol — create a new vehicle as normal.
                    let vid = Uuid::new_v4();
                    let vehicle = VehicleTrack {
                        vehicle_id: vid,
                        first_seen: sighting.ts,
                        last_seen: sighting.ts,
                        sighting_count: 0,
                        protocol: sighting.protocol.clone(),
                        rtl433_id,
                        fixed_sensor_id: Some(sensor_id),
                        pressure_signature: [sighting.pressure_kpa, 0.0, 0.0, 0.0],
                        make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                        battery_ok: sighting.battery_ok,
                        tx_intervals_ms: VecDeque::new(),
                        tx_interval_median_ms: None,
                        car_id: None,
                        receiver_sightings: HashMap::new(),
                        wheel_position: None,
                        vehicle_class: infer_vehicle_class(sighting.pressure_kpa, None),
                        fingerprint_id: None,
                    };
                    self.fixed_map.insert(key, vid);
                    self.vehicles.insert(vid, vehicle);
                    is_new_vehicle = true;
                    vid
                }
            } else {
                let vid = Uuid::new_v4();
                let vehicle = VehicleTrack {
                    vehicle_id: vid,
                    first_seen: sighting.ts,
                    last_seen: sighting.ts,
                    sighting_count: 0,
                    protocol: sighting.protocol.clone(),
                    rtl433_id,
                    fixed_sensor_id: Some(sensor_id),
                    pressure_signature: [sighting.pressure_kpa, 0.0, 0.0, 0.0],
                    make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                    battery_ok: sighting.battery_ok,
                    tx_intervals_ms: VecDeque::new(),
                    tx_interval_median_ms: None,
                    car_id: None,
                    receiver_sightings: HashMap::new(),
                    wheel_position: None,
                    vehicle_class: infer_vehicle_class(sighting.pressure_kpa, None),
                    fingerprint_id: None,
                };
                self.fixed_map.insert(key, vid);
                self.vehicles.insert(vid, vehicle);
                is_new_vehicle = true;
                vid
            }
        } else {
            let vid = Uuid::new_v4();
            let vehicle = VehicleTrack {
                vehicle_id: vid,
                first_seen: sighting.ts,
                last_seen: sighting.ts,
                sighting_count: 0,
                protocol: sighting.protocol.clone(),
                rtl433_id,
                fixed_sensor_id: Some(sensor_id),
                pressure_signature: [sighting.pressure_kpa, 0.0, 0.0, 0.0],
                make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                battery_ok: sighting.battery_ok,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
                car_id: None,
                receiver_sightings: HashMap::new(),
                wheel_position: None,
                vehicle_class: infer_vehicle_class(sighting.pressure_kpa, None),
                fingerprint_id: None,
            };
            self.fixed_map.insert(key, vid);
            self.vehicles.insert(vid, vehicle);
            is_new_vehicle = true;
            vid
        };

        // RESOLVE debug log for fixed-ID path.
        let result_str = if is_new_vehicle { "new" } else { "existing" };
        debug!(
            "{} | RESOLVE | sensor={:#010X} | zeros={} | valid=true | \
             path=fixed_id | key=({:#010X},{}) | result={} | pressure={:.1}",
            sighting.ts.format("%Y-%m-%d %H:%M:%S%.3f"),
            sensor_id,
            zeros,
            sensor_id,
            rtl433_id,
            result_str,
            sighting.pressure_kpa,
        );

        // TRACE event for vehicle creation.
        if is_new_vehicle {
            trace!(
                "{} | EVENT | type=vehicle_created | vehicle={} | protocol={} | \
                 rtl433={} | pressure={:.1} | path=fixed_id",
                sighting.ts.format("%Y-%m-%d %H:%M:%S%.3f"),
                &vehicle_id.to_string()[..8],
                sighting.protocol,
                rtl433_id,
                sighting.pressure_kpa,
            );
        }

        // Cross-receiver dedup check + update in-memory state.
        let is_dup = {
            let vehicle = self.vehicles.get(&vehicle_id).unwrap();
            Self::is_cross_receiver_duplicate(vehicle, &sighting)
        };
        {
            let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
            Self::record_receiver_sighting(vehicle, &sighting.receiver_id, sighting.ts);
            if !is_dup {
                // Accumulate inter-packet interval before updating last_seen.
                let interval_ms = (sighting.ts - vehicle.last_seen).num_milliseconds() as u32;
                if interval_ms > 0 && interval_ms < TX_INTERVAL_MAX_MS {
                    vehicle.tx_intervals_ms.push_back(interval_ms);
                    if vehicle.tx_intervals_ms.len() > TX_INTERVAL_WINDOW {
                        vehicle.tx_intervals_ms.pop_front();
                    }
                    vehicle.tx_interval_median_ms = compute_median(&vehicle.tx_intervals_ms);
                }
                vehicle.sighting_count += 1;
            }
            vehicle.last_seen = sighting.ts;
            vehicle.battery_ok = sighting.battery_ok;
            if sighting.pressure_reliable {
                let compensated = compensate_pressure(sighting.pressure_kpa, sighting.temp_c);
                ema_update(&mut vehicle.pressure_signature[0], compensated);
                // Recompute vehicle class from updated pressure average.
                let new_class = infer_vehicle_class(vehicle.pressure_signature[0], None);
                if new_class != vehicle.vehicle_class
                    && vehicle.sighting_count >= TX_INTERVAL_MIN_SAMPLES as u32
                {
                    eprintln!(
                        "warn: vehicle {} class changed from {} to {} after stabilisation",
                        vehicle.vehicle_id, vehicle.vehicle_class, new_class
                    );
                }
                vehicle.vehicle_class = new_class;
            }
        }

        // Record co-occurrence (only for non-duplicate sightings).
        if !is_dup {
            self.cooccurrence.record(vehicle_id);
        }

        // Advance window if enough time has elapsed.
        self.maybe_advance_window(sighting.ts);

        // Persist.
        let vehicle = self.vehicles.get(&vehicle_id).unwrap();
        self.db.upsert_vehicle(vehicle)?;
        self.db.insert_sighting(&sighting, vehicle_id)?;

        // Incrementally update presence slot if car_id is assigned.
        if let Some(car_id) = vehicle.car_id {
            if let Err(e) = self.db.upsert_presence_slot(
                &car_id.to_string(),
                &sighting.ts,
                &sighting.receiver_id,
            ) {
                eprintln!("warn: presence slot upsert failed: {e}");
            }
        }

        Ok(Some(vehicle_id))
    }

    // -----------------------------------------------------------------------
    // Fingerprint path (single-packet pressure correlation)
    // -----------------------------------------------------------------------

    /// Correlate a single packet against active vehicles of the same protocol
    /// using the pressure fingerprint.  Used for protocols whose sensor ID
    /// field is unreliable because bits flip between transmissions (e.g.
    /// EezTire), which would otherwise cause every packet to create a new
    /// vehicle via the fixed-ID path.
    fn process_fingerprint(&mut self, sighting: Sighting) -> Result<Option<Uuid>> {
        let now = sighting.ts;
        // Use temperature-compensated pressure for matching and EMA updates;
        // the raw pressure is persisted in the sighting row via insert_sighting.
        let pressure = compensate_pressure(sighting.pressure_kpa, sighting.temp_c);
        let protocol = sighting.protocol.clone();
        let rtl433_id = sighting.rtl433_id;
        let hint = sighting.tx_interval_hint_ms;
        let sensor_id = sighting.sensor_id;
        let zeros = sensor_id.count_zeros();
        let valid = is_valid_sensor_id(sensor_id);

        // Count candidates for RESOLVE logging.
        let candidates = self
            .vehicles
            .values()
            .filter(|v| v.rtl433_id == rtl433_id && v.fixed_sensor_id.is_none())
            .filter(|v| now.signed_duration_since(v.last_seen) < effective_expiry(v))
            .count();

        // Find an active vehicle of the same protocol whose pressure
        // signature is within tolerance.  Use `rtl433_id` (not the display
        // name string) so the filter applies unconditionally to all callers,
        // including sentinel-rejected fixed-ID packets that fall through to
        // this path.  Copy the Uuid so we drop the shared borrow before
        // taking a mutable one below.
        //
        // IMPORTANT: The protocol filter (`rtl433_id`) MUST be the first
        // iterator filter to prevent cross-protocol vehicle merging.  Moving
        // the pressure or expiry check before the protocol check would allow
        // a near-sentinel packet from one protocol to match a vehicle from
        // another protocol if their pressures happen to overlap.
        //
        // The TX interval is a soft tiebreaker: it can narrow the set of
        // pressure matches but must never eliminate all of them.
        //
        // Step 1: find all candidates matching on protocol + pressure (hard
        // constraints).
        let mut pressure_matches: Vec<&VehicleTrack> = self
            .vehicles
            .values()
            .filter(|v| v.rtl433_id == rtl433_id && v.fixed_sensor_id.is_none())
            .filter(|v| now.signed_duration_since(v.last_seen) < effective_expiry(v))
            .filter(|v| {
                let tolerance = v.vehicle_class.pressure_tolerance_kpa();
                (v.pressure_signature[0] - pressure).abs() <= tolerance
            })
            .collect();

        // Step 2: if multiple pressure matches, use TX interval as tiebreaker
        // (soft constraint).  The interval check can only *narrow* the set —
        // it must never eliminate all pressure matches.
        let mut interval_filter_log: Option<String> = None;
        if pressure_matches.len() > 1 {
            if let Some(s_interval) = hint {
                let before_count = pressure_matches.len();
                let interval_matches: Vec<&VehicleTrack> = pressure_matches
                    .iter()
                    .filter(|v| {
                        v.tx_interval_median_ms.map_or(true, |vi| {
                            (vi as i64 - s_interval as i64).unsigned_abs() as u32
                                <= TX_INTERVAL_TOLERANCE_MS
                        })
                    })
                    .copied()
                    .collect();

                // Only use interval filtering if it leaves at least one
                // candidate — never use it to reject all matches.
                if !interval_matches.is_empty() && interval_matches.len() < before_count {
                    interval_filter_log = Some(format!(
                        "narrowed:{}_to_{}",
                        before_count,
                        interval_matches.len()
                    ));
                    pressure_matches = interval_matches;
                }
            }
        }

        // Step 3: among remaining candidates, prefer most recently seen.
        let matched_vid: Option<Uuid> = pressure_matches
            .into_iter()
            .max_by_key(|v| v.last_seen)
            .map(|v| v.vehicle_id);

        let vehicle_id = if let Some(vid) = matched_vid {
            // RESOLVE: matched an existing fingerprint-correlator vehicle.
            let vehicle = &self.vehicles[&vid];
            let pressure_before = vehicle.pressure_signature[0];
            let interval_field = interval_filter_log
                .as_deref()
                .map_or(String::new(), |f| format!(" | interval_filter={}", f));
            debug!(
                "{} | RESOLVE | sensor={:#010X} | zeros={} | valid={} | \
                 path=fingerprint_correlator | candidates={} | matched={} | \
                 match_reason=pressure_delta:{:.1}_kpa | protocol_filter=pass | \
                 pressure_before={:.1} | pressure_after={:.1}{}",
                now.format("%Y-%m-%d %H:%M:%S%.3f"),
                sensor_id,
                zeros,
                valid,
                candidates,
                &vid.to_string()[..8],
                (pressure_before - pressure).abs(),
                pressure_before,
                pressure,
                interval_field,
            );
            vid
        } else {
            let fp_class = infer_vehicle_class(pressure, None);
            let fp_match = self.db.find_fingerprint(
                rtl433_id,
                pressure,
                &fp_class,
                hint,
                FINGERPRINT_MAX_GAP_DAYS,
                &now.to_rfc3339(),
            )?;

            let vid = Uuid::new_v4();
            let vehicle = VehicleTrack {
                vehicle_id: vid,
                first_seen: now,
                last_seen: now,
                sighting_count: 0,
                protocol: protocol.clone(),
                rtl433_id,
                fixed_sensor_id: None,
                pressure_signature: [pressure, 0.0, 0.0, 0.0],
                make_model_hint: make_model_hint(rtl433_id).map(str::to_owned),
                battery_ok: sighting.battery_ok,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
                car_id: None,
                receiver_sightings: HashMap::new(),
                wheel_position: None,
                vehicle_class: fp_class,
                fingerprint_id: fp_match.clone(),
            };
            self.vehicles.insert(vid, vehicle);

            let fp_path = if fp_match.is_some() {
                "fingerprint_store"
            } else {
                "fingerprint_correlator"
            };
            if let Some(ref fp_id) = fp_match {
                // Known sensor returning after a gap.
                self.vehicle_to_fingerprint.insert(vid, fp_id.clone());
                self.db.set_vehicle_fingerprint_id(vid, fp_id)?;
                self.db.increment_fingerprint_session(fp_id)?;
                // TRACE: fingerprint match event.
                trace!(
                    "{} | EVENT | type=fingerprint_match | vehicle={} | fp={}",
                    now.format("%Y-%m-%d %H:%M:%S%.3f"),
                    &vid.to_string()[..8],
                    fp_id,
                );
            } else {
                // Genuinely new sensor — create a new fingerprint.
                let fp_id = format!("fp-{}", &vid.to_string()[..8]);
                if let Some(v) = self.vehicles.get_mut(&vid) {
                    v.fingerprint_id = Some(fp_id.clone());
                }
                self.vehicle_to_fingerprint.insert(vid, fp_id.clone());
                self.db.create_fingerprint(
                    &fp_id,
                    rtl433_id,
                    fp_class.as_str(),
                    pressure,
                    hint,
                    &now.to_rfc3339(),
                    sighting.alarm,
                )?;
                self.db.set_vehicle_fingerprint_id(vid, &fp_id)?;
                // TRACE: fingerprint created event.
                trace!(
                    "{} | EVENT | type=fingerprint_created | fp={} | vehicle={} | \
                     protocol={} | pressure={:.1} | class={}",
                    now.format("%Y-%m-%d %H:%M:%S%.3f"),
                    fp_id,
                    &vid.to_string()[..8],
                    protocol,
                    pressure,
                    fp_class,
                );
            }

            // RESOLVE: new vehicle created via fingerprint path.
            debug!(
                "{} | RESOLVE | sensor={:#010X} | zeros={} | valid={} | \
                 path={} | candidates={} | matched=none | \
                 result=new_vehicle | pressure={:.1}",
                now.format("%Y-%m-%d %H:%M:%S%.3f"),
                sensor_id,
                zeros,
                valid,
                fp_path,
                candidates,
                pressure,
            );

            // TRACE: vehicle created event.
            trace!(
                "{} | EVENT | type=vehicle_created | vehicle={} | protocol={} | \
                 rtl433={} | pressure={:.1} | path={}",
                now.format("%Y-%m-%d %H:%M:%S%.3f"),
                &vid.to_string()[..8],
                protocol,
                rtl433_id,
                pressure,
                fp_path,
            );
            vid
        };

        // Cross-receiver dedup check + update in-memory state.
        // Note: jitter interval recording happens upstream in `process()` via
        // `RawIntervalTracker` / `RawIntervalBuffer`, so we no longer derive
        // intervals from `last_seen` here — those gaps measure inter-correlator-
        // match latency, not the sensor's TX cadence.
        let is_dup = {
            let vehicle = self.vehicles.get(&vehicle_id).unwrap();
            Self::is_cross_receiver_duplicate(vehicle, &sighting)
        };
        {
            let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
            Self::record_receiver_sighting(vehicle, &sighting.receiver_id, now);
            if !is_dup {
                // Accumulate inter-packet interval before updating last_seen.
                let interval_ms = (now - vehicle.last_seen).num_milliseconds() as u32;
                if interval_ms > 0 && interval_ms < TX_INTERVAL_MAX_MS {
                    vehicle.tx_intervals_ms.push_back(interval_ms);
                    if vehicle.tx_intervals_ms.len() > TX_INTERVAL_WINDOW {
                        vehicle.tx_intervals_ms.pop_front();
                    }
                    vehicle.tx_interval_median_ms = compute_median(&vehicle.tx_intervals_ms);
                }
                vehicle.sighting_count += 1;
            }
            vehicle.last_seen = now;
            vehicle.battery_ok = sighting.battery_ok;
            if sighting.pressure_reliable {
                ema_update(&mut vehicle.pressure_signature[0], pressure);
                // Recompute vehicle class from updated pressure average.
                let new_class = infer_vehicle_class(vehicle.pressure_signature[0], None);
                if new_class != vehicle.vehicle_class
                    && vehicle.sighting_count >= TX_INTERVAL_MIN_SAMPLES as u32
                {
                    eprintln!(
                        "warn: vehicle {} class changed from {} to {} after stabilisation",
                        vehicle.vehicle_id, vehicle.vehicle_class, new_class
                    );
                }
                vehicle.vehicle_class = new_class;
            }
        }

        // Record co-occurrence (only for non-duplicate sightings).
        if !is_dup {
            self.cooccurrence.record(vehicle_id);
        }

        // Advance window if enough time has elapsed.
        self.maybe_advance_window(now);

        // Persist.
        let vehicle = self.vehicles.get(&vehicle_id).unwrap();
        self.db.upsert_vehicle(vehicle)?;
        self.db.insert_sighting(&sighting, vehicle_id)?;

        // Update persistent fingerprint store.
        if let Some(fp_id) = self.vehicle_to_fingerprint.get(&vehicle_id) {
            let _ = self.db.update_fingerprint(
                fp_id,
                sighting.pressure_kpa,
                vehicle.tx_interval_median_ms,
                &now.to_rfc3339(),
                sighting.alarm,
            );
        }

        // Incrementally update presence slot if car_id is assigned.
        if let Some(car_id) = vehicle.car_id {
            if let Err(e) = self.db.upsert_presence_slot(
                &car_id.to_string(),
                &sighting.ts,
                &sighting.receiver_id,
            ) {
                eprintln!("warn: presence slot upsert failed: {e}");
            }
        }

        Ok(Some(vehicle_id))
    }

    // -----------------------------------------------------------------------
    // Rolling-ID path (burst accumulator)
    // -----------------------------------------------------------------------

    fn process_rolling(&mut self, sighting: Sighting) -> Result<Option<Uuid>> {
        // Unreliable readings (e.g. AVE half-range low-pressure frames) would
        // poison the pressure fingerprint if fed through the burst accumulator.
        // Drop them entirely: do not extend the pending burst, do not close it,
        // do not start a new one.  A sighting cannot be persisted here because
        // no vehicle UUID is known until a complete burst resolves — recording a
        // sighting requires a vehicle to attach it to.  The track for the
        // underlying vehicle keeps its last known good signature and is still
        // matched when the next normal packet arrives.
        if !sighting.pressure_reliable {
            return Ok(None);
        }

        let ts = sighting.ts;
        let protocol = sighting.protocol.clone();

        // Decide whether this packet extends the current burst or starts a new one.
        let start_new = match &self.pending_burst {
            Some(b) if b.protocol == protocol => {
                let gap_ms = (ts - b.last_ts).num_milliseconds();
                gap_ms > BURST_GAP_MS || b.pressures.len() >= BURST_MAX_WHEELS
            }
            _ => true,
        };

        let completed_vid = if start_new {
            // Close and resolve the completed burst (if any).
            let resolved = if let Some(old) = self.pending_burst.take() {
                if old.pressures.len() >= 2 {
                    self.resolve_burst(&old)?
                } else {
                    None
                }
            } else {
                None
            };

            // Open a new burst for this packet.
            self.pending_burst = Some(BurstAccumulator {
                protocol,
                rtl433_id: sighting.rtl433_id,
                sensor_id: sighting.sensor_id,
                pressures: vec![sighting.pressure_kpa],
                last_ts: ts,
            });

            resolved
        } else {
            // Extend the current burst.
            if let Some(burst) = &mut self.pending_burst {
                burst.pressures.push(sighting.pressure_kpa);
                burst.sensor_id = sighting.sensor_id;
                burst.last_ts = ts;
            }
            None
        };

        Ok(completed_vid)
    }

    fn resolve_burst(&mut self, burst: &BurstAccumulator) -> Result<Option<Uuid>> {
        let now = burst.last_ts;
        let sig = burst_to_signature(&burst.pressures);

        // Count candidates for RESOLVE logging.
        let candidates = self
            .vehicles
            .values()
            .filter(|v| v.fixed_sensor_id.is_none() && v.rtl433_id == burst.rtl433_id)
            .filter(|v| now.signed_duration_since(v.last_seen) < effective_expiry(v))
            .count();

        // Find an existing rolling-ID vehicle whose pressure signature is close
        // enough and that was seen recently enough to still be active.  We copy
        // the Uuid (it's Copy) so we drop the shared borrow before taking the
        // mutable one below.
        //
        // IMPORTANT: The protocol filter (`rtl433_id`) MUST be the first
        // iterator filter to prevent cross-protocol vehicle merging.  Moving
        // the pressure or expiry check before the protocol check would allow
        // a near-sentinel packet from one protocol to match a vehicle from
        // another protocol if their pressures happen to overlap.
        //
        // When a candidate vehicle has an established interval median, also
        // check that the gap since the vehicle's last sighting is compatible
        // with its typical TX interval.  This prevents merging two sensors at
        // the same pressure when they transmit at different rates.
        let matched_vid: Option<Uuid> = self
            .vehicles
            .values()
            .filter(|v| v.fixed_sensor_id.is_none() && v.rtl433_id == burst.rtl433_id)
            .filter(|v| now.signed_duration_since(v.last_seen) < effective_expiry(v))
            .find(|v| {
                let tolerance = v.vehicle_class.pressure_tolerance_kpa();
                let pressure_ok = l1_per_wheel(&v.pressure_signature, &sig) < tolerance;
                if !pressure_ok {
                    return false;
                }
                // Interval guard: if the vehicle has an established median,
                // verify the gap since its last sighting is compatible.
                if let Some(v_median) = v.tx_interval_median_ms {
                    let gap_ms = now.signed_duration_since(v.last_seen).num_milliseconds();
                    if gap_ms > 0 && (gap_ms as u32) < TX_INTERVAL_MAX_MS {
                        let delta = (v_median as i64 - gap_ms).unsigned_abs() as u32;
                        if delta > TX_INTERVAL_TOLERANCE_MS {
                            return false;
                        }
                    }
                }
                true
            })
            .map(|v| v.vehicle_id);

        let vehicle_id = if let Some(vid) = matched_vid {
            // RESOLVE: matched an existing rolling-ID vehicle.
            let vehicle = &self.vehicles[&vid];
            let pressure_before = vehicle
                .pressure_signature
                .iter()
                .find(|&&p| p > 0.0)
                .copied()
                .unwrap_or(0.0);
            let class_pressure = sig.iter().find(|&&p| p > 0.0).copied().unwrap_or(0.0);
            debug!(
                "{} | RESOLVE | sensor={:#010x} | zeros={} | valid={} | \
                 path=rolling_id | candidates={} | matched={} | \
                 match_reason=pressure_delta:{:.1}_kpa | protocol_filter=pass | \
                 pressure_before={:.1} | pressure_after={:.1}",
                now.format("%Y-%m-%d %H:%M:%S%.3f"),
                burst.sensor_id,
                burst.sensor_id.count_zeros(),
                is_valid_sensor_id(burst.sensor_id),
                candidates,
                &vid.to_string()[..8],
                (pressure_before - class_pressure).abs(),
                pressure_before,
                class_pressure,
            );
            vid
        } else {
            let vid = Uuid::new_v4();
            let class_pressure = sig.iter().find(|&&p| p > 0.0).copied().unwrap_or(0.0);
            let fp_class = infer_vehicle_class(class_pressure, None);
            let fp_match = self.db.find_fingerprint(
                burst.rtl433_id,
                class_pressure,
                &fp_class,
                None,
                FINGERPRINT_MAX_GAP_DAYS,
                &now.to_rfc3339(),
            )?;

            let vehicle = VehicleTrack {
                vehicle_id: vid,
                first_seen: now,
                last_seen: now,
                sighting_count: 0,
                protocol: burst.protocol.clone(),
                rtl433_id: burst.rtl433_id,
                fixed_sensor_id: None,
                pressure_signature: sig,
                make_model_hint: make_model_hint(burst.rtl433_id).map(str::to_owned),
                battery_ok: true,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
                car_id: None,
                receiver_sightings: HashMap::new(),
                wheel_position: None,
                vehicle_class: fp_class,
                fingerprint_id: fp_match.clone(),
            };
            self.vehicles.insert(vid, vehicle);

            if let Some(ref fp_id) = fp_match {
                self.vehicle_to_fingerprint.insert(vid, fp_id.clone());
                self.db.set_vehicle_fingerprint_id(vid, fp_id)?;
                self.db.increment_fingerprint_session(fp_id)?;
                trace!(
                    "{} | EVENT | type=fingerprint_match | vehicle={} | fp={}",
                    now.format("%Y-%m-%d %H:%M:%S%.3f"),
                    &vid.to_string()[..8],
                    fp_id,
                );
            } else {
                let fp_id = format!("fp-{}", &vid.to_string()[..8]);
                if let Some(v) = self.vehicles.get_mut(&vid) {
                    v.fingerprint_id = Some(fp_id.clone());
                }
                self.vehicle_to_fingerprint.insert(vid, fp_id.clone());
                self.db.create_fingerprint(
                    &fp_id,
                    burst.rtl433_id,
                    fp_class.as_str(),
                    class_pressure,
                    None,
                    &now.to_rfc3339(),
                    false,
                )?;
                self.db.set_vehicle_fingerprint_id(vid, &fp_id)?;
                trace!(
                    "{} | EVENT | type=fingerprint_created | fp={} | vehicle={} | \
                     protocol={} | pressure={:.1} | class={}",
                    now.format("%Y-%m-%d %H:%M:%S%.3f"),
                    fp_id,
                    &vid.to_string()[..8],
                    burst.protocol,
                    class_pressure,
                    fp_class,
                );
            }

            // RESOLVE: new vehicle created via rolling-ID burst.
            debug!(
                "{} | RESOLVE | sensor={:#010x} | zeros={} | valid={} | \
                 path=rolling_id | candidates={} | matched=none | \
                 result=new_vehicle | pressure={:.1}",
                now.format("%Y-%m-%d %H:%M:%S%.3f"),
                burst.sensor_id,
                burst.sensor_id.count_zeros(),
                is_valid_sensor_id(burst.sensor_id),
                candidates,
                class_pressure,
            );

            // TRACE: vehicle created event.
            trace!(
                "{} | EVENT | type=vehicle_created | vehicle={} | protocol={} | \
                 rtl433={} | pressure={:.1} | path=rolling_id",
                now.format("%Y-%m-%d %H:%M:%S%.3f"),
                &vid.to_string()[..8],
                burst.protocol,
                burst.rtl433_id,
                class_pressure,
            );

            vid
        };

        // Update in-memory state.
        // Note: rolling-ID bursts do not carry per-sighting receiver_id
        // metadata (the burst accumulator merges multiple packets), so
        // cross-receiver dedup is not applied here.  The burst uses the
        // resolver's own receiver_id.
        // Jitter interval recording happens upstream in `process()` via
        // `RawIntervalTracker` rather than from the inter-burst gap here.
        {
            let vehicle = self.vehicles.get_mut(&vehicle_id).unwrap();
            Self::record_receiver_sighting(vehicle, &self.receiver_id, now);
            // Accumulate inter-burst interval before updating last_seen.
            let interval_ms = (now - vehicle.last_seen).num_milliseconds() as u32;
            if interval_ms > 0 && interval_ms < TX_INTERVAL_MAX_MS {
                vehicle.tx_intervals_ms.push_back(interval_ms);
                if vehicle.tx_intervals_ms.len() > TX_INTERVAL_WINDOW {
                    vehicle.tx_intervals_ms.pop_front();
                }
                vehicle.tx_interval_median_ms = compute_median(&vehicle.tx_intervals_ms);
            }
            vehicle.last_seen = now;
            vehicle.sighting_count += 1;
            for (i, &p) in burst.pressures.iter().enumerate().take(4) {
                ema_update(&mut vehicle.pressure_signature[i], p);
            }
            // Recompute vehicle class from updated pressure average.
            let class_pressure = vehicle
                .pressure_signature
                .iter()
                .find(|&&p| p > 0.0)
                .copied()
                .unwrap_or(0.0);
            let new_class = infer_vehicle_class(class_pressure, None);
            if new_class != vehicle.vehicle_class
                && vehicle.sighting_count >= TX_INTERVAL_MIN_SAMPLES as u32
            {
                eprintln!(
                    "warn: vehicle {} class changed from {} to {} after stabilisation",
                    vehicle.vehicle_id, vehicle.vehicle_class, new_class
                );
            }
            vehicle.vehicle_class = new_class;
        }

        // Record co-occurrence.
        self.cooccurrence.record(vehicle_id);

        // Advance window if enough time has elapsed.
        self.maybe_advance_window(now);

        // Persist.
        let vehicle = self.vehicles.get(&vehicle_id).unwrap();
        self.db.upsert_vehicle(vehicle)?;

        // Update persistent fingerprint store.
        if let Some(fp_id) = self.vehicle_to_fingerprint.get(&vehicle_id) {
            let class_pressure = vehicle
                .pressure_signature
                .iter()
                .find(|&&p| p > 0.0)
                .copied()
                .unwrap_or(0.0);
            let _ = self.db.update_fingerprint(
                fp_id,
                class_pressure,
                vehicle.tx_interval_median_ms,
                &now.to_rfc3339(),
                false,
            );
        }

        // Incrementally update presence slot if car_id is assigned.
        if let Some(car_id) = vehicle.car_id {
            if let Err(e) =
                self.db
                    .upsert_presence_slot(&car_id.to_string(), &now, &self.receiver_id)
            {
                eprintln!("warn: presence slot upsert failed: {e}");
            }
        }

        Ok(Some(vehicle_id))
    }

    // -----------------------------------------------------------------------
    // Jaccard co-occurrence window management
    // -----------------------------------------------------------------------

    /// Advance the co-occurrence window if at least `WINDOW_SIZE_S` seconds
    /// have elapsed since the last advance.  After advancing, runs the grouping
    /// algorithm and persists car assignments.
    fn maybe_advance_window(&mut self, now: DateTime<Utc>) {
        let should_advance = match self.last_window_advance {
            Some(prev) => (now - prev).num_seconds() >= WINDOW_SIZE_S as i64,
            None => {
                // First packet ever — initialize the timer but don't advance.
                self.last_window_advance = Some(now);
                false
            }
        };
        if should_advance {
            self.last_window_advance = Some(now);
            self.cooccurrence.advance_window();

            // Emit TRACE events for vehicles that have just expired.
            for v in self.vehicles.values() {
                let age = now.signed_duration_since(v.last_seen);
                let expiry = effective_expiry(v);
                // Emit once: expired this window but not in the previous one.
                if age >= expiry && age < expiry + Duration::seconds(WINDOW_SIZE_S as i64) {
                    let fp = v.fingerprint_id.as_deref().unwrap_or("none");
                    let car_id = v
                        .car_id
                        .map(|c| c.to_string()[..8].to_string())
                        .unwrap_or_else(|| "none".to_string());
                    trace!(
                        "{} | EVENT | type=vehicle_expired | vehicle={} | fp={} | \
                         car={} | age_secs={} | sightings={} | last_pressure={:.1} | \
                         last_seen={}",
                        now.format("%Y-%m-%d %H:%M:%S%.3f"),
                        &v.vehicle_id.to_string()[..8],
                        fp,
                        car_id,
                        age.num_seconds(),
                        v.sighting_count,
                        v.pressure_signature[0],
                        v.last_seen.format("%H:%M:%S"),
                    );
                }
            }

            // Best-effort grouping; errors are non-fatal.
            let _ = self.run_grouping();
        }
    }

    /// Run the Jaccard grouping algorithm and persist car assignments.
    ///
    /// The algorithm produces fresh `CarGroup` objects with throwaway UUIDs on
    /// every call.  To keep `car_id` stable across evaluations we check whether
    /// any member of a newly-computed group already has a persisted `car_id` in
    /// `vehicle_to_car` and reuse it instead of minting a new one.
    ///
    /// When a fresh group contains members from multiple previously-separate
    /// car_ids, this is a *merge event*: the distinct car_ids are consolidated
    /// into one, the discarded car entries are removed from the database, and
    /// all affected vehicles are re-pointed to the surviving car_id.
    fn run_grouping(&mut self) -> Result<()> {
        if self.cooccurrence.windows_accumulated < jaccard::MIN_WINDOWS {
            return Ok(());
        }
        let ids: Vec<Uuid> = self.vehicles.keys().copied().collect();

        // Build vehicle metadata for prefix-based pre-filtering.
        let meta: Vec<VehicleMeta> = self
            .vehicles
            .values()
            .map(|v| VehicleMeta {
                vehicle_id: v.vehicle_id,
                rtl433_id: v.rtl433_id,
                fixed_sensor_id: v.fixed_sensor_id,
            })
            .collect();

        let groups = group_vehicles_into_cars_with_meta(&self.cooccurrence, &ids, &meta);
        for group in &groups {
            // Collect all distinct, previously-assigned car_ids for this group.
            let mut previous_car_ids: Vec<Uuid> = group
                .members
                .iter()
                .filter_map(|vid| self.vehicle_to_car.get(vid).copied())
                .collect();
            previous_car_ids.sort();
            previous_car_ids.dedup();

            // Reuse the first previously-assigned car_id, or mint a new one.
            let stable_car_id = previous_car_ids.first().copied().unwrap_or(group.car_id);
            let is_new_car = previous_car_ids.is_empty();

            // TRACE: car_group_created event.
            if is_new_car && !group.members.is_empty() {
                let first_vid = group.members.iter().next();
                let first_vehicle = first_vid.and_then(|vid| self.vehicles.get(vid));
                let protocol = first_vehicle
                    .map(|v| v.protocol.as_str())
                    .unwrap_or("unknown");
                trace!(
                    "EVENT | type=car_group_created | car={} | vehicle={} | protocol={}",
                    &stable_car_id.to_string()[..8],
                    first_vid
                        .map(|v| v.to_string()[..8].to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    protocol,
                );
            }

            // Detect and handle merge events: when the fresh grouping
            // consolidates vehicles from multiple old car_ids.
            if previous_car_ids.len() > 1 {
                for &discarded in &previous_car_ids[1..] {
                    // Compute the inter-group Jaccard score between the kept
                    // and discarded groups for the log message.
                    let kept_members: std::collections::HashSet<Uuid> = group
                        .members
                        .iter()
                        .filter(|vid| self.vehicle_to_car.get(vid).copied() == Some(stable_car_id))
                        .copied()
                        .collect();
                    let discarded_members: std::collections::HashSet<Uuid> = group
                        .members
                        .iter()
                        .filter(|vid| self.vehicle_to_car.get(vid).copied() == Some(discarded))
                        .copied()
                        .collect();
                    let score = self
                        .cooccurrence
                        .inter_group_jaccard(&kept_members, &discarded_members);

                    let merge_key = (stable_car_id, discarded);
                    if !self.merged_car_ids.contains(&merge_key) {
                        trace!(
                            "EVENT | type=jaccard_merge | keep={} | discard={} | \
                             score={:.3} | keep_size={} | discard_size={} | \
                             members=[{}]",
                            &stable_car_id.to_string()[..8],
                            &discarded.to_string()[..8],
                            score,
                            kept_members.len(),
                            discarded_members.len(),
                            group
                                .members
                                .iter()
                                .map(|m| m.to_string()[..8].to_string())
                                .collect::<Vec<_>>()
                                .join(","),
                        );
                    }
                    self.merged_car_ids.insert(merge_key);

                    // Reassign all vehicles in the database from discarded → kept.
                    self.db.reassign_vehicles_car_id(discarded, stable_car_id)?;
                    // Remove the now-empty car record.
                    self.db.delete_car(discarded)?;

                    // Remap ALL in-memory vehicle_to_car entries from
                    // discarded → kept so that no vehicle is orphaned and so
                    // that subsequent grouping passes do not re-trigger this
                    // merge (idempotency).
                    for car_id in self.vehicle_to_car.values_mut() {
                        if *car_id == discarded {
                            *car_id = stable_car_id;
                        }
                    }
                    for v in self.vehicles.values_mut() {
                        if v.car_id == Some(discarded) {
                            v.car_id = Some(stable_car_id);
                        }
                    }
                }
            }

            // Determine aggregate first/last seen for the car.
            let mut first = None::<DateTime<Utc>>;
            let mut last = None::<DateTime<Utc>>;
            let mut make_model: Option<String> = None;
            for &vid in &group.members {
                if let Some(v) = self.vehicles.get(&vid) {
                    first =
                        Some(first.map_or(v.first_seen, |f: DateTime<Utc>| f.min(v.first_seen)));
                    last = Some(last.map_or(v.last_seen, |l: DateTime<Utc>| l.max(v.last_seen)));
                    if make_model.is_none() {
                        make_model = v.make_model_hint.clone();
                    }
                }
            }
            let first_s = first.map(|d| d.to_rfc3339()).unwrap_or_default();
            let last_s = last.map(|d| d.to_rfc3339()).unwrap_or_default();

            self.db.upsert_car(
                stable_car_id,
                &first_s,
                &last_s,
                group.wheel_count(),
                make_model.as_deref(),
            )?;

            // Attempt wheel-position inference for groups of exactly 4 fixed-ID
            // sensors whose trailing bytes are consecutive.
            let group_sensor_ids: Vec<u32> = group
                .members
                .iter()
                .filter_map(|vid| self.vehicles.get(vid).and_then(|v| v.fixed_sensor_id))
                .collect();
            let wheel_map = infer_wheel_positions(&group_sensor_ids);

            for &vid in &group.members {
                if let Some(v) = self.vehicles.get_mut(&vid) {
                    v.car_id = Some(stable_car_id);
                    // Assign inferred wheel position if available.
                    if let (Some(wm), Some(sid)) = (&wheel_map, v.fixed_sensor_id) {
                        v.wheel_position = wm.get(&sid).copied();
                    }
                }
                self.vehicle_to_car.insert(vid, stable_car_id);
                self.db.set_vehicle_car_id(vid, stable_car_id)?;
            }

            // Persist updated vehicles (including wheel_position) after grouping.
            for &vid in &group.members {
                if let Some(v) = self.vehicles.get(&vid) {
                    self.db.upsert_vehicle(v)?;
                }
            }
        }
        Ok(())
    }

    /// Return a reference to the co-occurrence matrix for export.
    pub fn cooccurrence_matrix(&self) -> &CoOccurrenceMatrix {
        &self.cooccurrence
    }

    /// Return a reference to the in-memory vehicles map.
    pub fn vehicles(&self) -> &HashMap<Uuid, VehicleTrack> {
        &self.vehicles
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a 4-slot pressure signature from a burst (zero-pads unused slots).
fn burst_to_signature(pressures: &[f32]) -> [f32; 4] {
    let mut sig = [0.0f32; 4];
    for (i, &p) in pressures.iter().enumerate().take(4) {
        sig[i] = p;
    }
    sig
}

/// Mean per-wheel L1 distance between two pressure signatures.
/// Slots that are zero in either vector are excluded from the average so that
/// a vehicle seen with fewer than 4 wheels can still match.
fn l1_per_wheel(a: &[f32; 4], b: &[f32; 4]) -> f32 {
    let (sum, count) = a
        .iter()
        .zip(b.iter())
        .fold((0.0f32, 0u32), |(s, n), (&x, &y)| {
            if x == 0.0 || y == 0.0 {
                (s, n)
            } else {
                (s + (x - y).abs(), n + 1)
            }
        });
    if count == 0 {
        f32::MAX
    } else {
        sum / count as f32
    }
}

/// Exponential moving average update (in-place).
fn ema_update(slot: &mut f32, new_val: f32) {
    if *slot == 0.0 {
        *slot = new_val;
    } else {
        *slot = *slot * (1.0 - EMA_ALPHA) + new_val * EMA_ALPHA;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classification::VehicleClass;
    use crate::db::Database;
    use std::collections::HashSet;

    fn make_packet(
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
    ) -> TpmsPacket {
        make_packet_at(
            "2025-06-01 12:00:00.000",
            sensor_id,
            protocol,
            rtl433_id,
            pressure_kpa,
        )
    }

    fn make_packet_at(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
    ) -> TpmsPacket {
        make_packet_at_reliable(
            timestamp,
            sensor_id,
            protocol,
            rtl433_id,
            pressure_kpa,
            true,
        )
    }

    fn make_packet_at_reliable(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
        pressure_kpa_reliable: bool,
    ) -> TpmsPacket {
        TpmsPacket {
            timestamp: timestamp.to_string(),
            protocol: protocol.to_string(),
            rtl433_id,
            sensor_id: sensor_id.to_string(),
            pressure_kpa,
            temp_c: Some(25.0),
            battery_ok: Some(true),
            alarm: Some(false),
            confidence: 90,
            pressure_kpa_reliable,
            receiver_id: "test".to_string(),
        }
    }

    fn make_packet_at_battery(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
        battery_ok: bool,
    ) -> TpmsPacket {
        TpmsPacket {
            timestamp: timestamp.to_string(),
            protocol: protocol.to_string(),
            rtl433_id,
            sensor_id: sensor_id.to_string(),
            pressure_kpa,
            temp_c: Some(25.0),
            battery_ok: Some(battery_ok),
            alarm: Some(false),
            confidence: 90,
            pressure_kpa_reliable: true,
            receiver_id: "test".to_string(),
        }
    }

    fn in_memory_resolver() -> Resolver {
        let db = Database::open(":memory:").unwrap();
        Resolver::new(db).unwrap()
    }

    #[test]
    fn sentinel_0xffffffff_not_created_via_fixed_id_path() {
        let mut resolver = in_memory_resolver();

        // Send two packets with 0xFFFFFFFF from EezTire (fixed-ID protocol 241).
        let p1 = make_packet("0xFFFFFFFF", "EezTire", 241, 352.3);
        let p2 = make_packet("0xFFFFFFFF", "EezTire", 241, 352.9);
        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();
        resolver.flush().unwrap();

        // No vehicle should have a fixed_sensor_id of 0xFFFFFFFF.
        for v in resolver.vehicles.values() {
            assert_ne!(
                v.fixed_sensor_id,
                Some(0xFFFFFFFF),
                "sentinel 0xFFFFFFFF must not be stored as a fixed sensor ID"
            );
        }
        assert!(
            !resolver.fixed_map.keys().any(|(sid, _)| *sid == 0xFFFFFFFF),
            "sentinel 0xFFFFFFFF must not appear in fixed_map"
        );
    }

    #[test]
    fn sentinel_0x00000000_not_created_via_fixed_id_path() {
        let mut resolver = in_memory_resolver();

        let p1 = make_packet("0x00000000", "TRW-OOK", 298, 63.0);
        let p2 = make_packet("0x00000000", "TRW-OOK", 298, 63.1);
        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();
        resolver.flush().unwrap();

        for v in resolver.vehicles.values() {
            assert_ne!(
                v.fixed_sensor_id,
                Some(0x00000000),
                "sentinel 0x00000000 must not be stored as a fixed sensor ID"
            );
        }
        assert!(
            !resolver.fixed_map.keys().any(|(sid, _)| *sid == 0x00000000),
            "sentinel 0x00000000 must not appear in fixed_map"
        );
    }

    #[test]
    fn sentinel_different_protocols_do_not_merge() {
        let mut resolver = in_memory_resolver();

        // Two bursts with 0xFFFFFFFF from different protocols, with very
        // different pressures so the fingerprint correlator cannot merge them.
        let p1a = make_packet("0xFFFFFFFF", "EezTire", 241, 351.6);
        let p1b = make_packet("0xFFFFFFFF", "EezTire", 241, 52.0);
        resolver.process(&p1a).unwrap();
        resolver.process(&p1b).unwrap();
        resolver.flush().unwrap();

        let p2a = make_packet("0xFFFFFFFF", "Hyundai-Elantra", 140, 255.0);
        let p2b = make_packet("0xFFFFFFFF", "Hyundai-Elantra", 140, 254.0);
        resolver.process(&p2a).unwrap();
        resolver.process(&p2b).unwrap();
        resolver.flush().unwrap();

        // Collect all vehicle IDs — different protocols must not share a UUID.
        let eez_vids: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .map(|v| v.vehicle_id)
            .collect();
        let hyu_vids: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "Hyundai-Elantra")
            .map(|v| v.vehicle_id)
            .collect();

        for eid in &eez_vids {
            assert!(
                !hyu_vids.contains(eid),
                "EezTire and Hyundai-Elantra with sentinel ID must not share vehicle UUID"
            );
        }
    }

    #[test]
    fn valid_fixed_id_still_works() {
        let mut resolver = in_memory_resolver();

        // TRW sensor 0xDFEFDFFF — a valid fixed ID (3 bits cleared).
        for _ in 0..5 {
            let p = make_packet("0xDFEFDFFF", "TRW-OOK", 298, 63.8);
            resolver.process(&p).unwrap();
        }

        // All 5 sightings should map to the same vehicle via the fixed-ID path.
        assert!(
            resolver.fixed_map.contains_key(&(0xDFEFDFFF, 298)),
            "valid ID 0xDFEFDFFF must be in the fixed_map"
        );
        let vid = resolver.fixed_map[&(0xDFEFDFFF, 298)];
        let vehicle = &resolver.vehicles[&vid];
        assert_eq!(vehicle.sighting_count, 5);
        assert_eq!(vehicle.fixed_sensor_id, Some(0xDFEFDFFF));
    }

    #[test]
    fn eeztire_rolling_id_burst_resolves_to_single_vehicle() {
        // Regression for the 6-packet burst listed in the tracker issue:
        // EezTire (protocol 241) stable ~352.3 kPa with every packet reporting
        // a different bit-flipped sensor ID.  Prior behaviour was to create
        // one vehicle per packet; all six must now collapse to a single UUID.
        let mut resolver = in_memory_resolver();
        let bursts = [
            ("2025-06-01 12:00:00.000", "0xF7FFFFFF"),
            ("2025-06-01 12:00:30.000", "0xFFDFFFFF"),
            ("2025-06-01 12:01:00.000", "0xBFFFFFFF"),
            ("2025-06-01 12:01:30.000", "0xEFFFF5FF"),
            ("2025-06-01 12:02:00.000", "0x7FFFFF7F"),
            ("2025-06-01 12:02:30.000", "0x7FFEFFFE"),
        ];

        let mut vids = Vec::new();
        for (ts, sid) in bursts {
            let p = make_packet_at(ts, sid, "EezTire", 241, 352.3);
            let vid = resolver.process(&p).unwrap().expect("vid resolved");
            vids.push(vid);
        }

        let first = vids[0];
        for (i, v) in vids.iter().enumerate() {
            assert_eq!(
                *v, first,
                "packet {i} expected to resolve to the same vehicle as the first"
            );
        }

        let eez_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .count();
        assert_eq!(
            eez_count, 1,
            "all six EezTire packets must collapse into a single vehicle"
        );
        let v = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .unwrap();
        assert_eq!(v.sighting_count, 6);
        assert!(v.fixed_sensor_id.is_none());
    }

    #[test]
    fn eeztire_packet_after_expiry_creates_new_vehicle() {
        // After EezTire VEHICLE_EXPIRY (8 minutes / 480 s) of silence a new
        // EezTire packet at the same pressure should resolve to a *different*
        // vehicle UUID.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let p2 = make_packet_at(
            "2025-06-01 12:00:10.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(vid1, vid2, "packets inside the expiry window must merge");

        // > 8 minutes after last sighting, a new sighting should not merge
        // with the earlier vehicle. Last sighting was at 12:00:10.
        let p3 = make_packet_at(
            "2025-06-01 12:08:11.000",
            "0x7FFFFF7F",
            "EezTire",
            241,
            352.3,
        );
        let vid3 = resolver.process(&p3).unwrap().unwrap();
        assert_ne!(
            vid1, vid3,
            "a packet arriving after VEHICLE_EXPIRY silence must create a new vehicle"
        );
    }

    #[test]
    fn eeztire_two_sensors_at_different_pressures_do_not_merge() {
        // Two EezTire sensors with materially different pressures (352 kPa and
        // 172 kPa, both present in the real capture) must resolve to distinct
        // vehicles.
        let mut resolver = in_memory_resolver();

        let hi1 = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let lo1 = make_packet_at(
            "2025-06-01 12:00:01.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            172.4,
        );
        let hi2 = make_packet_at(
            "2025-06-01 12:00:02.000",
            "0x7FFEFFFE",
            "EezTire",
            241,
            352.9,
        );
        let lo2 = make_packet_at(
            "2025-06-01 12:00:03.000",
            "0xEFFFF5FF",
            "EezTire",
            241,
            171.7,
        );

        let vhi1 = resolver.process(&hi1).unwrap().unwrap();
        let vlo1 = resolver.process(&lo1).unwrap().unwrap();
        let vhi2 = resolver.process(&hi2).unwrap().unwrap();
        let vlo2 = resolver.process(&lo2).unwrap().unwrap();

        assert_ne!(vhi1, vlo1, "352 kPa and 172 kPa sensors must not merge");
        assert_eq!(
            vhi1, vhi2,
            "both 352 kPa packets must resolve to same vehicle"
        );
        assert_eq!(
            vlo1, vlo2,
            "both 172 kPa packets must resolve to same vehicle"
        );

        let eez_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .count();
        assert_eq!(eez_count, 2);
    }

    #[test]
    fn fixed_id_map_keyed_on_protocol_does_not_merge_across_decoders() {
        // Two sightings with identical sensor_id but different rtl433_id
        // values must resolve to distinct vehicle UUIDs via the fixed-ID
        // path. This is the defence-in-depth guard against cross-protocol
        // absorption — if two decoders happen to emit the same sensor_id,
        // keying the map on sensor_id alone would merge them. Keying on
        // (sensor_id, rtl433_id) must keep them separate.
        let mut resolver = in_memory_resolver();

        // Pick a sensor_id that passes is_valid_sensor_id (plenty of bits
        // cleared, not near the all-ones sentinel).
        let shared_id = "0x1A2B3C4D";

        let p_trw = make_packet(shared_id, "TRW-OOK", 298, 63.8);
        let p_hyu = make_packet(shared_id, "Hyundai-Elantra", 140, 255.0);

        let vid_trw = resolver.process(&p_trw).unwrap().expect("vid");
        let vid_hyu = resolver.process(&p_hyu).unwrap().expect("vid");

        assert_ne!(
            vid_trw, vid_hyu,
            "same sensor_id from two different decoders must produce distinct vehicle UUIDs"
        );

        // Both entries must be present in the fixed_map under their
        // (sensor_id, rtl433_id) keys.
        assert!(resolver.fixed_map.contains_key(&(0x1A2B3C4D, 298)));
        assert!(resolver.fixed_map.contains_key(&(0x1A2B3C4D, 140)));
        assert_eq!(resolver.vehicles.len(), 2);

        // A subsequent TRW packet with the shared ID must rejoin the TRW
        // vehicle, not flip to the Hyundai one.
        let p_trw2 = make_packet(shared_id, "TRW-OOK", 298, 63.9);
        let vid_trw2 = resolver.process(&p_trw2).unwrap().expect("vid");
        assert_eq!(vid_trw, vid_trw2);
    }

    #[test]
    fn ave_half_range_frame_does_not_split_vehicle() {
        // Regression for the AVE-TPMS dual-range encoding issue: a single
        // physical sensor occasionally emits a frame whose pressure field
        // decodes to roughly half the real pressure (~190 kPa vs ~382 kPa).
        // The sniffer decoder flags these as `pressure_kpa_reliable = false`;
        // the tracker must drop them so they do not create a distinct vehicle
        // or drift the pressure signature of the real vehicle.
        let mut resolver = in_memory_resolver();

        // Three AVE packets with rolling sensor IDs, tight timestamps so they
        // all fall inside one burst window (BURST_GAP_MS = 200ms).
        let p1 = make_packet_at(
            "2025-06-01 19:17:57.000",
            "0x12345678",
            "AVE-TPMS",
            208,
            382.5,
        );
        let p2 = make_packet_at_reliable(
            "2025-06-01 19:17:57.050",
            "0x87654321",
            "AVE-TPMS",
            208,
            190.5,
            false, // half-range artifact, flagged by the decoder
        );
        let p3 = make_packet_at(
            "2025-06-01 19:17:57.100",
            "0xDEADBEEF",
            "AVE-TPMS",
            208,
            382.5,
        );

        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();
        resolver.process(&p3).unwrap();
        resolver.flush().unwrap();

        // Exactly one AVE vehicle, whose signature has not been contaminated
        // by the 190.5 kPa half-range reading.
        let ave: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert_eq!(
            ave.len(),
            1,
            "the half-range 190.5 kPa frame must not spawn a second vehicle"
        );
        let sig0 = ave[0].pressure_signature[0];
        assert!(
            (sig0 - 382.5).abs() < 1.0,
            "pressure signature drifted from 382.5 kPa to {sig0}"
        );
    }

    #[test]
    fn ave_sequence_382_190_produces_one_vehicle_and_stable_average() {
        // Acceptance-criteria test: the sequence [382.5, 190.5, 382.5, 190.5, 382.5]
        // must produce exactly one vehicle UUID and the final stored pressure
        // average must be within 5.0 kPa of 382.5.  The 190.5 kPa packets are
        // AVE half-range artifacts (pressure_kpa_reliable = false) and must not
        // corrupt the running average.
        let mut resolver = in_memory_resolver();

        let pressures: &[(f32, bool)] = &[
            (382.5, true),
            (190.5, false),
            (382.5, true),
            (190.5, false),
            (382.5, true),
        ];

        // Use timestamps spaced > BURST_GAP_MS apart so each reliable packet
        // forms its own burst and resolves immediately.  Unreliable packets are
        // dropped before entering the burst accumulator.
        let base_ts = "2025-06-01 21:00:0";

        for (i, &(kpa, reliable)) in pressures.iter().enumerate() {
            // Construct two packets per reliable reading so each burst has the
            // minimum 2 slots required by resolve_burst.
            if reliable {
                let ts1 = format!("{}{}.000", base_ts, i);
                let ts2 = format!("{}{}.100", base_ts, i);
                let p1 = make_packet_at_reliable(
                    &ts1,
                    &format!("0x{:08X}", 0xAA000000 + i as u32 * 2),
                    "AVE-TPMS",
                    208,
                    kpa,
                    reliable,
                );
                let p2 = make_packet_at_reliable(
                    &ts2,
                    &format!("0x{:08X}", 0xAA000001 + i as u32 * 2),
                    "AVE-TPMS",
                    208,
                    kpa,
                    reliable,
                );
                resolver.process(&p1).unwrap();
                resolver.process(&p2).unwrap();
                resolver.flush().unwrap();
            } else {
                let ts = format!("{}{}.000", base_ts, i);
                let p = make_packet_at_reliable(
                    &ts,
                    &format!("0x{:08X}", 0xBB000000 + i as u32),
                    "AVE-TPMS",
                    208,
                    kpa,
                    reliable,
                );
                resolver.process(&p).unwrap();
            }
        }

        // Collect all AVE vehicles.
        let ave: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert_eq!(
            ave.len(),
            1,
            "AVE sequence [382.5, 190.5, 382.5, 190.5, 382.5] must produce exactly one vehicle"
        );

        let sig0 = ave[0].pressure_signature[0];
        assert!(
            (sig0 - 382.5).abs() < 5.0,
            "final stored pressure average ({sig0}) must be within 5.0 kPa of 382.5"
        );
    }

    #[test]
    fn per_protocol_vehicle_expiry() {
        // Documented per-protocol expiry values.
        assert_eq!(vehicle_expiry_for(208), Duration::seconds(600)); // AVE-TPMS
        assert_eq!(vehicle_expiry_for(241), Duration::seconds(480)); // EezTire
        assert_eq!(vehicle_expiry_for(298), Duration::seconds(480)); // TRW-OOK
        assert_eq!(vehicle_expiry_for(0), Duration::seconds(300)); // default
    }

    #[test]
    fn ave_burst_after_expiry_creates_new_vehicle() {
        // AVE-TPMS (protocol 208) has a 600 s expiry.  Two bursts of the same
        // pressure separated by more than 600 s must resolve to *distinct*
        // vehicle UUIDs.  This verifies that the expiry filter in resolve_burst
        // is actually evaluated (not just the function return value).
        let mut resolver = in_memory_resolver();

        // First burst: two packets within the 200 ms burst window.
        let p1a = make_packet_at(
            "2025-06-01 10:00:00.000",
            "0x11111111",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p1b = make_packet_at(
            "2025-06-01 10:00:00.100",
            "0x22222222",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p1a).unwrap();
        resolver.process(&p1b).unwrap();
        resolver.flush().unwrap();

        let vid1 = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "AVE-TPMS")
            .map(|v| v.vehicle_id)
            .expect("first burst must create a vehicle");

        // Second burst: exactly the same pressure, but more than 600 s later.
        // resolve_burst must reject the existing vehicle as stale and create a
        // new UUID instead of re-using vid1.
        let p2a = make_packet_at(
            "2025-06-01 10:10:01.000",
            "0x33333333",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p2b = make_packet_at(
            "2025-06-01 10:10:01.100",
            "0x44444444",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p2a).unwrap();
        resolver.process(&p2b).unwrap();
        resolver.flush().unwrap();

        let ave_vehicles: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert_eq!(
            ave_vehicles.len(),
            2,
            "burst after AVE expiry (600 s) must create a second vehicle"
        );
        let vid2 = ave_vehicles
            .iter()
            .find(|v| v.vehicle_id != vid1)
            .map(|v| v.vehicle_id)
            .expect("second vehicle must exist");
        assert_ne!(
            vid1, vid2,
            "the two bursts separated by >600 s must not share a vehicle UUID"
        );
    }

    #[test]
    fn ave_burst_within_expiry_reuses_vehicle() {
        // Complement of the above: two AVE bursts separated by less than 600 s
        // at matching pressure must still collapse into a single vehicle UUID.
        let mut resolver = in_memory_resolver();

        let p1a = make_packet_at(
            "2025-06-01 10:00:00.000",
            "0xAAAAAAAA",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p1b = make_packet_at(
            "2025-06-01 10:00:00.100",
            "0xBBBBBBBB",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p1a).unwrap();
        resolver.process(&p1b).unwrap();
        resolver.flush().unwrap();

        // Second burst 599 s later — still within the 600 s AVE window.
        let p2a = make_packet_at(
            "2025-06-01 10:09:59.000",
            "0xCCCCCCCC",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p2b = make_packet_at(
            "2025-06-01 10:09:59.100",
            "0xDDDDDDDD",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p2a).unwrap();
        resolver.process(&p2b).unwrap();
        resolver.flush().unwrap();

        let ave_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .count();
        assert_eq!(
            ave_count, 1,
            "two AVE bursts within the 600 s window must collapse to a single vehicle"
        );
    }

    #[test]
    fn is_valid_sensor_id_unit() {
        // Exact sentinels.
        assert!(!is_valid_sensor_id(0xFFFFFFFF));
        assert!(!is_valid_sensor_id(0x00000000));

        // Near-sentinels with a single bit cleared (decode artifacts).
        assert!(!is_valid_sensor_id(0xFFFFFFFD));
        assert!(!is_valid_sensor_id(0xFFFFFFF7));
        assert!(!is_valid_sensor_id(0xFFFFFFFB));
        assert!(!is_valid_sensor_id(0xFFFFFFBF));

        // Near-sentinel with exactly 2 bits cleared (decode artifact).
        assert!(!is_valid_sensor_id(0xFEFFFFFD));

        // Arbitrary real IDs.
        assert!(is_valid_sensor_id(0x1A2B3C4D));
        assert!(is_valid_sensor_id(1));
    }

    /// Popcount boundary coverage required by the acceptance criteria:
    /// IDs with 0, 1, 2, 3, and 4 bits cleared must return the correct
    /// valid/invalid result. The boundary sits between 2 and 3 bits cleared.
    #[test]
    fn is_valid_sensor_id_popcount_boundary() {
        // 0 bits cleared → invalid.
        assert_eq!(0xFFFFFFFFu32.count_zeros(), 0);
        assert!(!is_valid_sensor_id(0xFFFFFFFF));

        // 1 bit cleared → invalid.
        assert_eq!(0xFFFFFFFEu32.count_zeros(), 1);
        assert!(!is_valid_sensor_id(0xFFFFFFFE));

        // 2 bits cleared → invalid.
        assert_eq!(0xFFFFFFFCu32.count_zeros(), 2);
        assert!(!is_valid_sensor_id(0xFFFFFFFC));

        // 3 bits cleared → valid.
        assert_eq!(0xFFFFFFF8u32.count_zeros(), 3);
        assert!(is_valid_sensor_id(0xFFFFFFF8));

        // 4 bits cleared → valid.
        assert_eq!(0xFFFFFFF0u32.count_zeros(), 4);
        assert!(is_valid_sensor_id(0xFFFFFFF0));
    }

    /// Boundary values from issue #27 — verify every row of the acceptance
    /// table.
    #[test]
    fn is_valid_sensor_id_issue27_boundary_values() {
        // 0xFFFFFFFF → 0 zeros → false
        assert!(!is_valid_sensor_id(0xFFFFFFFF));
        // 0xFFFFFFFE → 1 zero → false
        assert!(!is_valid_sensor_id(0xFFFFFFFE));
        // 0x5FFFFFFF → 2 zeros → false
        assert_eq!(0x5FFFFFFFu32.count_zeros(), 2);
        assert!(!is_valid_sensor_id(0x5FFFFFFF));
        // 0xDFEFDFFF → 3 zeros → true (no regression)
        assert_eq!(0xDFEFDFFFu32.count_zeros(), 3);
        assert!(is_valid_sensor_id(0xDFEFDFFF));
        // 0xF77FEFFF → 3 zeros → true (no regression)
        assert_eq!(0xF77FEFFFu32.count_zeros(), 3);
        assert!(is_valid_sensor_id(0xF77FEFFF));
        // 0xFEFFFFFD → 2 zeros → false
        assert_eq!(0xFEFFFFFDu32.count_zeros(), 2);
        assert!(!is_valid_sensor_id(0xFEFFFFFD));
    }

    #[test]
    fn sentinel_trw_ook_never_resolves_to_ave_vehicle() {
        // A sentinel-rejected TRW-OOK packet (sensor_id = 0xFFFFFFFF) must
        // never be absorbed into an AVE-TPMS vehicle, regardless of pressure.
        // This is the unit test for AC #3: sentinel-rejected packet from
        // protocol A cannot match an active vehicle from protocol B.
        let mut resolver = in_memory_resolver();

        // Establish an AVE-TPMS vehicle via a rolling-ID burst at 63.8 kPa.
        let ave1 = make_packet_at(
            "2025-06-01 21:58:00.000",
            "0xA1A1A1A1",
            "AVE-TPMS",
            208,
            63.8,
        );
        let ave2 = make_packet_at(
            "2025-06-01 21:58:00.100",
            "0xB2B2B2B2",
            "AVE-TPMS",
            208,
            63.8,
        );
        resolver.process(&ave1).unwrap();
        resolver.process(&ave2).unwrap();
        resolver.flush().unwrap();

        let ave_vid = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "AVE-TPMS")
            .map(|v| v.vehicle_id)
            .expect("AVE vehicle must exist");

        // Send sentinel TRW-OOK packets at exactly the same pressure.
        // Because the sensor ID is invalid, these fall through to the
        // rolling-ID path.  With the rtl433_id filter in place, they must
        // never match the AVE vehicle.
        let trw_sentinel = make_packet_at(
            "2025-06-01 21:58:43.000",
            "0xFFFFFFFF",
            "TRW-OOK",
            298,
            63.8,
        );
        let result = resolver.process(&trw_sentinel).unwrap();

        // If it resolved to something, it must not be the AVE vehicle.
        if let Some(vid) = result {
            assert_ne!(
                vid, ave_vid,
                "sentinel TRW-OOK packet must not be absorbed into AVE-TPMS vehicle"
            );
        }

        // Also verify no AVE vehicle has been touched by the TRW packet.
        let ave_vehicle = &resolver.vehicles[&ave_vid];
        assert_eq!(
            ave_vehicle.protocol, "AVE-TPMS",
            "AVE vehicle protocol must remain AVE-TPMS"
        );
        assert_eq!(
            ave_vehicle.rtl433_id, 208,
            "AVE vehicle rtl433_id must remain 208"
        );
    }

    #[test]
    fn three_packet_trw_burst_regression() {
        // Regression test for the three-packet 21:58:42–21:58:45 burst from
        // the issue: all three TRW-OOK packets must resolve to the same
        // TRW-OOK vehicle, even when an AVE-TPMS vehicle exists at the same
        // pressure.  The sentinel packet (0xFFFFFFFF) must never resolve to
        // the AVE vehicle UUID.
        let mut resolver = in_memory_resolver();

        // Pre-seed an AVE-TPMS vehicle at 63.8 kPa.
        let ave1 = make_packet_at(
            "2025-06-01 21:57:50.000",
            "0xA1A1A1A1",
            "AVE-TPMS",
            208,
            63.8,
        );
        let ave2 = make_packet_at(
            "2025-06-01 21:57:50.100",
            "0xB2B2B2B2",
            "AVE-TPMS",
            208,
            63.8,
        );
        resolver.process(&ave1).unwrap();
        resolver.process(&ave2).unwrap();
        resolver.flush().unwrap();

        let ave_vid = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "AVE-TPMS")
            .map(|v| v.vehicle_id)
            .expect("AVE vehicle must exist");

        // Packet 1: TRW-OOK, valid fixed ID 0xDFEFDFFF (3 bits cleared).
        let p1 = make_packet_at(
            "2025-06-01 21:58:42.000",
            "0xDFEFDFFF",
            "TRW-OOK",
            298,
            63.8,
        );
        let vid1 = resolver
            .process(&p1)
            .unwrap()
            .expect("packet 1 must resolve");
        assert_ne!(vid1, ave_vid, "packet 1 must not resolve to AVE vehicle");

        // Packet 2: TRW-OOK, sentinel 0xFFFFFFFF.
        let p2 = make_packet_at(
            "2025-06-01 21:58:43.000",
            "0xFFFFFFFF",
            "TRW-OOK",
            298,
            63.8,
        );
        let result2 = resolver.process(&p2).unwrap();
        if let Some(vid2) = result2 {
            assert_ne!(
                vid2, ave_vid,
                "sentinel packet must not resolve to AVE vehicle"
            );
        }

        // Packet 3: TRW-OOK, valid fixed ID 0xDFEFDFFF (same as packet 1).
        let p3 = make_packet_at(
            "2025-06-01 21:58:45.000",
            "0xDFEFDFFF",
            "TRW-OOK",
            298,
            63.8,
        );
        let vid3 = resolver
            .process(&p3)
            .unwrap()
            .expect("packet 3 must resolve");
        assert_ne!(vid3, ave_vid, "packet 3 must not resolve to AVE vehicle");

        // Packets 1 and 3 share the same valid fixed ID — they must resolve
        // to the same TRW-OOK vehicle UUID.
        assert_eq!(
            vid1, vid3,
            "packets 1 and 3 (same fixed sensor ID) must share a vehicle UUID"
        );

        // Verify the TRW vehicle is actually a TRW-OOK vehicle.
        let trw_vehicle = &resolver.vehicles[&vid1];
        assert_eq!(trw_vehicle.protocol, "TRW-OOK");
        assert_eq!(trw_vehicle.rtl433_id, 298);
    }

    #[test]
    fn eeztire_low_battery_survives_9_minute_gap() {
        // EezTire (241) base expiry is 480 s.  With battery_ok=false, the
        // effective expiry extends by 300 s to 780 s (13 min).  A 9-minute
        // gap (540 s) must NOT create a new vehicle.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_battery(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
            false, // low battery
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();

        // 9 minutes later — within the extended 780 s window.
        let p2 = make_packet_at_battery(
            "2025-06-01 12:09:00.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            352.3,
            false,
        );
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(
            vid1, vid2,
            "EezTire with battery_ok=false must survive a 9-minute gap"
        );
    }

    #[test]
    fn eeztire_good_battery_expires_after_8_minutes() {
        // EezTire (241) base expiry is 480 s.  With battery_ok=true, there is
        // no extension.  A gap of 481 s must create a new vehicle.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_battery(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
            true, // good battery
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();

        // 8 minutes + 1 second later — just past the 480 s window.
        let p2 = make_packet_at_battery(
            "2025-06-01 12:08:01.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            352.3,
            true,
        );
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_ne!(
            vid1, vid2,
            "EezTire with battery_ok=true must expire after 8 minutes"
        );
    }

    #[test]
    fn effective_expiry_battery_extension() {
        // Verify the effective_expiry function applies the 300 s battery
        // extension correctly for various protocols.
        let make_track = |rtl433_id: u16, battery_ok: bool| -> VehicleTrack {
            VehicleTrack {
                vehicle_id: Uuid::new_v4(),
                first_seen: Utc::now(),
                last_seen: Utc::now(),
                sighting_count: 1,
                protocol: String::new(),
                rtl433_id,
                fixed_sensor_id: None,
                pressure_signature: [0.0; 4],
                make_model_hint: None,
                battery_ok,
                tx_intervals_ms: VecDeque::new(),
                tx_interval_median_ms: None,
                car_id: None,
                receiver_sightings: HashMap::new(),
                wheel_position: None,
                vehicle_class: VehicleClass::Unknown,
                fingerprint_id: None,
            }
        };

        // EezTire, good battery: 480 s
        assert_eq!(
            effective_expiry(&make_track(241, true)),
            Duration::seconds(480)
        );
        // EezTire, low battery: 480 + 300 = 780 s
        assert_eq!(
            effective_expiry(&make_track(241, false)),
            Duration::seconds(780)
        );
        // AVE-TPMS, good battery: 600 s
        assert_eq!(
            effective_expiry(&make_track(208, true)),
            Duration::seconds(600)
        );
        // AVE-TPMS, low battery: 600 + 300 = 900 s
        assert_eq!(
            effective_expiry(&make_track(208, false)),
            Duration::seconds(900)
        );
        // Default protocol, good battery: 300 s
        assert_eq!(
            effective_expiry(&make_track(0, true)),
            Duration::seconds(300)
        );
        // Default protocol, low battery: 300 + 300 = 600 s
        assert_eq!(
            effective_expiry(&make_track(0, false)),
            Duration::seconds(600)
        );
    }

    // -----------------------------------------------------------------------
    // TX-interval fingerprint tests
    // -----------------------------------------------------------------------

    #[test]
    fn compute_median_basic() {
        use crate::compute_median;

        let mut buf = VecDeque::new();
        // Not enough samples → None.
        buf.push_back(1000);
        buf.push_back(2000);
        assert_eq!(compute_median(&buf), None);

        // 3 samples → median is the middle value.
        buf.push_back(3000);
        assert_eq!(compute_median(&buf), Some(2000));

        // 4 samples → average of the two middle values.
        buf.push_back(4000);
        assert_eq!(compute_median(&buf), Some(2500));

        // 5 samples (odd).
        buf.push_back(5000);
        assert_eq!(compute_median(&buf), Some(3000));
    }

    #[test]
    fn tx_interval_accumulated_on_fingerprint_path() {
        // Feed EezTire (fingerprint path) packets at 30 s intervals and
        // verify the vehicle accumulates intervals and computes a median.
        let mut resolver = in_memory_resolver();

        let timestamps = [
            "2025-06-01 12:00:00.000",
            "2025-06-01 12:00:30.000",
            "2025-06-01 12:01:00.000",
            "2025-06-01 12:01:30.000",
            "2025-06-01 12:02:00.000",
        ];

        for ts in &timestamps {
            let p = make_packet_at(ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        let v = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .expect("EezTire vehicle must exist");

        // 5 packets → 4 intervals of 30_000 ms each.
        assert_eq!(v.tx_intervals_ms.len(), 4);
        assert_eq!(v.tx_interval_median_ms, Some(30_000));
    }

    #[test]
    fn two_ave_same_pressure_different_intervals_separate() {
        // Two simulated AVE-TPMS vehicles at identical pressure (382.5 kPa)
        // but different TX intervals (45 s vs 71 s) must resolve to separate
        // vehicle UUIDs after enough bursts to establish interval medians.
        //
        // Process bursts in chronological order, interleaving the two vehicles.
        let mut resolver = in_memory_resolver();

        // Chronological sequence of bursts (each burst = 2 packets 100 ms apart):
        // A@0s, B@5s, A@45s, B@76s, A@90s, B@147s, A@135s, B@218s
        let bursts: &[(&str, &str, &str)] = &[
            // (ts1, ts2, vehicle_label)
            ("2025-06-01 10:00:00.000", "2025-06-01 10:00:00.100", "A"),
            ("2025-06-01 10:00:05.000", "2025-06-01 10:00:05.100", "B"),
            ("2025-06-01 10:00:45.000", "2025-06-01 10:00:45.100", "A"),
            ("2025-06-01 10:01:16.000", "2025-06-01 10:01:16.100", "B"),
            ("2025-06-01 10:01:30.000", "2025-06-01 10:01:30.100", "A"),
            ("2025-06-01 10:02:27.000", "2025-06-01 10:02:27.100", "B"),
            ("2025-06-01 10:02:15.000", "2025-06-01 10:02:15.100", "A"),
            ("2025-06-01 10:03:38.000", "2025-06-01 10:03:38.100", "B"),
        ];

        // Sort by first timestamp to ensure chronological processing.
        let mut sorted: Vec<_> = bursts.to_vec();
        sorted.sort_by_key(|(ts1, _, _)| ts1.to_string());

        for (ts1, ts2, _label) in &sorted {
            let p1 = make_packet_at(ts1, "0xA1000001", "AVE-TPMS", 208, 382.5);
            let p2 = make_packet_at(ts2, "0xA1000002", "AVE-TPMS", 208, 382.5);
            resolver.process(&p1).unwrap();
            resolver.process(&p2).unwrap();
            resolver.flush().unwrap();
        }

        let ave_vehicles: Vec<_> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "AVE-TPMS")
            .collect();
        assert!(
            ave_vehicles.len() >= 2,
            "two AVE-TPMS sensors at identical pressure but different TX intervals \
             must resolve to separate vehicle UUIDs (got {} vehicles)",
            ave_vehicles.len()
        );
    }

    #[test]
    fn single_vehicle_noisy_interval_does_not_split() {
        // A single EezTire vehicle with jittery interval (30 s ± 5 s) must
        // remain a single vehicle UUID.
        let mut resolver = in_memory_resolver();

        let timestamps = [
            "2025-06-01 12:00:00.000",
            "2025-06-01 12:00:25.000", // 25 s
            "2025-06-01 12:00:55.000", // 30 s
            "2025-06-01 12:01:30.000", // 35 s
            "2025-06-01 12:01:58.000", // 28 s
            "2025-06-01 12:02:31.000", // 33 s
            "2025-06-01 12:02:56.000", // 25 s
            "2025-06-01 12:03:26.000", // 30 s
        ];

        for ts in &timestamps {
            let p = make_packet_at(ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        let eez_count = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .count();
        assert_eq!(
            eez_count, 1,
            "a single vehicle with ±5 s jitter must not be split into multiple UUIDs"
        );
    }

    #[test]
    fn interval_samples_populated_via_fingerprint_correlator_path() {
        // Regression test: EezTire packets carry near-sentinel sensor IDs
        // (zeros < 3, valid=false) and are routed through process_fingerprint,
        // resolving via the fingerprint_correlator path — never via fixed_id.
        // Before the integration fix, extract_interval was never called from
        // this path so interval_samples stayed empty regardless of how many
        // packets arrived.  This test sends a stream of EezTire packets at
        // ~22 s intervals and verifies that interval_samples accumulates rows.
        let mut resolver = in_memory_resolver();

        // 12 packets ~22 s apart.  Each consecutive gap (~22 s) falls
        // inside [MIN_PLAUSIBLE_GAP_MS, MAX_PLAUSIBLE_GAP_MS) and so
        // produces an interval_samples row.
        let timestamps = [
            "2025-06-01 12:00:00.000",
            "2025-06-01 12:00:22.000",
            "2025-06-01 12:00:44.000",
            "2025-06-01 12:01:06.000",
            "2025-06-01 12:01:28.000",
            "2025-06-01 12:01:50.000",
            "2025-06-01 12:02:12.000",
            "2025-06-01 12:02:34.000",
            "2025-06-01 12:02:56.000",
            "2025-06-01 12:03:18.000",
            "2025-06-01 12:03:40.000",
            "2025-06-01 12:04:02.000",
        ];

        for ts in &timestamps {
            // 0xF7FFFFFF: only 1 zero bit → is_valid_sensor_id() = false
            // (matches the rolling/bit-flipping behaviour of real EezTire IDs).
            let p = make_packet_at(ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        // Sanity: all packets resolved to a single EezTire vehicle that has
        // been assigned a fingerprint_id by the fingerprint_correlator path.
        let eez = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .expect("EezTire vehicle must exist");
        let fp_id = eez
            .fingerprint_id
            .clone()
            .expect("EezTire vehicle must have a fingerprint_id");

        // The actual bug: interval_samples must be populated.  Before the fix
        // this was always 0; after the fix it should be > 0 (we expect roughly
        // one row per packet beyond the first ~3 needed to establish the
        // median, so at least a few rows for 12 packets at 22 s spacing).
        let count = resolver.db.interval_sample_count(&fp_id).unwrap();
        assert!(
            count > 0,
            "interval_samples must be populated for EezTire fingerprint_correlator \
             matches (path=fingerprint_correlator, valid=false), but count={count} \
             for fingerprint_id={fp_id}"
        );
    }

    #[test]
    fn raw_interval_pipeline_yields_per_transmission_samples() {
        // Issue #42: a sensor that transmits every 22 s should produce one
        // interval_samples row per inter-packet gap, not one per correlator
        // match.  100 packets at 22 s spacing must produce 99 samples whose
        // mean is ≈ 22 000 ms with sub-100 ms sigma (the input is exactly
        // periodic, so the only spread comes from rounding at the ms level).
        use crate::jitter::compute_jitter_profile;

        let mut resolver = in_memory_resolver();

        // 100 packets at exactly 22 s spacing, same near-sentinel sensor_id
        // (matches the production EezTire scenario from issue #42).  Routes
        // through process_fingerprint, which creates a fingerprint on the
        // first packet — every subsequent packet reuses it.
        let base = chrono::NaiveDateTime::parse_from_str(
            "2026-04-26 12:00:00.000",
            "%Y-%m-%d %H:%M:%S%.3f",
        )
        .unwrap();
        for i in 0..100 {
            let ts = base + chrono::Duration::milliseconds(i * 22_000);
            let timestamp = ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
            let p = make_packet_at(&timestamp, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        let veh = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .expect("EezTire vehicle must exist");
        let fp_id = veh
            .fingerprint_id
            .clone()
            .expect("vehicle must have a fingerprint_id");

        let count = resolver.db.interval_sample_count(&fp_id).unwrap();
        assert_eq!(
            count, 99,
            "100 packets at 22 s spacing must produce 99 interval samples"
        );

        let samples = resolver
            .db
            .get_interval_samples(&fp_id, jitter::MAX_INTERVAL_SAMPLES)
            .unwrap();
        // Every sample is exactly 22_000 ms (timestamps are millisecond-
        // accurate and evenly spaced), so the profile must be tightly
        // clustered around the mean with σ well under 100 ms.
        let profile =
            compute_jitter_profile(&samples).expect("profile should compute from 99 samples");
        assert!(
            profile.sigma_ms < 100.0,
            "sigma_ms = {} must be < 100 ms for an evenly-spaced stream",
            profile.sigma_ms
        );
    }

    #[test]
    fn raw_interval_buffer_flushes_after_first_resolution() {
        // Each subsequent packet from the same sensor must produce exactly
        // one interval_samples row — the buffer is flushed in the same
        // `process()` call that pushed the interval.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at(
            "2026-04-26 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let p2 = make_packet_at(
            "2026-04-26 12:00:22.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let p3 = make_packet_at(
            "2026-04-26 12:00:44.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );

        resolver.process(&p1).unwrap();
        let veh = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .expect("EezTire vehicle must exist");
        let fp_id = veh.fingerprint_id.clone().unwrap();

        // After packet 1: no interval recorded (first observation only sets
        // the tracker baseline).
        assert_eq!(resolver.db.interval_sample_count(&fp_id).unwrap(), 0);

        resolver.process(&p2).unwrap();
        // After packet 2: one interval (22_000 ms) flushed under fp_id.
        assert_eq!(resolver.db.interval_sample_count(&fp_id).unwrap(), 1);

        resolver.process(&p3).unwrap();
        assert_eq!(resolver.db.interval_sample_count(&fp_id).unwrap(), 2);
    }

    #[test]
    fn raw_interval_burst_duplicate_rejected() {
        // Two packets 200 ms apart from the same sensor — well below
        // RAW_INTERVAL_MIN_MS — must not create an interval sample.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at(
            "2026-04-26 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let p2 = make_packet_at(
            "2026-04-26 12:00:00.200",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );

        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();

        let veh = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .expect("EezTire vehicle must exist");
        let fp_id = veh.fingerprint_id.clone().unwrap();
        assert_eq!(
            resolver.db.interval_sample_count(&fp_id).unwrap(),
            0,
            "burst-duplicate gaps must not produce interval samples"
        );
    }

    #[test]
    fn interval_check_skipped_when_insufficient_samples() {
        // When a vehicle has fewer than TX_INTERVAL_MIN_SAMPLES intervals, the
        // interval check must be skipped and only pressure matching is used.
        // Two packets 1 s apart at the same pressure must merge even though
        // the vehicle has only 1 interval sample (below MIN_SAMPLES).
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let p2 = make_packet_at(
            "2025-06-01 12:00:01.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(
            vid1, vid2,
            "with < MIN_SAMPLES intervals, pressure match alone must be sufficient"
        );

        // Vehicle should have 1 interval sample and no usable median yet.
        let v = &resolver.vehicles[&vid1];
        assert_eq!(v.tx_intervals_ms.len(), 1);
        assert_eq!(v.tx_interval_median_ms, None);
    }

    #[test]
    fn tx_interval_ring_buffer_capped() {
        // Verify the ring buffer never exceeds TX_INTERVAL_WINDOW.
        let mut resolver = in_memory_resolver();

        // Send 12 EezTire packets 30 s apart → 11 intervals.
        for i in 0..12 {
            let ts = format!("2025-06-01 12:{:02}:{:02}.000", i / 2, (i % 2) * 30);
            let p = make_packet_at(&ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        let v = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .unwrap();
        assert!(
            v.tx_intervals_ms.len() <= crate::TX_INTERVAL_WINDOW,
            "ring buffer must not exceed TX_INTERVAL_WINDOW ({}), got {}",
            crate::TX_INTERVAL_WINDOW,
            v.tx_intervals_ms.len()
        );
    }

    /// Helper: create a packet with a specific receiver_id.
    fn make_packet_at_receiver(
        timestamp: &str,
        sensor_id: &str,
        protocol: &str,
        rtl433_id: u16,
        pressure_kpa: f32,
        receiver_id: &str,
    ) -> TpmsPacket {
        let mut p = make_packet_at(timestamp, sensor_id, protocol, rtl433_id, pressure_kpa);
        p.receiver_id = receiver_id.to_string();
        p
    }

    #[test]
    fn cross_receiver_duplicate_does_not_increment_tx_interval() {
        // AC: duplicate sighting from second receiver within window does not
        // increment tx_intervals_ms ring buffer.
        let mut resolver = in_memory_resolver();

        // First sighting from node-01.
        let p1 = make_packet_at_receiver(
            "2025-06-01 12:00:00.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-01",
        );
        let vid1 = resolver.process(&p1).unwrap().unwrap();

        let v = &resolver.vehicles[&vid1];
        let intervals_before = v.tx_intervals_ms.len();

        // Duplicate sighting from node-02 within the 5-second window (2 s later).
        let p2 = make_packet_at_receiver(
            "2025-06-01 12:00:02.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-02",
        );
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(
            vid1, vid2,
            "same vehicle from different receiver within window"
        );

        let v = &resolver.vehicles[&vid2];
        assert_eq!(
            v.tx_intervals_ms.len(),
            intervals_before,
            "cross-receiver duplicate must not add to tx_intervals_ms"
        );
    }

    #[test]
    fn cross_receiver_duplicate_does_not_increment_sighting_count() {
        // Cross-receiver duplicate should not increment sighting_count.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_receiver(
            "2025-06-01 12:00:00.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-01",
        );
        let vid = resolver.process(&p1).unwrap().unwrap();
        let count_after_first = resolver.vehicles[&vid].sighting_count;

        let p2 = make_packet_at_receiver(
            "2025-06-01 12:00:02.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-02",
        );
        resolver.process(&p2).unwrap();
        assert_eq!(
            resolver.vehicles[&vid].sighting_count, count_after_first,
            "cross-receiver duplicate must not increment sighting_count"
        );
    }

    #[test]
    fn two_receiver_fixture_produces_one_vehicle() {
        // AC: replay of a two-receiver fixture (same vehicle appearing on
        // both nodes within 2 seconds) produces one vehicle UUID, not two.
        let mut resolver = in_memory_resolver();

        let p_node1 = make_packet_at_receiver(
            "2025-06-01 14:00:00.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-01",
        );
        let p_node2 = make_packet_at_receiver(
            "2025-06-01 14:00:02.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-02",
        );

        let vid1 = resolver.process(&p_node1).unwrap().unwrap();
        let vid2 = resolver.process(&p_node2).unwrap().unwrap();

        assert_eq!(
            vid1, vid2,
            "same vehicle from two receivers within 2 s must produce one UUID"
        );
        assert_eq!(resolver.vehicles.len(), 1, "only one vehicle should exist");
    }

    #[test]
    fn receiver_sightings_tracks_both_receivers() {
        // AC: receiver_sightings correctly records which receivers saw the
        // vehicle and when.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_receiver(
            "2025-06-01 14:00:00.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-01",
        );
        let p2 = make_packet_at_receiver(
            "2025-06-01 14:00:02.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-02",
        );

        let vid = resolver.process(&p1).unwrap().unwrap();
        resolver.process(&p2).unwrap();

        let v = &resolver.vehicles[&vid];
        assert!(
            v.receiver_sightings.contains_key("node-01"),
            "receiver_sightings must contain node-01"
        );
        assert!(
            v.receiver_sightings.contains_key("node-02"),
            "receiver_sightings must contain node-02"
        );
        assert_eq!(v.receiver_sightings["node-01"].len(), 1);
        assert_eq!(v.receiver_sightings["node-02"].len(), 1);
    }

    #[test]
    fn same_receiver_outside_window_still_counts() {
        // When a second sighting arrives from the SAME receiver, it should
        // not be treated as a cross-receiver duplicate, even if within the
        // 5 s window.
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_receiver(
            "2025-06-01 12:00:00.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-01",
        );
        let vid = resolver.process(&p1).unwrap().unwrap();
        let count1 = resolver.vehicles[&vid].sighting_count;

        // Same receiver, 2 s later — should NOT be a cross-receiver duplicate.
        let p2 = make_packet_at_receiver(
            "2025-06-01 12:00:02.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-01",
        );
        resolver.process(&p2).unwrap();
        assert!(
            resolver.vehicles[&vid].sighting_count > count1,
            "sighting from the same receiver must still increment sighting_count"
        );
    }

    #[test]
    fn cross_receiver_outside_window_counts_normally() {
        // A sighting from a different receiver that arrives OUTSIDE the 5 s
        // window should be treated as a normal sighting (not a duplicate).
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_receiver(
            "2025-06-01 12:00:00.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-01",
        );
        let vid = resolver.process(&p1).unwrap().unwrap();
        let count1 = resolver.vehicles[&vid].sighting_count;

        // Different receiver, 10 s later — outside the 5 s window.
        let p2 = make_packet_at_receiver(
            "2025-06-01 12:00:10.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
            "node-02",
        );
        resolver.process(&p2).unwrap();
        assert!(
            resolver.vehicles[&vid].sighting_count > count1,
            "cross-receiver sighting outside window must increment sighting_count"
        );
    }

    #[test]
    fn fingerprint_path_cross_receiver_dedup() {
        // Cross-receiver deduplication also works on the fingerprint path
        // (e.g. EezTire).
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at_receiver(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
            "node-01",
        );
        let vid = resolver.process(&p1).unwrap().unwrap();
        let count1 = resolver.vehicles[&vid].sighting_count;
        let intervals1 = resolver.vehicles[&vid].tx_intervals_ms.len();

        // Same vehicle from a different receiver within the window.
        let p2 = make_packet_at_receiver(
            "2025-06-01 12:00:03.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            352.3,
            "node-02",
        );
        let vid2 = resolver.process(&p2).unwrap().unwrap();
        assert_eq!(vid, vid2, "same EezTire vehicle from different receiver");

        let v = &resolver.vehicles[&vid2];
        assert_eq!(
            v.sighting_count, count1,
            "cross-receiver EezTire duplicate must not increment sighting_count"
        );
        assert_eq!(
            v.tx_intervals_ms.len(),
            intervals1,
            "cross-receiver EezTire duplicate must not add to tx_intervals_ms"
        );
    }

    #[test]
    fn receiver_id_populates_from_packet() {
        // The receiver_id from the TpmsPacket should flow through to the
        // Sighting and into the vehicle's receiver_sightings.
        let db = Database::open(":memory:").unwrap();
        let mut resolver = Resolver::with_receiver_id(db, "central".to_string()).unwrap();

        let mut p = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0x1A2B3C4D",
            "TRW-OOK",
            298,
            63.8,
        );
        p.receiver_id = "node-42".to_string();

        let vid = resolver.process(&p).unwrap().unwrap();
        let v = &resolver.vehicles[&vid];
        assert!(
            v.receiver_sightings.contains_key("node-42"),
            "receiver_sightings must reflect the packet's receiver_id"
        );
    }

    /// Helper: send several sightings of the same fixed-ID sensor across enough
    /// windows to trigger the Jaccard grouper, then return the set of distinct
    /// car_id values assigned to the vehicle across all evaluations.
    fn collect_car_ids_for_repeated_sightings(count: usize) -> (Resolver, Vec<Option<Uuid>>) {
        let db = Database::open(":memory:").unwrap();
        let mut resolver = Resolver::new(db).unwrap();
        let mut car_ids: Vec<Option<Uuid>> = Vec::new();

        // Use a TRW sensor (fixed-ID protocol 298) so it gets a stable
        // vehicle UUID and enters the co-occurrence matrix.
        for i in 0..count {
            // Space packets 65 s apart so each one falls in a new window
            // (WINDOW_SIZE_S = 60).  After MIN_WINDOWS (3) windows the grouper
            // runs on every subsequent advance.
            let ts = format!("2025-06-01 12:{:02}:{:02}.000", i * 65 / 60, (i * 65) % 60);
            let p = make_packet_at(&ts, "0x1A2B3C01", "TRW-OOK", 298, 230.0);
            resolver.process(&p).unwrap();

            // Snapshot the car_id after each sighting.
            let vid = resolver
                .vehicles
                .values()
                .find(|v| v.fixed_sensor_id == Some(0x1A2B3C01))
                .expect("vehicle should exist");
            car_ids.push(vid.car_id);
        }
        resolver.flush().unwrap();
        (resolver, car_ids)
    }

    #[test]
    fn car_id_stable_across_repeated_sightings() {
        // Acceptance criterion: 10 sightings of the same vehicle produce
        // exactly 1 distinct car_id value (after the initial `None` period
        // before enough windows accumulate).
        let (_resolver, car_ids) = collect_car_ids_for_repeated_sightings(10);
        let assigned: std::collections::HashSet<Uuid> = car_ids.iter().filter_map(|c| *c).collect();
        assert!(
            assigned.len() <= 1,
            "expected at most 1 distinct car_id, got {}: {:?}",
            assigned.len(),
            assigned
        );
    }

    #[test]
    fn car_id_survives_tracker_restart() {
        // First session: send enough packets to trigger grouping and get a
        // car_id assigned.
        let tmp = std::env::temp_dir().join("tpms_test_restart.db");
        // Ensure clean state.
        let _ = std::fs::remove_file(&tmp);

        let car_id_first_session;
        let vehicle_id;
        {
            let db = Database::open(tmp.to_str().unwrap()).unwrap();
            let mut resolver = Resolver::new(db).unwrap();
            for i in 0..10 {
                let ts = format!("2025-06-01 12:{:02}:{:02}.000", i * 65 / 60, (i * 65) % 60);
                let p = make_packet_at(&ts, "0x1A2B3C02", "TRW-OOK", 298, 230.0);
                resolver.process(&p).unwrap();
            }
            resolver.flush().unwrap();

            let v = resolver
                .vehicles
                .values()
                .find(|v| v.fixed_sensor_id == Some(0x1A2B3C02))
                .expect("vehicle should exist");
            car_id_first_session = v.car_id;
            vehicle_id = v.vehicle_id;
        }

        // Second session: reopen the same database.  The vehicle should still
        // have the same car_id without needing any new packets.
        {
            let db = Database::open(tmp.to_str().unwrap()).unwrap();
            let resolver = Resolver::new(db).unwrap();
            let v = resolver
                .vehicles
                .get(&vehicle_id)
                .expect("vehicle should be restored from DB");
            assert_eq!(
                v.car_id, car_id_first_session,
                "car_id must survive tracker restart"
            );
            // Also verify the vehicle_to_car index was restored.
            if let Some(car_id) = car_id_first_session {
                assert_eq!(
                    resolver.vehicle_to_car.get(&vehicle_id).copied(),
                    Some(car_id),
                    "vehicle_to_car index must be restored from DB"
                );
            }
        }

        let _ = std::fs::remove_file(&tmp);
    }

    // -----------------------------------------------------------------------
    // Cross-protocol vehicle merge regression (issue #20)
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_cross_protocol_merge() {
        // Regression test for issue #20: TRW-OOK near-sentinel at 62.8 kPa
        // and AVE-TPMS near-sentinel at 382.5 kPa must never share a vehicle
        // UUID.  Both sensor IDs have only 1 bit cleared and should be
        // rejected from the fixed-ID path by the popcount sentinel check.
        let mut resolver = in_memory_resolver();

        // TRW-OOK near-sentinel burst at 62.8 kPa.
        let trw1 = make_packet_at(
            "2026-04-15 08:28:41.000",
            "0xFFFFFFFB",
            "TRW-OOK",
            298,
            62.8,
        );
        let trw2 = make_packet_at(
            "2026-04-15 08:28:41.100",
            "0xFFFFFFFB",
            "TRW-OOK",
            298,
            62.9,
        );
        resolver.process(&trw1).unwrap();
        resolver.process(&trw2).unwrap();
        resolver.flush().unwrap();

        let trw_vids: Vec<Uuid> = resolver
            .vehicles
            .values()
            .filter(|v| v.rtl433_id == 298)
            .map(|v| v.vehicle_id)
            .collect();

        // AVE-TPMS near-sentinel burst at 382.5 kPa — must NOT merge with
        // the TRW vehicle.
        let ave1 = make_packet_at(
            "2026-04-15 08:33:43.000",
            "0xFFFFFFF7",
            "AVE-TPMS",
            208,
            382.5,
        );
        let ave2 = make_packet_at(
            "2026-04-15 08:33:43.100",
            "0xFFFFBFFF",
            "AVE-TPMS",
            208,
            382.5,
        );
        resolver.process(&ave1).unwrap();
        resolver.process(&ave2).unwrap();
        resolver.flush().unwrap();

        let ave_vids: Vec<Uuid> = resolver
            .vehicles
            .values()
            .filter(|v| v.rtl433_id == 208)
            .map(|v| v.vehicle_id)
            .collect();

        // TRW and AVE vehicles must be completely disjoint.
        for trw_vid in &trw_vids {
            assert!(
                !ave_vids.contains(trw_vid),
                "TRW-OOK and AVE-TPMS must never share a vehicle UUID"
            );
        }

        // No vehicle should span both protocols.
        for v in resolver.vehicles.values() {
            assert!(
                v.rtl433_id == 298 || v.rtl433_id == 208 || v.rtl433_id == 0,
                "unexpected rtl433_id: {}",
                v.rtl433_id
            );
        }

        // Near-sentinel IDs must not appear in the fixed_map.
        assert!(
            !resolver.fixed_map.contains_key(&(0xFFFFFFFB, 298)),
            "near-sentinel 0xFFFFFFFB must not be in fixed_map"
        );
        assert!(
            !resolver.fixed_map.contains_key(&(0xFFFFFFF7, 208)),
            "near-sentinel 0xFFFFFFF7 must not be in fixed_map"
        );
    }

    #[test]
    fn db_integrity_no_vehicle_spans_multiple_rtl433_ids() {
        // Acceptance criterion: no vehicle UUID in the database contains
        // sightings from more than one rtl433_id value.  We verify this
        // through the in-memory vehicles map, which is authoritative.
        let mut resolver = in_memory_resolver();

        // Inject a mix of protocols and sentinel IDs.
        let packets = [
            make_packet_at(
                "2026-04-15 10:00:00.000",
                "0xFFFFFFFB",
                "TRW-OOK",
                298,
                62.8,
            ),
            make_packet_at(
                "2026-04-15 10:00:00.100",
                "0xFFFFFFFB",
                "TRW-OOK",
                298,
                62.9,
            ),
            make_packet_at(
                "2026-04-15 10:00:05.000",
                "0xFFFFFFF7",
                "AVE-TPMS",
                208,
                382.5,
            ),
            make_packet_at(
                "2026-04-15 10:00:05.100",
                "0xFFFFBFFF",
                "AVE-TPMS",
                208,
                382.5,
            ),
            make_packet_at(
                "2026-04-15 10:00:10.000",
                "0x1A2B3C4D",
                "TRW-OOK",
                298,
                63.0,
            ),
            make_packet_at(
                "2026-04-15 10:00:15.000",
                "0x1A2B3C4D",
                "TRW-OOK",
                298,
                63.1,
            ),
        ];
        for p in &packets {
            resolver.process(p).unwrap();
        }
        resolver.flush().unwrap();

        // Build a map from vehicle_id to the set of rtl433_ids seen.
        // Each vehicle must contain exactly one rtl433_id.
        let mut vid_to_protocols: HashMap<Uuid, HashSet<u16>> = HashMap::new();
        for v in resolver.vehicles.values() {
            vid_to_protocols
                .entry(v.vehicle_id)
                .or_default()
                .insert(v.rtl433_id);
        }

        for (vid, protocols) in &vid_to_protocols {
            assert!(
                protocols.len() <= 1,
                "vehicle {} has sightings from {} distinct rtl433_ids ({:?}) — expected at most 1",
                vid,
                protocols.len(),
                protocols,
            );
        }
    }

    #[test]
    fn sentinel_not_restored_to_fixed_map_from_db() {
        // Verify that near-sentinel fixed_sensor_ids persisted in a previous
        // session are not restored into fixed_map on restart.
        let tmp = format!("/tmp/tpms_test_sentinel_restore_{}.sqlite", Uuid::new_v4());

        // Session 1: force-persist a vehicle with a near-sentinel fixed_sensor_id.
        {
            let db = Database::open(&tmp).unwrap();
            let mut resolver = Resolver::new(db).unwrap();

            // Use a valid sensor ID to create a vehicle via the fixed-ID path.
            let p = make_packet_at(
                "2026-04-15 10:00:00.000",
                "0xFEFFFFFD",
                "TRW-OOK",
                298,
                63.0,
            );
            resolver.process(&p).unwrap();
            resolver.flush().unwrap();
        }

        // Corrupt the DB externally: change the fixed_sensor_id to a
        // near-sentinel value using a separate connection.
        {
            let conn = rusqlite::Connection::open(&tmp).unwrap();
            conn.execute(
                "UPDATE vehicles SET sensor_id = ?1 WHERE sensor_id = ?2",
                rusqlite::params![0xFFFFFFFBi64, 0xFEFFFFFDi64],
            )
            .unwrap();
        }

        // Session 2: reopen and verify the near-sentinel is NOT in fixed_map.
        {
            let db = Database::open(&tmp).unwrap();
            let resolver = Resolver::new(db).unwrap();

            assert!(
                !resolver.fixed_map.keys().any(|(sid, _)| *sid == 0xFFFFFFFB),
                "near-sentinel 0xFFFFFFFB must not be restored into fixed_map from DB"
            );
        }

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn protocol_filter_is_first_in_fingerprint_correlator() {
        // Verify that the protocol filter (rtl433_id) in process_fingerprint
        // prevents cross-protocol matches even when pressure overlaps.
        let mut resolver = in_memory_resolver();

        // Create an EezTire vehicle at 352.3 kPa via the fingerprint path.
        let eez1 = make_packet_at(
            "2026-04-15 10:00:00.000",
            "0xF7FFFFFF",
            "EezTire/Carchet/TST-507",
            241,
            352.3,
        );
        let eez2 = make_packet_at(
            "2026-04-15 10:00:30.000",
            "0xFFDFFFFF",
            "EezTire/Carchet/TST-507",
            241,
            352.3,
        );
        resolver.process(&eez1).unwrap();
        resolver.process(&eez2).unwrap();

        let eez_vid = resolver
            .vehicles
            .values()
            .find(|v| v.rtl433_id == 241)
            .map(|v| v.vehicle_id);

        // Now send a different-protocol packet at the same pressure.
        // This would match the EezTire vehicle on pressure alone, but the
        // rtl433_id filter must prevent the match.
        let other = make_packet_at(
            "2026-04-15 10:00:35.000",
            "0xFFBFFFFF",
            "SomeOther",
            999,
            352.3,
        );
        // This goes to process_rolling (invalid sensor ID), which won't match
        // the EezTire vehicle because rtl433_id differs.
        resolver.process(&other).unwrap();
        resolver.flush().unwrap();

        // The EezTire vehicle must not have been modified by the other packet.
        if let Some(vid) = eez_vid {
            let v = &resolver.vehicles[&vid];
            assert_eq!(
                v.rtl433_id, 241,
                "EezTire vehicle rtl433_id must remain 241"
            );
        }
    }

    #[test]
    fn run_grouping_merges_cars_when_vehicles_cooccur() {
        // Set up two vehicles that initially have separate car_ids,
        // then simulate high co-occurrence so run_grouping merges them.
        let mut resolver = in_memory_resolver();

        let v1 = Uuid::new_v4();
        let v2 = Uuid::new_v4();
        let car_a = Uuid::new_v4();
        let car_b = Uuid::new_v4();

        let now = Utc::now();

        // Create car records first (before vehicles that reference them).
        resolver
            .db
            .upsert_car(car_a, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();
        resolver
            .db
            .upsert_car(car_b, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();

        let track1 = VehicleTrack {
            vehicle_id: v1,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_a),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };
        let track2 = VehicleTrack {
            vehicle_id: v2,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_b),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };

        resolver.db.upsert_vehicle(&track1).unwrap();
        resolver.db.upsert_vehicle(&track2).unwrap();
        resolver.db.set_vehicle_car_id(v1, car_a).unwrap();
        resolver.db.set_vehicle_car_id(v2, car_b).unwrap();

        resolver.vehicles.insert(v1, track1);
        resolver.vehicles.insert(v2, track2);
        resolver.vehicle_to_car.insert(v1, car_a);
        resolver.vehicle_to_car.insert(v2, car_b);

        // Build co-occurrence: both appear together in 5/6 windows (≈83%).
        // Keep within N_WINDOWS to avoid eviction.
        for i in 0..6 {
            resolver.cooccurrence.record(v1);
            if i < 5 {
                resolver.cooccurrence.record(v2);
            }
            resolver.cooccurrence.advance_window();
        }

        // Run grouping — should detect a merge event.
        resolver.run_grouping().unwrap();

        // Both vehicles must now share the same car_id.
        let cid1 = resolver.vehicle_to_car[&v1];
        let cid2 = resolver.vehicle_to_car[&v2];
        assert_eq!(
            cid1, cid2,
            "vehicles with high co-occurrence should share a car_id after merge"
        );

        // The discarded car should be removed from the database.
        let all_cars = resolver.db.all_car_ids().unwrap();
        let remaining: Vec<&String> = all_cars
            .iter()
            .filter(|c| *c == &car_a.to_string() || *c == &car_b.to_string())
            .collect();
        assert_eq!(
            remaining.len(),
            1,
            "only one of the two original car_ids should remain, got {remaining:?}"
        );
    }

    #[test]
    fn run_grouping_does_not_merge_low_cooccurrence() {
        // Two vehicles with low co-occurrence should remain in separate cars.
        let mut resolver = in_memory_resolver();

        let v1 = Uuid::new_v4();
        let v2 = Uuid::new_v4();
        let car_a = Uuid::new_v4();
        let car_b = Uuid::new_v4();

        let now = Utc::now();

        // Create car records first (before vehicles that reference them).
        resolver
            .db
            .upsert_car(car_a, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();
        resolver
            .db
            .upsert_car(car_b, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();

        let track1 = VehicleTrack {
            vehicle_id: v1,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_a),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };
        let track2 = VehicleTrack {
            vehicle_id: v2,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_b),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };

        resolver.db.upsert_vehicle(&track1).unwrap();
        resolver.db.upsert_vehicle(&track2).unwrap();
        resolver.db.set_vehicle_car_id(v1, car_a).unwrap();
        resolver.db.set_vehicle_car_id(v2, car_b).unwrap();

        resolver.vehicles.insert(v1, track1);
        resolver.vehicles.insert(v2, track2);
        resolver.vehicle_to_car.insert(v1, car_a);
        resolver.vehicle_to_car.insert(v2, car_b);

        // Build co-occurrence: only 1 overlap in 5 windows, then b1 alone.
        // Total 8 advances, within N_WINDOWS (no eviction).
        for i in 0..5 {
            resolver.cooccurrence.record(v1);
            if i == 0 {
                resolver.cooccurrence.record(v2);
            }
            resolver.cooccurrence.advance_window();
        }
        for _ in 0..3 {
            resolver.cooccurrence.record(v2);
            resolver.cooccurrence.advance_window();
        }

        resolver.run_grouping().unwrap();

        // They should end up with different car_ids (not merged).
        let cid1 = resolver.vehicle_to_car[&v1];
        let cid2 = resolver.vehicle_to_car[&v2];
        assert_ne!(
            cid1, cid2,
            "vehicles with low co-occurrence should NOT share a car_id"
        );
    }

    #[test]
    fn merge_updates_all_vehicle_to_car_entries() {
        // Regression test for Bug B: a third vehicle assigned to the discarded
        // car_id but NOT a member of the merged group must still have its
        // vehicle_to_car entry remapped after the merge.
        let mut resolver = in_memory_resolver();

        let v1 = Uuid::new_v4();
        let v2 = Uuid::new_v4();
        let v3 = Uuid::new_v4(); // third-party vehicle, same car as v2
        let car_a = Uuid::new_v4();
        let car_b = Uuid::new_v4();

        let now = Utc::now();

        resolver
            .db
            .upsert_car(car_a, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();
        resolver
            .db
            .upsert_car(car_b, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();

        let track1 = VehicleTrack {
            vehicle_id: v1,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_a),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };
        let track2 = VehicleTrack {
            vehicle_id: v2,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_b),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };
        // v3 also points to car_b but will NOT be part of the merged group
        // (it has no co-occurrence data so it ends up in its own group).
        let track3 = VehicleTrack {
            vehicle_id: v3,
            first_seen: now,
            last_seen: now,
            sighting_count: 1,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [90.0, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_b),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };

        for track in [&track1, &track2, &track3] {
            resolver.db.upsert_vehicle(track).unwrap();
        }
        resolver.db.set_vehicle_car_id(v1, car_a).unwrap();
        resolver.db.set_vehicle_car_id(v2, car_b).unwrap();
        resolver.db.set_vehicle_car_id(v3, car_b).unwrap();

        resolver.vehicles.insert(v1, track1);
        resolver.vehicles.insert(v2, track2);
        resolver.vehicles.insert(v3, track3);
        resolver.vehicle_to_car.insert(v1, car_a);
        resolver.vehicle_to_car.insert(v2, car_b);
        resolver.vehicle_to_car.insert(v3, car_b);

        // v1 and v2 co-occur strongly; v3 does not co-occur with either.
        for i in 0..6 {
            resolver.cooccurrence.record(v1);
            if i < 5 {
                resolver.cooccurrence.record(v2);
            }
            resolver.cooccurrence.advance_window();
        }

        resolver.run_grouping().unwrap();

        // v1 and v2 must share the same car_id after merge.
        let cid1 = resolver.vehicle_to_car[&v1];
        let cid2 = resolver.vehicle_to_car[&v2];
        assert_eq!(cid1, cid2, "v1 and v2 must share a car_id after merge");

        // Bug B check: v3 must also point to the surviving car_id even though
        // it was not a member of the merged group.
        let cid3 = resolver.vehicle_to_car[&v3];
        assert_eq!(
            cid3, cid1,
            "v3 must be remapped to the surviving car_id, got {cid3} instead of {cid1}"
        );

        // The in-memory VehicleTrack.car_id must also be updated.
        assert_eq!(
            resolver.vehicles[&v3].car_id,
            Some(cid1),
            "VehicleTrack.car_id for v3 must be updated after merge"
        );

        // No vehicle in the DB should reference a non-existent car.
        let all_cars = resolver.db.all_car_ids().unwrap();
        let car_set: HashSet<String> = all_cars.into_iter().collect();
        for v in resolver.vehicles.values() {
            if let Some(cid) = v.car_id {
                assert!(
                    car_set.contains(&cid.to_string()),
                    "vehicle {} points to car_id {} which does not exist in cars table",
                    v.vehicle_id,
                    cid
                );
            }
        }
    }

    #[test]
    fn merge_is_idempotent_across_grouping_passes() {
        // Regression test for Bug A: calling run_grouping twice must not
        // re-trigger the merge for the same pair.
        let mut resolver = in_memory_resolver();

        let v1 = Uuid::new_v4();
        let v2 = Uuid::new_v4();
        let car_a = Uuid::new_v4();
        let car_b = Uuid::new_v4();

        let now = Utc::now();

        resolver
            .db
            .upsert_car(car_a, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();
        resolver
            .db
            .upsert_car(car_b, &now.to_rfc3339(), &now.to_rfc3339(), 1, None)
            .unwrap();

        let track1 = VehicleTrack {
            vehicle_id: v1,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_a),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };
        let track2 = VehicleTrack {
            vehicle_id: v2,
            first_seen: now,
            last_seen: now,
            sighting_count: 5,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [351.6, 0.0, 0.0, 0.0],
            make_model_hint: None,
            battery_ok: true,
            tx_intervals_ms: VecDeque::new(),
            tx_interval_median_ms: None,
            car_id: Some(car_b),
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class: VehicleClass::Unknown,
            fingerprint_id: None,
        };

        resolver.db.upsert_vehicle(&track1).unwrap();
        resolver.db.upsert_vehicle(&track2).unwrap();
        resolver.db.set_vehicle_car_id(v1, car_a).unwrap();
        resolver.db.set_vehicle_car_id(v2, car_b).unwrap();

        resolver.vehicles.insert(v1, track1);
        resolver.vehicles.insert(v2, track2);
        resolver.vehicle_to_car.insert(v1, car_a);
        resolver.vehicle_to_car.insert(v2, car_b);

        // High co-occurrence to trigger a merge.
        for i in 0..6 {
            resolver.cooccurrence.record(v1);
            if i < 5 {
                resolver.cooccurrence.record(v2);
            }
            resolver.cooccurrence.advance_window();
        }

        // First pass — triggers the merge.
        resolver.run_grouping().unwrap();

        let cid1_after_first = resolver.vehicle_to_car[&v1];
        let cid2_after_first = resolver.vehicle_to_car[&v2];
        assert_eq!(
            cid1_after_first, cid2_after_first,
            "vehicles must share a car_id after first merge"
        );

        let cars_after_first = resolver.db.all_car_ids().unwrap();

        // Second pass — must NOT re-trigger the merge.
        resolver.run_grouping().unwrap();

        let cid1_after_second = resolver.vehicle_to_car[&v1];
        let cid2_after_second = resolver.vehicle_to_car[&v2];
        assert_eq!(
            cid1_after_second, cid2_after_second,
            "vehicles must still share a car_id after second pass"
        );
        assert_eq!(
            cid1_after_first, cid1_after_second,
            "car_id must be stable across grouping passes"
        );

        // The set of cars must be identical — no car was recreated or removed.
        let cars_after_second = resolver.db.all_car_ids().unwrap();
        assert_eq!(
            cars_after_first, cars_after_second,
            "car set must be stable across grouping passes (no duplicate merge)"
        );

        // Only one of the original car_ids should remain.
        let remaining: Vec<&String> = cars_after_second
            .iter()
            .filter(|c| *c == &car_a.to_string() || *c == &car_b.to_string())
            .collect();
        assert_eq!(
            remaining.len(),
            1,
            "only one of the two original car_ids should remain, got {remaining:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Fingerprint store tests
    // -----------------------------------------------------------------------

    #[test]
    fn fingerprint_created_for_new_eeztire_vehicle() {
        let mut resolver = in_memory_resolver();

        let p = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let vid = resolver.process(&p).unwrap().unwrap();

        // A fingerprint should have been created and linked.
        let vehicle = &resolver.vehicles[&vid];
        assert!(
            vehicle.fingerprint_id.is_some(),
            "new EezTire vehicle must have a fingerprint_id"
        );
        assert!(
            resolver.vehicle_to_fingerprint.contains_key(&vid),
            "vehicle_to_fingerprint must contain the new vehicle"
        );

        // Verify fingerprint is in the DB.
        let fps = resolver.db.api_fingerprints().unwrap();
        assert_eq!(fps.len(), 1);
        assert_eq!(
            fps[0].fingerprint_id,
            vehicle.fingerprint_id.as_ref().unwrap().clone()
        );
        assert_eq!(fps[0].rtl433_id, 241);
    }

    #[test]
    fn fingerprint_created_for_new_ave_burst() {
        let mut resolver = in_memory_resolver();

        let p1 = make_packet_at(
            "2025-06-01 10:00:00.000",
            "0x11111111",
            "AVE-TPMS",
            208,
            380.0,
        );
        let p2 = make_packet_at(
            "2025-06-01 10:00:00.100",
            "0x22222222",
            "AVE-TPMS",
            208,
            380.0,
        );
        resolver.process(&p1).unwrap();
        resolver.process(&p2).unwrap();
        resolver.flush().unwrap();

        let ave_vehicle = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "AVE-TPMS")
            .expect("AVE vehicle must exist");
        assert!(
            ave_vehicle.fingerprint_id.is_some(),
            "new AVE vehicle must have a fingerprint_id"
        );

        let fps = resolver.db.api_fingerprints().unwrap();
        assert_eq!(fps.len(), 1);
        assert_eq!(fps[0].rtl433_id, 208);
    }

    #[test]
    fn fingerprint_links_across_sessions() {
        // Simulate two sessions: first creates a vehicle and fingerprint,
        // the second (after expiry) should find the existing fingerprint.
        let mut resolver = in_memory_resolver();

        // Session 1: Create an EezTire vehicle with enough sightings.
        for i in 0..6 {
            let ts = format!("2025-06-01 12:00:{:02}.000", i * 10);
            let p = make_packet_at(&ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        let vid1 = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .map(|v| v.vehicle_id)
            .unwrap();
        let fp_id1 = resolver.vehicles[&vid1]
            .fingerprint_id
            .clone()
            .expect("must have fingerprint");

        // After expiry (>480s), a new packet creates a new vehicle but
        // should match the same fingerprint since it has ≥5 sightings.
        let p_late = make_packet_at(
            "2025-06-01 12:09:00.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let vid2 = resolver.process(&p_late).unwrap().unwrap();

        assert_ne!(vid1, vid2, "after expiry, a new vehicle UUID is created");
        let fp_id2 = resolver.vehicles[&vid2]
            .fingerprint_id
            .clone()
            .expect("second vehicle must have fingerprint");
        assert_eq!(
            fp_id1, fp_id2,
            "both vehicles must share the same fingerprint_id"
        );

        // Verify the DB shows one fingerprint with session_count=2.
        let fps = resolver.db.api_fingerprints().unwrap();
        assert_eq!(fps.len(), 1);
        assert_eq!(fps[0].session_count, 2);
        assert_eq!(fps[0].vehicle_ids.len(), 2);
    }

    #[test]
    fn different_pressures_produce_separate_fingerprints() {
        let mut resolver = in_memory_resolver();

        // Two EezTire sensors at very different pressures.
        let hi = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let lo = make_packet_at(
            "2025-06-01 12:00:01.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            172.4,
        );
        let vhi = resolver.process(&hi).unwrap().unwrap();
        let vlo = resolver.process(&lo).unwrap().unwrap();

        assert_ne!(vhi, vlo);
        let fp_hi = resolver.vehicles[&vhi].fingerprint_id.clone().unwrap();
        let fp_lo = resolver.vehicles[&vlo].fingerprint_id.clone().unwrap();
        assert_ne!(
            fp_hi, fp_lo,
            "sensors at different pressures must produce separate fingerprints"
        );
    }

    #[test]
    fn fingerprint_store_schema_and_index_exist() {
        // Verify the fingerprint store works by creating and querying a fingerprint.
        let mut resolver = in_memory_resolver();

        let p = make_packet_at(
            "2025-06-01 12:00:00.000",
            "0xF7FFFFFF",
            "EezTire",
            241,
            352.3,
        );
        resolver.process(&p).unwrap();

        let fps = resolver.db.api_fingerprints().unwrap();
        assert_eq!(fps.len(), 1, "one fingerprint must be created");
        assert_eq!(fps[0].rtl433_id, 241);
        assert!(
            fps[0].pressure_median_kpa > 340.0 && fps[0].pressure_median_kpa < 360.0,
            "pressure_median_kpa must be near 352.3"
        );
    }

    #[test]
    fn fingerprint_three_sessions_three_vehicles_one_fingerprint() {
        // Acceptance criterion: same sensor across 3 simulated sessions → 3
        // vehicle UUIDs, 1 fingerprint, session_count=3.
        let mut resolver = in_memory_resolver();

        // Session 1: 6 sightings to establish the fingerprint.
        for i in 0..6 {
            let ts = format!("2025-06-01 12:00:{:02}.000", i * 10);
            let p = make_packet_at(&ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }
        let vids: Vec<Uuid> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .map(|v| v.vehicle_id)
            .collect();
        assert_eq!(vids.len(), 1);
        let vid1 = vids[0];

        // Session 2: after VEHICLE_EXPIRY (>480s gap).
        for i in 0..3 {
            let ts = format!("2025-06-01 12:09:{:02}.000", i * 10);
            let p = make_packet_at(&ts, "0xBFFFFFFF", "EezTire", 241, 351.6);
            resolver.process(&p).unwrap();
        }
        let vid2_candidates: Vec<Uuid> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire" && v.vehicle_id != vid1)
            .map(|v| v.vehicle_id)
            .collect();
        assert_eq!(vid2_candidates.len(), 1);
        let vid2 = vid2_candidates[0];

        // Session 3: another >480s gap.
        for i in 0..3 {
            let ts = format!("2025-06-01 12:18:{:02}.000", i * 10);
            let p = make_packet_at(&ts, "0xDFFFFFFF", "EezTire", 241, 352.9);
            resolver.process(&p).unwrap();
        }
        let vid3_candidates: Vec<Uuid> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire" && v.vehicle_id != vid1 && v.vehicle_id != vid2)
            .map(|v| v.vehicle_id)
            .collect();
        assert_eq!(vid3_candidates.len(), 1);
        let vid3 = vid3_candidates[0];

        // All 3 vehicle UUIDs must be different.
        assert_ne!(vid1, vid2);
        assert_ne!(vid2, vid3);
        assert_ne!(vid1, vid3);

        // All 3 must share the same fingerprint_id.
        let fp1 = resolver.vehicles[&vid1]
            .fingerprint_id
            .clone()
            .expect("vid1 must have fingerprint");
        let fp2 = resolver.vehicles[&vid2]
            .fingerprint_id
            .clone()
            .expect("vid2 must have fingerprint");
        let fp3 = resolver.vehicles[&vid3]
            .fingerprint_id
            .clone()
            .expect("vid3 must have fingerprint");
        assert_eq!(fp1, fp2);
        assert_eq!(fp2, fp3);

        // Database must have 1 fingerprint with session_count=3.
        let fps = resolver.db.api_fingerprints().unwrap();
        assert_eq!(fps.len(), 1, "one fingerprint for the same sensor");
        assert_eq!(fps[0].session_count, 3);
        assert_eq!(fps[0].vehicle_ids.len(), 3);
    }

    #[test]
    fn two_sensors_same_pressure_different_tx_interval_two_fingerprints() {
        // Acceptance criterion: two sensors at the same pressure but different
        // TX intervals produce 2 fingerprints once both have enough samples.
        let mut resolver = in_memory_resolver();

        // Sensor A: ~352 kPa, ~10s intervals.
        for i in 0..8 {
            let ts = format!("2025-06-01 12:00:{:02}.000", i * 10);
            let p = make_packet_at(&ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        // After expiry, sensor B: ~352 kPa but ~50s intervals.
        for i in 0..8 {
            let ts = format!("2025-06-01 12:09:{:02}.000", i * 50);
            let p = make_packet_at(&ts, "0xBFFFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }

        let eez_vehicles: Vec<&VehicleTrack> = resolver
            .vehicles
            .values()
            .filter(|v| v.protocol == "EezTire")
            .collect();

        // Both sensors should have fingerprints.
        let fp_ids: HashSet<String> = eez_vehicles
            .iter()
            .filter_map(|v| v.fingerprint_id.clone())
            .collect();

        // The test verifies at least 2 fingerprints exist — the tx interval
        // differentiation prevents the second sensor from matching the first
        // once both have accumulated enough interval samples.
        assert!(
            fp_ids.len() >= 2,
            "two sensors at the same pressure but different TX intervals \
             should produce separate fingerprints, got {} fingerprint(s)",
            fp_ids.len()
        );
    }

    #[test]
    fn min_sightings_guard_prevents_premature_matching() {
        // A fingerprint with fewer than FINGERPRINT_MIN_SIGHTINGS should not
        // be used as a match candidate.
        let mut resolver = in_memory_resolver();

        // Create a vehicle with only 2 sightings (below threshold).
        for i in 0..2 {
            let ts = format!("2025-06-01 12:00:{:02}.000", i * 10);
            let p = make_packet_at(&ts, "0xF7FFFFFF", "EezTire", 241, 352.3);
            resolver.process(&p).unwrap();
        }
        let vid1 = resolver
            .vehicles
            .values()
            .find(|v| v.protocol == "EezTire")
            .map(|v| v.vehicle_id)
            .unwrap();

        // After expiry, a new packet at the same pressure should NOT match
        // the immature fingerprint.
        let p_late = make_packet_at(
            "2025-06-01 12:09:00.000",
            "0xBFFFFFFF",
            "EezTire",
            241,
            352.3,
        );
        let vid2 = resolver.process(&p_late).unwrap().unwrap();
        assert_ne!(vid1, vid2, "new vehicle UUID after expiry");

        // The two vehicles should have different fingerprints because the
        // first had too few sightings to be a match candidate.
        let fp1 = resolver.vehicles[&vid1].fingerprint_id.clone().unwrap();
        let fp2 = resolver.vehicles[&vid2].fingerprint_id.clone().unwrap();
        assert_ne!(fp1, fp2, "immature fingerprint must not be matched");
    }

    // -------------------------------------------------------------------
    // Pressure plausibility guard
    // -------------------------------------------------------------------

    #[test]
    fn implausible_negative_pressure_discarded() {
        // A packet with pressure=-0.2 kPa must be discarded: no vehicle
        // created, no error returned.
        let mut resolver = in_memory_resolver();
        let p = make_packet("0x12345678", "TRW-OOK", 298, -0.2);
        let result = resolver.process(&p).unwrap();
        assert_eq!(result, None, "implausible packet must return Ok(None)");
        assert!(
            resolver.vehicles.is_empty(),
            "no vehicle must be created for pressure=-0.2"
        );
    }

    #[test]
    fn implausible_boundary_pressure_discarded_and_accepted() {
        // pressure=1.4 must be discarded; pressure=1.5 must be processed.
        let mut resolver = in_memory_resolver();

        let p_low = make_packet("0xAABBCCDD", "TRW-OOK", 298, 1.4);
        let result = resolver.process(&p_low).unwrap();
        assert_eq!(result, None, "pressure=1.4 must be discarded");
        assert!(
            resolver.vehicles.is_empty(),
            "no vehicle must be created for pressure=1.4"
        );

        let p_ok = make_packet("0xAABBCCDD", "TRW-OOK", 298, 1.5);
        let result = resolver.process(&p_ok).unwrap();
        // Fixed-ID path creates a vehicle on first valid sighting.
        assert!(
            result.is_some(),
            "pressure=1.5 must be processed (not discarded)"
        );
        assert_eq!(
            resolver.vehicles.len(),
            1,
            "exactly one vehicle for the accepted packet"
        );
    }

    #[test]
    fn implausible_zero_pressure_discarded() {
        let mut resolver = in_memory_resolver();
        let p = make_packet("0x11111111", "TRW-OOK", 298, 0.0);
        let result = resolver.process(&p).unwrap();
        assert_eq!(result, None);
        assert!(
            resolver.vehicles.is_empty(),
            "no vehicle must be created for pressure=0.0"
        );
    }

    #[test]
    fn implausible_high_pressure_discarded() {
        let mut resolver = in_memory_resolver();
        let p = make_packet("0x22222222", "TRW-OOK", 298, 901.0);
        let result = resolver.process(&p).unwrap();
        assert_eq!(result, None);
        assert!(
            resolver.vehicles.is_empty(),
            "no vehicle must be created for pressure=901.0"
        );
    }

    // -------------------------------------------------------------------
    // Issue #29 — TX interval as tiebreaker, not hard gate
    // -------------------------------------------------------------------

    /// Helper: inject a fingerprint-path VehicleTrack directly into the
    /// resolver with the given pressure, tx_interval_median, and last_seen
    /// offset (seconds before `base`).
    fn inject_fingerprint_vehicle(
        resolver: &mut Resolver,
        pressure_kpa: f32,
        tx_interval_median_ms: Option<u32>,
        last_seen: DateTime<Utc>,
    ) -> Uuid {
        let vid = Uuid::new_v4();
        let vehicle_class = infer_vehicle_class(pressure_kpa, None);
        let mut intervals = VecDeque::new();
        // If a median is provided, fill the ring buffer with enough samples
        // so that compute_median returns the same value.
        if let Some(median) = tx_interval_median_ms {
            for _ in 0..TX_INTERVAL_MIN_SAMPLES {
                intervals.push_back(median);
            }
        }
        let vehicle = VehicleTrack {
            vehicle_id: vid,
            first_seen: last_seen,
            last_seen,
            sighting_count: 10,
            protocol: "EezTire".to_string(),
            rtl433_id: 241,
            fixed_sensor_id: None,
            pressure_signature: [pressure_kpa, 0.0, 0.0, 0.0],
            make_model_hint: make_model_hint(241).map(str::to_owned),
            battery_ok: true,
            tx_intervals_ms: intervals,
            tx_interval_median_ms,
            car_id: None,
            receiver_sightings: HashMap::new(),
            wheel_position: None,
            vehicle_class,
            fingerprint_id: None,
        };
        // Persist to DB so that subsequent process() calls don't hit FK errors.
        resolver.db.upsert_vehicle(&vehicle).unwrap();
        resolver.vehicles.insert(vid, vehicle);
        vid
    }

    #[test]
    fn four_vehicles_same_pressure_no_hint_matches_most_recent() {
        // AC: 4 active vehicles at 352.3 kPa with established intervals,
        // incoming packet at 352.3 kPa with no interval hint → matched to
        // most recently seen, not new vehicle.
        let mut resolver = in_memory_resolver();
        let base = chrono::Utc::now();

        // Inject 4 vehicles at the same compensated pressure with different
        // intervals and staggered last_seen times.
        let compensated = compensate_pressure(352.3, Some(25.0));
        let _v1 = inject_fingerprint_vehicle(
            &mut resolver,
            compensated,
            Some(30_000),
            base - Duration::seconds(120),
        );
        let _v2 = inject_fingerprint_vehicle(
            &mut resolver,
            compensated,
            Some(45_000),
            base - Duration::seconds(90),
        );
        let _v3 = inject_fingerprint_vehicle(
            &mut resolver,
            compensated,
            Some(60_000),
            base - Duration::seconds(60),
        );
        let v4 = inject_fingerprint_vehicle(
            &mut resolver,
            compensated,
            Some(71_000),
            base - Duration::seconds(30),
        );

        assert_eq!(resolver.vehicles.len(), 4, "setup: 4 vehicles must exist");

        // Feed a new EezTire packet at 352.3 kPa.  The timestamp must be
        // formatted in local time because TpmsPacket::parsed_ts interprets it
        // as local.
        let now_local = base.with_timezone(&chrono::Local);
        let ts_str = now_local.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let p = make_packet_at(&ts_str, "0xFDFFFFFF", "EezTire", 241, 352.3);
        let result = resolver.process(&p).unwrap();

        assert!(
            result.is_some(),
            "must match an existing vehicle, not return None"
        );
        let matched = result.unwrap();

        // Should match v4 — the most recently seen.
        assert_eq!(
            matched, v4,
            "must match the most recently seen vehicle (v4), not create a new one"
        );

        // No new vehicle should have been created.
        assert_eq!(
            resolver.vehicles.len(),
            4,
            "no new vehicle should be created when pressure matches exist"
        );
    }

    #[test]
    fn two_vehicles_same_pressure_hint_selects_closer_interval() {
        // AC: 2 active vehicles at 352.3 kPa with intervals 45s and 71s,
        // incoming packet at 352.3 kPa with hint 44s → matched to the 45s
        // vehicle.
        let mut resolver = in_memory_resolver();
        let base = chrono::Utc::now();

        let compensated = compensate_pressure(352.3, Some(25.0));
        let v_45 = inject_fingerprint_vehicle(
            &mut resolver,
            compensated,
            Some(45_000),
            base - Duration::seconds(60),
        );
        let _v_71 = inject_fingerprint_vehicle(
            &mut resolver,
            compensated,
            Some(71_000),
            base - Duration::seconds(30),
        );

        assert_eq!(resolver.vehicles.len(), 2);

        // Craft a sighting with tx_interval_hint_ms = 44_000.
        // To get this hint, we set last_seen_by_protocol for rtl433_id=241
        // to 44 seconds before the sighting.
        let sighting_ts = base;
        let hint_origin = sighting_ts - Duration::milliseconds(44_000);
        resolver.last_seen_by_protocol.insert(241, hint_origin);

        let now_local = sighting_ts.with_timezone(&chrono::Local);
        let ts_str = now_local.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let p = make_packet_at(&ts_str, "0xFDFFFFFF", "EezTire", 241, 352.3);
        let result = resolver.process(&p).unwrap();

        assert!(result.is_some(), "must match an existing vehicle");
        let matched = result.unwrap();
        assert_eq!(
            matched, v_45,
            "hint=44s is within tolerance of the 45s vehicle but not the 71s vehicle; \
             must match the 45s vehicle"
        );

        // No new vehicle created.
        assert_eq!(
            resolver.vehicles.len(),
            2,
            "no new vehicle should be created"
        );
    }
}
