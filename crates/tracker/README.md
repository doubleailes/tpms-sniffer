# tpms-tracker

Stateful consumer of `tpms-sniffer` JSON output that consolidates repeated
sightings into persistent vehicle tracks.

## Per-protocol `VEHICLE_EXPIRY`

`VEHICLE_EXPIRY` is the window after a vehicle's most recent sighting during
which a new packet can still be correlated to it via the pressure fingerprint.
Once the window lapses, the next matching packet spawns a new vehicle UUID
because we can no longer be sure the sensor is the same physical one returning
into range.

The default is **5 minutes** (300 s). A few protocols have known low
transmission rates that require a longer window; the mapping lives in
`vehicle_expiry_for()` in `src/resolver.rs`.

| rtl_433 ID | Protocol  | Expiry | Rationale |
|-----------:|-----------|-------:|-----------|
| 208        | AVE-TPMS  | 600 s  | Aftermarket AVE clip-on sensors transmit every 2–8 minutes when the vehicle is stationary. A 5-minute window leaks the track between sightings and spawns a fresh UUID for what is the same physical vehicle. |
| 241        | EezTire   | 480 s  | EezTire sensors may transmit infrequently when the battery is low, with gaps of up to 6+ minutes observed in the field. |
| 298        | TRW-OOK   | 480 s  | Similar slow stationary TX cadence to EezTire. |
| *(any other)* | *(default)* | 300 s | Matches the prior behaviour of a single global constant. |

If a new protocol is added that transmits less often than once every 5 minutes,
extend the `match` arm in `vehicle_expiry_for()` and add a row to the table
above. Keep the entries in `rtl_433` ID order.

### Battery-state extension

When a sensor reports `battery_ok = false`, the effective expiry is extended by
an additional **5 minutes** (300 s) on top of the per-protocol base value. This
acknowledges that a low-battery sensor may transmit less frequently but is still
physically present. The extension avoids spurious track restarts for sensors
whose transmit power or duty cycle is degraded by a weak battery.

The logic lives in `effective_expiry()` in `src/resolver.rs`:

| Protocol  | Good battery | Low battery |
|-----------|------------:|------------:|
| AVE-TPMS  | 600 s       | 900 s       |
| EezTire   | 480 s       | 780 s       |
| TRW-OOK   | 480 s       | 780 s       |
| *(default)* | 300 s     | 600 s       |

## AVE-TPMS half-range pressure artifact

The AVE decoder (protocol 208) occasionally emits a frame whose pressure field
decodes to roughly half the real operating pressure — a ~382 kPa sensor will
occasionally transmit a ~190 kPa frame. This is a protocol-level dual-range
encoding quirk used for low-pressure detection, not a genuine pressure reading.
See the [rtl_433 AVE decoder source](https://github.com/merbanan/rtl_433/blob/master/src/devices/tpms_ave.c)
for the field definition (`pressure = byte * 1.5 kPa`).

The sniffer side sets `pressure_kpa_reliable = false` on those frames (see
`AVE_MIN_RELIABLE_KPA` in `crates/sniffer/src/decoder.rs`). The tracker then
skips pressure-fingerprint updates for them so the half-range value does not
drift the vehicle's signature and does not spawn a duplicate UUID.

## TX-interval fingerprint

Rolling-ID sensors (AVE-TPMS, post-2018 EezTire, etc.) rotate their `sensor_id`
per packet, making the ID useless as a stable identifier.  However, the
transmission interval — the median time between consecutive packets from the
same sensor — is determined by the sensor's firmware and crystal oscillator.
It is stable across ID rotations, across sessions, and across power cycles.

The tracker accumulates inter-packet (or inter-burst) intervals in a ring
buffer on each `VehicleTrack` and computes a running median.  When both the
candidate vehicle and the incoming sighting carry enough interval data, the
interval is used as a secondary matching signal alongside the pressure
fingerprint.  This prevents false merges between sensors at the same pressure
but different TX rates.

The implementation lives in `src/lib.rs` (data types, constants, `compute_median`)
and `src/resolver.rs` (accumulation logic, matching guards).

### Tuning constants

All constants are defined in `src/lib.rs` and require calibration against real
capture data before the feature is considered stable.

| Constant | Value | Rationale |
|---|---:|---|
| `TX_INTERVAL_WINDOW` | 8 | Ring buffer depth — last 8 intervals (9 consecutive packets). Enough for a stable median without significant memory cost per vehicle. |
| `TX_INTERVAL_MIN_SAMPLES` | 3 | Minimum intervals before the median is used in matching. Below this threshold the interval check is skipped and pressure match alone is sufficient. Avoids false rejections during track establishment. |
| `TX_INTERVAL_MAX_MS` | 120 000 (2 min) | Gap threshold above which an interval is not recorded. Gaps longer than 2 minutes are likely caused by the sensor going out of range or the SDR restarting, not by normal TX cadence. |
| `TX_INTERVAL_TOLERANCE_MS` | 8 000 (±8 s) | Match tolerance for comparing two interval medians (or the observed gap vs. a median). Deliberately wide for the initial implementation: sensor crystals drift with temperature, and the tracker's interval measurement includes jitter from SDR sample timing. Tighten after collecting calibration data from known single-vehicle sessions. |

### Database schema

Two columns are added to the `vehicles` table via an automatic migration:

```sql
ALTER TABLE vehicles ADD COLUMN tx_interval_median_ms INTEGER;
ALTER TABLE vehicles ADD COLUMN tx_interval_samples    INTEGER NOT NULL DEFAULT 0;
```

`tx_interval_median_ms` is `NULL` until at least `TX_INTERVAL_MIN_SAMPLES`
intervals have been collected.  `tx_interval_samples` reflects the current
ring-buffer depth (0–8).

### Matching behaviour

- **Fingerprint path** (EezTire, sentinel-rejected IDs): the interval check
  compares the vehicle's stored median against the sighting's
  `tx_interval_hint_ms` (gap since the last packet of the same protocol).
  If both sides have data and the intervals differ by more than the tolerance,
  the candidate is rejected even if pressure matches.

- **Burst path** (AVE-TPMS rolling-ID): the interval check compares the
  vehicle's stored median against the actual gap between the new burst and
  the vehicle's `last_seen`.  Same tolerance applies.

- **Fixed-ID path**: the interval is accumulated for fingerprint enrichment
  but is not used for matching (the fixed sensor ID is already a stable key).

In all cases, the interval check is **skipped** when either side has fewer than
`TX_INTERVAL_MIN_SAMPLES` — pressure match alone is sufficient during track
establishment.
