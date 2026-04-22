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

- **Fingerprint path** (EezTire, sentinel-rejected IDs): the correlator first
  collects all candidates matching on protocol + pressure (hard constraints),
  then uses the TX interval as a **soft tiebreaker** to prefer among multiple
  pressure matches.  The interval check compares the vehicle's stored median
  against the sighting's `tx_interval_hint_ms` (gap since the last packet of
  the same protocol).  If interval filtering would eliminate all pressure
  matches, it is ignored — pressure match always takes precedence.  Among
  remaining candidates, the most recently seen vehicle is preferred.

- **Burst path** (AVE-TPMS rolling-ID): the interval check compares the
  vehicle's stored median against the actual gap between the new burst and
  the vehicle's `last_seen`.  Same tolerance applies.

- **Fixed-ID path**: the interval is accumulated for fingerprint enrichment
  but is not used for matching (the fixed sensor ID is already a stable key).

In all cases, the interval check is **skipped** when either side has fewer than
`TX_INTERVAL_MIN_SAMPLES` — pressure match alone is sufficient during track
establishment.

## Vehicle class inference

The tracker infers a `VehicleClass` from tire pressure range and optional
sensor count.  Classification is available from the first packet and is refined
as the pressure running average stabilises.

| Class               | Typical pressure | Sensor count | `expected_sensor_count()` |
|---------------------|------------------|--------------|---------------------------|
| Motorcycle          | 200–290 kPa      | 2            | `Some(2)`                 |
| Passenger car       | 200–280 kPa      | 4            | `Some(4)`                 |
| SUV / light truck   | 260–340 kPa      | 4            | `Some(4)`                 |
| Light commercial van| 350–480 kPa      | 4+           | `Some(4)`                 |
| Heavy truck         | 550–800 kPa      | 6–18         | **`None`**                |
| Unknown             | —                | —            | `None`                    |

`HeavyTruck` returns `None` from `expected_sensor_count()` because heavy trucks
have 6–18 sensors depending on axle count, which is too variable to fix as a
target group size for the Jaccard grouper.

### Per-class pressure tolerance

Each class has a `pressure_tolerance_kpa()` value used for fingerprint matching
instead of the former global constant.  Heavier vehicles have larger absolute
pressure variation under load and temperature change:

| Class               | Tolerance |
|---------------------|-----------|
| Motorcycle          | 4.0 kPa   |
| PassengerCar        | 5.0 kPa   |
| SuvLightTruck       | 6.0 kPa   |
| LightCommercialVan  | 8.0 kPa   |
| HeavyTruck          | 15.0 kPa  |
| Unknown             | 5.0 kPa   |

### Temperature-pressure compensation

When a valid temperature reading is available (0 < temp_c < 100 °C), the
tracker applies an approximate compensation of **0.9 kPa per °C** to adjust
the pressure back to a cold-equivalent baseline (20 °C reference).  Sentinel
values (e.g. 215 °C) and missing temperatures are left uncompensated.

### Database schema

The `vehicle_class` column is added to the `vehicles` table via an automatic
migration:

```sql
ALTER TABLE vehicles ADD COLUMN vehicle_class TEXT NOT NULL DEFAULT 'Unknown';
```

`vehicle_class` is also included in JSON report output via `VehicleSummary`.

## Persistent fingerprint store

The fingerprint store links vehicle UUIDs across sessions via a stable
`fingerprint_id`.  When a sensor's track expires (`VEHICLE_EXPIRY`) and a new
UUID is created, the tracker queries the `fingerprints` table to determine
whether the sensor has been seen before.  If a match is found, the new vehicle
is linked to the same `fingerprint_id`, avoiding fragmentation of the tracking
history.

### Tuning constants

| Constant | Value | Rationale |
|---|---:|---|
| `FINGERPRINT_MIN_SIGHTINGS` | 5 | Minimum sightings before a stored fingerprint is used as a match candidate. A fingerprint with fewer sightings may represent a noise event. |
| `FINGERPRINT_MAX_GAP_DAYS` | 30 | Maximum days since `last_seen` before a fingerprint is considered stale and excluded from matching. Stale fingerprints are ignored; a new one is created. |

### Database schema

The fingerprint store adds a `fingerprints` table and a `fingerprint_id` FK
column on the `vehicles` table.  Both are created via automatic migrations.

```sql
CREATE TABLE IF NOT EXISTS fingerprints (
    fingerprint_id        TEXT PRIMARY KEY,
    rtl433_id             INTEGER NOT NULL,
    vehicle_class         TEXT NOT NULL,
    pressure_median_kpa   REAL NOT NULL,
    tx_interval_median_ms INTEGER,
    first_seen            TEXT NOT NULL,
    last_seen             TEXT NOT NULL,
    total_sighting_count  INTEGER NOT NULL DEFAULT 0,
    session_count         INTEGER NOT NULL DEFAULT 1,
    alarm_rate            REAL,
    notes                 TEXT
);

ALTER TABLE vehicles ADD COLUMN fingerprint_id TEXT REFERENCES fingerprints(fingerprint_id);
```

### API

- `GET /api/fingerprints` — returns all stored fingerprints ordered by
  `last_seen DESC`, including linked `vehicle_ids`.
- `GET /api/cars` — vehicle rows now include `fingerprint_id`.

## Structured logging

The tracker supports three log levels, controlled via `--log-level` (default:
`info`) or the `RUST_LOG` environment variable.  All log output goes to
**stderr** so that the stdout JSON-L stream remains clean for piping.

```bash
# Sightings only (current default behaviour)
./tpms-tracker --db tpms.db --log-level info

# + resolver decisions (one RESOLVE line per packet)
./tpms-tracker --db tpms.db --log-level debug

# + tracker lifecycle events (vehicle created/expired, merges, fingerprints)
./tpms-tracker --db tpms.db --log-level trace
```

The `RUST_LOG` environment variable overrides `--log-level` without a
recompile:

```bash
RUST_LOG=tpms_tracker=debug ./tpms-tracker --db tpms.db
```

### Log streams

#### INFO — sightings (`--verbose`)

When `--verbose` is set, one `SIGHT` line per resolved packet:

```
2026-04-17 10:07:30.938 | SIGHT | vehicle=8294c101 | car=3c14b2d0 | fp=fp-8294c101 |
  sensor=0xFFFBFFFB | zeros=5 | pressure=319.7 | temp=null |
  alarm=true | battery=false | protocol=EezTire/Carchet/TST-507 |
  rtl433=241 | class=PassengerCar | receiver=default
```

#### DEBUG — resolver decisions

One `RESOLVE` line per packet showing which code path was taken:

```
2026-04-17 10:07:30.937 | RESOLVE | sensor=0xFFFBFFFB | zeros=5 | valid=true |
  path=fingerprint_correlator | candidates=3 | matched=8294c101 |
  match_reason=pressure_delta:0.0_kpa | protocol_filter=pass |
  pressure_before=46.8 | pressure_after=46.4

2026-04-17 10:35:39.495 | RESOLVE | sensor=0x7C410C2A | zeros=12 | valid=true |
  path=fixed_id | key=(0x7C410C2A,241) | result=new |
  pressure=51.1
```

| Field | Description |
|---|---|
| `zeros=N` | Bit count from `sensor_id.count_zeros()` |
| `valid=bool` | Result of `is_valid_sensor_id()` |
| `path=X` | Which resolution path ran (`fixed_id`, `fingerprint_correlator`, `fingerprint_store`, `rolling_id`) |
| `candidates=N` | Number of active vehicles considered by the correlator |
| `matched=uuid` | Vehicle matched, or `none` if new vehicle created |
| `match_reason=X` | Primary reason for the match |
| `pressure_before=N` | Vehicle's pressure average before this sighting |
| `pressure_after=N` | Vehicle's pressure average after update |

#### TRACE — tracker events

State change events emitted when the tracker takes a significant action:

```
EVENT | type=vehicle_created | vehicle=new-uuid | protocol=EezTire | rtl433=241 |
  pressure=46.4 | path=fingerprint_correlator

EVENT | type=vehicle_expired | vehicle=8294c101 | fp=fp-8294c101 | car=3c14b2d0 |
  age_secs=2490 | sightings=9 | last_pressure=46.4 | last_seen=10:07:30

EVENT | type=jaccard_merge | keep=192e01cb | discard=959c175f | score=0.750 |
  keep_size=2 | discard_size=1 | members=[0ddfed40,5651851f]

EVENT | type=fingerprint_created | fp=fp-new | vehicle=new-uuid | protocol=TRW-OOK |
  pressure=63.75 | class=Unknown

EVENT | type=fingerprint_match | vehicle=new-uuid | fp=fp-8294c101

EVENT | type=car_group_created | car=new-car-uuid | vehicle=new-uuid | protocol=EezTire
```

| Event type | Trigger |
|---|---|
| `vehicle_created` | New vehicle UUID assigned |
| `vehicle_expired` | Vehicle removed from active set after `VEHICLE_EXPIRY` |
| `car_group_created` | New CarGroup instantiated |
| `jaccard_merge` | Jaccard merge fires between two car groups |
| `fingerprint_created` | New fingerprint written to DB |
| `fingerprint_match` | Returning vehicle matched to existing fingerprint |

### Useful grep recipes

```bash
# Quick grep for sentinel failures (should be zero in production)
grep 'zeros=[12] ' replay_debug.log | grep 'valid=true'

# Find all new vehicle creations
grep 'result=new_vehicle' replay_debug.log

# Find all Jaccard merges
grep 'type=jaccard_merge' replay_trace.log

# Find all fingerprint matches (returning vehicles)
grep 'type=fingerprint_match' replay_trace.log

# Find all expired vehicles
grep 'type=vehicle_expired' replay_trace.log

# Find all packets for a specific sensor
grep 'sensor=0xFFDFBFFF' replay_debug.log
```

### Capture workflow

```bash
# Live capture — sightings to terminal, debug log to file
./tpms-sniffer --json 2>/dev/null \
  | ./tpms-tracker --db tpms.db --log-level debug 2>session_debug.log

# Replay with debug log for analysis
./tpms-tracker \
  --replay tests/fixtures/wild/session_20260415_1000.jsonl.gz \
  --db :memory: \
  --log-level debug \
  --assert-consistency \
  2>replay_debug.log
```
