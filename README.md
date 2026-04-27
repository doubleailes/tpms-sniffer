# tpms-sniffer

Passive TPMS vehicle tracking and fingerprinting toolkit in Rust.
Built on [`rtlsdr-next`](https://github.com/mattdelashaw/rtlsdr-next),
protocol details ported from [`rtl_433`](https://github.com/merbanan/rtl_433).

Designed for long-term unattended field deployment on a Raspberry Pi 4
with an outdoor antenna. Powers ongoing research into passive vehicle
re-identification and physical-layer fingerprinting.

> **Research context:** This project is the foundation for a white paper on
> passive vehicle fingerprinting from 433 MHz TPMS transmissions, combining
> pressure signatures, temporal behavioural patterns, and crystal oscillator
> jitter for persistent identity tracking across rolling sensor ID rotations.

---

## Architecture

```text
RTL-SDR (433.92 MHz)
    │
    └─ tpms-sniffer   (Rust, 25 protocol decoders)
           │ JSON lines
           └─ tpms-tracker  (Rust, SQLite + axum)
                  ├─ Resolver         rolling-ID correlation + fingerprint store
                  ├─ Jaccard grouper  multi-sensor vehicle assembly
                  ├─ Jitter engine    oscillator fingerprinting
                  ├─ TBF engine       temporal behavioural fingerprinting
                  ├─ Web dashboard    axum (http://pi:8080)
                  └─ tpms.db          SQLite (WAL mode)
```

### Crates

|            Crate |                                                                Role |
|------------------|---------------------------------------------------------------------|
| `crates/sniffer` | IQ demodulation, framing, 25 protocol decoders, JSON reporter       |
| `crates/tracker` | Resolver, SQLite persistence, fingerprinting, analytics, web server |

---

## Supported protocols (25 total)

| `--protocol` | Protocol / Sensor | rtl\_433 # | Vehicles | Freq | Mod |
|---|---|---|---|---|---|
| `steelmate` | Steelmate TPMS | 59 | Generic | 315/433 | OOK-PWM |
| `schrader` | Schrader generic | 60 | Various | 315/433 | FSK-Manch |
| `citroen` | Citroen / Peugeot / Fiat VDO | 82 | Citroen, Peugeot, Fiat | 433 | FSK-Manch |
| `toyota` | Toyota PMV-C210 | 88 | Toyota Auris/Corolla/Lexus | 433 | FSK-DMC |
| `ford` | Ford / Continental VDO | 89 | Ford Fiesta/Focus/Kuga | 315/433 | FSK-Manch |
| `renault` | Renault / Dacia | 90 | Renault Clio/Captur/Zoe | 433 | FSK-Manch |
| `schrader_eg53` | Schrader EG53MA4 | 95 | Saab, Opel, Vauxhall | 315/433 | FSK-Manch |
| `toyota107j` | Toyota PMV-107J | 110 | Toyota (US) | 315 | FSK-DMC |
| `jansite` | Jansite TY02S | 123 | Aftermarket | 433 | OOK |
| `elantra` | Hyundai Elantra 2012 | 140 | Hyundai Elantra | 433 | OOK |
| `abarth` | Abarth 124 Spider | 156 | Abarth 124 | 433 | FSK-Manch |
| `schrader_smd` | Schrader SMD3MA4 | 168 | Subaru, Infiniti, Nissan | 433 | FSK-Manch |
| `jansite_solar` | Jansite Solar | 180 | Aftermarket motorcycle/scooter | 433 | OOK |
| `hyundai_vdo` | Hyundai VDO | 186 | Hyundai, Kia, Genesis | 433 | FSK-Manch |
| `truck` | Solar / Truck TPMS | 201 | Heavy-duty aftermarket | 433 | FSK-Manch |
| `porsche` | Porsche Boxster / Cayman | 203 | Porsche | 433 | FSK-Manch |
| `ave` | AVE TPMS | 208 | Aftermarket clip-on | 433 | OOK |
| `tyreguard` | TyreGuard 400 | 225 | Generic aftermarket | 433 | FSK-Manch |
| `eeztire` | EezTire E618 / Carchet / TST-507 | 241 | External clip-on | 433 | OOK |
| `bmw_gen45` | BMW Gen4/5 + Audi / HUF | 252 | BMW, Audi, VW | 433 | FSK-Manch |
| `bmw_gen23` | BMW Gen2/3 | 257 | BMW | 433 | FSK-Manch |
| `gm` | GM Aftermarket | 275 | GM (US) | 315 | OOK |
| `airpuxem` | Airpuxem TYH11 | 295 | Aftermarket | 433 | FSK-Manch |
| `trw_ook` | TRW OOK (FCC-ID GQ4-70T) | 298 | Chrysler/Dodge/Jeep 2014–2022 | 433 | OOK |
| `trw_fsk` | TRW FSK (FCC-ID GQ4-70T) | 299 | Chrysler/Dodge/Jeep 2014–2022 | 433 | FSK-Manch |

Modulation: **OOK** = On-Off Keying, **FSK-Manch** = FSK + Manchester,
**FSK-DMC** = FSK + Differential Manchester.

---

## Hardware

| Component | Model | Notes |
|---|---|---|
| SBC | Raspberry Pi 4 | 4 GB RAM, DietPi OS |
| SDR | RTL-SDR V3 | TCXO reference, SMA connector |
| Antenna | Sirio HGO 433 & 868 | 2 dBi @ 433 MHz, 4 dBi @ 868 MHz, outdoor |

The antenna covers both 433 MHz (TPMS, RKE) and 868 MHz (newer EU
vehicles), enabling future dual-band capture with a second RTL-SDR dongle.

---

## Build & run

```bash
# System dependencies
sudo apt install libusb-1.0-0-dev pkg-config

# Build both crates
cargo build --release

# Run sniffer piped to tracker
./target/release/tpms-sniffer --json | \
./target/release/tpms-tracker \
  --db tpms.db \
  --serve 0.0.0.0:8080 \
  --log-level info \
  --log-file logs/tpms-tracker.log
```

Or use the provided `start.sh`.

### Sniffer flags

```
--freq <HZ>           Centre frequency [default: 433920000]
--protocol <NAME>     Decode only this protocol (default: all)
--gain <TENTHS_DB>    RTL-SDR gain in tenths of dB (default: auto)
--confidence <PCT>    Minimum confidence threshold [default: 65]
--json                Emit JSON lines (required for tracker pipe)
```

### Tracker flags

```
--db <PATH>            SQLite database path
--serve <ADDR>         Bind web dashboard (e.g. 0.0.0.0:8080)
--log-level <LEVEL>    error | warn | info | debug | trace
--log-file <PATH>      Rotating log file (written alongside stderr)
--log-max-size <MB>    Rotate at this file size [default: 10]
--log-max-files <N>    Rotated files to retain [default: 5]
--replay <PATH>        Replay a JSON fixture file (testing)
```

---

## Three-layer identity model

```
fingerprint_id   permanent physical sensor identity (cross-session)
    │
    └─ vehicle_id   session-scoped (expires after VEHICLE_EXPIRY seconds)
           │
           └─ sightings   individual decoded packets
```

### Sentinel detection

Valid sensor IDs require `count_zeros(id) >= 3`. All-ones noise patterns
(`0xFFFFFFFF`) are discarded before any fingerprint lookup.

### Rolling-ID resolution

EezTire, TRW-OOK, and TRW-FSK sensors change their sensor_id on every
transmission (bit-flip rolling scheme). The fingerprint correlator
resolves them by matching on protocol + pressure delta. The TX interval
is used as a tiebreaker only, never as a veto.

### Jaccard vehicle grouper

Sensors co-occurring within 60-second windows are grouped into car
entities. Merge threshold: `JACCARD_MERGE_THRESHOLD = 0.60`.

---

## Vehicle classification

Inferred from corrected pressure after unit conversion:

| Class | Pressure range | Typical vehicles |
|---|---|---|
| Motorcycle | 150–200 kPa | Scooters, motorcycles |
| PassengerCar | 200–280 kPa | Cars, small SUVs |
| SuvLightTruck | 260–340 kPa | SUVs, crossovers |
| LightCommercialVan | 350–480 kPa | Delivery vans |
| HeavyTruck | 550–800 kPa | HGVs |

Protocol-level overrides apply where pressure alone is unreliable
(Jansite-Solar → always Motorcycle).

---

## Pressure unit corrections

Several protocols encode pressure in PSI with the unit incorrectly
described in earlier decoder implementations:

| Protocol | Raw unit | Multiplier to kPa |
|---|---|---|
| EezTire (241) | 0.1 PSI/unit | × 0.68948 |
| TRW OOK/FSK (298/299) | 0.4 PSI/unit | × 2.75790 |
| Jansite (123/180) | 0.25 PSI/unit | × 1.7 |
| AVE TPMS (208) | 1.5 kPa/unit | correct |
| Hyundai Elantra (140) | 1 kPa/unit | correct |

The database migration script (`scripts/migrate.py`) corrects historical
data when decoders are fixed.

---

## Physical-layer fingerprinting

The tracker fingerprints sensors by the timing jitter of their crystal
oscillator — a hardware characteristic that is stable across the sensor's
lifetime and survives rolling ID rotation and pressure variation.

### Interval collection

For **fixed-ID protocols** (Jansite, Hyundai, AVE): consecutive packets
from the same `sensor_id` are used directly.

For **rolling-ID protocols** (EezTire, TRW): consecutive packets from
the same physical sensor are identified using Hamming distance between
successive sensor IDs. Same-sensor pairs differ by 1–3 bits; different
sensors differ by ~16 bits on average.

```
MAX_HAMMING_DISTANCE  = 3      consecutive-packet same-sensor threshold
MAX_PRESSURE_DELTA    = 5 kPa  additional cross-sensor guard
RAW_INTERVAL_MIN      = 5,000 ms   reject burst duplicates
RAW_INTERVAL_MAX      = 300,000 ms reject cross-session gaps
```

### Jitter statistics (computed after 50 samples)

| Metric | Physical meaning |
|---|---|
| `sigma_ms` | Oscillator stability — expected 1–30 ms for ±20 ppm crystal |
| `skewness` | Asymmetry of timing distribution |
| `kurtosis` | Tail weight — high for sensors with collision-avoidance retransmit |
| `acf_lag1` | Lag-1 autocorrelation — negative indicates active frequency correction |

---

## Temporal behavioural fingerprinting

Each fingerprint accumulates a behavioural profile from `session_log`:

- **Arrival distribution** — Gaussian mixture over hour-of-day
- **Dwell time** — log-normal (drive-by median < 30s vs parked > 1h)
- **Periodicity** — dominant cycle via autocorrelation (daily / weekly)
- **Presence map** — 24×7 probability matrix

Minimum 7 sessions required. Distinguishes commuters (bimodal arrival,
zero dwell, 24h period) from stationary vehicles (flat arrival, hours
dwell, no period) from transients (single drive-by sessions).

---

## Web dashboard

At `http://pi:8080` when `--serve` is passed.

| Endpoint | Description |
|---|---|
| `GET /` | Embedded dashboard |
| `GET /api/stats` | Summary statistics |
| `GET /api/cars` | All tracked vehicles |
| `GET /api/cars/:id` | Single vehicle detail |
| `GET /api/fingerprints/:id/jitter` | Jitter profile + histogram |

---

## Database schema

SQLite WAL mode. Current schema version: **v9**.

| Table | Contents |
|---|---|
| `sightings` | Individual decoded packets |
| `vehicles` | Session-scoped vehicle records |
| `cars` | Jaccard-grouped car entities |
| `fingerprints` | Persistent sensor identities + jitter columns |
| `session_log` | Per-session start/end/dwell for TBF |
| `temporal_fingerprints` | Computed TBF profiles |
| `interval_samples` | Raw TX intervals, ring buffer (10k rows per fingerprint) |
| `rke_events` | Remote Keyless Entry events (planned, issue #35) |
| `vehicle_state_transitions` | Arrival/departure from RKE fusion (planned) |
| `schema_migrations` | Applied migration history |

### Migrations

```bash
python3 scripts/migrate.py tpms.db            # apply all pending
python3 scripts/migrate.py tpms.db --dry-run  # preview
python3 scripts/migrate.py tpms.db --step 6   # up to v6 only
```

Automatic timestamped backup before any write.

---

## Structured logging

```
2026-04-26 18:41:39 | RESOLVE | sensor=0xFFFFFFFF | zeros=0 | valid=false
  | path=fingerprint_correlator | candidates=2 | matched=c7e69c9d
  | match_reason=pressure_delta:0.0_kpa | protocol_filter=pass
  | pressure_before=352.3 | pressure_after=352.3
```

With `--log-file`, logs rotate by size and are retained for configurable
history. Useful for post-hoc jitter and interval analysis without touching
the SQLite database.

---

## Confidence scoring

| Condition | Points |
|---|---|
| CRC / checksum passes | +60 |
| Pressure + temp in sane range | +20 |
| Pressure in typical tyre range 150–350 kPa | +10 |
| Temperature in -20–80 °C range | +5 |
| Base bonus | +5 |

Default threshold: **65%**.

---

## Fixture replay & testing

```bash
cargo test

# Replay a specific fixture against in-memory database
./target/release/tpms-tracker \
  --replay tests/fixtures/parking_lot.json \
  --db :memory: \
  --assert-consistency
```

---

## Key constants

```rust
VEHICLE_EXPIRY              = 300s     (EezTire/TRW: 480s, AVE: 600s)
FINGERPRINT_MIN_SIGHTINGS   = 5
JACCARD_MERGE_THRESHOLD     = 0.60
MIN_PLAUSIBLE_PRESSURE_KPA  = 1.5
MIN_JITTER_SAMPLES          = 50
MAX_INTERVAL_SAMPLES        = 10_000
JITTER_RECOMPUTE_INTERVAL   = 300s
MAX_HAMMING_DISTANCE        = 3
MIN_TBF_SESSIONS            = 7
SESSION_GAP_THRESHOLD       = 600s
```

---

## Research roadmap

| Issue | Feature | Status |
|---|---|---|
| #61 | Temporal behavioural fingerprinting | In progress |
| #63 | RKE + TPMS multi-modal fusion | Designed |
| #36 | Meshtastic C2 integration | Designed |
| #64 | EezTire pressure unit fix | Merged |
| #66 | TRW-OOK/FSK decoder fix | Merged |
| #68 | Jansite pressure unit fix | Merged |
| #70 | TX interval jitter infrastructure | Merged |
| #74 | Rotating log file system | Merged |
| #77 | Raw interval collection upstream of correlator | Merged |
| #79 | Hamming-distance grouping for rolling-ID protocols | In test |

Target white paper: *Passive Vehicle Re-identification from 433 MHz TPMS:
Temporal Behavioural Fingerprinting, Physical-Layer Oscillator Signatures,
and Cross-Band Identity Fusion.*

---

## Scientific references

### Primary inspiration — TPMS passive tracking

**[1] Lizarribar, Y., Scalingi, A., Giustiniano, D., Sánchez Sánchez, P. M.,
Calvo-Palomino, R., Bovet, G., & Lenders, V. (2026).**
*Can't Hide Your Stride: Inferring Car Movement Patterns from Passive TPMS
Measurements.*
IEEE Wireless On-demand Network Systems and Services (WONS 2026).
IMDEA Networks Institute.
→ The baseline paper that established Jaccard co-occurrence grouping for
passive TPMS vehicle tracking. This project extends their approach with
rolling-ID resolution, physical-layer fingerprinting, and temporal
behavioural analysis.

### RF device fingerprinting (physical-layer jitter, issues #40–#43)

**[2] Danev, B., Luecke, T. S., & Capkun, S. (2009).**
*Transient-based identification of wireless sensor nodes.*
Proceedings of the 8th ACM/IEEE International Conference on Information
Processing in Sensor Networks (IPSN '09), pp. 25–36.
→ Foundational paper on physical-layer fingerprinting of wireless sensor
nodes using RF transient characteristics. Establishes the theoretical basis
for oscillator-based device identification.

**[3] Brik, V., Banerjee, S., Gruteser, M., & Oh, S. (2008).**
*Wireless device identification with radiometric signatures.*
Proceedings of the 14th ACM International Conference on Mobile Computing and
Networking (MobiCom '08), pp. 116–127.
→ First large-scale demonstration of 802.11 device fingerprinting using
clock offset, frequency error, and timing jitter. Motivates the
`acf_lag1` feature in the jitter profile.

**[4] Rasmussen, K. B., & Capkun, S. (2007).**
*Implications of radio fingerprinting on the security of sensor networks.*
Proceedings of the 3rd International Conference on Security and Privacy in
Communications Networks (SecureComm '07), pp. 331–340.
→ Analysis of radio fingerprinting applicability to sensor network security.
Establishes the discriminative power of crystal oscillator frequency offset
as a device identifier.

**[5] Xu, Q., Zheng, R., Saad, W., & Han, Z. (2016).**
*Device fingerprinting in wireless networks: Challenges and opportunities.*
IEEE Communications Surveys & Tutorials, 18(1), pp. 94–119.
→ Survey of RF device fingerprinting techniques including clock skew, carrier
frequency offset, and modulation imperfections. Used to calibrate expected
sigma ranges for TPMS crystal oscillators.

### TPMS security background

**[6] Rouf, I., Miller, R., Mustafa, H., Taylor, T., Oh, S., Xu, W.,
Gruteser, M., Trappe, W., & Seskar, I. (2010).**
*Security and privacy vulnerabilities of in-car wireless networks: A tire
pressure monitoring system case study.*
Proceedings of the 19th USENIX Security Symposium, pp. 323–338.
→ The first systematic security analysis of TPMS, demonstrating passive
tracking and active injection attacks. Establishes the threat model this
project operates within.

**[7] Sterne, K. T., Ernst, J. M., Kilcoyne, D. K., Michaels, A. J.,
& Moy, G. (2017).**
*Tire pressure monitoring system sensor radio frequency measurements for
privacy concerns.*
Transportation Research Record, 2643(1), pp. 10–17.
→ Empirical RF measurements of TPMS transmission characteristics including
frequency stability and timing — directly relevant to jitter fingerprinting.

### MAC randomisation and rolling ID evasion (related context)

**[8] Martin, J., Mayberry, T., Donahue, C., Foppe, L., Brown, L.,
Riggins, C., Rye, E. C., & Brown, D. (2017).**
*A study of MAC address randomization in mobile devices and when it fails.*
Proceedings on Privacy Enhancing Technologies (PETS 2017), pp. 365–383.
→ Motivates the analogy between WiFi MAC randomisation evasion and TPMS
rolling-ID resolution. The Hamming-distance approach in issue #43 draws on
the same category of techniques used to defeat MAC randomisation.

### Software references

**[9] Lukeswitz. (2024).**
*AntiHunter — ESP32-S3 WiFi/BLE perimeter defense with Meshtastic mesh.*
GitHub. https://github.com/lukeswitz/AntiHunter
→ Complementary project covering 2.4 GHz (WiFi/BLE) device detection.
Inspiration for cross-band fusion (issue #35) and Meshtastic C2
integration (issue #36).

**[10] merbanan. (2024).**
*rtl\_433 — Decode radio transmissions from devices operating on 433.9 MHz,
868 MHz, 315 MHz, 345 MHz, and 915 MHz ISM bands.*
GitHub. https://github.com/merbanan/rtl_433
→ Reference implementation for all 25 TPMS protocol decoders. Protocol
byte layouts, pressure unit encodings, and CRC polynomials are ported
directly from this codebase.

---

## Legal notice

Passive reception only. No transmission. The project does not interact
with any vehicle system. Verify local regulations regarding passive
reception of ISM-band RF signals before deployment.

---

## License

MIT.
