# 🛞📡 tpms-sniffer

> *Your tyres are talking. Have been for years. We just started listening.*

Every modern car has four tiny radios bolted inside its wheels. Their whole job is to shout
**"I'M AT 230 kPa AND 18 °C!"** into the void at 433 MHz, roughly once a minute, forever, to anyone
who cares to hear. Spoiler: we care.

**tpms-sniffer** is a passive TPMS (Tyre Pressure Monitoring System) tracking and fingerprinting toolkit
written in Rust 🦀. It sits quietly on a Raspberry Pi with an antenna, listens to tyres gossip, and slowly
works out *who's who* — even when the sensors try to be sneaky and change their IDs on every packet.

Built on [`rtlsdr-next`](https://github.com/mattdelashaw/rtlsdr-next), with protocol details lovingly
ported from the legendary [`rtl_433`](https://github.com/merbanan/rtl_433).

> 🎓 **Serious-business corner:** this project is the foundation for a white paper on passive vehicle
> fingerprinting from 433 MHz TPMS transmissions — combining pressure signatures, temporal behavioural
> patterns, and crystal oscillator jitter for persistent identity tracking across rolling sensor ID
> rotations. Yes, the fun tone stops at the README. The maths does not joke.

---

## 🧭 Table of contents

- [How it works (the 10-second version)](#-how-it-works-the-10-second-version)
- [Supported protocols — the guest list](#-supported-protocols--the-guest-list)
- [Shopping list](#-shopping-list)
- [Build & run](#-build--run)
- [The three-layer identity onion](#-the-three-layer-identity-onion)
- [Catching the shapeshifters (rolling IDs)](#-catching-the-shapeshifters-rolling-ids)
- [Is it a scooter or a lorry?](#-is-it-a-scooter-or-a-lorry)
- [Physical-layer fingerprinting](#-physical-layer-fingerprinting-aka-every-crystal-has-a-heartbeat)
- [Temporal behavioural fingerprinting](#-temporal-behavioural-fingerprinting-aka-creature-of-habit)
- [Web dashboard](#-web-dashboard)
- [Database](#-database)
- [Testing & fixture replay](#-testing--fixture-replay)
- [Roadmap](#-roadmap)
- [References](#-scientific-references-the-giants-whose-shoulders-we-stand-on)
- [Legal stuff](#-legal-stuff-please-read-it-we-mean-it)

---

## ⚡ How it works (the 10-second version)

```text
   🛞 🛞 🛞 🛞   "psst… 230 kPa… 18°C… battery ok…"
        │
        ▼  433.92 MHz
   📡 RTL-SDR dongle
        │
        └─ tpms-sniffer      IQ → demod → framing → 25 protocol decoders
               │  (JSON lines, one per packet)
               ▼
           tpms-tracker      the detective 🕵️
               ├─ Resolver         rolling-ID correlation + fingerprint store
               ├─ Jaccard grouper  "these four tyres always show up together… suspicious"
               ├─ Jitter engine    oscillator fingerprinting
               ├─ TBF engine       temporal behavioural fingerprinting
               ├─ Web dashboard    axum, at http://pi:8080
               └─ tpms.db          SQLite (WAL mode)
```

Two crates, one pipe, zero transmissions:

|            Crate | What it does                                                         |
|------------------|----------------------------------------------------------------------|
| `crates/sniffer` | The ears 👂 — IQ demodulation, framing, 25 decoders, JSON reporter   |
| `crates/tracker` | The brain 🧠 — resolver, SQLite, fingerprinting, analytics, web UI   |

---

## 🎟️ Supported protocols — the guest list

25 protocols, from posh Porsches to humble clip-on aftermarket gadgets. Everybody's welcome at this party.

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

**Modulation decoder ring:** **OOK** = On-Off Keying (the radio equivalent of blinking a torch),
**FSK-Manch** = FSK + Manchester, **FSK-DMC** = FSK + Differential Manchester.

---

## 🛒 Shopping list

| Component | Model | Why |
|---|---|---|
| SBC | Raspberry Pi 4 | 4 GB RAM, DietPi OS. Small, cheap, runs for months unattended. |
| SDR | RTL-SDR V3 | TCXO reference, so *our* crystal doesn't wobble while we measure *theirs*. |
| Antenna | Sirio HGO 433 & 868 | 2 dBi @ 433 MHz, 4 dBi @ 868 MHz, outdoor. Bigger ears, more gossip. |

The antenna covers both 433 MHz (TPMS, RKE) and 868 MHz (newer EU vehicles), so a second RTL-SDR dongle
can join the party later for dual-band capture.

---

## 🚀 Build & run

```bash
# 1. Feed the system some dependencies
sudo apt install libusb-1.0-0-dev pkg-config

# 2. Summon the crab 🦀
cargo build --release

# 3. Plug the ears into the brain
./target/release/tpms-sniffer --json | \
./target/release/tpms-tracker \
  --db tpms.db \
  --serve 0.0.0.0:8080 \
  --log-level info \
  --log-file logs/tpms-tracker.log
```

Too much typing? `./start.sh` does it for you. Want it to survive reboots like a cockroach? There's a
systemd unit in [`deploy/tpms-tracker.service`](deploy/tpms-tracker.service).

### 🎛️ Sniffer knobs

```text
-d, --device <N>          RTL-SDR device index                 [default: 0]
-f, --freq <HZ>           Centre frequency (315 MHz US / 433.92 MHz EU) [default: 433920000]
-r, --rate <HZ>           Sample rate                          [default: 250000]
-g, --gain <TENTHS_DB>    RF gain in tenths of dB, 0 = auto    [default: 0]
    --protocol <NAME>     Only decode this protocol            [default: all]
    --confidence <PCT>    Minimum confidence 0–100             [default: 65]
    --json                Emit JSON lines (required for the tracker pipe)
    --duration-secs <S>   Stop after S seconds (great for fixture captures)
```

### 🎛️ Tracker knobs

```text
    --db <PATH>              SQLite database (created if missing) [default: tpms.db]
    --serve <ADDR>           Start the web dashboard (e.g. 0.0.0.0:8080)
    --log-level <LEVEL>      info | debug | trace (RUST_LOG wins if set) [default: info]
    --log-file <PATH>        Rotating log file, alongside stderr
    --log-max-size <MB>      Rotate at this size                  [default: 10]
    --log-max-files <N>      Rotated files to keep                [default: 5]
    --confidence <PCT>       Minimum confidence to process        [default: 65]
    --receiver-id <ID>       Name this node for multi-receiver setups [default: default]
    --replay <PATH>          Replay a .jsonl / .jsonl.gz fixture instead of stdin
    --assert-consistency     After replay, fail loudly if anything looks off
    --export-jaccard <PATH>  Dump the co-occurrence matrix as JSON
-v, --verbose                Print a line for every resolved packet
```

Plus some subcommands for the curious:

```bash
tpms-tracker report [--car <ID>] [--from ISO] [--to ISO] [--json] [--temporal]   # 📋 behavioural reports
tpms-tracker backfill                                                            # 🧹 rebuild presence slots
tpms-tracker geojson [--car <ID>] [-o out.geojson]                               # 🗺️ export paths
```

---

## 🧅 The three-layer identity onion

```text
fingerprint_id   "this is THE sensor"            permanent, cross-session
    │
    └─ vehicle_id   "this is the sensor… today"  session-scoped, expires after VEHICLE_EXPIRY
           │
           └─ sightings   "it said something!"   individual decoded packets
```

### 🚫 Sentinel detection

Not everything that decodes is a tyre. Valid sensor IDs must satisfy `count_zeros(id) >= 3`, so all-ones
noise like `0xFFFFFFFF` is shown the door before it can pollute the fingerprint store.

### 🧮 Confidence scoring

Every packet has to earn its place:

| Condition | Points |
|---|---|
| CRC / checksum passes | +60 |
| Pressure + temp in a sane range | +20 |
| Pressure in typical tyre range 150–350 kPa | +10 |
| Temperature in -20–80 °C | +5 |
| Participation trophy 🏅 | +5 |

Default bouncer threshold: **65%**.

---

## 🦎 Catching the shapeshifters (rolling IDs)

EezTire, TRW-OOK and TRW-FSK sensors are drama queens: they change their `sensor_id` on **every single
transmission** using a bit-flip rolling scheme. Cute. Doesn't work.

The fingerprint correlator resolves them by matching on **protocol + pressure delta**. The TX interval is a
tiebreaker only — never a veto — so a slightly late packet can't get a sensor disowned.

### 👨‍👩‍👧‍👦 Jaccard vehicle grouper

Four tyres that keep turning up in the same 60-second windows are probably the same car (or a *very*
coordinated flash mob). Sensors are grouped into car entities when their co-occurrence crosses
`JACCARD_MERGE_THRESHOLD = 0.60`.

---

## 🛵🚚 Is it a scooter or a lorry?

Tyre pressure gives the game away:

| Class | Pressure range | Typical suspects |
|---|---|---|
| Motorcycle | 150–200 kPa | Scooters, motorcycles |
| PassengerCar | 200–280 kPa | Cars, small SUVs |
| SuvLightTruck | 260–340 kPa | SUVs, crossovers |
| LightCommercialVan | 350–480 kPa | That delivery van that's always double-parked |
| HeavyTruck | 550–800 kPa | HGVs |

Some protocols get overrides where pressure lies (Jansite-Solar → always Motorcycle).

### 🔧 Pressure unit corrections

Several protocols encode PSI while earlier decoders swore it was something else. We fixed the maths:

| Protocol | Raw unit | Multiplier to kPa |
|---|---|---|
| EezTire (241) | 0.1 PSI/unit | × 0.68948 |
| TRW OOK/FSK (298/299) | 0.4 PSI/unit | × 2.75790 |
| Jansite (123/180) | 0.25 PSI/unit | × 1.7 |
| AVE TPMS (208) | 1.5 kPa/unit | already correct ✅ |
| Hyundai Elantra (140) | 1 kPa/unit | already correct ✅ |

`scripts/migrate.py` retro-fixes historical data whenever a decoder gets corrected — no sighting left behind.

---

## 💓 Physical-layer fingerprinting (a.k.a. every crystal has a heartbeat)

A sensor can change its ID. It can't change its **crystal oscillator**. The tiny timing wobble in when a
sensor transmits is a hardware quirk that stays stable for its whole life — through ID rotations, pressure
changes, the lot. We measure that wobble.

### Interval collection

- **Fixed-ID protocols** (Jansite, Hyundai, AVE): consecutive packets from the same `sensor_id`. Easy mode.
- **Rolling-ID protocols** (EezTire, TRW): consecutive packets are paired using **Hamming distance** between
  successive IDs. Same-sensor pairs differ by 1–3 bits; strangers differ by ~16 bits on average.
  Shapeshifters, but bad at it.

```text
MAX_HAMMING_DISTANCE  = 3            same-sensor threshold
MAX_PRESSURE_DELTA    = 5 kPa        extra cross-sensor guard
RAW_INTERVAL_MIN      = 5,000 ms     reject burst duplicates
RAW_INTERVAL_MAX      = 300,000 ms   reject cross-session gaps
```

### Jitter statistics (unlocked after 50 samples)

| Metric | What it's telling you |
|---|---|
| `sigma_ms` | Oscillator stability — expect 1–30 ms for a ±20 ppm crystal |
| `skewness` | Is the timing distribution lopsided? |
| `kurtosis` | Tail weight — high for sensors doing collision-avoidance retransmits |
| `acf_lag1` | Lag-1 autocorrelation — negative means active frequency correction |

---

## 🕰️ Temporal behavioural fingerprinting (a.k.a. creature of habit)

Cars are like cats: they have routines. Each fingerprint builds a behavioural profile from `session_log`:

- **Arrival distribution** — Gaussian mixture over hour-of-day ("always here at 08:47, bless")
- **Dwell time** — log-normal (drive-by median < 30 s vs. parked > 1 h)
- **Periodicity** — dominant cycle via autocorrelation (daily / weekly)
- **Presence map** — a 24×7 probability matrix

Needs at least 7 sessions. Tells apart **commuters** (bimodal arrival, zero dwell, 24 h period) from
**couch potatoes** (flat arrival, hours of dwell, no period) from **tourists** (one drive-by and gone).

---

## 📊 Web dashboard

Pass `--serve` and point a browser at `http://pi:8080`.

| Endpoint | What you get |
|---|---|
| `GET /` | The embedded dashboard |
| `GET /api/stats` | Summary statistics |
| `GET /api/cars` | Every tracked vehicle |
| `GET /api/cars/:id` | One vehicle, in detail |
| `GET /api/fingerprints` | All persistent fingerprints |
| `GET /api/fingerprints/:id/jitter` | Jitter profile + histogram |

---

## 🗄️ Database

SQLite in WAL mode. Current schema version: **v9**.

| Table | Contents |
|---|---|
| `sightings` | Individual decoded packets |
| `vehicles` | Session-scoped vehicle records |
| `cars` | Jaccard-grouped car entities |
| `fingerprints` | Persistent sensor identities + jitter columns |
| `session_log` | Per-session start/end/dwell for TBF |
| `temporal_fingerprints` | Computed TBF profiles |
| `interval_samples` | Raw TX intervals, ring buffer (10k rows per fingerprint) |
| `rke_events` | Remote Keyless Entry events (coming soon™, issue #35) |
| `vehicle_state_transitions` | Arrival/departure from RKE fusion (planned) |
| `schema_migrations` | Applied migration history |

### Migrations

```bash
python3 scripts/migrate.py tpms.db            # apply everything pending
python3 scripts/migrate.py tpms.db --dry-run  # look before you leap
python3 scripts/migrate.py tpms.db --step 6   # stop at v6
```

A timestamped backup is taken before any write, because we've all been there.

### 📝 Structured logging

```text
2026-04-26 18:41:39 | RESOLVE | sensor=0xFFFFFFFF | zeros=0 | valid=false
  | path=fingerprint_correlator | candidates=2 | matched=c7e69c9d
  | match_reason=pressure_delta:0.0_kpa | protocol_filter=pass
  | pressure_before=352.3 | pressure_after=352.3
```

With `--log-file`, logs rotate by size with configurable history — perfect for post-hoc jitter and interval
archaeology without poking the database.

---

## 🧪 Testing & fixture replay

```bash
cargo test

# Replay a real-world capture against an in-memory DB and make sure nothing's wonky
./target/release/tpms-tracker \
  --replay tests/fixtures/wild/session_20260415_1000.jsonl.gz \
  --db :memory: \
  --assert-consistency
```

Want to contribute a capture? See [`tests/fixtures/README.md`](tests/fixtures/README.md) for the recipe.
Tracker internals (per-protocol expiry, TX-interval matching, class tolerances) are documented in
[`crates/tracker/README.md`](crates/tracker/README.md).

### 🔩 Key constants

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

## 🗺️ Roadmap

| Issue | Feature | Status |
|---|---|---|
| #61 | Temporal behavioural fingerprinting | 🚧 In progress |
| #63 | RKE + TPMS multi-modal fusion | 📐 Designed |
| #36 | Meshtastic C2 integration | 📐 Designed |
| #64 | EezTire pressure unit fix | ✅ Merged |
| #66 | TRW-OOK/FSK decoder fix | ✅ Merged |
| #68 | Jansite pressure unit fix | ✅ Merged |
| #70 | TX interval jitter infrastructure | ✅ Merged |
| #74 | Rotating log file system | ✅ Merged |
| #77 | Raw interval collection upstream of correlator | ✅ Merged |
| #79 | Hamming-distance grouping for rolling-ID protocols | 🧪 In test |

Target white paper: *Passive Vehicle Re-identification from 433 MHz TPMS: Temporal Behavioural
Fingerprinting, Physical-Layer Oscillator Signatures, and Cross-Band Identity Fusion.*

---

## 📚 Scientific references (the giants whose shoulders we stand on)

### Primary inspiration — TPMS passive tracking

**[1] Lizarribar, Y., Scalingi, A., Giustiniano, D., Sánchez Sánchez, P. M., Calvo-Palomino, R., Bovet, G.,
& Lenders, V. (2026).** *Can't Hide Your Stride: Inferring Car Movement Patterns from Passive TPMS
Measurements.* IEEE Wireless On-demand Network Systems and Services (WONS 2026). IMDEA Networks Institute.
→ The baseline paper that established Jaccard co-occurrence grouping for passive TPMS vehicle tracking. This
project extends their approach with rolling-ID resolution, physical-layer fingerprinting, and temporal
behavioural analysis.

### RF device fingerprinting (physical-layer jitter, issues #40–#43)

**[2] Danev, B., Luecke, T. S., & Capkun, S. (2009).** *Transient-based identification of wireless sensor
nodes.* IPSN '09, pp. 25–36.
→ Foundational work on physical-layer fingerprinting of wireless sensor nodes via RF transients.

**[3] Brik, V., Banerjee, S., Gruteser, M., & Oh, S. (2008).** *Wireless device identification with
radiometric signatures.* MobiCom '08, pp. 116–127.
→ First large-scale 802.11 fingerprinting using clock offset, frequency error, and timing jitter. Motivates
the `acf_lag1` feature.

**[4] Rasmussen, K. B., & Capkun, S. (2007).** *Implications of radio fingerprinting on the security of
sensor networks.* SecureComm '07, pp. 331–340.
→ Establishes crystal oscillator frequency offset as a discriminative device identifier.

**[5] Xu, Q., Zheng, R., Saad, W., & Han, Z. (2016).** *Device fingerprinting in wireless networks:
Challenges and opportunities.* IEEE Communications Surveys & Tutorials, 18(1), pp. 94–119.
→ Survey used to calibrate expected sigma ranges for TPMS crystal oscillators.

### TPMS security background

**[6] Rouf, I., Miller, R., Mustafa, H., Taylor, T., Oh, S., Xu, W., Gruteser, M., Trappe, W., & Seskar, I.
(2010).** *Security and privacy vulnerabilities of in-car wireless networks: A tire pressure monitoring
system case study.* 19th USENIX Security Symposium, pp. 323–338.
→ The OG TPMS security analysis — passive tracking and active injection. Defines the threat model we live in.

**[7] Sterne, K. T., Ernst, J. M., Kilcoyne, D. K., Michaels, A. J., & Moy, G. (2017).** *Tire pressure
monitoring system sensor radio frequency measurements for privacy concerns.* Transportation Research
Record, 2643(1), pp. 10–17.
→ Empirical RF measurements of TPMS frequency stability and timing — directly relevant to jitter work.

### MAC randomisation and rolling-ID evasion

**[8] Martin, J., Mayberry, T., Donahue, C., Foppe, L., Brown, L., Riggins, C., Rye, E. C., & Brown, D.
(2017).** *A study of MAC address randomization in mobile devices and when it fails.* PETS 2017,
pp. 365–383.
→ The WiFi cousin of our rolling-ID problem. The Hamming-distance approach in issue #43 borrows from the same
toolbox.

### Software references

**[9] Lukeswitz. (2024).** *AntiHunter — ESP32-S3 WiFi/BLE perimeter defense with Meshtastic mesh.*
<https://github.com/lukeswitz/AntiHunter>
→ The 2.4 GHz sibling. Inspiration for cross-band fusion (#35) and Meshtastic C2 (#36).

**[10] merbanan. (2024).** *rtl\_433 — Decode radio transmissions from devices on the 433.9 / 868 / 315 /
345 / 915 MHz ISM bands.* <https://github.com/merbanan/rtl_433>
→ Reference implementation for all 25 decoders. Byte layouts, pressure encodings and CRC polynomials are
ported straight from here. Eternal gratitude. 🙏

---

## ⚖️ Legal stuff (please read it, we mean it)

**Passive reception only.** This project never transmits a single bit and never interacts with any vehicle
system — it just listens. Check your local regulations on passive reception of ISM-band RF signals before
deploying, and be a decent human about what you do with the data. Tyres deserve privacy too. Well. Their
owners do.

---

## 📜 License

MIT. Go forth and listen responsibly. 🎧
