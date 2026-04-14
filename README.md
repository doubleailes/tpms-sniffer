# tpms-sniffer

A passive TPMS (Tire Pressure Monitoring System) sniffer and vehicle tracker written in Rust. Decodes over-the-air transmissions from 25 sensor protocols at 315/433 MHz using a low-cost RTL-SDR dongle, and correlates packets into stable vehicle tracks using pressure fingerprinting and transmission-interval analysis.

> **Legal notice:** Passive reception only. No transmission. Verify local regulations regarding the reception of automotive RF signals before use.

---

## Overview

Every car with direct TPMS (dTPMS) broadcasts tire pressure and temperature in cleartext over 315 or 433 MHz ISM radio. These transmissions are receivable up to 40–50 m away with a $30 RTL-SDR dongle. `tpms-sniffer` decodes them in real time and feeds a tracker that builds persistent vehicle identities from the data stream — even for modern sensors that rotate their identifiers to defeat tracking.

The tracker pipeline is:

```text
RTL-SDR (IQ stream)
  └─ Demodulator (OOK / FSK)
       └─ Framer (preamble detection)
            └─ Protocol decoders (25 protocols)
                 └─ Tracker (fingerprint correlator → vehicle UUIDs)
                      └─ SQLite store + JSON-L output
```

---

## Supported protocols

25 protocols ported from the [rtl_433](https://github.com/merbanan/rtl_433) reference implementation.

| Flag | Protocol | rtl_433 # | Vehicles | Freq | Modulation |
|---|---|---|---|---|---|
| `steelmate` | Steelmate TPMS | 59 | Generic | 315/433 | OOK-PWM |
| `schrader` | Schrader generic | 60 | Various | 315/433 | FSK-Manch |
| `citroen` | Citroën / Peugeot / Fiat VDO | 82 | Citroën, Peugeot, Fiat, Mitsubishi | 433 | FSK-Manch |
| `toyota` | Toyota PMV-C210 | 88 | Toyota Auris / Corolla / Lexus | 433 | FSK-DMC |
| `ford` | Ford / Continental VDO | 89 | Ford Fiesta / Focus / Kuga / Transit | 315/433 | FSK-Manch |
| `renault` | Renault / Dacia | 90 | Renault Clio / Captur / Zoe, Dacia | 433 | FSK-Manch |
| `schrader_eg53` | Schrader EG53MA4 | 95 | Saab, Opel, Vauxhall, Chevrolet | 315/433 | FSK-Manch |
| `toyota107j` | Toyota PMV-107J | 110 | Toyota (US market) | 315 | FSK-DMC |
| `jansite` | Jansite TY02S | 123 | Aftermarket | 433 | OOK |
| `elantra` | Hyundai Elantra 2012 | 140 | Hyundai Elantra | 433 | OOK |
| `abarth` | Abarth 124 Spider | 156 | Abarth 124 | 433 | FSK-Manch |
| `schrader_smd` | Schrader SMD3MA4 / 3039 | 168 | Subaru, Infiniti, Nissan, Renault | 433 | FSK-Manch |
| `jansite_solar` | Jansite Solar | 180 | Aftermarket solar | 433 | OOK |
| `hyundai_vdo` | Hyundai VDO | 186 | Hyundai, Kia, Genesis | 433 | FSK-Manch |
| `truck` | Solar / Truck TPMS | 201 | Trucks / heavy-duty aftermarket | 433 | FSK-Manch |
| `porsche` | Porsche Boxster / Cayman | 203 | Porsche Boxster, Cayman | 433 | FSK-Manch |
| `ave` | AVE TPMS | 208 | Aftermarket clip-on | 433 | OOK |
| `tyreguard` | TyreGuard 400 | 225 | Generic aftermarket | 433 | FSK-Manch |
| `eeztire` | EezTire E618 / Carchet / TST-507 | 241 | External clip-on | 433 | OOK |
| `bmw_gen45` | BMW Gen4/5 + Audi / HUF / Continental | 252 | BMW, Audi, VW | 433 | FSK-Manch |
| `bmw_gen23` | BMW Gen2/3 | 257 | BMW | 433 | FSK-Manch |
| `gm` | GM Aftermarket | 275 | GM US models | 315 | OOK |
| `airpuxem` | Airpuxem TYH11_EU6_ZQ | 295 | Aftermarket | 433 | FSK-Manch |
| `trw_ook` | TRW OOK OEM + clones | 298 | VW, Audi, Renault (OEM) | 433 | OOK |
| `trw_fsk` | TRW FSK OEM + clones | 299 | VW, Audi, Renault (OEM) | 433 | FSK-Manch |

Modulation codes: **OOK** = On-Off Keying, **FSK-Manch** = FSK + Manchester encoding, **FSK-DMC** = FSK + Differential Manchester encoding.

---

## Hardware requirements

- Any RTL-SDR compatible dongle (RTL2832U chipset, ~$25–35)
- Antenna tuned for 315 or 433 MHz ISM band
- Linux or macOS host

Reception range is typically 20–50 m in open-air conditions. NLOS (through walls) reception is possible at reduced range. A vehicle passing at 50 km/h within 20 m of the receiver will typically yield 3–5 packets per pass.

---

## Build

```bash
# Linux
sudo apt install libusb-1.0-0-dev

# macOS
brew install libusb

cargo build --release
```

---

## Usage

```bash
# EU band (433.92 MHz, default)
./target/release/tpms-sniffer

# US band (315 MHz)
./target/release/tpms-sniffer --freq 315000000

# Decode only Ford sensors
./target/release/tpms-sniffer --protocol ford

# JSON output — pipe to jq, InfluxDB, the tracker, etc.
./target/release/tpms-sniffer --json | jq .

# High gain, lower confidence threshold
./target/release/tpms-sniffer --gain 400 --confidence 50
```

---

## Output format

### Human-readable (default)

```
[2026-04-13 15:00:26] [241] EezTire/Carchet/TST-507  0xFFFFFFFF   51.1 kPa (  7.4 psi)  205.0 °C  conf=65% ⚠ALARM 🔋LOW  raw=[FF FF FF FF FB FF FF EF FF]
[2026-04-13 15:00:26] [208] AVE-TPMS                 0xFFFFBFFF  382.5 kPa ( 55.5 psi)  151.0 °C  conf=65%               raw=[FF FF BF FF FF BF]
[2026-04-13 15:02:22] [140] Hyundai-Elantra-2012      0xFFFFFFFF  253.0 kPa ( 36.7 psi)  215.0 °C  conf=75%               raw=[FF FF FF FF FD FF FF F7]
```

Each line contains: timestamp, protocol ID, protocol name, sensor ID, pressure (kPa and psi), temperature (°C), confidence score, alarm/battery flags, and raw bytes.

### JSON (`--json`)

```json
{
  "timestamp":    "2026-04-13 15:00:26.692",
  "protocol":     "EezTire/Carchet/TST-507",
  "rtl433_id":    241,
  "sensor_id":    "0xFFFFFFFF",
  "pressure_kpa": 51.1,
  "pressure_psi": 7.4,
  "temp_c":       null,
  "battery_ok":   false,
  "alarm":        true,
  "raw_hex":      "FF FF FF FF FB FF FF EF FF",
  "confidence":   65
}
```

Temperature is emitted as `null` when the sensor reports a sentinel value (≥ 200 °C) indicating the field is unavailable.

---

## Confidence scoring

Packets are scored before output. The default threshold is **65%** — adjust with `--confidence`.

| Condition | Points |
|---|---|
| CRC / checksum passes | +60 |
| Pressure in sane range | +20 |
| Pressure in typical tyre range (150–350 kPa) | +10 |
| Temperature in typical range (−20–80 °C) | +5 |
| Base bonus | +5 |

---

## Tracker

The tracker layer consumes JSON-L output from the sniffer and builds stable vehicle identities. Run it as a pipeline:

```bash
./target/release/tpms-sniffer --json | ./target/release/tpms-tracker
```

### How vehicle tracking works

Modern TPMS sensors (post-2018, EU mandate) rotate their sensor ID per packet to defeat passive tracking. The tracker handles both fixed-ID and rolling-ID sensors:

**Fixed-ID sensors** (Hyundai, TRW-OOK, EezTire, most OEM protocols): the sensor ID is stable over the lifetime of the sensor. The tracker maintains a `(sensor_id, protocol)` → `vehicle_uuid` map. IDs with fewer than 3 bits cleared from `0xFFFFFFFF` are treated as decode artifacts and routed to the fingerprint correlator instead.

**Rolling-ID sensors** (AVE-TPMS, post-2018 aftermarket): the sensor ID changes every packet. The tracker correlates these by **pressure fingerprint** — the median pressure reading for each active vehicle — and **transmission interval** — the median time between consecutive packets. Two sensors at 382.5 kPa are ambiguous; two sensors at 382.5 kPa with transmission intervals of 45 s and 71 s are distinct.

### Tracker output

```text
2026-04-13 17:05:34 | vehicle=7178d3ed-f88f-4f00-8ad0-6ca8dace30e5 | sensor=0xFEFFFFFD | 63.8 kPa | TRW-OOK
2026-04-13 17:06:47 | vehicle=7178d3ed-f88f-4f00-8ad0-6ca8dace30e5 | sensor=0xFEFFFFFD | 63.8 kPa | TRW-OOK
2026-04-13 17:11:16 | vehicle=9d4c2a96-6392-4087-ba33-30d7ebae8f90 | sensor=0xFFFF7FFF | 334.5 kPa | AVE-TPMS
```

Each sighting line carries a stable `vehicle` UUID that persists across ID rotations for the lifetime of the session.

### Tuning parameters

| Parameter | Default | Description |
|---|---|---|
| `PRESSURE_TOLERANCE_KPA` | 5.0 | Maximum pressure delta for fingerprint match |
| `VEHICLE_EXPIRY` | 300 s (EezTire/TRW: 480 s, AVE: 600 s) | Silence window before a track expires |
| `TX_INTERVAL_TOLERANCE_MS` | 8 000 | Transmission interval match tolerance |
| `TX_INTERVAL_MIN_SAMPLES` | 3 | Minimum intervals before TX matching is used |

---

## Architecture

```text
src/
├── demod.rs       OOK envelope + FSK FM-discriminator, clock recovery
├── framer.rs      Alternating (0xAA…) and Huf (0x00FF) preamble detection
├── manchester.rs  Manchester and Differential Manchester decoders
├── decoder.rs     25 protocol decoders ported from rtl_433
└── reporter.rs    Pretty-print and JSON-L output

tracker/
├── lib.rs         Vehicle resolver (fixed-ID map + fingerprint correlator)
├── store.rs       SQLite persistence layer
└── main.rs        JSON-L ingestion loop
```

---

## Database schema

The tracker persists sightings to a local SQLite database (`tpms.db` by default).

```sql
CREATE TABLE vehicles (
    vehicle_id            TEXT PRIMARY KEY,
    first_seen            TEXT,
    protocol              TEXT,
    rtl433_id             INTEGER,
    sensor_id             TEXT,
    make_model            TEXT,
    pressure_kpa          REAL,
    tx_interval_median_ms INTEGER
);

CREATE TABLE sightings (
    id           INTEGER PRIMARY KEY,
    vehicle_id   TEXT REFERENCES vehicles(vehicle_id),
    ts           TEXT,
    pressure_kpa REAL,
    temp_c       REAL,
    alarm        INTEGER,
    battery_ok   INTEGER,
    confidence   INTEGER,
    receiver_id  TEXT NOT NULL DEFAULT 'default',
    lat          REAL,
    lon          REAL
);
```

---

## Research context

This tool implements findings from:

> Lizarribar et al., **"Can't Hide Your Stride: Inferring Car Movement Patterns from Passive TPMS Measurements"** (2024). Five low-cost RTL-SDR receivers deployed over 10 weeks, 6 million packets, 20 000+ vehicles observed. Key result: a network of $100 receivers can reliably reconstruct work schedules, commute patterns, and behavioural routines from passive TPMS capture alone.

The tracker's Jaccard co-occurrence grouping (coming in a future release) is based directly on Section VI of that paper, which demonstrates 12/12 correct car identification using 1-minute co-occurrence windows.

Prior work: Rouf et al., **"Security and Privacy Vulnerabilities of In-Car Wireless Networks: A TPMS Case Study"**, USENIX Security 2010 — the original demonstration that TPMS enables passive vehicle tracking.

---

## Contributing

Issues and pull requests welcome. The backlog is tracked as GitHub issues — see the open issues for the current roadmap, which includes:

- Jaccard-index 4-tire vehicle grouping (#13)
- Multi-receiver deduplication and MQTT ingestion (#14)
- Long-term behavioural pattern analysis and GeoJSON export (#15)
- Common-byte prefix seeding for wheel-position inference (#16)

---

## License

MIT