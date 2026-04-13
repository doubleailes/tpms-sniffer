# tpms-sniffer v0.2

Passive TPMS sniffer in Rust, built on
[`rtlsdr-next`](https://github.com/mattdelashaw/rtlsdr-next).
Protocol details ported directly from
[`rtl_433`](https://github.com/merbanan/rtl_433) — the reference
implementation.

---

## Supported protocols (25 total)

| `--protocol` flag | Protocol / Sensor | rtl_433 # | Vehicles | Freq | Mod |
|---|---|---|---|---|---|
| `steelmate` | Steelmate TPMS | 59 | Generic | 315/433 | OOK-PWM |
| `schrader` | Schrader generic | 60 | Various | 315/433 | FSK-Manch |
| `citroen` | Citroen / Peugeot / Fiat VDO | 82 | Citroen, Peugeot, Fiat, Mitsubishi | 433 | FSK-Manch |
| `toyota` | Toyota PMV-C210 (Pacific Ind.) | 88 | Toyota Auris/Corolla/Lexus | 433 | FSK-DMC |
| `ford` | Ford / Continental VDO | 89 | Ford Fiesta/Focus/Kuga/Transit | 315/433 | FSK-Manch |
| `renault` | Renault / Dacia | 90 | Renault Clio/Captur/Zoe, Dacia | 433 | FSK-Manch |
| `schrader_eg53` | Schrader EG53MA4 | 95 | Saab, Opel, Vauxhall, Chevrolet | 315/433 | FSK-Manch |
| `toyota107j` | Toyota PMV-107J | 110 | Toyota (US market) | 315 | FSK-DMC |
| `jansite` | Jansite TY02S | 123 | Aftermarket / homebrew | 433 | OOK |
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

Modulation codes: **OOK** = On-Off Keying, **FSK-Manch** = FSK + Manchester,
**FSK-DMC** = FSK + Differential Manchester.

---

## Architecture

```
rtlsdr-next (async IQ stream)
       │
       ├─ OokDemod ──┐
       │              ├─▶ Framer ─▶ decode() ─▶ Reporter
       └─ FskDemod ──┘
```

| File | Responsibility |
|---|---|
| `src/demod.rs` | OOK envelope + FSK FM-discriminator, clock recovery |
| `src/framer.rs` | Alternating (0xAA…) and Huf (0x00FF) preamble detect |
| `src/manchester.rs` | Manchester and Differential Manchester decoders |
| `src/decoder.rs` | 25 protocol decoders ported from rtl_433 |
| `src/reporter.rs` | Pretty-print and JSON-line output |

---

## Build & run

```bash
# system deps
sudo apt install libusb-1.0-0-dev    # Linux
brew install libusb                  # macOS

cargo build --release

# EU (default)
./target/release/tpms-sniffer

# US
./target/release/tpms-sniffer --freq 315000000

# decode only Ford sensors
./target/release/tpms-sniffer --protocol ford

# JSON output (pipe to jq, InfluxDB, etc.)
./target/release/tpms-sniffer --json | jq .

# high gain, low confidence threshold
./target/release/tpms-sniffer --gain 400 --confidence 50
```

---

## Sample output

```
[2026-04-10 14:32:01.123] [ 89] Ford-VDO                    0x1A2B3C4D   231.2 kPa ( 33.5 psi)    21.0 °C  conf= 95% [moving]  raw=[1A 2B 3C 4D 8C D6 44 7E]
[2026-04-10 14:32:01.445] [ 82] Citroen/Peugeot/Fiat        0xABCD1234   218.4 kPa ( 31.7 psi)    20.0 °C  conf= 90%           raw=[00 AB CD 12 34 0C 9E 46 00 XX]
[2026-04-10 14:32:02.001] [ 88] Toyota-PMV-C210             0x0D5AEE30   234.4 kPa ( 34.0 psi)    25.0 °C  conf= 95%           raw=[0D 5A EE 30 80 8C 41 00 7B 0C]
[2026-04-10 14:32:03.200] [252] BMW-Gen4/5+Audi             0xDEADBEEF   230.0 kPa ( 33.4 psi)    22.0 °C  conf= 90%  ⚠ALARM   raw=[DE AD BE EF 01 94 48 02 FF A3]
```

---

## JSON schema

```json
{
  "timestamp":    "2026-04-10 14:32:01.123",
  "protocol":     "Ford-VDO",
  "rtl433_id":    89,
  "sensor_id":    "0x1A2B3C4D",
  "pressure_kpa": 231.2,
  "pressure_psi": 33.5,
  "temp_c":       21.0,
  "battery_ok":   true,
  "flags":        68,
  "moving":       true,
  "alarm":        false,
  "raw_hex":      "1A 2B 3C 4D 8C D6 44 7E",
  "confidence":   95
}
```

---

## Confidence scoring

| Condition | Points |
|---|---|
| CRC / checksum passes | +60 |
| Pressure + temp in sane range | +20 |
| Pressure in typical tyre range 150–350 kPa | +10 |
| Temperature in typical tyre range −20–80 °C | +5 |
| Base bonus | +5 |

Default threshold: **65%** (use `--confidence` to adjust).

---

## Legal notice

Passive reception only. No transmission. Verify local regulations
regarding reception of automotive RF signals before use.

## License

Apache-2.0 (matching rtlsdr-next upstream).
