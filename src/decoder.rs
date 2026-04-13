// ============================================================
//  decoder.rs — Multi-protocol TPMS decoder
//
//  All protocol details ported from rtl_433 source:
//  https://github.com/merbanan/rtl_433/src/devices/
//
//  Protocols (rtl_433 number in brackets):
//   [59]  Steelmate          OOK PWM, 315/433 MHz
//   [60]  Schrader generic   FSK Manchester, 315/433 MHz
//   [82]  Citroen/Peugeot/Fiat FSK Manchester, 433 MHz
//   [88]  Toyota PMV-C210    FSK Diff-Manchester, 433 MHz
//   [89]  Ford (VDO)         FSK Manchester, 315/433 MHz
//   [90]  Renault/Dacia      FSK Manchester, 433 MHz
//   [95]  Schrader EG53MA4   FSK Manchester, 315/433 MHz
//  [110]  Toyota PMV-107J    FSK Diff-Manchester, 315 MHz
//  [123]  Jansite TY02S      OOK, 433 MHz
//  [140]  Elantra 2012       OOK, 433 MHz
//  [156]  Abarth 124         FSK Manchester, 433 MHz
//  [168]  Schrader SMD3MA4   FSK Manchester, 433 MHz (Subaru/Infiniti/Nissan)
//  [180]  Jansite Solar      OOK, 433 MHz
//  [186]  Hyundai VDO        FSK Manchester, 433 MHz
//  [201]  Solar/Truck TPMS   FSK Manchester, 433 MHz
//  [203]  Porsche Boxster    FSK Manchester, 433 MHz
//  [208]  AVE TPMS           OOK, 433 MHz
//  [225]  TyreGuard 400      FSK Manchester, 433 MHz
//  [241]  EezTire/Carchet    OOK, 433 MHz
//  [252]  BMW Gen4/5 + Audi  FSK Manchester, 433 MHz
//  [257]  BMW Gen2/3         FSK Manchester, 433 MHz
//  [275]  GM Aftermarket     OOK, 315 MHz
//  [295]  Airpuxem TYH11     FSK Manchester, 433 MHz
//  [298]  TRW OOK            OOK, 433 MHz
//  [299]  TRW FSK            FSK Manchester, 433 MHz
// ============================================================

use crate::manchester::{differential_manchester_decode, manchester_decode};
use chrono::Local;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq)]
pub enum TpmsProtocol {
    Schrader,
    SchraderEg53ma4,
    SchraderSmd3ma4,
    Ford,
    Citroen,
    Toyota,
    ToyotaPmv107j,
    Renault,
    Steelmate,
    Jansite,
    JansiteSolar,
    Elantra,
    Abarth,
    HyundaiVdo,
    BmwGen45,
    BmwGen23,
    Porsche,
    Ave,
    TyreGuard,
    EezTire,
    GmAftermarket,
    TrwOok,
    TrwFsk,
    Airpuxem,
    SolarTruck,
    All,
}

#[derive(Debug, Clone, Serialize)]
pub struct TpmsPacket {
    pub timestamp: String,
    pub protocol: String,
    pub rtl433_id: u16,
    pub sensor_id: String,
    pub pressure_kpa: f32,
    pub pressure_psi: f32,
    pub temp_c: Option<f32>,
    pub battery_ok: Option<bool>,
    pub flags: Option<u8>,
    pub moving: Option<bool>,
    pub alarm: Option<bool>,
    pub raw_hex: String,
    pub confidence: u8,
}

// ─── Entry point ─────────────────────────────────────────────

pub fn decode(bits: &[u8], protocol: &TpmsProtocol, min_confidence: u8) -> Option<TpmsPacket> {
    let all: &[fn(&[u8]) -> Option<TpmsPacket>] = &[
        decode_ford,
        decode_citroen,
        decode_toyota,
        decode_toyota_pmv107j,
        decode_renault,
        decode_schrader_eg53ma4,
        decode_schrader_smd3ma4,
        decode_schrader_generic,
        decode_steelmate,
        decode_jansite,
        decode_jansite_solar,
        decode_elantra,
        decode_abarth,
        decode_hyundai_vdo,
        decode_bmw_gen45,
        decode_bmw_gen23,
        decode_porsche,
        decode_ave,
        decode_tyreguard,
        decode_eeztire,
        decode_gm_aftermarket,
        decode_trw_ook,
        decode_trw_fsk,
        decode_airpuxem,
        decode_solar_truck,
    ];

    let fns: Vec<fn(&[u8]) -> Option<TpmsPacket>> = match protocol {
        TpmsProtocol::All => all.to_vec(),
        TpmsProtocol::Ford => vec![decode_ford],
        TpmsProtocol::Citroen => vec![decode_citroen],
        TpmsProtocol::Toyota => vec![decode_toyota],
        TpmsProtocol::ToyotaPmv107j => vec![decode_toyota_pmv107j],
        TpmsProtocol::Renault => vec![decode_renault],
        TpmsProtocol::Schrader => vec![decode_schrader_generic],
        TpmsProtocol::SchraderEg53ma4 => vec![decode_schrader_eg53ma4],
        TpmsProtocol::SchraderSmd3ma4 => vec![decode_schrader_smd3ma4],
        TpmsProtocol::Steelmate => vec![decode_steelmate],
        TpmsProtocol::Jansite => vec![decode_jansite],
        TpmsProtocol::JansiteSolar => vec![decode_jansite_solar],
        TpmsProtocol::Elantra => vec![decode_elantra],
        TpmsProtocol::Abarth => vec![decode_abarth],
        TpmsProtocol::HyundaiVdo => vec![decode_hyundai_vdo],
        TpmsProtocol::BmwGen45 => vec![decode_bmw_gen45],
        TpmsProtocol::BmwGen23 => vec![decode_bmw_gen23],
        TpmsProtocol::Porsche => vec![decode_porsche],
        TpmsProtocol::Ave => vec![decode_ave],
        TpmsProtocol::TyreGuard => vec![decode_tyreguard],
        TpmsProtocol::EezTire => vec![decode_eeztire],
        TpmsProtocol::GmAftermarket => vec![decode_gm_aftermarket],
        TpmsProtocol::TrwOok => vec![decode_trw_ook],
        TpmsProtocol::TrwFsk => vec![decode_trw_fsk],
        TpmsProtocol::Airpuxem => vec![decode_airpuxem],
        TpmsProtocol::SolarTruck => vec![decode_solar_truck],
    };

    fns.iter()
        .filter_map(|f| f(bits))
        .filter(|p| p.confidence >= min_confidence)
        .max_by_key(|p| p.confidence)
}

// ─── Helpers ─────────────────────────────────────────────────

fn now() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}
fn hex_bytes(b: &[u8]) -> String {
    b.iter()
        .map(|x| format!("{x:02X}"))
        .collect::<Vec<_>>()
        .join(" ")
}
fn crc8(data: &[u8], init: u8) -> u8 {
    let mut c = init;
    for &b in data {
        c ^= b;
        for _ in 0..8 {
            c = if c & 0x80 != 0 {
                (c << 1) ^ 0x07
            } else {
                c << 1
            };
        }
    }
    c
}
fn psi_to_kpa(p: f32) -> f32 {
    p * 6.894_757
}
fn kpa_to_psi(k: f32) -> f32 {
    k * 0.145_038
}

fn raw_bits_to_bytes(bits: &[u8]) -> Vec<u8> {
    bits.chunks(8)
        .filter(|c| c.len() == 8)
        .map(|c| c.iter().fold(0u8, |a, &x| (a << 1) | (x & 1)))
        .collect()
}

fn score(crc_ok: bool, sane: bool, kpa: f32, temp: Option<f32>) -> u8 {
    let mut s = 0u8;
    if crc_ok {
        s += 60;
    }
    if sane {
        s += 20;
    }
    if (150.0..=350.0).contains(&kpa) {
        s += 10;
    }
    if let Some(t) = temp {
        if (-20.0..=80.0).contains(&t) {
            s += 5;
        }
    }
    s.saturating_add(5).min(100)
}

fn pkt(
    proto: &str,
    rid: u16,
    id: u32,
    kpa: f32,
    temp: Option<f32>,
    batt: Option<bool>,
    flags: Option<u8>,
    moving: Option<bool>,
    alarm: Option<bool>,
    raw: &[u8],
    conf: u8,
) -> TpmsPacket {
    TpmsPacket {
        timestamp: now(),
        protocol: proto.into(),
        rtl433_id: rid,
        sensor_id: format!("0x{id:08X}"),
        pressure_kpa: kpa,
        pressure_psi: kpa_to_psi(kpa),
        temp_c: temp,
        battery_ok: batt,
        flags,
        moving,
        alarm,
        raw_hex: hex_bytes(raw),
        confidence: conf,
    }
}

// ═══════════════════════════════════════════════════════════
//  [89] Ford / Continental VDO
//  FSK, Manchester 52µs/bit, preamble 55 55 55 56 (inv: AA AA AA A9)
//  8 bytes: [ID:4][P:1][T:1][F:1][SUM:1]
//  P: PSI*4 (+ F[5] as 9th bit)   T: C+56 (when T[7]==0)
//  SUM = bytes 0..6 summed
// ═══════════════════════════════════════════════════════════
fn decode_ford(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 8 {
        return None;
    }
    let sum = (0..7usize).fold(0u8, |a, i| a.wrapping_add(b[i]));
    let crc_ok = sum == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let psibits = (((b[6] as u16 & 0x20) << 3) | b[4] as u16) as f32;
    let kpa = psi_to_kpa(psibits * 0.25);
    let temp = if b[5] & 0x80 == 0 {
        Some((b[5] & 0x7f) as f32 - 56.0)
    } else {
        None
    };
    let moving = Some(b[6] & 0x44 == 0x44);
    let sane = (0.0..=500.0).contains(&kpa);
    Some(pkt(
        "Ford-VDO",
        89,
        id,
        kpa,
        temp,
        None,
        Some(b[6]),
        moving,
        None,
        &b[..8],
        score(crc_ok, sane, kpa, temp),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [82] Citroen / Peugeot / Fiat (VDO)
//  FSK, Manchester 52µs, preamble AA AA AA A9
//  10 bytes: [state:1][ID:4][FR:1][P:1][T:1][bat?:1][XOR:1]
//  XOR bytes 1..9 == 0
//  P: kPa * 1.364    T: C + 50
// ═══════════════════════════════════════════════════════════
fn decode_citroen(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 10 {
        return None;
    }
    let xor = (1..10usize).fold(0u8, |a, i| a ^ b[i]);
    let crc_ok = xor == 0 && b[6] != 0 && b[7] != 0;
    let id = u32::from_be_bytes([b[1], b[2], b[3], b[4]]);
    let kpa = b[6] as f32 * 1.364;
    let temp = b[7] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Citroen/Peugeot/Fiat",
        82,
        id,
        kpa,
        Some(temp),
        Some(b[8] < 0x10),
        Some(b[5] >> 4),
        None,
        None,
        &b[..10],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [88] Toyota PMV-C210 (Pacific Industries / TRW)
//  FSK, Differential Manchester 52µs, 433 MHz
//  9 bytes after DMC: [ID:4][S:5bits][P:8][T:8][S2:7bits][P_inv:8][CRC8:8]
//  P: PSI/4 - 7    T: C + 40    CRC-8 poly 0x07 init 0x80
// ═══════════════════════════════════════════════════════════
fn decode_toyota(bits: &[u8]) -> Option<TpmsPacket> {
    let b = differential_manchester_decode(bits);
    if b.len() < 9 {
        return None;
    }
    let crc_ok = crc8(&b[..8], 0x80) == b[8];
    let pinv_ok = b[4].wrapping_add(b[7]) == 0xFF;
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = psi_to_kpa(b[5] as f32 / 4.0 - 7.0);
    let temp = b[6] as f32 - 40.0;
    let sane = kpa >= 0.0 && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Toyota-PMV-C210",
        88,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[4]),
        None,
        None,
        &b[..9],
        score(crc_ok && pinv_ok, sane, kpa, Some(temp)),
    ))
}

fn decode_toyota_pmv107j(bits: &[u8]) -> Option<TpmsPacket> {
    let mut p = decode_toyota(bits)?;
    p.protocol = "Toyota-PMV-107J".into();
    p.rtl433_id = 110;
    Some(p)
}

// ═══════════════════════════════════════════════════════════
//  [90] Renault / Dacia (Schrader/Continental)
//  FSK, Manchester, 433 MHz
//  9 bytes: [ID:4][flags:1][P:1][T:1][unk:1][CRC-8:1]
//  P: kPa * 0.5    T: C + 30   CRC-8 poly 0x07 init 0x00
// ═══════════════════════════════════════════════════════════
fn decode_renault(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 9 {
        return None;
    }
    let crc_ok = crc8(&b[..8], 0x00) == b[8];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.5;
    let temp = b[6] as f32 - 30.0;
    let alarm = Some(b[4] & 0x18 != 0);
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Renault/Dacia",
        90,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[4]),
        None,
        alarm,
        &b[..9],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [95] Schrader EG53MA4 (Saab/Opel/Vauxhall/Chevrolet)
//  FSK, Manchester
//  7 bytes: [ID:4][flags:1][P:1][T_F:1] + 1 CRC byte (8 total)
//  P: kPa * 0.1    T: Fahrenheit direct
// ═══════════════════════════════════════════════════════════
fn decode_schrader_eg53ma4(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 8 {
        return None;
    }
    let crc_ok = crc8(&b[..7], 0x00) == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.1;
    let temp = (b[6] as f32 - 32.0) / 1.8; // F → C
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Schrader-EG53MA4",
        95,
        id,
        kpa,
        Some(temp),
        Some(b[4] & 0x04 == 0),
        Some(b[4]),
        None,
        Some(b[4] & 0x08 != 0),
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [168] Schrader SMD3MA4 / 3039 (Subaru / Infiniti / Nissan / Renault)
//  FSK, Manchester, 433 MHz
//  8 bytes: [ID:4][flags:1][P:1][T:1][CRC-8:1]
//  P: kPa * 0.1    T: direct Celsius
// ═══════════════════════════════════════════════════════════
fn decode_schrader_smd3ma4(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 8 {
        return None;
    }
    let crc_ok = crc8(&b[..7], 0x00) == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.1;
    let temp = b[6] as f32;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Schrader-SMD3MA4/3039",
        168,
        id,
        kpa,
        Some(temp),
        Some(b[4] & 0x04 == 0),
        Some(b[4]),
        None,
        None,
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [60] Schrader generic
// ═══════════════════════════════════════════════════════════
fn decode_schrader_generic(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 8 {
        return None;
    }
    let sum = (0..7usize).fold(0u8, |a, i| a.wrapping_add(b[i]));
    let crc_ok = sum == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[4] as f32 * 0.25;
    let temp = b[5] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Schrader",
        60,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[6]),
        None,
        None,
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [59] Steelmate TPMS
//  OOK PWM, 315/433 MHz, 5 bytes NRZ, no CRC
//  ID: 3 bytes   P: kPa*2.5   T: C+55
// ═══════════════════════════════════════════════════════════
fn decode_steelmate(bits: &[u8]) -> Option<TpmsPacket> {
    let b = raw_bits_to_bytes(bits);
    if b.len() < 5 {
        return None;
    }
    let id = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
    let kpa = b[3] as f32 * 2.5;
    let temp = b[4] as f32 - 55.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Steelmate",
        59,
        id,
        kpa,
        Some(temp),
        None,
        None,
        None,
        None,
        &b[..5],
        score(false, sane, kpa, Some(temp)).min(55),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [123] Jansite TY02S  /  [180] Solar variant
//  OOK, 433.92 MHz, 19.2 kbps
//  8 bytes: [ID:4][flags:1][P:1][T:1][CRC-8:1]
//  P: kPa * 0.1    T: C - 50
// ═══════════════════════════════════════════════════════════
fn decode_jansite(bits: &[u8]) -> Option<TpmsPacket> {
    let b = raw_bits_to_bytes(bits);
    if b.len() < 8 {
        return None;
    }
    let crc_ok = crc8(&b[..7], 0x00) == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.1;
    let temp = b[6] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Jansite-TY02S",
        123,
        id,
        kpa,
        Some(temp),
        Some(b[4] & 0x08 == 0),
        Some(b[4]),
        None,
        Some(b[4] & 0x02 != 0),
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}
fn decode_jansite_solar(bits: &[u8]) -> Option<TpmsPacket> {
    let mut p = decode_jansite(bits)?;
    p.protocol = "Jansite-Solar".into();
    p.rtl433_id = 180;
    Some(p)
}

// ═══════════════════════════════════════════════════════════
//  [140] Hyundai Elantra 2012
//  OOK, 433.92 MHz, 8 bytes, byte-sum CRC
//  P: kPa direct   T: C + 40
// ═══════════════════════════════════════════════════════════
fn decode_elantra(bits: &[u8]) -> Option<TpmsPacket> {
    let b = raw_bits_to_bytes(bits);
    if b.len() < 8 {
        return None;
    }
    let sum = (0..7usize).fold(0u8, |a, i| a.wrapping_add(b[i]));
    let crc_ok = sum == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[4] as f32;
    let temp = b[5] as f32 - 40.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Hyundai-Elantra-2012",
        140,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[6]),
        None,
        None,
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [156] Abarth 124 Spider
//  FSK, Manchester, 433 MHz, 9 bytes
//  XOR bytes 0..8 == 0
//  P: kPa * 1.364   T: C - 50
// ═══════════════════════════════════════════════════════════
fn decode_abarth(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 9 {
        return None;
    }
    let xor = (0..9usize).fold(0u8, |a, i| a ^ b[i]);
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 1.364;
    let temp = b[6] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Abarth-124-Spider",
        156,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[4]),
        None,
        None,
        &b[..9],
        score(xor == 0, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [186] Hyundai VDO (Kia / Genesis)
//  FSK, Manchester, 433.92 MHz
//  9 bytes: [B:1][ID:4][flags:1][P:1][T:1][R:1]  XOR 1..8 == 0
//  P: kPa * 0.75    T: C - 50
// ═══════════════════════════════════════════════════════════
fn decode_hyundai_vdo(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 9 {
        return None;
    }
    let xor = (1..9usize).fold(0u8, |a, i| a ^ b[i]);
    let id = u32::from_be_bytes([b[1], b[2], b[3], b[4]]);
    let kpa = b[6] as f32 * 0.75;
    let temp = b[7] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Hyundai-VDO",
        186,
        id,
        kpa,
        Some(temp),
        Some(b[5] & 0x01 == 0),
        Some(b[5]),
        None,
        Some(b[5] & 0x04 != 0),
        &b[..9],
        score(xor == 0, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [252] BMW Gen4/5 + Audi / HUF / Continental / Schrader-Sensata
//  FSK, Manchester, 433 MHz
//  10 bytes: [ID:4][status:1][P:1][T:1][counter:1][xor:1][CRC-8:1]
//  P: kPa * 0.75    T: C - 50    CRC-8 poly 0x07 init 0x00
// ═══════════════════════════════════════════════════════════
fn decode_bmw_gen45(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 10 {
        return None;
    }
    let crc_ok = crc8(&b[..9], 0x00) == b[9];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.75;
    let temp = b[6] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "BMW-Gen4/5+Audi",
        252,
        id,
        kpa,
        Some(temp),
        Some(b[4] & 0x04 == 0),
        Some(b[4]),
        None,
        Some(b[4] & 0x01 != 0),
        &b[..10],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [257] BMW Gen2/Gen3
//  FSK, Manchester, 433 MHz, 8 bytes
//  P: kPa direct    T: C - 50
// ═══════════════════════════════════════════════════════════
fn decode_bmw_gen23(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 8 {
        return None;
    }
    let crc_ok = crc8(&b[..7], 0x00) == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32;
    let temp = b[6] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "BMW-Gen2/3",
        257,
        id,
        kpa,
        Some(temp),
        Some(b[4] & 0x04 == 0),
        Some(b[4]),
        None,
        None,
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [203] Porsche Boxster/Cayman
//  FSK, Manchester, 433 MHz, 10 bytes
//  P: kPa * 0.3     T: C - 40
// ═══════════════════════════════════════════════════════════
fn decode_porsche(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 10 {
        return None;
    }
    let crc_ok = crc8(&b[..9], 0x00) == b[9];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.3;
    let temp = b[6] as f32 - 40.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Porsche-Boxster/Cayman",
        203,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[4]),
        None,
        None,
        &b[..10],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [208] AVE TPMS (aftermarket clip-on)
//  OOK, 433.92 MHz, 6 bytes, XOR == 0
//  P: kPa * 1.5    T: C - 40
// ═══════════════════════════════════════════════════════════
fn decode_ave(bits: &[u8]) -> Option<TpmsPacket> {
    let b = raw_bits_to_bytes(bits);
    if b.len() < 6 {
        return None;
    }
    let xor = b[0] ^ b[1] ^ b[2] ^ b[3] ^ b[4] ^ b[5];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[4] as f32 * 1.5;
    let temp = b[5] as f32 - 40.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "AVE-TPMS",
        208,
        id,
        kpa,
        Some(temp),
        None,
        None,
        None,
        None,
        &b[..6],
        score(xor == 0, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [225] TyreGuard 400
//  FSK, Manchester, 433 MHz, 8 bytes, CRC-8
//  P: kPa * 0.5    T: C - 40
// ═══════════════════════════════════════════════════════════
fn decode_tyreguard(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 8 {
        return None;
    }
    let crc_ok = crc8(&b[..7], 0x00) == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.5;
    let temp = b[6] as f32 - 40.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "TyreGuard-400",
        225,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[4]),
        None,
        None,
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [241] EezTire E618 / Carchet TPMS / TST-507
//  OOK, 433.92 MHz, 9 bytes, CRC-8
//  P: 9-bit word * 0.1 kPa    T: C - 50
// ═══════════════════════════════════════════════════════════
fn decode_eeztire(bits: &[u8]) -> Option<TpmsPacket> {
    let b = raw_bits_to_bytes(bits);
    if b.len() < 9 {
        return None;
    }
    let crc_ok = crc8(&b[..8], 0x00) == b[8];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = ((b[5] as u16) | ((b[4] as u16 & 0x01) << 8)) as f32 * 0.1;
    let temp = b[6] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "EezTire/Carchet/TST-507",
        241,
        id,
        kpa,
        Some(temp),
        Some(b[4] & 0x04 == 0),
        Some(b[4]),
        None,
        Some(b[4] & 0x02 != 0),
        &b[..9],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [275] GM Aftermarket TPMS (315 MHz US)
//  OOK, 8 bytes, byte-sum CRC
//  P: kPa * 0.25   T: C - 40
// ═══════════════════════════════════════════════════════════
fn decode_gm_aftermarket(bits: &[u8]) -> Option<TpmsPacket> {
    let b = raw_bits_to_bytes(bits);
    if b.len() < 8 {
        return None;
    }
    let sum = (0..7usize).fold(0u8, |a, i| a.wrapping_add(b[i]));
    let crc_ok = sum == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[4] as f32 * 0.25;
    let temp = b[5] as f32 - 40.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "GM-Aftermarket",
        275,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[6]),
        None,
        None,
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [298] TRW OOK   /  [299] TRW FSK
//  OOK or FSK (Manchester), 433.92 MHz, 8 bytes, CRC-8
//  P: kPa * 0.25   T: C - 40
// ═══════════════════════════════════════════════════════════
fn decode_trw_ook(bits: &[u8]) -> Option<TpmsPacket> {
    let b = raw_bits_to_bytes(bits);
    if b.len() < 8 {
        return None;
    }
    let crc_ok = crc8(&b[..7], 0x00) == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[4] as f32 * 0.25;
    let temp = b[5] as f32 - 40.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "TRW-OOK",
        298,
        id,
        kpa,
        Some(temp),
        Some(b[6] & 0x02 == 0),
        Some(b[6]),
        None,
        Some(b[6] & 0x04 != 0),
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}
fn decode_trw_fsk(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 8 {
        return None;
    }
    let crc_ok = crc8(&b[..7], 0x00) == b[7];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[4] as f32 * 0.25;
    let temp = b[5] as f32 - 40.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "TRW-FSK",
        299,
        id,
        kpa,
        Some(temp),
        Some(b[6] & 0x02 == 0),
        Some(b[6]),
        None,
        Some(b[6] & 0x04 != 0),
        &b[..8],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [295] Airpuxem TYH11_EU6_ZQ
//  FSK, Manchester, 433.92 MHz, 9 bytes, CRC-8
//  P: kPa * 0.75    T: C - 50
// ═══════════════════════════════════════════════════════════
fn decode_airpuxem(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 9 {
        return None;
    }
    let crc_ok = crc8(&b[..8], 0x00) == b[8];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = b[5] as f32 * 0.75;
    let temp = b[6] as f32 - 50.0;
    let sane = (0.0..=400.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Airpuxem-TYH11",
        295,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[4]),
        None,
        None,
        &b[..9],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}

// ═══════════════════════════════════════════════════════════
//  [201] Unbranded Solar / Truck TPMS
//  FSK, Manchester, sync word 0x001A, 9 bytes
//  Bytes: [ID:4][wheel:1][P_hi:1][P_lo:1][T:1][CRC-8:1]
//  P: 16-bit word in kPa   T: C - 40
// ═══════════════════════════════════════════════════════════
fn decode_solar_truck(bits: &[u8]) -> Option<TpmsPacket> {
    let b = manchester_decode(bits);
    if b.len() < 9 {
        return None;
    }
    let crc_ok = crc8(&b[..8], 0x00) == b[8];
    let id = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    let kpa = (b[5] as u32 * 256 + b[6] as u32) as f32;
    let temp = b[7] as f32 - 40.0;
    // Truck tyres run 600-900 kPa; tolerate wider range
    let sane = (0.0..=1200.0).contains(&kpa) && (-40.0..=125.0).contains(&temp);
    Some(pkt(
        "Solar/Truck-TPMS",
        201,
        id,
        kpa,
        Some(temp),
        None,
        Some(b[4]),
        None,
        None,
        &b[..9],
        score(crc_ok, sane, kpa, Some(temp)),
    ))
}
