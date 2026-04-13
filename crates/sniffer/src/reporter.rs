// ============================================================
//  reporter.rs — pretty-print and JSON output
// ============================================================

use crate::decoder::TpmsPacket;

pub struct Reporter {
    json: bool,
}

impl Reporter {
    pub fn new(json: bool) -> Self {
        Self { json }
    }

    pub fn report(&self, p: &TpmsPacket) {
        if self.json {
            println!("{}", serde_json::to_string(p).unwrap_or_default());
        } else {
            self.pretty(p);
        }
    }

    fn pretty(&self, p: &TpmsPacket) {
        let temp_str = match p.temp_c {
            Some(t) => format!("{t:>6.1} °C"),
            None => "   N/A °C".into(),
        };
        let batt = match p.battery_ok {
            Some(true) => "",
            Some(false) => " 🔋LOW",
            None => "",
        };
        let alarm = match p.alarm {
            Some(true) => " ⚠ALARM",
            _ => "",
        };
        let moving = match p.moving {
            Some(true) => " [moving]",
            Some(false) => " [parked]",
            None => "",
        };

        println!(
            "[{ts}] [{rid:>3}] {proto:<28} {id}  \
             {kpa:>6.1} kPa ({psi:>5.1} psi)  {tmp}  \
             conf={c:>3}%{mv}{alarm}{batt}  raw=[{raw}]",
            ts = p.timestamp,
            rid = p.rtl433_id,
            proto = p.protocol,
            id = p.sensor_id,
            kpa = p.pressure_kpa,
            psi = p.pressure_psi,
            tmp = temp_str,
            c = p.confidence,
            mv = moving,
            alarm = alarm,
            batt = batt,
            raw = p.raw_hex,
        );
    }
}
