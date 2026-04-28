// `cfo` exposes more surface (constants, IqWindow, refine_cfo) than the
// sniffer binary itself consumes — `estimate_cfo` and `PREAMBLE_SAMPLES`
// are the only items the live pipeline needs.  The rest are public API
// for the tracker and unit tests, so silence dead-code warnings here.
#[allow(dead_code)]
mod cfo;
mod decoder;
mod demod;
mod framer;
#[allow(dead_code)]
mod iq_buffer;
mod manchester;
mod reporter;

use std::time::Instant;

use clap::Parser;
use tracing::info;

use decoder::TpmsProtocol;
use reporter::Reporter;
use rtlsdr_next::Driver;

#[derive(Parser, Debug)]
#[command(
    name = "tpms-sniffer",
    about = "Passive TPMS sniffer — 25 protocols from rtl_433, via rtlsdr-next"
)]
struct Args {
    /// RTL-SDR device index
    #[arg(short, long, default_value_t = 0)]
    device: u32,

    /// Centre frequency in Hz (315_000_000 US / 433_920_000 EU)
    #[arg(short, long, default_value_t = 433_920_000)]
    freq: u32,

    /// Sample-rate in Hz
    #[arg(short, long, default_value_t = 250_000)]
    rate: u32,

    /// RF gain in tenths of dB (0 = auto-gain)
    #[arg(short, long, default_value_t = 0)]
    gain: i32,

    /// Output JSON lines instead of pretty-print
    #[arg(long)]
    json: bool,

    /// Minimum confidence 0-100
    #[arg(long, default_value_t = 65)]
    confidence: u8,

    /// Protocol filter: all | ford | citroen | toyota | toyota107j |
    ///   renault | schrader | schrader_eg53 | schrader_smd |
    ///   steelmate | jansite | jansite_solar | elantra | abarth |
    ///   hyundai_vdo | bmw_gen45 | bmw_gen23 | porsche | ave |
    ///   tyreguard | eeztire | gm | trw_ook | trw_fsk |
    ///   airpuxem | truck
    #[arg(long, default_value = "all")]
    protocol: String,

    /// Stop capturing after this many seconds (omit for unlimited)
    #[arg(long)]
    duration_secs: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("tpms_sniffer=debug,warn")
        .init();

    let args = Args::parse();

    let protocol = match args.protocol.as_str() {
        "ford" => TpmsProtocol::Ford,
        "citroen" => TpmsProtocol::Citroen,
        "toyota" => TpmsProtocol::Toyota,
        "toyota107j" => TpmsProtocol::ToyotaPmv107j,
        "renault" => TpmsProtocol::Renault,
        "schrader" => TpmsProtocol::Schrader,
        "schrader_eg53" => TpmsProtocol::SchraderEg53ma4,
        "schrader_smd" => TpmsProtocol::SchraderSmd3ma4,
        "steelmate" => TpmsProtocol::Steelmate,
        "jansite" => TpmsProtocol::Jansite,
        "jansite_solar" => TpmsProtocol::JansiteSolar,
        "elantra" => TpmsProtocol::Elantra,
        "abarth" => TpmsProtocol::Abarth,
        "hyundai_vdo" => TpmsProtocol::HyundaiVdo,
        "bmw_gen45" => TpmsProtocol::BmwGen45,
        "bmw_gen23" => TpmsProtocol::BmwGen23,
        "porsche" => TpmsProtocol::Porsche,
        "ave" => TpmsProtocol::Ave,
        "tyreguard" => TpmsProtocol::TyreGuard,
        "eeztire" => TpmsProtocol::EezTire,
        "gm" => TpmsProtocol::GmAftermarket,
        "trw_ook" => TpmsProtocol::TrwOok,
        "trw_fsk" => TpmsProtocol::TrwFsk,
        "airpuxem" => TpmsProtocol::Airpuxem,
        "truck" => TpmsProtocol::SolarTruck,
        _ => TpmsProtocol::All,
    };

    info!(
        "Opening RTL-SDR #{} @ {:.3} MHz  SR={} kHz",
        args.device,
        args.freq as f64 / 1e6,
        args.rate / 1000,
    );

    // rtlsdr-next selects the device via RTLSDR_DEVICE_INDEX
    // SAFETY: called once before any parallel USB threads are spawned
    unsafe {
        std::env::set_var("RTLSDR_DEVICE_INDEX", args.device.to_string());
    }
    let mut driver = Driver::new()?;
    driver.set_frequency(args.freq as u64)?;
    driver.set_sample_rate(args.rate)?;
    if args.gain == 0 {
        info!("Auto-gain mode (using tuner default)");
    } else {
        driver.tuner.set_gain(args.gain as f32 / 10.0)?;
        info!("Gain = {:.1} dB", args.gain as f32 / 10.0);
    }

    let reporter = Reporter::new(args.json);
    let mut stream = driver.stream();

    eprintln!("═══════════════════════════════════════════════════════");
    eprintln!(
        " TPMS Sniffer  │  {:.3} MHz  │  {} kHz sample-rate",
        args.freq as f64 / 1e6,
        args.rate / 1000
    );
    eprintln!(" 25 protocols  │  rtl_433 reference  │  rtlsdr-next");
    eprintln!("═══════════════════════════════════════════════════════");

    let mut ook_demod = demod::OokDemod::new(args.rate);
    let mut fsk_demod = demod::FskDemod::new(args.rate);
    let mut framer = framer::Framer::new();
    let mut iq_ring = iq_buffer::IqRingBuffer::new(iq_buffer::IQ_RING_SAMPLES);

    // Per-symbol sample count at the configured rate (used to size the
    // IQ window pulled out of the ring buffer for CFO estimation).
    // Schrader-family symbol rate is 52 µs.
    let sps = ((args.rate as f32 * 52e-6) as usize).max(1);

    let start = Instant::now();
    while let Some(Ok(chunk)) = stream.next().await {
        if let Some(d) = args.duration_secs {
            if start.elapsed().as_secs() >= d {
                break;
            }
        }

        // Capture the raw IQ samples *before* demod consumes them.
        iq_ring.push_chunk(&chunk);
        let chunk_samples = chunk.len() / 2;

        let bits_ook = ook_demod.process(&chunk);
        let bits_fsk = fsk_demod.process(&chunk);

        for bits in [bits_ook, bits_fsk] {
            let frames = framer.feed(&bits);
            for frame in frames {
                // Locate the preamble's IQ samples in the ring buffer.
                // The framer reports the chunk-relative sample index of
                // the frame's *last* bit; from there we walk back over
                // the frame body plus the alternating preamble (≥16
                // bits) to land on the start of the burst.
                //
                // samples_back is measured from "now" (which is the
                // end of the chunk we just pushed):
                //   samples_back =
                //       (chunk_samples - last_bit_sample_idx)        // post-burst tail in chunk
                //     + (frame.bits.len() + 16) * sps                // burst length
                //
                // The fallback to "last 64 samples" is intentionally
                // gone — it always landed in post-burst silence and
                // produced CFO ≡ 0 Hz.  Skipping CFO when the window
                // can't be extracted is the correct behaviour.
                let samples_since_end =
                    chunk_samples.saturating_sub(frame.last_bit_sample_idx as usize);
                let burst_samples = (frame.bits.len() + 16) * sps;
                let samples_back = samples_since_end + burst_samples;
                let iq_window = iq_ring.extract_window(samples_back, cfo::PREAMBLE_SAMPLES);

                if let Some(pkt) = decoder::decode(
                    &frame.bits,
                    iq_window.as_deref(),
                    args.rate,
                    &protocol,
                    args.confidence,
                ) {
                    reporter.report(&pkt);
                }
            }
        }
    }

    Ok(())
}

// ============================================================
//  End-to-end CFO pipeline regression test (issue #45)
//
//  Synthesises a u8 IQ chunk containing an alternating-bit
//  preamble followed by 128 random bits, all riding a known
//  +3,000 Hz carrier offset.  Runs the chunk through the live
//  OokDemod → Framer → IqRingBuffer chain exactly as `main`
//  does, then verifies that the IQ window returned by
//  `extract_window` produces a CFO estimate within ±200 Hz of
//  the synthesised offset.
//
//  This test catches the regression where `samples_back` was
//  computed without `frame.last_bit_sample_idx`, causing the
//  preamble window to land in post-burst silence and CFO ≡ 0.
// ============================================================
#[cfg(test)]
mod cfo_pipeline_tests {
    use super::*;
    use std::f32::consts::PI;

    const FS: u32 = 250_000;
    const SYMBOL_US: f32 = 52.0;
    const CFO_HZ: f32 = 3_000.0;

    /// Synthesise a u8 IQ chunk: `silence_before` samples of DC
    /// noise, then a burst of `bit_pattern` modulated as OOK at
    /// 52 µs/bit on a +CFO_HZ carrier, then `silence_after`
    /// samples of DC.  Returns interleaved u8 IQ bytes.
    fn synth_chunk(bit_pattern: &[u8], silence_before: usize, silence_after: usize) -> Vec<u8> {
        let sps = (FS as f32 * SYMBOL_US / 1e6) as usize; // ≈13
        let burst_samples = bit_pattern.len() * sps;
        let total = silence_before + burst_samples + silence_after;
        let mut bytes = Vec::with_capacity(total * 2);

        // Pre-burst silence at DC.
        for _ in 0..silence_before {
            bytes.push(127);
            bytes.push(127);
        }

        // Modulated burst.  "1" bits transmit the offset carrier
        // at full amplitude; "0" bits are at the noise floor.
        let dphi = 2.0 * PI * CFO_HZ / FS as f32;
        for (b_idx, &b) in bit_pattern.iter().enumerate() {
            for s in 0..sps {
                let n = silence_before + b_idx * sps + s;
                let phi = dphi * n as f32;
                let amp = if b == 1 { 100.0 } else { 0.0 };
                let i = (127.5 + amp * phi.cos()).clamp(0.0, 255.0) as u8;
                let q = (127.5 + amp * phi.sin()).clamp(0.0, 255.0) as u8;
                bytes.push(i);
                bytes.push(q);
            }
        }

        // Post-burst silence at DC.
        for _ in 0..silence_after {
            bytes.push(127);
            bytes.push(127);
        }

        bytes
    }

    /// Run a single chunk through the live OokDemod → Framer
    /// → IqRingBuffer chain and return any CFO estimate
    /// computed from the first frame the framer emits.
    fn run_pipeline(chunk: &[u8]) -> Option<f32> {
        let mut ook = demod::OokDemod::new(FS);
        let mut framer = framer::Framer::new();
        let mut ring = iq_buffer::IqRingBuffer::new(iq_buffer::IQ_RING_SAMPLES);
        let sps = ((FS as f32 * SYMBOL_US / 1e6) as usize).max(1);

        ring.push_chunk(chunk);
        let chunk_samples = chunk.len() / 2;
        let bits = ook.process(chunk);
        let frames = framer.feed(&bits);

        let frame = frames.into_iter().next()?;
        let samples_since_end = chunk_samples.saturating_sub(frame.last_bit_sample_idx as usize);
        let burst_samples = (frame.bits.len() + 16) * sps;
        let samples_back = samples_since_end + burst_samples;
        let win = ring.extract_window(samples_back, cfo::PREAMBLE_SAMPLES)?;
        cfo::estimate_cfo(&win, FS)
    }

    fn alt_preamble(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i & 1) as u8).collect()
    }

    #[test]
    fn frame_at_chunk_end_recovers_cfo() {
        // Burst sits at the very end of the chunk: ~no post-burst
        // silence.  This is the case the old (broken) code already
        // handled accidentally, included as a baseline.
        let mut bits = alt_preamble(20);
        bits.extend(std::iter::repeat(1u8).take(128));
        let chunk = synth_chunk(&bits, 1024, 0);
        let cfo = run_pipeline(&chunk).expect("pipeline must produce a CFO estimate");
        assert!(
            (cfo - CFO_HZ).abs() < 200.0,
            "frame at chunk end: expected ≈{CFO_HZ} Hz, got {cfo}"
        );
    }

    #[test]
    fn frame_in_middle_of_chunk_recovers_cfo() {
        // Burst sits in the middle of a long chunk with significant
        // post-burst silence.  Without the `last_bit_sample_idx`
        // fix the IQ window lands in this trailing silence and the
        // estimator returns 0 Hz exactly.
        let mut bits = alt_preamble(20);
        bits.extend(std::iter::repeat(1u8).take(128));
        let chunk = synth_chunk(&bits, 512, 4_096);
        let cfo = run_pipeline(&chunk).expect("pipeline must produce a CFO estimate");
        assert!(
            cfo.abs() > 100.0,
            "CFO must not be ≈ 0 (would mean we sampled silence): got {cfo}"
        );
        assert!(
            (cfo - CFO_HZ).abs() < 200.0,
            "frame in middle of chunk: expected ≈{CFO_HZ} Hz, got {cfo}"
        );
    }
}
