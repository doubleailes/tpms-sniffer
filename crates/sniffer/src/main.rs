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

        let bits_ook = ook_demod.process(&chunk);
        let bits_fsk = fsk_demod.process(&chunk);

        for bits in [bits_ook, bits_fsk] {
            let frames = framer.feed(&bits);
            for frame in frames {
                // Build the IQ window for CFO measurement.  The frame
                // body is `frame.len()` bits long, and the alternating
                // preamble that triggered framing was at least 16 bits
                // ahead of it.  We pull `PREAMBLE_SAMPLES` samples
                // starting from the *beginning* of that span so the
                // window lands inside the carrier rather than in the
                // post-burst silence.  CFO is constant across the
                // burst, so any sub-window of the carrier is fine.
                let burst_samples = (frame.len() + 16) * sps;
                let iq_window = iq_ring
                    .extract_window(burst_samples, cfo::PREAMBLE_SAMPLES)
                    .or_else(|| {
                        // Fall back to the trailing portion of the
                        // ring buffer if the burst extends further
                        // back than the buffer can serve.
                        iq_ring.extract_window(0, cfo::PREAMBLE_SAMPLES)
                    });

                if let Some(pkt) = decoder::decode(
                    &frame,
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
