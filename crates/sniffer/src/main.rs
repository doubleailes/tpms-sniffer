mod decoder;
mod demod;
mod framer;
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

    let start = Instant::now();
    while let Some(Ok(chunk)) = stream.next().await {
        if let Some(d) = args.duration_secs {
            if start.elapsed().as_secs() >= d {
                break;
            }
        }

        let bits_ook = ook_demod.process(&chunk);
        let bits_fsk = fsk_demod.process(&chunk);

        for bits in [bits_ook, bits_fsk] {
            let frames = framer.feed(&bits);
            for frame in frames {
                if let Some(pkt) = decoder::decode(&frame, &protocol, args.confidence) {
                    reporter.report(&pkt);
                }
            }
        }
    }

    Ok(())
}
