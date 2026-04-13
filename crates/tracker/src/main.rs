use std::io::{self, BufRead};

use anyhow::Result;
use clap::Parser;
use tpms_tracker::{TpmsPacket, db::Database, resolver::Resolver};

#[derive(Parser)]
#[command(
    name = "tpms-tracker",
    about = "Vehicle tracker that consumes tpms-sniffer JSON-line output.\n\
             Pipe the sniffer into this tool:\n\
             \n  tpms-sniffer --json | tpms-tracker --db tpms.db\n"
)]
struct Args {
    /// SQLite database file (created if it does not exist).
    #[arg(long, default_value = "tpms.db")]
    db: String,

    /// Print a line for every resolved packet.
    #[arg(long, short)]
    verbose: bool,

    /// Minimum confidence score (0–100) to process.
    #[arg(long, default_value_t = 65)]
    confidence: u8,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let db = Database::open(&args.db)?;
    let mut resolver = Resolver::new(db)?;

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let packet: TpmsPacket = match serde_json::from_str(line) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("warn: skipping malformed JSON: {e}");
                continue;
            }
        };

        if packet.confidence < args.confidence {
            continue;
        }

        match resolver.process(&packet) {
            Ok(Some(vid)) if args.verbose => {
                println!(
                    "{} | vehicle={vid} | sensor={} | {:.1} kPa | {}",
                    packet.timestamp, packet.sensor_id, packet.pressure_kpa, packet.protocol,
                );
            }
            Ok(_) => {}
            Err(e) => eprintln!("error: {e}"),
        }
    }

    // Flush any incomplete rolling-ID burst accumulated at end of stream.
    resolver.flush()?;

    Ok(())
}
