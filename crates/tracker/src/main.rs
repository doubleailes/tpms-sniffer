use std::io::{self, BufRead};

use anyhow::Result;
use clap::{Parser, Subcommand};
use tpms_tracker::{TpmsPacket, analytics, db::Database, resolver::Resolver};

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

    /// Export the Jaccard co-occurrence matrix as JSON to the specified file
    /// after processing all input.
    #[arg(long)]
    export_jaccard: Option<String>,

    /// Identifier for this receiver node (e.g. "node-01").
    /// Used for cross-receiver deduplication in multi-node deployments.
    #[arg(long, default_value = "default")]
    receiver_id: String,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a behavioural report for a car or all cars.
    Report {
        /// Car ID to report on.  If omitted, reports on all cars.
        #[arg(long)]
        car: Option<String>,

        /// Start date filter (ISO 8601).
        #[arg(long)]
        from: Option<String>,

        /// End date filter (ISO 8601).
        #[arg(long)]
        to: Option<String>,

        /// Output as JSON instead of plain text.
        #[arg(long)]
        json: bool,
    },

    /// Backfill presence_slots from existing sighting data.
    Backfill,

    /// Export vehicle sighting paths as GeoJSON.
    Geojson {
        /// Car ID to export.  If omitted, exports all cars.
        #[arg(long)]
        car: Option<String>,

        /// Output file path (default: stdout).
        #[arg(long, short)]
        output: Option<String>,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    match &args.command {
        Some(Command::Report { car, from, to, json }) => {
            return run_report(&args.db, car.as_deref(), from.as_deref(), to.as_deref(), *json);
        }
        Some(Command::Backfill) => {
            return run_backfill(&args.db);
        }
        Some(Command::Geojson { car, output }) => {
            return run_geojson(&args.db, car.as_deref(), output.as_deref());
        }
        None => {}
    }

    // Default: stdin processing mode.
    let db = Database::open(&args.db)?;
    let mut resolver = Resolver::with_receiver_id(db, args.receiver_id.clone())?;

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
                let car_id = resolver
                    .vehicles()
                    .get(&vid)
                    .and_then(|v| v.car_id)
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "none".to_string());
                println!(
                    "{} | vehicle={vid} | car={car_id} | sensor={} | {:.1} kPa | {} | receiver={}",
                    packet.timestamp, packet.sensor_id, packet.pressure_kpa, packet.protocol,
                    packet.receiver_id,
                );
            }
            Ok(_) => {}
            Err(e) => eprintln!("error: {e}"),
        }
    }

    // Flush any incomplete rolling-ID burst accumulated at end of stream.
    resolver.flush()?;

    // Export Jaccard matrix if requested.
    if let Some(ref path) = args.export_jaccard {
        let export = resolver.cooccurrence_matrix().export();
        let json = serde_json::to_string_pretty(&export)?;
        std::fs::write(path, json)?;
        eprintln!("Jaccard matrix exported to {path}");
    }

    Ok(())
}

fn run_report(
    db_path: &str,
    car: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    json_output: bool,
) -> Result<()> {
    let db = Database::open(db_path)?;

    let car_ids: Vec<String> = if let Some(cid) = car {
        vec![cid.to_string()]
    } else {
        db.all_car_ids()?
    };

    if car_ids.is_empty() {
        eprintln!("No cars found in the database.");
        return Ok(());
    }

    let mut reports = Vec::new();
    for car_id in &car_ids {
        let report = db.generate_car_report(car_id, from, to)?;
        reports.push(report);
    }

    if json_output {
        let json = serde_json::to_string_pretty(&reports)?;
        println!("{json}");
    } else {
        for report in &reports {
            println!("Car {}", report.car_id);
            if let Some(ref first) = report.first_seen {
                println!("  First seen:     {first}");
            }
            if let Some(ref last) = report.last_seen {
                println!("  Last seen:      {last}");
            }
            println!("  Total sessions: {}", report.total_sessions);

            if let Some(ref routine) = report.routine {
                if let Some(arr) = routine.typical_arrival_hour {
                    println!(
                        "  Typical arrival:    {:02}:00–{:02}:00 (Mon–Fri)",
                        arr,
                        arr + 1
                    );
                }
                if let Some(dep) = routine.typical_departure_hour {
                    println!(
                        "  Typical departure:  {:02}:00–{:02}:00 (Mon–Fri)",
                        dep,
                        dep + 1
                    );
                }
                let absence_count = routine.anomalous_absences.len();
                let presence_count = routine.anomalous_presences.len();
                if absence_count > 0 || presence_count > 0 {
                    println!(
                        "  Anomalies:      {} unexpected absences, {} after-hours presences",
                        absence_count, presence_count
                    );
                }
            }

            for v in &report.vehicles {
                let mm = v
                    .make_model
                    .as_deref()
                    .unwrap_or("unknown");
                println!(
                    "  Vehicle {} ({}) — avg {:.0} kPa",
                    v.vehicle_id, mm, v.avg_pressure_kpa
                );
            }

            if !report.pressure_events.is_empty() {
                println!("  Pressure events:");
                for event in &report.pressure_events {
                    match event {
                        analytics::PressureEvent::SlowDecline {
                            rate_kpa_per_day,
                            vehicle_id,
                            ..
                        } => {
                            println!(
                                "    {vehicle_id}: slow decline {rate_kpa_per_day:.1} kPa/day"
                            );
                        }
                        analytics::PressureEvent::SuddenIncrease {
                            delta_kpa,
                            vehicle_id,
                            ts,
                            ..
                        } => {
                            println!(
                                "    {vehicle_id}: sudden increase +{delta_kpa:.1} kPa at {ts}"
                            );
                        }
                        analytics::PressureEvent::AlarmThreshold {
                            pressure_kpa,
                            vehicle_id,
                            ts,
                            ..
                        } => {
                            println!(
                                "    {vehicle_id}: ALARM {pressure_kpa:.0} kPa at {ts}"
                            );
                        }
                    }
                }
            }

            println!();
        }
    }

    Ok(())
}

fn run_backfill(db_path: &str) -> Result<()> {
    let db = Database::open(db_path)?;
    let count = db.backfill_presence_slots()?;
    eprintln!("Backfilled {count} sighting(s) into presence_slots.");
    Ok(())
}

fn run_geojson(
    db_path: &str,
    car: Option<&str>,
    output: Option<&str>,
) -> Result<()> {
    let db = Database::open(db_path)?;
    let sightings = db.get_geo_sightings(car)?;
    let fc = analytics::build_geojson(&sightings);
    let json = serde_json::to_string_pretty(&fc)?;
    if let Some(path) = output {
        std::fs::write(path, &json)?;
        eprintln!("GeoJSON exported to {path}");
    } else {
        println!("{json}");
    }
    Ok(())
}
