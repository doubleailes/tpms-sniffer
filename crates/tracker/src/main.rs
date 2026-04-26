use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Mutex;

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::info;
use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use tpms_tracker::{
    TpmsPacket, analytics, db::Database, jitter, replay, resolver::Resolver, server,
};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

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

    /// Log level: info (sightings only), debug (+resolver decisions),
    /// trace (+tracker events).  Overridden by the RUST_LOG env var.
    #[arg(long, default_value = "info")]
    log_level: String,

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

    /// Replay a JSON-L (or .jsonl.gz) fixture file instead of reading stdin.
    #[arg(long)]
    replay: Option<PathBuf>,

    /// After replay, run consistency assertions and exit non-zero on failure.
    #[arg(long)]
    assert_consistency: bool,

    /// Start HTTP dashboard server on this address (e.g. 0.0.0.0:8080)
    #[arg(long, value_name = "ADDR")]
    serve: Option<String>,

    /// Path to log file. When set, logs are written to this file (with
    /// rotation) in addition to stderr. The directory is created if it
    /// does not exist.
    #[arg(long, value_name = "PATH")]
    log_file: Option<PathBuf>,

    /// Maximum log file size in MB before rotation.
    #[arg(long, default_value_t = 10, value_name = "MB")]
    log_max_size: u64,

    /// Number of rotated log files to retain.
    #[arg(long, default_value_t = 5, value_name = "N")]
    log_max_files: usize,

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

        /// Show temporal behavioural fingerprints instead of car reports.
        #[arg(long)]
        temporal: bool,
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

fn init_logging(
    level: &str,
    log_file: Option<&Path>,
    max_size_mb: u64,
    max_files: usize,
) -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(format!("tpms_tracker={level}")));

    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(true)
        .with_target(false)
        .with_level(false);

    match log_file {
        None => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(stderr_layer)
                .init();
        }
        Some(path) => {
            // Ensure log directory exists
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            let condition = RollingConditionBasic::new().max_size(max_size_mb * 1024 * 1024);

            let file_appender = BasicRollingFileAppender::new(path, condition, max_files)?;

            let file_layer = fmt::layer()
                .with_writer(Mutex::new(file_appender))
                .with_ansi(false) // no ANSI colour codes in log files
                .with_target(false)
                .with_level(false);

            tracing_subscriber::registry()
                .with(env_filter)
                .with(stderr_layer)
                .with(file_layer)
                .init();
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    init_logging(
        &args.log_level,
        args.log_file.as_deref(),
        args.log_max_size,
        args.log_max_files,
    )?;

    match &args.command {
        Some(Command::Report {
            car,
            from,
            to,
            json,
            temporal,
        }) => {
            if *temporal {
                return run_temporal_report(&args.db, *json);
            }
            return run_report(
                &args.db,
                car.as_deref(),
                from.as_deref(),
                to.as_deref(),
                *json,
            );
        }
        Some(Command::Backfill) => {
            return run_backfill(&args.db);
        }
        Some(Command::Geojson { car, output }) => {
            return run_geojson(&args.db, car.as_deref(), output.as_deref());
        }
        None => {}
    }

    // Replay mode: read from a fixture file instead of stdin.
    if let Some(ref path) = args.replay {
        let result = replay::replay(path, args.confidence)?;
        if args.assert_consistency {
            let errors = replay::assert_consistency(&result);
            replay::print_summary(&result, &errors);
            if !errors.is_empty() {
                process::exit(1);
            }
        }
        return Ok(());
    }

    // Default: stdin processing mode (with optional web server).
    let db = Database::open(&args.db)?;

    // If --serve is given, use a tokio runtime so we can spawn the server.
    if let Some(ref addr) = args.serve {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(run_with_server(db, &args, addr))
    } else {
        run_stdin(db, &args)
    }
}

/// Periodically recompute jitter profiles for all eligible fingerprints.
///
/// Runs every `JITTER_RECOMPUTE_INTERVAL_SECS` seconds.  Opens its own
/// database connection so it does not contend with the HTTP server or stdin
/// ingestion path.
async fn jitter_recompute_task(db_path: String) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(
            jitter::JITTER_RECOMPUTE_INTERVAL_SECS,
        ))
        .await;

        let result = tokio::task::spawn_blocking({
            let db_path = db_path.clone();
            move || -> anyhow::Result<usize> {
                let db = Database::open(&db_path)?;
                let fp_ids =
                    db.fingerprints_eligible_for_jitter_recompute(jitter::MIN_JITTER_SAMPLES)?;
                let mut updated = 0usize;
                for fp_id in &fp_ids {
                    let samples = db.get_interval_samples(fp_id, jitter::MAX_INTERVAL_SAMPLES)?;
                    if let Some(profile) = jitter::compute_jitter_profile(&samples) {
                        db.update_fingerprint_jitter(fp_id, &profile)?;
                        updated += 1;
                    }
                }
                Ok(updated)
            }
        })
        .await;

        match result {
            Ok(Ok(n)) if n > 0 => eprintln!("jitter: recomputed profiles for {n} fingerprint(s)"),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => eprintln!("jitter: recompute error: {e}"),
            Err(e) => eprintln!("jitter: task panicked: {e}"),
        }
    }
}

/// Run the stdin ingestion loop alongside the HTTP dashboard server.
async fn run_with_server(db: Database, args: &Args, addr: &str) -> Result<()> {
    let db_path = args.db.clone();
    let addr_owned = addr.to_string();
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server::serve(&db_path, &addr_owned).await {
            eprintln!("server error: {e}");
        }
    });

    tokio::spawn(jitter_recompute_task(args.db.clone()));

    // Run the blocking stdin loop on a blocking thread so we don't starve
    // the tokio runtime.
    let verbose = args.verbose;
    let confidence = args.confidence;
    let export_jaccard = args.export_jaccard.clone();
    let receiver_id = args.receiver_id.clone();

    tokio::task::spawn_blocking(move || {
        run_stdin_inner(
            db,
            verbose,
            confidence,
            &receiver_id,
            export_jaccard.as_deref(),
        )
    })
    .await??;

    // Stdin has ended but the server should keep running. Wait for it.
    eprintln!("stdin closed — dashboard server still running");
    server_handle.await?;

    Ok(())
}

/// Stdin ingestion without a tokio runtime.
fn run_stdin(db: Database, args: &Args) -> Result<()> {
    run_stdin_inner(
        db,
        args.verbose,
        args.confidence,
        &args.receiver_id,
        args.export_jaccard.as_deref(),
    )
}

/// Core stdin processing loop shared by sync and async paths.
fn run_stdin_inner(
    db: Database,
    verbose: bool,
    confidence: u8,
    receiver_id: &str,
    export_jaccard: Option<&str>,
) -> Result<()> {
    let mut resolver = Resolver::with_receiver_id(db, receiver_id.to_string())?;

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || !line.starts_with('{') {
            continue;
        }

        let packet: TpmsPacket = match serde_json::from_str(line) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("warn: skipping malformed JSON: {e}");
                continue;
            }
        };

        if packet.confidence < confidence {
            continue;
        }

        match resolver.process(&packet) {
            Ok(Some(vid)) if verbose => {
                let vehicle = resolver.vehicles().get(&vid);
                let car_id = vehicle
                    .and_then(|v| v.car_id)
                    .map(|c| c.to_string()[..8].to_string())
                    .unwrap_or_else(|| "none".to_string());
                let fp = vehicle
                    .and_then(|v| v.fingerprint_id.as_deref())
                    .unwrap_or("none");
                let class = vehicle
                    .map(|v| v.vehicle_class.as_str())
                    .unwrap_or("Unknown");
                let zeros = packet
                    .sensor_id_u32()
                    .map(|sid| sid.count_zeros())
                    .unwrap_or(0);
                let temp_str = packet
                    .temp_c
                    .filter(|&t| t < 200.0)
                    .map(|t| format!("{t:.1}"))
                    .unwrap_or_else(|| "null".to_string());
                let alarm = packet.alarm.unwrap_or(false);
                let battery = !packet.battery_ok.unwrap_or(true);
                info!(
                    "{} | SIGHT | vehicle={} | car={car_id} | fp={fp} | \
                     sensor={} | zeros={zeros} | pressure={:.1} | temp={temp_str} | \
                     alarm={alarm} | battery={battery} | protocol={} | \
                     rtl433={} | class={class} | receiver={}",
                    packet.timestamp,
                    &vid.to_string()[..8],
                    packet.sensor_id,
                    packet.pressure_kpa,
                    packet.protocol,
                    packet.rtl433_id,
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
    if let Some(path) = export_jaccard {
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
                let mm = v.make_model.as_deref().unwrap_or("unknown");
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
                            println!("    {vehicle_id}: ALARM {pressure_kpa:.0} kPa at {ts}");
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

fn run_geojson(db_path: &str, car: Option<&str>, output: Option<&str>) -> Result<()> {
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

fn run_temporal_report(db_path: &str, json_output: bool) -> Result<()> {
    use tpms_tracker::temporal;

    let db = Database::open(db_path)?;

    // Compute TBFs for all eligible fingerprints.
    let count = db.compute_all_temporal_fingerprints()?;
    eprintln!("Computed {count} temporal fingerprint(s).");

    let rows = db.all_temporal_fingerprints()?;

    if rows.is_empty() {
        eprintln!("No fingerprints with enough sessions for temporal analysis.");
        return Ok(());
    }

    if json_output {
        let json = serde_json::to_string_pretty(&rows)?;
        println!("{json}");
        return Ok(());
    }

    println!("Temporal Behavioural Fingerprints");
    println!("==================================");
    println!();

    for row in &rows {
        let gmm: Vec<temporal::GaussianComponent> =
            serde_json::from_str(&row.arrival_gmm_json).unwrap_or_default();
        let arrival_str = temporal::format_arrival(&gmm);
        let dwell_str = temporal::format_dwell(row.dwell_median_secs);

        let period_str = match (
            &row.periodicity_class,
            row.dominant_period_hrs,
            row.acf_peak_value,
        ) {
            (Some(cls), Some(period), Some(strength)) => {
                format!("{cls} (period {period:.1}h, strength {strength:.2})")
            }
            _ => "stationary (no dominant period)".to_string(),
        };

        let classification = row.classification.replace('_', " ").to_uppercase();

        println!(
            "{} | {} {:.0} kPa | {} sessions | {} day(s)",
            row.fingerprint_id,
            row.vehicle_class,
            row.pressure_kpa,
            row.session_count,
            row.observation_days,
        );
        println!("  Arrival:      {arrival_str}");
        println!(
            "  Dwell:        {} (median {dwell_str})",
            row.dwell_class.replace('_', " ")
        );
        println!("  Periodicity:  {period_str}");
        println!("  Classification: {classification}");

        // ASCII presence map.
        let presence_map: [[f32; temporal::PRESENCE_MAP_DAYS]; temporal::PRESENCE_MAP_HOURS] =
            serde_json::from_str(&row.presence_map_json)
                .unwrap_or([[0.0; temporal::PRESENCE_MAP_DAYS]; temporal::PRESENCE_MAP_HOURS]);
        println!();
        print!("{}", temporal::ascii_presence_map(&presence_map));
        println!();
    }

    Ok(())
}
