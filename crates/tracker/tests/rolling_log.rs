use rolling_file::{BasicRollingFileAppender, RollingConditionBasic};
use std::io::Write;

/// Write > 10 MB to a rolling file appender configured with max_size=10 MB
/// and max_files=5. Verify that rotation occurs and total disk usage is bounded.
#[test]
fn rolling_file_rotation_creates_rotated_files() {
    let dir = tempfile::tempdir().expect("failed to create tempdir");
    let log_path = dir.path().join("test.log");

    // 10 MB max per file, keep up to 5 rotated files
    let max_size: u64 = 10 * 1024 * 1024;
    let max_files: usize = 5;
    let condition = RollingConditionBasic::new().max_size(max_size);

    let mut appender =
        BasicRollingFileAppender::new(&log_path, condition, max_files).expect("appender creation");

    // Write ~11 MB of data (enough to trigger at least one rotation)
    let line = "X".repeat(1000) + "\n"; // ~1001 bytes per line
    let target_bytes: u64 = 11 * 1024 * 1024;
    let mut written: u64 = 0;
    while written < target_bytes {
        appender
            .write_all(line.as_bytes())
            .expect("write should succeed");
        written += line.len() as u64;
    }
    drop(appender);

    // Count log files (test.log, test.log.1, ...)
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("test.log"))
        .collect();

    assert!(
        entries.len() >= 2,
        "Expected at least 2 log files after writing 11 MB with 10 MB max, got {}",
        entries.len()
    );

    // Total size should be <= max_files * max_size (but typically much less
    // since we only wrote 11 MB)
    let total_size: u64 = entries.iter().map(|e| e.metadata().unwrap().len()).sum();
    let upper_bound = (max_files as u64 + 1) * max_size;
    assert!(
        total_size <= upper_bound,
        "Total log size {} exceeds upper bound {}",
        total_size,
        upper_bound,
    );
}
