cargo build --release
./target/release/tpms-sniffer --json | ./target/release/tpms-tracker --db tpms_011.db --serve 0.0.0.0:8080 --log-level debug --log-file ~/logs/tpms-tracker.log