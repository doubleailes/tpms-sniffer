cargo build --release
./target/release/tpms-sniffer --json | ./target/release/tpms-tracker --db tpms_010.db --log-level debug --log-file ~/logs/tpms-tracker.log