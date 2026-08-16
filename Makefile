.PHONY: rust_process_e2e cutover perf_check process_e2e extended doctor ci

rust_process_e2e:
	cargo test -p agent-broker-runtime --test process_restart_e2e -- --nocapture
	cargo test -p agent-broker-runtime --test tcp_runtime_e2e -- --nocapture

cutover:
	cargo xtask cutover

perf_check:
	cargo xtask perf

process_e2e: rust_process_e2e

extended:
	cargo xtask extended

doctor:
	cargo run -p agent-broker-runtime --bin agentbrokerd -- doctor --nodes 1
	cargo run -p agent-broker-runtime --bin agentbrokerd -- doctor --nodes 3

ci: cutover rust_process_e2e
