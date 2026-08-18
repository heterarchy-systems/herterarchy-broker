use std::error::Error;
use std::path::PathBuf;

#[path = "../test_support/tls_fixture.rs"]
mod tls_fixture;

fn main() {
    if let Err(error) = run() {
        eprintln!("Agent Broker TLS fixture generation failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let directory = PathBuf::from(
        args.next()
            .ok_or("usage: generate_cluster_tls <directory> <node-id> <node-id> <node-id>")?,
    );
    let node_ids = args
        .map(|value| value.parse::<u64>())
        .collect::<Result<Vec<_>, _>>()?;
    if node_ids.len() != 3 || node_ids.contains(&0) {
        return Err("generate_cluster_tls requires exactly three positive node ids".into());
    }
    tls_fixture::write_cluster_tls_fixture(&directory, &node_ids)
}
