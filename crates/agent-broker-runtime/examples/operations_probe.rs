use std::error::Error;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

fn main() {
    if let Err(error) = run() {
        eprintln!("Agent Broker operations probe failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or(
        "usage: operations_probe assert-one-ready <host:port>... | assert-not-ready <host:port>",
    )?;
    match mode.as_str() {
        "assert-one-ready" => {
            let addresses = args
                .map(|value| value.parse::<SocketAddr>())
                .collect::<Result<Vec<_>, _>>()?;
            assert_one_ready(&addresses)
        }
        "assert-not-ready" => {
            let address = args
                .next()
                .ok_or("operations_probe assert-not-ready address is missing")?
                .parse::<SocketAddr>()?;
            if args.next().is_some() {
                return Err("operations_probe assert-not-ready accepts one address".into());
            }
            assert_not_ready(address)
        }
        _ => Err(format!("unsupported operations_probe mode: {mode}").into()),
    }
}

fn assert_one_ready(addresses: &[SocketAddr]) -> Result<(), Box<dyn Error>> {
    if addresses.is_empty() {
        return Err("operations_probe requires at least one address".into());
    }
    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        let mut ready = Vec::new();
        let mut followers = Vec::new();
        let mut all_live = true;
        for &address in addresses {
            let liveness = match request(address, "liveness") {
                Ok(value) => value,
                Err(_error) => {
                    all_live = false;
                    continue;
                }
            };
            if liveness.get("live").and_then(Value::as_bool) != Some(true) {
                all_live = false;
                continue;
            }
            let readiness = match request(address, "readiness") {
                Ok(value) => value,
                Err(_error) => {
                    all_live = false;
                    continue;
                }
            };
            if readiness.get("write_ready").and_then(Value::as_bool) == Some(true) {
                ready.push((address, readiness));
            } else if readiness.get("reason").and_then(Value::as_str) == Some("follower") {
                followers.push(address);
            }
        }
        if all_live && ready.len() == 1 && followers.len() + 1 == addresses.len() {
            let (address, readiness) = ready.pop().ok_or("missing ready operations result")?;
            println!(
                "{}",
                json!({
                    "status": "one_ready",
                    "address": address.to_string(),
                    "node_id": readiness
                        .pointer("/consensus/progress/node_id")
                        .and_then(Value::as_u64),
                    "leader": readiness
                        .pointer("/consensus/progress/current_leader")
                        .and_then(Value::as_u64),
                    "followers": followers.iter().map(ToString::to_string).collect::<Vec<_>>(),
                })
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "operations endpoints did not converge to exactly one ready leader and {} live followers",
                addresses.len().saturating_sub(1)
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn assert_not_ready(address: SocketAddr) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        let liveness = request(address, "liveness")?;
        if liveness.get("live").and_then(Value::as_bool) != Some(true) {
            return Err(format!("operations endpoint {address} is not live").into());
        }
        let readiness = request(address, "readiness")?;
        if readiness.get("write_ready").and_then(Value::as_bool) == Some(false) {
            println!(
                "{}",
                json!({
                    "status": "not_ready",
                    "address": address.to_string(),
                    "reason": readiness.get("reason").and_then(Value::as_str),
                    "consensus_status": readiness
                        .pointer("/consensus/status")
                        .and_then(Value::as_str),
                })
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("operations endpoint {address} remained write-ready").into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn request(address: SocketAddr, operation: &str) -> Result<Value, Box<dyn Error>> {
    let mut stream = TcpStream::connect_timeout(&address, IO_TIMEOUT)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    writeln!(
        stream,
        "{{\"schema_version\":1,\"operation\":\"{operation}\"}}"
    )?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    if reader.read_line(&mut response)? == 0 {
        return Err(format!("operations endpoint {address} closed before response").into());
    }
    Ok(serde_json::from_str(&response)?)
}
