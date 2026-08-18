use std::error::Error;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use agent_broker_application::BrokerErrorCode;
use agent_broker_client::{BrokerClient, BrokerClientConfig, ClientError};
use agent_broker_domain::NamespaceId;
use serde_json::json;

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const MAX_RESPONSE_FRAME_BYTES: usize = 128 * 1024;

#[derive(Debug, Copy, Clone)]
struct CommittedProbe {
    leader: SocketAddr,
    term: u64,
    revision: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Agent Broker cluster probe failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let first = args.next().ok_or(
        "usage: cluster_probe <namespace> <min-reachable> <host:port>... | expect-rejected <namespace> <host:port> | assert-exact <term> <revision> <min-reachable> <host:port>...",
    )?;
    match first.as_str() {
        "expect-rejected" => return expect_rejected_write(args),
        "assert-exact" => return assert_exact_convergence(args),
        _ => {}
    }
    let namespace = NamespaceId::new(first)?;
    let min_reachable = args
        .next()
        .ok_or("cluster_probe min-reachable is missing")?
        .parse::<usize>()?;
    let addresses = args
        .map(|value| value.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()?;
    if addresses.len() < min_reachable || min_reachable == 0 {
        return Err(
            "cluster_probe min-reachable must be positive and no greater than address count".into(),
        );
    }

    let deadline = Instant::now() + PROBE_TIMEOUT;
    let committed = loop {
        if let Some(committed) = try_commit(&namespace, &addresses)? {
            break committed;
        }
        if Instant::now() >= deadline {
            return Err("no cluster member accepted the probe mutation before timeout".into());
        }
        thread::sleep(POLL_INTERVAL);
    };

    wait_for_convergence(
        &addresses,
        min_reachable,
        committed.term,
        committed.revision,
        deadline,
    )?;
    println!(
        "{}",
        json!({
            "status": "ok",
            "leader": committed.leader.to_string(),
            "term": committed.term,
            "revision": committed.revision,
            "min_reachable": min_reachable,
            "address_count": addresses.len(),
        })
    );
    Ok(())
}

fn expect_rejected_write(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let namespace = NamespaceId::new(
        args.next()
            .ok_or("cluster_probe expect-rejected namespace is missing")?,
    )?;
    let address = args
        .next()
        .ok_or("cluster_probe expect-rejected address is missing")?
        .parse::<SocketAddr>()?;
    if args.next().is_some() {
        return Err("cluster_probe expect-rejected accepts exactly one address".into());
    }

    let mut client = new_client(address)?;
    let before = client.health()?;
    let started = Instant::now();
    match client.ensure_namespace(namespace) {
        Ok(_) => Err("isolated cluster member unexpectedly ACKed a mutation".into()),
        Err(ClientError::Broker(error)) if error.code() == BrokerErrorCode::TransportError => {
            println!(
                "{}",
                json!({
                    "status": "rejected",
                    "address": address.to_string(),
                    "term_before": before.term.get(),
                    "revision_before": before.revision.get(),
                    "elapsed_ms": started.elapsed().as_millis(),
                    "failure": "broker_transport_error",
                })
            );
            Ok(())
        }
        Err(ClientError::Transport(_)) => {
            println!(
                "{}",
                json!({
                    "status": "rejected",
                    "address": address.to_string(),
                    "term_before": before.term.get(),
                    "revision_before": before.revision.get(),
                    "elapsed_ms": started.elapsed().as_millis(),
                    "failure": "client_transport_timeout_or_close",
                })
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn assert_exact_convergence(mut args: impl Iterator<Item = String>) -> Result<(), Box<dyn Error>> {
    let expected_term = args
        .next()
        .ok_or("cluster_probe assert-exact term is missing")?
        .parse::<u64>()?;
    let expected_revision = args
        .next()
        .ok_or("cluster_probe assert-exact revision is missing")?
        .parse::<u64>()?;
    let min_reachable = args
        .next()
        .ok_or("cluster_probe assert-exact min-reachable is missing")?
        .parse::<usize>()?;
    let addresses = args
        .map(|value| value.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()?;
    if addresses.len() < min_reachable || min_reachable == 0 {
        return Err(
            "cluster_probe assert-exact min-reachable must be positive and no greater than address count"
                .into(),
        );
    }

    let deadline = Instant::now() + PROBE_TIMEOUT;
    loop {
        let mut converged = 0_usize;
        for &address in &addresses {
            let mut client = match new_client(address) {
                Ok(client) => client,
                Err(ClientError::Transport(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            match client.health() {
                Ok(health)
                    if health.term.get() == expected_term
                        && health.revision.get() == expected_revision =>
                {
                    converged = converged.saturating_add(1);
                }
                Ok(health) if health.revision.get() > expected_revision => {
                    return Err(format!(
                        "cluster member {address} advanced to revision {} beyond expected {expected_revision}; possible late/ghost apply",
                        health.revision.get()
                    )
                    .into());
                }
                Ok(_) | Err(ClientError::Transport(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if converged >= min_reachable {
            println!(
                "{}",
                json!({
                    "status": "exact",
                    "term": expected_term,
                    "revision": expected_revision,
                    "min_reachable": min_reachable,
                    "address_count": addresses.len(),
                })
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "only {converged} cluster members reached exact term={expected_term} revision={expected_revision}; required {min_reachable}"
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn try_commit(
    namespace: &NamespaceId,
    addresses: &[SocketAddr],
) -> Result<Option<CommittedProbe>, Box<dyn Error>> {
    for &address in addresses {
        let mut client = match new_client(address) {
            Ok(client) => client,
            Err(ClientError::Transport(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        match client.ensure_namespace(namespace.clone()) {
            Ok(_) => {
                let health = client.health()?;
                return Ok(Some(CommittedProbe {
                    leader: address,
                    term: health.term.get(),
                    revision: health.revision.get(),
                }));
            }
            Err(ClientError::Broker(error)) if error.code() == BrokerErrorCode::TransportError => {}
            Err(ClientError::Transport(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

fn wait_for_convergence(
    addresses: &[SocketAddr],
    min_reachable: usize,
    target_term: u64,
    target_revision: u64,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    loop {
        let mut converged = 0_usize;
        for &address in addresses {
            let mut client = match new_client(address) {
                Ok(client) => client,
                Err(ClientError::Transport(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            match client.health() {
                Ok(health)
                    if health.term.get() >= target_term
                        && health.revision.get() >= target_revision =>
                {
                    converged = converged.saturating_add(1);
                }
                Ok(_) | Err(ClientError::Transport(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if converged >= min_reachable {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "only {converged} cluster members converged; required {min_reachable}"
            )
            .into());
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn new_client(address: SocketAddr) -> Result<BrokerClient, ClientError> {
    BrokerClient::new(BrokerClientConfig {
        address,
        timeout: CLIENT_TIMEOUT,
        max_response_frame_bytes: MAX_RESPONSE_FRAME_BYTES,
    })
}
