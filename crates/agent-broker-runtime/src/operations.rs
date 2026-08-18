use std::io::BufReader;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use agent_broker_consensus::{
    ClusterRaftObserver, ClusterRaftProgress, ClusterRaftReadiness, ClusterRaftReadinessStatus,
};
use serde_json::{Value, json};

use crate::tcp_server::{FrameRead, configure_connection, read_request_frame, write_frame};
use crate::{
    BrokerServerLoad, BrokerServerObserver, RuntimeError, StateOwnerHandle, StateOwnerLoad,
};

const OPERATIONS_SCHEMA_VERSION: u64 = 1;
const DEFAULT_OPERATIONS_PORT: u16 = 8_812;
const DEFAULT_MAX_FRAME_BYTES: usize = 4 * 1024;
const MAX_FRAME_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 32;
const MAX_CONNECTIONS: usize = 256;
const DEFAULT_CONNECTION_IO_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CONNECTION_IO_TIMEOUT: Duration = Duration::from_secs(30);
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_REPORTED_MEMBERS: usize = 64;

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub enum OperationsBindPolicy {
    #[default]
    LocalOnly,
    ContainerBridge,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OperationsServerConfig {
    pub address: SocketAddr,
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub connection_io_timeout: Duration,
}

impl Default for OperationsServerConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_OPERATIONS_PORT),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            connection_io_timeout: DEFAULT_CONNECTION_IO_TIMEOUT,
        }
    }
}

impl OperationsServerConfig {
    fn validate(self, bind_policy: OperationsBindPolicy) -> Result<Self, RuntimeError> {
        if bind_policy == OperationsBindPolicy::LocalOnly && !self.address.ip().is_loopback() {
            return Err(RuntimeError::InvalidConfiguration(
                "operations server address must be loopback-only",
            ));
        }
        if !(256..=MAX_FRAME_BYTES).contains(&self.max_frame_bytes) {
            return Err(RuntimeError::InvalidConfiguration(
                "operations max_frame_bytes must be between 256 and 16384",
            ));
        }
        if !(1..=MAX_CONNECTIONS).contains(&self.max_connections) {
            return Err(RuntimeError::InvalidConfiguration(
                "operations max_connections must be between 1 and 256",
            ));
        }
        if self.connection_io_timeout.is_zero()
            || self.connection_io_timeout > MAX_CONNECTION_IO_TIMEOUT
        {
            return Err(RuntimeError::InvalidConfiguration(
                "operations connection_io_timeout must be between 1ns and 30s",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ClusterOperationsReason {
    Ready,
    BrokerServerNotServing,
    StateOwnerSaturated,
    Consensus(ClusterRaftReadinessStatus),
}

impl ClusterOperationsReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::BrokerServerNotServing => "broker_server_not_serving",
            Self::StateOwnerSaturated => "state_owner_saturated",
            Self::Consensus(status) => status.as_str(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClusterOperationsSnapshot {
    pub live: bool,
    pub write_ready: bool,
    pub reason: ClusterOperationsReason,
    pub broker_server: BrokerServerLoad,
    pub state_owner: StateOwnerLoad,
    pub consensus: ClusterRaftReadiness,
    pub maintenance_authority: bool,
}

#[derive(Clone)]
pub struct ClusterOperationsObserver {
    consensus: ClusterRaftObserver,
    state_owner: StateOwnerHandle,
    broker_server: BrokerServerObserver,
}

impl ClusterOperationsObserver {
    #[must_use]
    pub fn new(
        consensus: ClusterRaftObserver,
        state_owner: StateOwnerHandle,
        broker_server: BrokerServerObserver,
    ) -> Self {
        Self {
            consensus,
            state_owner,
            broker_server,
        }
    }

    #[must_use]
    pub fn liveness(&self) -> bool {
        self.broker_server.load().serving
    }

    #[must_use]
    pub fn snapshot(&self) -> ClusterOperationsSnapshot {
        let broker_server = self.broker_server.load();
        let state_owner = self.state_owner.load();
        let consensus = self.consensus.readiness();
        let state_owner_saturated = state_owner.queued_jobs >= state_owner.capacity;
        let live = broker_server.serving;
        let reason = if !live {
            ClusterOperationsReason::BrokerServerNotServing
        } else if !consensus.is_write_ready() {
            ClusterOperationsReason::Consensus(consensus.status)
        } else if state_owner_saturated {
            ClusterOperationsReason::StateOwnerSaturated
        } else {
            ClusterOperationsReason::Ready
        };
        let write_ready = reason == ClusterOperationsReason::Ready;
        let maintenance_authority = write_ready
            && consensus
                .progress
                .as_ref()
                .is_some_and(|progress| progress.current_leader == Some(progress.node_id));
        ClusterOperationsSnapshot {
            live,
            write_ready,
            reason,
            broker_server,
            state_owner,
            consensus,
            maintenance_authority,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum OperationsRequest {
    Liveness,
    Readiness,
    Status,
}

/// Separate read-only operations-v1 TCP server.
pub struct OperationsServer {
    listener: TcpListener,
    config: OperationsServerConfig,
    observer: ClusterOperationsObserver,
}

impl OperationsServer {
    /// Bind a local-only operations listener.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for invalid bounds or socket setup failures.
    pub fn bind(
        config: OperationsServerConfig,
        observer: ClusterOperationsObserver,
    ) -> Result<Self, RuntimeError> {
        Self::bind_with_policy(config, observer, OperationsBindPolicy::LocalOnly)
    }

    /// Bind an operations listener using an explicit exposure policy.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for invalid bounds, exposure, or socket setup failures.
    pub fn bind_with_policy(
        config: OperationsServerConfig,
        observer: ClusterOperationsObserver,
        bind_policy: OperationsBindPolicy,
    ) -> Result<Self, RuntimeError> {
        let config = config.validate(bind_policy)?;
        let listener = TcpListener::bind(config.address)
            .map_err(|error| RuntimeError::io("operations TCP bind failed", error))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| RuntimeError::io("operations TCP nonblocking setup failed", error))?;
        Ok(Self {
            listener,
            config,
            observer,
        })
    }

    /// Return the actual bound address, including an ephemeral port selected from port zero.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the listener address cannot be queried.
    pub fn local_addr(&self) -> Result<SocketAddr, RuntimeError> {
        self.listener
            .local_addr()
            .map_err(|error| RuntimeError::io("operations TCP local address read failed", error))
    }

    /// Serve bounded read-only operations connections until `stop` is set.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] on accept or worker-thread creation failures.
    pub fn serve_until(&self, stop: &AtomicBool) -> Result<(), RuntimeError> {
        let active = Arc::new(AtomicUsize::new(0));
        while !stop.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _peer)) => {
                    if active.fetch_add(1, Ordering::AcqRel) >= self.config.max_connections {
                        active.fetch_sub(1, Ordering::AcqRel);
                        drop(stream);
                        continue;
                    }
                    let active_guard = Arc::clone(&active);
                    let observer = self.observer.clone();
                    let max_frame_bytes = self.config.max_frame_bytes;
                    let connection_io_timeout = self.config.connection_io_timeout;
                    let spawn = thread::Builder::new()
                        .name("agent-broker-operations-connection".to_owned())
                        .spawn(move || {
                            let _guard = OperationsConnectionGuard(active_guard);
                            if let Err(error) = handle_operations_connection(
                                stream,
                                &observer,
                                max_frame_bytes,
                                connection_io_timeout,
                            ) {
                                eprintln!("agentbrokerd operations connection failed: {error}");
                            }
                        });
                    if let Err(error) = spawn {
                        active.fetch_sub(1, Ordering::AcqRel);
                        return Err(RuntimeError::io(
                            "operations connection thread spawn failed",
                            error,
                        ));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(ACCEPT_POLL_INTERVAL);
                }
                Err(error) => return Err(RuntimeError::io("operations TCP accept failed", error)),
            }
        }
        Ok(())
    }
}

struct OperationsConnectionGuard(Arc<AtomicUsize>);

impl Drop for OperationsConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn handle_operations_connection(
    stream: TcpStream,
    observer: &ClusterOperationsObserver,
    max_frame_bytes: usize,
    connection_io_timeout: Duration,
) -> Result<(), RuntimeError> {
    configure_connection(&stream, connection_io_timeout)?;
    let mut reader = BufReader::new(stream);
    let response = match read_request_frame(&mut reader, max_frame_bytes)? {
        FrameRead::Eof => return Ok(()),
        FrameRead::TooLarge => encode_error("frame_too_large", max_frame_bytes)?,
        FrameRead::Frame(frame) => match parse_request(&frame) {
            Ok(request) => encode_response(request, observer, max_frame_bytes)?,
            Err(code) => encode_error(code, max_frame_bytes)?,
        },
    };
    write_frame(reader.get_mut(), &response)
}

fn parse_request(frame: &[u8]) -> Result<OperationsRequest, &'static str> {
    let value: Value = serde_json::from_slice(frame).map_err(|_error| "invalid_json")?;
    let object = value.as_object().ok_or("request_must_be_object")?;
    if object.len() != 2 {
        return Err("unexpected_fields");
    }
    if object.get("schema_version").and_then(Value::as_u64) != Some(OPERATIONS_SCHEMA_VERSION) {
        return Err("unsupported_schema_version");
    }
    match object.get("operation").and_then(Value::as_str) {
        Some("liveness") => Ok(OperationsRequest::Liveness),
        Some("readiness") => Ok(OperationsRequest::Readiness),
        Some("status") => Ok(OperationsRequest::Status),
        _ => Err("unsupported_operation"),
    }
}

fn encode_response(
    request: OperationsRequest,
    observer: &ClusterOperationsObserver,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let value = match request {
        OperationsRequest::Liveness => json!({
            "schema_version": OPERATIONS_SCHEMA_VERSION,
            "operation": "liveness",
            "live": observer.liveness(),
        }),
        OperationsRequest::Readiness => snapshot_json("readiness", &observer.snapshot()),
        OperationsRequest::Status => snapshot_json("status", &observer.snapshot()),
    };
    encode_bounded(&value, max_frame_bytes)
}

fn encode_error(code: &'static str, max_frame_bytes: usize) -> Result<Vec<u8>, RuntimeError> {
    encode_bounded(
        &json!({
            "schema_version": OPERATIONS_SCHEMA_VERSION,
            "status": "error",
            "code": code,
        }),
        max_frame_bytes,
    )
}

fn encode_bounded(value: &Value, max_frame_bytes: usize) -> Result<Vec<u8>, RuntimeError> {
    let mut encoded = serde_json::to_vec(&value).map_err(|error| {
        RuntimeError::io(
            "operations response serialization failed",
            std::io::Error::other(error),
        )
    })?;
    encoded.push(b'\n');
    if encoded.len() > max_frame_bytes {
        return Err(RuntimeError::InvalidConfiguration(
            "operations response exceeded configured max_frame_bytes",
        ));
    }
    Ok(encoded)
}

fn snapshot_json(operation: &'static str, snapshot: &ClusterOperationsSnapshot) -> Value {
    json!({
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "operation": operation,
        "live": snapshot.live,
        "write_ready": snapshot.write_ready,
        "reason": snapshot.reason.as_str(),
        "maintenance_authority": snapshot.maintenance_authority,
        "broker_server": {
            "serving": snapshot.broker_server.serving,
            "active_connections": snapshot.broker_server.active_connections,
            "max_connections": snapshot.broker_server.max_connections,
            "saturated": snapshot.broker_server.active_connections
                >= snapshot.broker_server.max_connections,
        },
        "state_owner": {
            "active_jobs": snapshot.state_owner.active_jobs,
            "queued_jobs": snapshot.state_owner.queued_jobs,
            "capacity": snapshot.state_owner.capacity,
            "saturated": snapshot.state_owner.queued_jobs >= snapshot.state_owner.capacity,
        },
        "consensus": {
            "status": snapshot.consensus.status.as_str(),
            "write_ready": snapshot.consensus.is_write_ready(),
            "progress": snapshot.consensus.progress.as_ref().map(progress_json),
        },
    })
}

fn progress_json(progress: &ClusterRaftProgress) -> Value {
    let voters = progress
        .voters
        .iter()
        .copied()
        .take(MAX_REPORTED_MEMBERS)
        .collect::<Vec<_>>();
    let learners = progress
        .learners
        .iter()
        .copied()
        .take(MAX_REPORTED_MEMBERS)
        .collect::<Vec<_>>();
    json!({
        "node_id": progress.node_id,
        "raft_rpc_addr": progress.raft_rpc_addr.to_string(),
        "raft_rpc_queued_connections": progress.raft_rpc_queued_connections,
        "raft_rpc_active_connections": progress.raft_rpc_active_connections,
        "raft_term": progress.raft_term,
        "last_log_index": progress.last_log_index,
        "committed_index": progress.committed_index,
        "applied_index": progress.applied_index,
        "snapshot_index": progress.snapshot_index,
        "purged_index": progress.purged_index,
        "broker_term": progress.broker_term.get(),
        "broker_revision": progress.broker_revision.get(),
        "current_leader": progress.current_leader,
        "voters": voters,
        "learners": learners,
        "membership_truncated": progress.voters.len() > MAX_REPORTED_MEMBERS
            || progress.learners.len() > MAX_REPORTED_MEMBERS,
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use super::{
        OPERATIONS_SCHEMA_VERSION, OperationsBindPolicy, OperationsRequest, OperationsServerConfig,
        parse_request,
    };

    #[test]
    fn operations_request_requires_exact_versioned_read_only_shape() {
        for (operation, expected) in [
            ("liveness", OperationsRequest::Liveness),
            ("readiness", OperationsRequest::Readiness),
            ("status", OperationsRequest::Status),
        ] {
            let frame = format!(
                "{{\"schema_version\":{OPERATIONS_SCHEMA_VERSION},\"operation\":\"{operation}\"}}\n"
            );
            assert_eq!(parse_request(frame.as_bytes()), Ok(expected));
        }
        assert_eq!(
            parse_request(br#"{"schema_version":2,"operation":"status"}"#),
            Err("unsupported_schema_version")
        );
        assert_eq!(
            parse_request(br#"{"schema_version":1,"operation":"status","mutation":"forbidden"}"#),
            Err("unexpected_fields")
        );
        assert_eq!(
            parse_request(br#"{"schema_version":1,"operation":"mutate"}"#),
            Err("unsupported_operation")
        );
    }

    #[test]
    fn operations_server_bounds_listener_resources_and_exposure() {
        let default = OperationsServerConfig::default();
        assert!(default.validate(OperationsBindPolicy::LocalOnly).is_ok());
        assert!(
            OperationsServerConfig {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8_812),
                ..default
            }
            .validate(OperationsBindPolicy::LocalOnly)
            .is_err()
        );
        assert!(
            OperationsServerConfig {
                max_connections: 0,
                ..default
            }
            .validate(OperationsBindPolicy::LocalOnly)
            .is_err()
        );
        assert!(
            OperationsServerConfig {
                connection_io_timeout: Duration::ZERO,
                ..default
            }
            .validate(OperationsBindPolicy::LocalOnly)
            .is_err()
        );
    }
}
