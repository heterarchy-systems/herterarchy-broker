use std::io::BufReader;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use agent_broker_consensus::{
    ClusterRaftObserver, ClusterRaftProgress, ClusterRaftReadiness, ClusterRaftReadinessStatus,
};
use agent_broker_domain::{ConsumerGroupDirectory, ConsumerGroupId, ConsumerGroupSummary};
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
const DEFAULT_GROUP_PAGE_LIMIT: usize = 8;
const MAX_GROUP_PAGE_LIMIT: usize = 8;

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

/// Standalone topology observer for the same read-only operations-v1 listener.
///
/// Standalone mode has no Raft readiness surface, but it shares the same bounded state-owner
/// query path for authoritative Consumer Group management reads.
#[derive(Clone)]
pub struct StandaloneOperationsObserver {
    state_owner: StateOwnerHandle,
    broker_server: BrokerServerObserver,
}

impl StandaloneOperationsObserver {
    #[must_use]
    pub fn new(state_owner: StateOwnerHandle, broker_server: BrokerServerObserver) -> Self {
        Self {
            state_owner,
            broker_server,
        }
    }

    #[must_use]
    pub fn liveness(&self) -> bool {
        self.broker_server.load().serving
    }
}

#[derive(Clone)]
enum OperationsObserver {
    Cluster(ClusterOperationsObserver),
    Standalone(StandaloneOperationsObserver),
}

impl OperationsObserver {
    fn state_owner(&self) -> &StateOwnerHandle {
        match self {
            Self::Cluster(observer) => &observer.state_owner,
            Self::Standalone(observer) => &observer.state_owner,
        }
    }

    fn liveness(&self) -> bool {
        match self {
            Self::Cluster(observer) => observer.liveness(),
            Self::Standalone(observer) => observer.liveness(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum OperationsRequest {
    Liveness,
    Readiness,
    Status,
    DescribeGroup {
        group_id: ConsumerGroupId,
    },
    ListGroups {
        after_group_id: Option<ConsumerGroupId>,
        limit: usize,
    },
}

/// Separate read-only operations-v1 TCP server.
pub struct OperationsServer {
    listener: TcpListener,
    config: OperationsServerConfig,
    observer: OperationsObserver,
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
        Self::bind_with_observer(
            config,
            OperationsObserver::Cluster(observer),
            OperationsBindPolicy::LocalOnly,
        )
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
        Self::bind_with_observer(config, OperationsObserver::Cluster(observer), bind_policy)
    }

    /// Bind a local-only operations listener for standalone Broker topology.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for invalid bounds or socket setup failures.
    pub fn bind_standalone(
        config: OperationsServerConfig,
        observer: StandaloneOperationsObserver,
    ) -> Result<Self, RuntimeError> {
        Self::bind_with_observer(
            config,
            OperationsObserver::Standalone(observer),
            OperationsBindPolicy::LocalOnly,
        )
    }

    /// Bind a standalone operations listener using an explicit exposure policy.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for invalid bounds, exposure, or socket setup failures.
    pub fn bind_standalone_with_policy(
        config: OperationsServerConfig,
        observer: StandaloneOperationsObserver,
        bind_policy: OperationsBindPolicy,
    ) -> Result<Self, RuntimeError> {
        Self::bind_with_observer(
            config,
            OperationsObserver::Standalone(observer),
            bind_policy,
        )
    }

    fn bind_with_observer(
        config: OperationsServerConfig,
        observer: OperationsObserver,
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
    observer: &OperationsObserver,
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
    if object.get("schema_version").and_then(Value::as_u64) != Some(OPERATIONS_SCHEMA_VERSION) {
        return Err("unsupported_schema_version");
    }
    match object.get("operation").and_then(Value::as_str) {
        Some("liveness") => parse_exact_status_request(object, OperationsRequest::Liveness),
        Some("readiness") => parse_exact_status_request(object, OperationsRequest::Readiness),
        Some("status") => parse_exact_status_request(object, OperationsRequest::Status),
        Some("describe_group") => parse_describe_group_request(object),
        Some("list_groups") => parse_list_groups_request(object),
        _ => Err("unsupported_operation"),
    }
}

fn parse_exact_status_request(
    object: &serde_json::Map<String, Value>,
    request: OperationsRequest,
) -> Result<OperationsRequest, &'static str> {
    if object.len() != 2 {
        return Err("unexpected_fields");
    }
    Ok(request)
}

fn parse_describe_group_request(
    object: &serde_json::Map<String, Value>,
) -> Result<OperationsRequest, &'static str> {
    if object.len() != 3 {
        return Err("unexpected_fields");
    }
    let group_id = object
        .get("group_id")
        .and_then(Value::as_str)
        .ok_or("group_id_required")?;
    Ok(OperationsRequest::DescribeGroup {
        group_id: ConsumerGroupId::new(group_id).map_err(|_error| "invalid_group_id")?,
    })
}

fn parse_list_groups_request(
    object: &serde_json::Map<String, Value>,
) -> Result<OperationsRequest, &'static str> {
    if object.len() > 4 {
        return Err("unexpected_fields");
    }
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "schema_version" | "operation" | "limit" | "after_group_id"
        )
    }) {
        return Err("unexpected_fields");
    }
    let limit = match object.get("limit") {
        None => DEFAULT_GROUP_PAGE_LIMIT,
        Some(value) => usize::try_from(value.as_u64().ok_or("invalid_limit")?)
            .map_err(|_error| "invalid_limit")?,
    };
    if !(1..=MAX_GROUP_PAGE_LIMIT).contains(&limit) {
        return Err("invalid_limit");
    }
    let after_group_id = match object.get("after_group_id") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            ConsumerGroupId::new(value.as_str().ok_or("invalid_after_group_id")?)
                .map_err(|_error| "invalid_after_group_id")?,
        ),
    };
    Ok(OperationsRequest::ListGroups {
        after_group_id,
        limit,
    })
}

fn encode_response(
    request: OperationsRequest,
    observer: &OperationsObserver,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let value = match request {
        OperationsRequest::Liveness => json!({
            "schema_version": OPERATIONS_SCHEMA_VERSION,
            "operation": "liveness",
            "live": observer.liveness(),
        }),
        OperationsRequest::Readiness => match observer {
            OperationsObserver::Cluster(observer) => {
                snapshot_json("readiness", &observer.snapshot())
            }
            OperationsObserver::Standalone(_) => {
                operations_error_json("unsupported_operation_for_topology")
            }
        },
        OperationsRequest::Status => match observer {
            OperationsObserver::Cluster(observer) => snapshot_json("status", &observer.snapshot()),
            OperationsObserver::Standalone(_) => {
                operations_error_json("unsupported_operation_for_topology")
            }
        },
        OperationsRequest::DescribeGroup { group_id } => {
            group_directory_value(observer, |directory| match directory.group(&group_id) {
                Some(group) => json!({
                    "schema_version": OPERATIONS_SCHEMA_VERSION,
                    "operation": "describe_group",
                    "status": "ok",
                    "broker_term": directory.term().get(),
                    "broker_revision": directory.revision().get(),
                    "group": group_summary_json(group),
                }),
                None => operations_error_json("group_not_found"),
            })
        }
        OperationsRequest::ListGroups {
            after_group_id,
            limit,
        } => group_directory_value(observer, |directory| {
            group_page_json(&directory, after_group_id.as_ref(), limit)
        }),
    };
    encode_bounded(&value, max_frame_bytes)
}

fn group_directory_value(
    observer: &OperationsObserver,
    build: impl FnOnce(ConsumerGroupDirectory) -> Value,
) -> Value {
    match observer.state_owner().group_directory() {
        Ok(Ok(directory)) => build(directory),
        Ok(Err(error)) => operations_error_json(match error.code() {
            agent_broker_application::BrokerErrorCode::TransportError => {
                "read_authority_unavailable"
            }
            agent_broker_application::BrokerErrorCode::PersistenceError => "broker_fail_stopped",
            _ => "broker_read_failed",
        }),
        Err(RuntimeError::StateOwnerSaturated) => operations_error_json("state_owner_saturated"),
        Err(RuntimeError::StateOwnerStopped | RuntimeError::StateOwnerReplyDropped) => {
            operations_error_json("state_owner_unavailable")
        }
        Err(_) => operations_error_json("broker_read_failed"),
    }
}

fn group_page_json(
    directory: &ConsumerGroupDirectory,
    after_group_id: Option<&ConsumerGroupId>,
    limit: usize,
) -> Value {
    let groups = directory.groups();
    let start = after_group_id.map_or(0, |group_id| {
        groups.partition_point(|group| group.group_id() <= group_id)
    });
    let end = start.saturating_add(limit).min(groups.len());
    let page = &groups[start..end];
    let has_more = end < groups.len();
    json!({
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "operation": "list_groups",
        "status": "ok",
        "broker_term": directory.term().get(),
        "broker_revision": directory.revision().get(),
        "groups": page.iter().map(group_summary_json).collect::<Vec<_>>(),
        "next_after_group_id": has_more.then(|| page.last().map(ConsumerGroupSummary::group_id).map(ToString::to_string)).flatten(),
    })
}

fn group_summary_json(group: &ConsumerGroupSummary) -> Value {
    json!({
        "group_id": group.group_id().as_str(),
        "namespace_id": group.namespace_id().as_str(),
        "generation": group.generation().get(),
        "group_revision": group.revision().get(),
        "consumer_count": group.consumer_count(),
    })
}

fn operations_error_json(code: &'static str) -> Value {
    json!({
        "schema_version": OPERATIONS_SCHEMA_VERSION,
        "status": "error",
        "code": code,
    })
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
    use std::error::Error;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use agent_broker_domain::{
        ConsumerGroup, ConsumerGroupDirectory, ConsumerGroupId, NamespaceId, Revision, Term,
    };

    use super::{
        OPERATIONS_SCHEMA_VERSION, OperationsBindPolicy, OperationsRequest, OperationsServerConfig,
        group_page_json, parse_request,
    };

    #[test]
    fn operations_request_requires_exact_versioned_read_only_shape() -> Result<(), Box<dyn Error>> {
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

        let group_id = ConsumerGroupId::new("backend-company")?;
        assert_eq!(
            parse_request(
                br#"{"schema_version":1,"operation":"describe_group","group_id":"backend-company"}"#
            ),
            Ok(OperationsRequest::DescribeGroup {
                group_id: group_id.clone(),
            })
        );
        assert_eq!(
            parse_request(br#"{"schema_version":1,"operation":"describe_group"}"#),
            Err("unexpected_fields")
        );
        assert_eq!(
            parse_request(
                br#"{"schema_version":1,"operation":"list_groups","limit":8,"after_group_id":"backend-company"}"#
            ),
            Ok(OperationsRequest::ListGroups {
                after_group_id: Some(group_id),
                limit: 8,
            })
        );
        assert_eq!(
            parse_request(br#"{"schema_version":1,"operation":"list_groups","limit":0}"#),
            Err("invalid_limit")
        );
        assert_eq!(
            parse_request(br#"{"schema_version":1,"operation":"list_groups","limit":9}"#),
            Err("invalid_limit")
        );
        assert_eq!(
            parse_request(
                br#"{"schema_version":1,"operation":"list_groups","mutation":"forbidden"}"#
            ),
            Err("unexpected_fields")
        );
        Ok(())
    }

    #[test]
    fn group_listing_is_bounded_and_cursor_ordered() -> Result<(), Box<dyn Error>> {
        let namespace_id = NamespaceId::new("project")?;
        let groups = (0..10)
            .map(|index| {
                Ok::<_, agent_broker_domain::IdentifierError>(
                    ConsumerGroup::new(
                        ConsumerGroupId::new(format!("company-{index:02}"))?,
                        namespace_id.clone(),
                    )
                    .summary(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let directory = ConsumerGroupDirectory::new(Term::INITIAL, Revision::new(10), groups);

        let first = group_page_json(&directory, None, 8);
        let Some(first_groups) = first["groups"].as_array() else {
            return Err(std::io::Error::other("first page groups must be an array").into());
        };
        assert_eq!(first_groups.len(), 8);
        assert_eq!(first_groups[0]["group_id"], "company-00");
        assert_eq!(first_groups[7]["group_id"], "company-07");
        assert_eq!(first["next_after_group_id"], "company-07");

        let cursor = ConsumerGroupId::new("company-07")?;
        let second = group_page_json(&directory, Some(&cursor), 8);
        let Some(second_groups) = second["groups"].as_array() else {
            return Err(std::io::Error::other("second page groups must be an array").into());
        };
        assert_eq!(second_groups.len(), 2);
        assert_eq!(second_groups[0]["group_id"], "company-08");
        assert_eq!(second_groups[1]["group_id"], "company-09");
        assert!(second["next_after_group_id"].is_null());
        Ok(())
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
