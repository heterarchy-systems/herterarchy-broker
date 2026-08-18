use std::error::Error;
use std::fmt;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::num::NonZeroUsize;
use std::time::Duration;

use agent_broker_application::{
    BrokerErrorDisposition, CommandIdentity, CommandSessionId, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
use agent_broker_protocol::{
    BrokerRequest, OwnerAcquisitionRequestV3, OwnerIdentifiedBrokerRequestV3, RequestId,
    ResponseDecodeError, SuccessPayload, decode_owner_acquisition_response_with_limit,
    decode_owner_mutation_response_with_limit, encode_owner_acquisition_request_with_limit,
    encode_owner_mutation_request_with_limit,
};
use serde_json::Value;

use crate::{BrokerClient, BrokerClientConfig, ClientError};

const OPERATIONS_SCHEMA_VERSION: u64 = 1;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_MAX_FRAME_BYTES: usize = 128 * 1024;
const MIN_FRAME_BYTES: usize = 4 * 1024;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const OPERATIONS_READINESS_REQUEST: &[u8] = b"{\"schema_version\":1,\"operation\":\"readiness\"}\n";

/// One fixed member of the currently supported static three-node client topology.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StaticClusterNode {
    pub node_id: u64,
    pub broker_address: SocketAddr,
    pub operations_address: SocketAddr,
}

/// Bounded exact-frame retry policy for static cluster routing.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StaticClusterRoutingRetryPolicy {
    max_attempts: NonZeroUsize,
}

impl StaticClusterRoutingRetryPolicy {
    #[must_use]
    pub const fn new(max_attempts: NonZeroUsize) -> Self {
        Self { max_attempts }
    }

    #[must_use]
    pub const fn max_attempts(self) -> NonZeroUsize {
        self.max_attempts
    }
}

impl Default for StaticClusterRoutingRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// Configuration for fail-closed discovery of the single write-ready node in a static cluster.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StaticClusterRouterConfig {
    pub nodes: [StaticClusterNode; 3],
    pub timeout: Duration,
    pub max_response_frame_bytes: usize,
    pub retry_policy: StaticClusterRoutingRetryPolicy,
}

impl StaticClusterRouterConfig {
    /// Validate fixed topology, loopback-only client surfaces, timeout, and response bounds.
    ///
    /// # Errors
    /// Returns [`StaticClusterRoutingError::InvalidConfiguration`] for unsafe or ambiguous input.
    pub fn validate(self) -> Result<Self, StaticClusterRoutingError> {
        if self.timeout.is_zero() {
            return Err(StaticClusterRoutingError::InvalidConfiguration(
                "static cluster router timeout must be positive".to_owned(),
            ));
        }
        if !(MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(&self.max_response_frame_bytes) {
            return Err(StaticClusterRoutingError::InvalidConfiguration(
                "static cluster router max_response_frame_bytes must be in 4096..=1048576"
                    .to_owned(),
            ));
        }
        for node in self.nodes {
            if node.node_id == 0 {
                return Err(StaticClusterRoutingError::InvalidConfiguration(
                    "static cluster node_id must be positive".to_owned(),
                ));
            }
            if !node.broker_address.ip().is_loopback()
                || !node.operations_address.ip().is_loopback()
            {
                return Err(StaticClusterRoutingError::InvalidConfiguration(
                    "static cluster Broker and operations addresses must be loopback-only"
                        .to_owned(),
                ));
            }
            if node.broker_address == node.operations_address {
                return Err(StaticClusterRoutingError::InvalidConfiguration(
                    "static cluster Broker and operations addresses must be distinct".to_owned(),
                ));
            }
        }
        for left in 0..self.nodes.len() {
            for right in (left + 1)..self.nodes.len() {
                if self.nodes[left].node_id == self.nodes[right].node_id
                    || self.nodes[left].broker_address == self.nodes[right].broker_address
                    || self.nodes[left].operations_address == self.nodes[right].operations_address
                {
                    return Err(StaticClusterRoutingError::InvalidConfiguration(
                        "static cluster node IDs and endpoint addresses must be unique".to_owned(),
                    ));
                }
            }
        }
        Ok(self)
    }
}

impl Default for StaticClusterRouterConfig {
    fn default() -> Self {
        let localhost = std::net::Ipv4Addr::LOCALHOST;
        Self {
            nodes: [
                StaticClusterNode {
                    node_id: 1,
                    broker_address: SocketAddr::from((localhost, 8_811)),
                    operations_address: SocketAddr::from((localhost, 9_811)),
                },
                StaticClusterNode {
                    node_id: 2,
                    broker_address: SocketAddr::from((localhost, 8_812)),
                    operations_address: SocketAddr::from((localhost, 9_812)),
                },
                StaticClusterNode {
                    node_id: 3,
                    broker_address: SocketAddr::from((localhost, 8_813)),
                    operations_address: SocketAddr::from((localhost, 9_813)),
                },
            ],
            timeout: DEFAULT_TIMEOUT,
            max_response_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            retry_policy: StaticClusterRoutingRetryPolicy::default(),
        }
    }
}

#[derive(Debug)]
pub enum StaticClusterRoutingError {
    InvalidConfiguration(String),
    InvalidOperationsResponse { node_id: u64, reason: String },
    NoWriteReadyLeader,
    MultipleWriteReadyLeaders(Vec<u64>),
    Client(ClientError),
}

impl fmt::Display for StaticClusterRoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(reason) => {
                write!(
                    formatter,
                    "invalid static cluster router configuration: {reason}"
                )
            }
            Self::InvalidOperationsResponse { node_id, reason } => {
                write!(
                    formatter,
                    "invalid operations-v1 response from node {node_id}: {reason}"
                )
            }
            Self::NoWriteReadyLeader => {
                formatter.write_str("static cluster has no observed write-ready leader")
            }
            Self::MultipleWriteReadyLeaders(node_ids) => {
                write!(
                    formatter,
                    "static cluster reported multiple write-ready leaders: {node_ids:?}"
                )
            }
            Self::Client(error) => error.fmt(formatter),
        }
    }
}

impl Error for StaticClusterRoutingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Client(error) => Some(error),
            Self::InvalidConfiguration(_)
            | Self::InvalidOperationsResponse { .. }
            | Self::NoWriteReadyLeader
            | Self::MultipleWriteReadyLeaders(_) => None,
        }
    }
}

impl From<ClientError> for StaticClusterRoutingError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

/// Static three-node leader router using read-only operations-v1 readiness as routing authority.
pub struct StaticClusterRouter {
    config: StaticClusterRouterConfig,
    request_sequence: u64,
}

impl StaticClusterRouter {
    /// Construct a disconnected router.
    ///
    /// # Errors
    /// Returns a configuration error when topology or bounds are unsafe.
    pub fn new(config: StaticClusterRouterConfig) -> Result<Self, StaticClusterRoutingError> {
        Ok(Self {
            config: config.validate()?,
            request_sequence: 0,
        })
    }

    /// Probe all configured operations endpoints and return the single currently write-ready node.
    ///
    /// Unreachable operations endpoints are treated as unavailable, which permits 2/3 failover.
    /// Reachable malformed or identity-mismatched endpoints fail closed. Zero or multiple ready
    /// nodes also fail closed.
    ///
    /// # Errors
    /// Returns a routing error when no unique verified write-ready node can be established.
    pub fn discover_write_leader(&self) -> Result<StaticClusterNode, StaticClusterRoutingError> {
        let mut ready = Vec::with_capacity(1);
        for node in self.config.nodes {
            if let Some(()) = probe_operations(
                node,
                self.config.timeout,
                self.config.max_response_frame_bytes,
            )? {
                ready.push(node);
            }
        }
        match ready.as_slice() {
            [leader] => Ok(*leader),
            [] => Err(StaticClusterRoutingError::NoWriteReadyLeader),
            _ => Err(StaticClusterRoutingError::MultipleWriteReadyLeaders(
                ready.iter().map(|node| node.node_id).collect(),
            )),
        }
    }

    /// Acquire command-session ownership on the single verified write-ready node.
    ///
    /// Transport and Broker `UNKNOWN` outcomes may trigger bounded rediscovery. Every attempt sends
    /// the exact same serialized protocol-v3 frame, including request ID, session, expected epoch,
    /// and owner-instance ID. Stable `REJECTED`/`COMMITTED` Broker outcomes and protocol failures are
    /// never redirected automatically.
    ///
    /// # Errors
    /// Returns routing or client failure after bounded exact-frame attempts.
    pub fn acquire_command_session_owner(
        &mut self,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, StaticClusterRoutingError> {
        let request_id = self.next_request_id()?;
        let request = OwnerAcquisitionRequestV3::new(
            request_id.clone(),
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        );
        let frame = encode_owner_acquisition_request_with_limit(
            &request,
            self.config.max_response_frame_bytes,
        )
        .map_err(ClientError::Protocol)?;
        self.route_exact_frame(
            &frame,
            |response| match decode_owner_acquisition_response_with_limit(
                response,
                &request_id,
                self.config.max_response_frame_bytes,
            ) {
                Ok(epoch) => Ok(epoch),
                Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
                Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
            },
        )
    }

    /// Execute one owner-aware mutation on the single verified write-ready node.
    ///
    /// The request is serialized exactly once before any endpoint is chosen. Rediscovery after a
    /// transport/`UNKNOWN` outcome therefore cannot mutate request ID or command identity.
    ///
    /// # Errors
    /// Returns routing or client failure after bounded exact-frame attempts.
    pub fn execute_owned(
        &self,
        identity: &CommandIdentity,
        request: &BrokerRequest,
    ) -> Result<SuccessPayload, StaticClusterRoutingError> {
        let operation = request.operation();
        let request_id = request.request_id().clone();
        let owned = OwnerIdentifiedBrokerRequestV3::new(identity.clone(), request.clone())
            .map_err(ClientError::Protocol)?;
        let frame =
            encode_owner_mutation_request_with_limit(&owned, self.config.max_response_frame_bytes)
                .map_err(ClientError::Protocol)?;
        self.route_exact_frame(
            &frame,
            |response| match decode_owner_mutation_response_with_limit(
                response,
                &request_id,
                operation,
                self.config.max_response_frame_bytes,
            ) {
                Ok(payload) => Ok(payload),
                Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
                Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
            },
        )
    }

    fn route_exact_frame<T, F>(
        &self,
        frame: &[u8],
        decode: F,
    ) -> Result<T, StaticClusterRoutingError>
    where
        F: Fn(&[u8]) -> Result<T, ClientError>,
    {
        for attempt in 1..=self.config.retry_policy.max_attempts().get() {
            let leader = self.discover_write_leader()?;
            let mut client = BrokerClient::new(BrokerClientConfig {
                address: leader.broker_address,
                timeout: self.config.timeout,
                max_response_frame_bytes: self.config.max_response_frame_bytes,
            })?;
            let result = client
                .round_trip_encoded(frame)
                .and_then(|response| decode(&response));
            match result {
                Ok(value) => return Ok(value),
                Err(error)
                    if should_rediscover(&error)
                        && attempt < self.config.retry_policy.max_attempts().get() => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(StaticClusterRoutingError::NoWriteReadyLeader)
    }

    fn next_request_id(&mut self) -> Result<RequestId, StaticClusterRoutingError> {
        self.request_sequence = self
            .request_sequence
            .checked_add(1)
            .ok_or(ClientError::RequestIdExhausted)?;
        RequestId::new(format!("rust-cluster-router-{}", self.request_sequence))
            .map_err(|error| {
                ClientError::Protocol(agent_broker_protocol::ProtocolCodecError::InvalidRequest(
                    error.to_string(),
                ))
            })
            .map_err(Into::into)
    }
}

fn should_rediscover(error: &ClientError) -> bool {
    match error {
        ClientError::Transport(_) => true,
        ClientError::Broker(error) => error.disposition() == BrokerErrorDisposition::Unknown,
        ClientError::Protocol(_)
        | ClientError::UnexpectedPayload(_)
        | ClientError::RequestIdExhausted => false,
    }
}

fn probe_operations(
    node: StaticClusterNode,
    timeout: Duration,
    max_response_frame_bytes: usize,
) -> Result<Option<()>, StaticClusterRoutingError> {
    let Ok(mut stream) = TcpStream::connect_timeout(&node.operations_address, timeout) else {
        return Ok(None);
    };
    if stream.set_nodelay(true).is_err()
        || stream.set_read_timeout(Some(timeout)).is_err()
        || stream.set_write_timeout(Some(timeout)).is_err()
        || stream.write_all(OPERATIONS_READINESS_REQUEST).is_err()
        || stream.flush().is_err()
    {
        return Ok(None);
    }
    let mut reader = BufReader::new(stream);
    let frame = match read_bounded_operations_frame(&mut reader, max_response_frame_bytes) {
        Ok(frame) => frame,
        Err(OperationsReadError::Transport) => return Ok(None),
        Err(OperationsReadError::FrameTooLarge) => {
            return Err(invalid_operations(
                node.node_id,
                "response frame exceeded configured bound",
            ));
        }
    };
    let value = serde_json::from_slice::<Value>(&frame)
        .map_err(|error| invalid_operations(node.node_id, format!("invalid JSON: {error}")))?;
    validate_readiness(node, &value)
}

pub(crate) fn validate_readiness(
    node: StaticClusterNode,
    value: &Value,
) -> Result<Option<()>, StaticClusterRoutingError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid_operations(node.node_id, "response must be a JSON object"))?;
    if object.get("schema_version").and_then(Value::as_u64) != Some(OPERATIONS_SCHEMA_VERSION)
        || object.get("operation").and_then(Value::as_str) != Some("readiness")
    {
        return Err(invalid_operations(
            node.node_id,
            "schema_version/operation mismatch",
        ));
    }
    let write_ready = object
        .get("write_ready")
        .and_then(Value::as_bool)
        .ok_or_else(|| invalid_operations(node.node_id, "write_ready must be boolean"))?;
    if !write_ready {
        return Ok(None);
    }
    if object.get("live").and_then(Value::as_bool) != Some(true)
        || object.get("reason").and_then(Value::as_str) != Some("ready")
        || object.get("maintenance_authority").and_then(Value::as_bool) != Some(true)
    {
        return Err(invalid_operations(
            node.node_id,
            "write-ready response lacked live/ready/maintenance authority",
        ));
    }
    let consensus = object
        .get("consensus")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid_operations(node.node_id, "consensus must be an object"))?;
    if consensus.get("status").and_then(Value::as_str) != Some("ready")
        || consensus.get("write_ready").and_then(Value::as_bool) != Some(true)
    {
        return Err(invalid_operations(
            node.node_id,
            "write-ready response had non-ready consensus",
        ));
    }
    let progress = consensus
        .get("progress")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid_operations(node.node_id, "write-ready consensus lacked progress"))?;
    let reported_node = progress
        .get("node_id")
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_operations(node.node_id, "progress.node_id must be unsigned"))?;
    let current_leader = progress
        .get("current_leader")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            invalid_operations(node.node_id, "progress.current_leader must be unsigned")
        })?;
    if reported_node != node.node_id || current_leader != node.node_id {
        return Err(invalid_operations(
            node.node_id,
            format!(
                "write-ready identity mismatch: configured={}, reported={reported_node}, current_leader={current_leader}",
                node.node_id
            ),
        ));
    }
    Ok(Some(()))
}

fn invalid_operations(node_id: u64, reason: impl Into<String>) -> StaticClusterRoutingError {
    StaticClusterRoutingError::InvalidOperationsResponse {
        node_id,
        reason: reason.into(),
    }
}

enum OperationsReadError {
    Transport,
    FrameTooLarge,
}

fn read_bounded_operations_frame(
    reader: &mut BufReader<TcpStream>,
    max_bytes: usize,
) -> Result<Vec<u8>, OperationsReadError> {
    let mut frame = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|_| OperationsReadError::Transport)?;
        if available.is_empty() {
            return Err(OperationsReadError::Transport);
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if frame.len().saturating_add(consumed) > max_bytes {
            return Err(OperationsReadError::FrameTooLarge);
        }
        frame.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(frame);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io;
    use std::io::{Read, Write};
    use std::net::{Ipv4Addr, SocketAddr, TcpListener};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use serde_json::{Value, json};

    use super::{
        StaticClusterNode, StaticClusterRouter, StaticClusterRouterConfig,
        StaticClusterRoutingError,
    };

    fn start_operations_response(
        value: Value,
    ) -> io::Result<(SocketAddr, JoinHandle<io::Result<()>>)> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;
        let thread = thread::spawn(move || -> io::Result<()> {
            let (mut stream, _) = listener.accept()?;
            stream.set_read_timeout(Some(Duration::from_secs(1)))?;
            let mut request = [0_u8; 128];
            let read = stream.read(&mut request)?;
            assert!(request[..read].ends_with(b"\n"));
            let mut response = serde_json::to_vec(&value).map_err(io::Error::other)?;
            response.push(b'\n');
            stream.write_all(&response)?;
            Ok(())
        });
        Ok((address, thread))
    }

    fn unused_address() -> io::Result<SocketAddr> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;
        drop(listener);
        Ok(address)
    }

    fn join_operations_thread(thread: JoinHandle<io::Result<()>>) -> io::Result<()> {
        match thread.join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("operations test thread panicked")),
        }
    }

    fn response(node_id: u64, ready: bool) -> Value {
        if !ready {
            return json!({
                "schema_version": 1,
                "operation": "readiness",
                "live": true,
                "write_ready": false,
                "reason": "follower",
                "maintenance_authority": false,
                "consensus": {"status": "follower", "write_ready": false, "progress": null}
            });
        }
        json!({
            "schema_version": 1,
            "operation": "readiness",
            "live": true,
            "write_ready": true,
            "reason": "ready",
            "maintenance_authority": true,
            "consensus": {
                "status": "ready",
                "write_ready": true,
                "progress": {"node_id": node_id, "current_leader": node_id}
            }
        })
    }

    fn config(operations: [SocketAddr; 3]) -> StaticClusterRouterConfig {
        let localhost = Ipv4Addr::LOCALHOST;
        StaticClusterRouterConfig {
            nodes: [
                StaticClusterNode {
                    node_id: 1,
                    broker_address: SocketAddr::from((localhost, 18_811)),
                    operations_address: operations[0],
                },
                StaticClusterNode {
                    node_id: 2,
                    broker_address: SocketAddr::from((localhost, 18_812)),
                    operations_address: operations[1],
                },
                StaticClusterNode {
                    node_id: 3,
                    broker_address: SocketAddr::from((localhost, 18_813)),
                    operations_address: operations[2],
                },
            ],
            ..StaticClusterRouterConfig::default()
        }
    }

    #[test]
    fn discovery_accepts_one_verified_ready_node_with_one_unavailable_peer()
    -> Result<(), Box<dyn Error>> {
        let unavailable = unused_address()?;
        let (ops_two, two) = start_operations_response(response(2, true))?;
        let (ops_three, three) = start_operations_response(response(3, false))?;
        let router = StaticClusterRouter::new(config([unavailable, ops_two, ops_three]))?;
        let leader = router.discover_write_leader()?;
        assert_eq!(leader.node_id, 2);
        join_operations_thread(two)?;
        join_operations_thread(three)?;
        Ok(())
    }

    #[test]
    fn discovery_rejects_multiple_ready_nodes() -> Result<(), Box<dyn Error>> {
        let (ops_one, one) = start_operations_response(response(1, true))?;
        let (ops_two, two) = start_operations_response(response(2, true))?;
        let (ops_three, three) = start_operations_response(response(3, false))?;
        let router = StaticClusterRouter::new(config([ops_one, ops_two, ops_three]))?;
        assert!(matches!(
            router.discover_write_leader(),
            Err(StaticClusterRoutingError::MultipleWriteReadyLeaders(ref nodes)) if nodes == &[1, 2]
        ));
        join_operations_thread(one)?;
        join_operations_thread(two)?;
        join_operations_thread(three)?;
        Ok(())
    }

    #[test]
    fn discovery_rejects_ready_identity_mismatch() -> Result<(), Box<dyn Error>> {
        let (ops_one, one) = start_operations_response(response(2, true))?;
        let ops_two = unused_address()?;
        let ops_three = unused_address()?;
        let router = StaticClusterRouter::new(config([ops_one, ops_two, ops_three]))?;
        assert!(matches!(
            router.discover_write_leader(),
            Err(StaticClusterRoutingError::InvalidOperationsResponse { node_id: 1, .. })
        ));
        join_operations_thread(one)?;
        Ok(())
    }

    #[test]
    fn config_rejects_duplicate_node_identity() {
        let mut config = StaticClusterRouterConfig::default();
        config.nodes[1].node_id = config.nodes[0].node_id;
        assert!(matches!(
            StaticClusterRouter::new(config),
            Err(StaticClusterRoutingError::InvalidConfiguration(_))
        ));
    }
}
