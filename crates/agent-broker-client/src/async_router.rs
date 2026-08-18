use std::sync::atomic::{AtomicU64, Ordering};

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};

use crate::router::validate_readiness;
use crate::{
    AsyncBrokerClient, BrokerClientConfig, ClientError, StaticClusterNode,
    StaticClusterRouterConfig, StaticClusterRoutingError,
};

const OPERATIONS_READINESS_REQUEST: &[u8] = b"{\"schema_version\":1,\"operation\":\"readiness\"}\n";

/// Native-Tokio fixed-three-node leader router.
pub struct AsyncStaticClusterRouter {
    config: StaticClusterRouterConfig,
    request_sequence: AtomicU64,
}

impl AsyncStaticClusterRouter {
    /// Construct an async router with the same fail-closed topology constraints as the sync router.
    ///
    /// # Errors
    /// Returns a static cluster configuration error for unsafe inputs.
    pub fn new(config: StaticClusterRouterConfig) -> Result<Self, StaticClusterRoutingError> {
        let config = config.validate()?;
        if Instant::now().checked_add(config.timeout).is_none() {
            return Err(StaticClusterRoutingError::InvalidConfiguration(
                "static cluster async timeout exceeds the platform instant range".to_owned(),
            ));
        }
        Ok(Self {
            config,
            request_sequence: AtomicU64::new(0),
        })
    }

    /// Concurrently probe all operations endpoints and require exactly one verified writer.
    ///
    /// # Errors
    /// Returns fail-closed routing errors for malformed, ambiguous, or absent authority.
    pub async fn discover_write_leader(
        &self,
    ) -> Result<StaticClusterNode, StaticClusterRoutingError> {
        let [node_1, node_2, node_3] = self.config.nodes;
        let timeout = self.config.timeout;
        let max_response_frame_bytes = self.config.max_response_frame_bytes;
        let (result_1, result_2, result_3) = tokio::join!(
            probe_operations(node_1, timeout, max_response_frame_bytes),
            probe_operations(node_2, timeout, max_response_frame_bytes),
            probe_operations(node_3, timeout, max_response_frame_bytes),
        );
        let mut ready = Vec::with_capacity(1);
        for (node, result) in [(node_1, result_1), (node_2, result_2), (node_3, result_3)] {
            if result? {
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

    /// Acquire owner authority with exact-frame bounded rediscovery.
    ///
    /// # Errors
    /// Returns routing or client failure after bounded attempts.
    pub async fn acquire_command_session_owner(
        &self,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, StaticClusterRoutingError> {
        let request_id = self.next_request_id()?;
        self.acquire_command_session_owner_with_request_id(
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        )
        .await
    }

    /// Acquire owner authority while preserving a caller-supplied request ID across recovery.
    ///
    /// # Errors
    /// Returns routing or client failure after bounded exact-frame attempts.
    pub async fn acquire_command_session_owner_with_request_id(
        &self,
        request_id: RequestId,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, StaticClusterRoutingError> {
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
        .await
    }

    /// Execute one owner-aware mutation with exact-frame bounded rediscovery.
    ///
    /// # Errors
    /// Returns routing or client failure after bounded attempts.
    pub async fn execute_owned(
        &self,
        identity: &CommandIdentity,
        request: &BrokerRequest,
    ) -> Result<SuccessPayload, StaticClusterRoutingError> {
        let request_id = request.request_id().clone();
        let operation = request.operation();
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
        .await
    }

    async fn route_exact_frame<T, F>(
        &self,
        frame: &[u8],
        decode: F,
    ) -> Result<T, StaticClusterRoutingError>
    where
        F: Fn(&[u8]) -> Result<T, ClientError>,
    {
        for attempt in 1..=self.config.retry_policy.max_attempts().get() {
            let leader = self.discover_write_leader().await?;
            let client = AsyncBrokerClient::new(BrokerClientConfig {
                address: leader.broker_address,
                timeout: self.config.timeout,
                max_response_frame_bytes: self.config.max_response_frame_bytes,
            })?;
            let result = client
                .round_trip_encoded(frame)
                .await
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

    fn next_request_id(&self) -> Result<RequestId, StaticClusterRoutingError> {
        let previous = self
            .request_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ClientError::RequestIdExhausted)?;
        RequestId::new(format!("rust-async-cluster-router-{}", previous + 1))
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

async fn probe_operations(
    node: StaticClusterNode,
    timeout: std::time::Duration,
    max_response_frame_bytes: usize,
) -> Result<bool, StaticClusterRoutingError> {
    let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
        StaticClusterRoutingError::InvalidConfiguration(
            "static cluster async timeout exceeds the platform instant range".to_owned(),
        )
    })?;
    let operation = async {
        let Ok(mut stream) = TcpStream::connect(node.operations_address).await else {
            return Ok(false);
        };
        if stream.set_nodelay(true).is_err() {
            return Ok(false);
        }
        if stream
            .write_all(OPERATIONS_READINESS_REQUEST)
            .await
            .is_err()
            || stream.flush().await.is_err()
        {
            return Ok(false);
        }
        let mut reader = BufReader::with_capacity(max_response_frame_bytes.min(64 * 1024), stream);
        let frame = match read_bounded_operations_frame(&mut reader, max_response_frame_bytes).await
        {
            Ok(frame) => frame,
            Err(AsyncOperationsReadError::Transport) => return Ok(false),
            Err(AsyncOperationsReadError::FrameTooLarge) => {
                return Err(invalid_operations(
                    node.node_id,
                    "response frame exceeded configured bound",
                ));
            }
        };
        let value = serde_json::from_slice::<Value>(&frame)
            .map_err(|error| invalid_operations(node.node_id, format!("invalid JSON: {error}")))?;
        validate_readiness(node, &value).map(|ready| ready.is_some())
    };
    match timeout_at(deadline, operation).await {
        Ok(result) => result,
        Err(_) => Ok(false),
    }
}

fn invalid_operations(node_id: u64, reason: impl Into<String>) -> StaticClusterRoutingError {
    StaticClusterRoutingError::InvalidOperationsResponse {
        node_id,
        reason: reason.into(),
    }
}

enum AsyncOperationsReadError {
    Transport,
    FrameTooLarge,
}

async fn read_bounded_operations_frame<R>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Vec<u8>, AsyncOperationsReadError>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut frame = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|_| AsyncOperationsReadError::Transport)?;
        if available.is_empty() {
            return Err(AsyncOperationsReadError::Transport);
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if frame.len().saturating_add(consumed) > max_bytes {
            return Err(AsyncOperationsReadError::FrameTooLarge);
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
    use std::time::Duration;

    use super::AsyncStaticClusterRouter;
    use crate::{StaticClusterRouterConfig, StaticClusterRoutingError};

    #[test]
    fn async_router_rejects_unrepresentable_timeout_without_panicking() {
        let config = StaticClusterRouterConfig {
            timeout: Duration::MAX,
            ..StaticClusterRouterConfig::default()
        };
        assert!(matches!(
            AsyncStaticClusterRouter::new(config),
            Err(StaticClusterRoutingError::InvalidConfiguration(_))
        ));
    }
}
