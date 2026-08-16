use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

use agent_broker_application::BrokerHealth;
use agent_broker_domain::results::{
    ConsumerGroupResult, HeartbeatResult, MutationMetadata, NamespaceResult, TaskClaimResult,
    TaskCompletedResult, TaskLeaseRenewedResult, TaskPublishedResult,
};
use agent_broker_domain::{
    ConsumerGroupId, Generation, LeaseDurationMs, LeaseEpoch, LeaseId, MemberId, NamespaceId,
    TaskId, TaskObjective, TaskResult, Term,
};
use agent_broker_protocol::{
    BrokerRequest, ClaimTaskRequest, CompleteTaskRequest, DeclaredCapabilities,
    EnsureConsumerGroupRequest, EnsureNamespaceRequest, HealthRequest, HeartbeatRequest,
    JoinConsumerGroupRequest, LeaveConsumerGroupRequest, Operation, PublishTaskRequest,
    RenewTaskLeaseRequest, RequestId, ResponseDecodeError, SuccessPayload,
    decode_response_for_operation_with_limit, encode_request,
};

use crate::ClientError;

const DEFAULT_MAX_RESPONSE_FRAME_BYTES: usize = 128 * 1024;
const MIN_FRAME_BYTES: usize = 4_096;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const DEFAULT_PORT: u16 = 8_811;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Connection and response-bound policy for the synchronous Rust Broker client.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrokerClientConfig {
    pub address: SocketAddr,
    pub timeout: Duration,
    pub max_response_frame_bytes: usize,
}

impl Default for BrokerClientConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_PORT),
            timeout: DEFAULT_TIMEOUT,
            max_response_frame_bytes: DEFAULT_MAX_RESPONSE_FRAME_BYTES,
        }
    }
}

impl BrokerClientConfig {
    /// Validate bounded response framing and timeout configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Protocol`] when the frame bound is outside the server-compatible
    /// range or the address is not loopback.
    pub fn validate(self) -> Result<Self, ClientError> {
        if !self.address.ip().is_loopback() {
            return Err(ClientError::Protocol(
                agent_broker_protocol::ProtocolCodecError::InvalidRequest(
                    "Broker client address must be loopback-only.".to_owned(),
                ),
            ));
        }
        if !(MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(&self.max_response_frame_bytes) {
            return Err(ClientError::Protocol(
                agent_broker_protocol::ProtocolCodecError::InvalidRequest(
                    "max_response_frame_bytes must be between 4096 and 1048576.".to_owned(),
                ),
            ));
        }
        if self.timeout.is_zero() {
            return Err(ClientError::Protocol(
                agent_broker_protocol::ProtocolCodecError::InvalidRequest(
                    "Broker client timeout must be positive.".to_owned(),
                ),
            ));
        }
        Ok(self)
    }
}

/// Typed claim parameters that become one protocol-v1 request without local clock fields.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClaimInput {
    pub group_id: ConsumerGroupId,
    pub member_id: MemberId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub lease_id: LeaseId,
    pub lease_duration: LeaseDurationMs,
}

/// Typed lease-renewal parameters for the synchronous client.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RenewInput {
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: MemberId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub lease_duration: LeaseDurationMs,
}

/// Typed completion parameters for the synchronous client.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CompleteInput {
    pub task_id: TaskId,
    pub group_id: ConsumerGroupId,
    pub member_id: MemberId,
    pub expected_term: Term,
    pub expected_generation: Generation,
    pub expected_lease_epoch: LeaseEpoch,
    pub lease_id: LeaseId,
    pub result: TaskResult,
}

/// Reusable synchronous protocol-v1 client. Mutating requests are never retried automatically.
pub struct BrokerClient {
    config: BrokerClientConfig,
    connection: Option<BufReader<TcpStream>>,
    request_sequence: u64,
}

impl BrokerClient {
    /// Construct a disconnected reusable client.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] when configuration violates loopback/frame/timeout constraints.
    pub fn new(config: BrokerClientConfig) -> Result<Self, ClientError> {
        Ok(Self {
            config: config.validate()?,
            connection: None,
            request_sequence: 0,
        })
    }

    /// Close the current transport without changing client configuration or request sequencing.
    pub fn close(&mut self) {
        self.connection = None;
    }

    /// Send one already-typed request and decode the correlated operation-specific response.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] for connection/write/read failures, protocol mismatch, or a Broker
    /// application error. The method performs no automatic retry.
    pub fn execute(&mut self, request: &BrokerRequest) -> Result<SuccessPayload, ClientError> {
        let operation = request.operation();
        let request_id = request.request_id().clone();
        let frame = encode_request(request).map_err(ClientError::Protocol)?;
        let result = self.execute_frame(&frame, &request_id, operation);
        if matches!(
            result,
            Err(ClientError::Transport(_) | ClientError::Protocol(_))
        ) {
            self.close();
        }
        result
    }

    /// Read current Broker health without mutating state.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn health(&mut self) -> Result<BrokerHealth, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::Health(HealthRequest { request_id }))?;
        let SuccessPayload::Health {
            protocol_version,
            term,
            revision,
        } = payload
        else {
            return Err(ClientError::UnexpectedPayload(Operation::Health));
        };
        Ok(BrokerHealth {
            protocol_version,
            term,
            revision,
        })
    }

    /// Idempotently ensure one namespace.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn ensure_namespace(
        &mut self,
        namespace_id: NamespaceId,
    ) -> Result<NamespaceResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
            request_id,
            namespace_id,
        }))?;
        namespace_result(payload)
    }

    /// Publish one Task without automatic retries.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn publish_task(
        &mut self,
        namespace_id: NamespaceId,
        task_id: TaskId,
        objective: TaskObjective,
    ) -> Result<TaskPublishedResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::PublishTask(PublishTaskRequest {
            request_id,
            namespace_id,
            task_id,
            objective,
        }))?;
        task_published_result(payload)
    }

    /// Idempotently ensure one Consumer Group.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn ensure_consumer_group(
        &mut self,
        namespace_id: NamespaceId,
        group_id: ConsumerGroupId,
    ) -> Result<ConsumerGroupResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::EnsureConsumerGroup(
            EnsureConsumerGroupRequest {
                request_id,
                namespace_id,
                group_id,
            },
        ))?;
        consumer_group_result(payload)
    }

    /// Join a Consumer Group with wire-order-preserving capability declarations.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn join_consumer_group(
        &mut self,
        group_id: ConsumerGroupId,
        member_id: MemberId,
        capabilities: DeclaredCapabilities,
    ) -> Result<ConsumerGroupResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::JoinConsumerGroup(
            JoinConsumerGroupRequest {
                request_id,
                group_id,
                member_id,
                capabilities,
            },
        ))?;
        consumer_group_result(payload)
    }

    /// Send a fenced Consumer Group heartbeat.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn heartbeat(
        &mut self,
        group_id: ConsumerGroupId,
        member_id: MemberId,
        expected_generation: Generation,
    ) -> Result<HeartbeatResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::Heartbeat(HeartbeatRequest {
            request_id,
            group_id,
            member_id,
            expected_generation,
        }))?;
        heartbeat_result(payload)
    }

    /// Leave a Consumer Group using the current generation fence.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn leave_consumer_group(
        &mut self,
        group_id: ConsumerGroupId,
        member_id: MemberId,
        expected_generation: Generation,
    ) -> Result<ConsumerGroupResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::LeaveConsumerGroup(
            LeaveConsumerGroupRequest {
                request_id,
                group_id,
                member_id,
                expected_generation,
            },
        ))?;
        consumer_group_result(payload)
    }

    /// Claim the oldest ready Task using an explicit term/generation/lease fence.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn claim_task(&mut self, input: ClaimInput) -> Result<TaskClaimResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::ClaimTask(ClaimTaskRequest {
            request_id,
            group_id: input.group_id,
            member_id: input.member_id,
            expected_term: input.expected_term,
            expected_generation: input.expected_generation,
            lease_id: input.lease_id,
            lease_duration: input.lease_duration,
        }))?;
        task_claim_result(payload)
    }

    /// Renew an active Task lease without changing its lease epoch.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn renew_task_lease(
        &mut self,
        input: RenewInput,
    ) -> Result<TaskLeaseRenewedResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::RenewTaskLease(RenewTaskLeaseRequest {
            request_id,
            task_id: input.task_id,
            group_id: input.group_id,
            member_id: input.member_id,
            expected_term: input.expected_term,
            expected_generation: input.expected_generation,
            expected_lease_epoch: input.expected_lease_epoch,
            lease_id: input.lease_id,
            lease_duration: input.lease_duration,
        }))?;
        task_lease_renewed_result(payload)
    }

    /// Complete an active Task lease without automatic retry.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport/protocol/Broker failure.
    pub fn complete_task(
        &mut self,
        input: CompleteInput,
    ) -> Result<TaskCompletedResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self.execute(&BrokerRequest::CompleteTask(CompleteTaskRequest {
            request_id,
            task_id: input.task_id,
            group_id: input.group_id,
            member_id: input.member_id,
            expected_term: input.expected_term,
            expected_generation: input.expected_generation,
            expected_lease_epoch: input.expected_lease_epoch,
            lease_id: input.lease_id,
            result: input.result,
        }))?;
        task_completed_result(payload)
    }

    fn execute_frame(
        &mut self,
        frame: &[u8],
        request_id: &RequestId,
        operation: Operation,
    ) -> Result<SuccessPayload, ClientError> {
        self.ensure_connection()?;
        let reader = self.connection.as_mut().ok_or_else(|| {
            ClientError::Transport(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Broker connection was not established",
            ))
        })?;
        reader
            .get_mut()
            .write_all(frame)
            .map_err(ClientError::Transport)?;
        reader.get_mut().flush().map_err(ClientError::Transport)?;
        let response = read_bounded_frame(reader, self.config.max_response_frame_bytes)?;
        match decode_response_for_operation_with_limit(
            &response,
            request_id,
            operation,
            self.config.max_response_frame_bytes,
        ) {
            Ok(payload) => Ok(payload),
            Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
            Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
        }
    }

    fn ensure_connection(&mut self) -> Result<(), ClientError> {
        if self.connection.is_some() {
            return Ok(());
        }
        let stream = TcpStream::connect_timeout(&self.config.address, self.config.timeout)
            .map_err(ClientError::Transport)?;
        stream
            .set_read_timeout(Some(self.config.timeout))
            .map_err(ClientError::Transport)?;
        stream
            .set_write_timeout(Some(self.config.timeout))
            .map_err(ClientError::Transport)?;
        stream.set_nodelay(true).map_err(ClientError::Transport)?;
        self.connection = Some(BufReader::new(stream));
        Ok(())
    }

    fn next_request_id(&mut self) -> Result<RequestId, ClientError> {
        self.request_sequence = self
            .request_sequence
            .checked_add(1)
            .ok_or(ClientError::RequestIdExhausted)?;
        RequestId::new(format!("rust-client-{}", self.request_sequence)).map_err(|error| {
            ClientError::Protocol(agent_broker_protocol::ProtocolCodecError::InvalidRequest(
                error.to_string(),
            ))
        })
    }
}

fn read_bounded_frame(
    reader: &mut BufReader<TcpStream>,
    max_bytes: usize,
) -> Result<Vec<u8>, ClientError> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(ClientError::Transport)?;
        if available.is_empty() {
            return Err(ClientError::Transport(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Broker closed the connection before a complete response",
            )));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if frame.len().saturating_add(consumed) > max_bytes {
            return Err(ClientError::Protocol(
                agent_broker_protocol::ProtocolCodecError::FrameTooLarge {
                    actual: frame.len().saturating_add(consumed),
                    max: max_bytes,
                },
            ));
        }
        frame.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(frame);
        }
    }
}

fn metadata(term: Term, revision: agent_broker_domain::Revision) -> MutationMetadata {
    MutationMetadata { term, revision }
}

fn namespace_result(payload: SuccessPayload) -> Result<NamespaceResult, ClientError> {
    let SuccessPayload::Namespace {
        term,
        revision,
        namespace_id,
        namespace_revision,
    } = payload
    else {
        return Err(ClientError::UnexpectedPayload(Operation::EnsureNamespace));
    };
    Ok(NamespaceResult {
        metadata: metadata(term, revision),
        namespace_id,
        namespace_revision,
    })
}

fn task_published_result(payload: SuccessPayload) -> Result<TaskPublishedResult, ClientError> {
    let SuccessPayload::TaskPublished {
        term,
        revision,
        task_id,
        task_revision,
        status,
    } = payload
    else {
        return Err(ClientError::UnexpectedPayload(Operation::PublishTask));
    };
    Ok(TaskPublishedResult {
        metadata: metadata(term, revision),
        task_id,
        task_revision,
        status,
    })
}

fn consumer_group_result(payload: SuccessPayload) -> Result<ConsumerGroupResult, ClientError> {
    let SuccessPayload::ConsumerGroup {
        term,
        revision,
        group_id,
        generation,
        group_revision,
        member_count,
    } = payload
    else {
        return Err(ClientError::UnexpectedPayload(
            Operation::EnsureConsumerGroup,
        ));
    };
    Ok(ConsumerGroupResult {
        metadata: metadata(term, revision),
        group_id,
        generation,
        group_revision,
        member_count,
    })
}

fn heartbeat_result(payload: SuccessPayload) -> Result<HeartbeatResult, ClientError> {
    let SuccessPayload::Heartbeat {
        term,
        revision,
        group_id,
        member_id,
        generation,
        member_revision,
    } = payload
    else {
        return Err(ClientError::UnexpectedPayload(Operation::Heartbeat));
    };
    Ok(HeartbeatResult {
        metadata: metadata(term, revision),
        group_id,
        member_id,
        generation,
        member_revision,
    })
}

fn task_claim_result(payload: SuccessPayload) -> Result<TaskClaimResult, ClientError> {
    let SuccessPayload::TaskClaimed {
        term,
        revision,
        task_id,
        objective,
        task_revision,
        lease_id,
        lease_epoch,
        lease_expires_at_ms,
        generation,
    } = payload
    else {
        return Err(ClientError::UnexpectedPayload(Operation::ClaimTask));
    };
    Ok(TaskClaimResult {
        metadata: metadata(term, revision),
        task_id,
        objective,
        task_revision,
        lease_id,
        lease_epoch,
        lease_expires_at_ms,
        generation,
    })
}

fn task_lease_renewed_result(
    payload: SuccessPayload,
) -> Result<TaskLeaseRenewedResult, ClientError> {
    let SuccessPayload::TaskLeaseRenewed {
        term,
        revision,
        task_id,
        task_revision,
        lease_id,
        lease_epoch,
        lease_expires_at_ms,
        generation,
    } = payload
    else {
        return Err(ClientError::UnexpectedPayload(Operation::RenewTaskLease));
    };
    Ok(TaskLeaseRenewedResult {
        metadata: metadata(term, revision),
        task_id,
        task_revision,
        lease_id,
        lease_epoch,
        lease_expires_at_ms,
        generation,
    })
}

fn task_completed_result(payload: SuccessPayload) -> Result<TaskCompletedResult, ClientError> {
    let SuccessPayload::TaskCompleted {
        term,
        revision,
        task_id,
        task_revision,
        status,
    } = payload
    else {
        return Err(ClientError::UnexpectedPayload(Operation::CompleteTask));
    };
    Ok(TaskCompletedResult {
        metadata: metadata(term, revision),
        task_id,
        task_revision,
        status,
    })
}
