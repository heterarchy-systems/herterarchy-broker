use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use agent_broker_application::{
    BrokerErrorDisposition, BrokerHealth, CommandIdentity, CommandSessionId, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};
use agent_broker_domain::results::{
    ConsumerGroupResult, HeartbeatResult, NamespaceResult, TaskClaimResult, TaskCompletedResult,
    TaskLeaseRenewedResult, TaskPublishedResult,
};
use agent_broker_domain::{
    ConsumerGroupId, Generation, MemberId, NamespaceId, TaskId, TaskObjective,
};
use agent_broker_protocol::{
    BrokerRequest, EnsureConsumerGroupRequest, EnsureNamespaceRequest, HealthRequest,
    HeartbeatRequest, IdentifiedBrokerRequest, JoinConsumerGroupRequest, LeaveConsumerGroupRequest,
    Operation, OwnerAcquisitionRequestV3, OwnerIdentifiedBrokerRequestV3, PublishTaskRequest,
    RequestId, ResponseDecodeError, SuccessPayload, decode_owner_acquisition_response_with_limit,
    decode_owner_mutation_response_with_limit, decode_response_for_operation_with_limit,
    decode_response_v2_for_operation_with_limit, encode_identified_request,
    encode_owner_acquisition_request_with_limit, encode_owner_mutation_request_with_limit,
    encode_request,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};

use crate::client::{
    BrokerClientConfig, ClaimInput, CompleteInput, RenewInput, consumer_group_result,
    heartbeat_result, namespace_result, task_claim_result, task_completed_result,
    task_lease_renewed_result, task_published_result,
};
use crate::{
    AsyncDurableClientSessionStore, ClientError, ClientSessionStoreError, DurableExecutionError,
    DurableRetryPolicy, ReservedCommand,
};

/// Native-Tokio Broker client.
///
/// Each request owns one TCP connection. This intentionally avoids multiplexing a newline-framed
/// request/response stream and contains cancellation to one in-flight operation without a shared
/// connection lock. Mutation retry/identity policy remains caller-owned.
pub struct AsyncBrokerClient {
    config: BrokerClientConfig,
    request_sequence: AtomicU64,
}

impl AsyncBrokerClient {
    /// Construct a native async client using the same bounds as the synchronous client.
    ///
    /// # Errors
    /// Returns [`ClientError`] when the client configuration is invalid.
    pub fn new(config: BrokerClientConfig) -> Result<Self, ClientError> {
        let config = config.validate()?;
        if Instant::now().checked_add(config.timeout).is_none() {
            return Err(ClientError::Transport(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Broker async timeout exceeds the platform instant range",
            )));
        }
        Ok(Self {
            config,
            request_sequence: AtomicU64::new(0),
        })
    }

    /// Send one protocol request without automatic mutation retry.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors with the same classification as the sync client.
    pub async fn execute(&self, request: &BrokerRequest) -> Result<SuccessPayload, ClientError> {
        let request_id = request.request_id().clone();
        let operation = request.operation();
        let frame = encode_request(request).map_err(ClientError::Protocol)?;
        let response = self.round_trip_encoded(&frame).await?;
        decode_response(
            &response,
            &request_id,
            operation,
            self.config.max_response_frame_bytes,
        )
    }

    /// Execute one protocol-v2 identified mutation without automatic retry.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors with caller-owned command identity preserved.
    pub async fn execute_identified(
        &self,
        identity: &CommandIdentity,
        request: &BrokerRequest,
    ) -> Result<SuccessPayload, ClientError> {
        let identified = IdentifiedBrokerRequest::new(identity.clone(), request.clone())
            .map_err(ClientError::Protocol)?;
        let request_id = request.request_id().clone();
        let operation = request.operation();
        let frame = encode_identified_request(&identified).map_err(ClientError::Protocol)?;
        let response = self.round_trip_encoded(&frame).await?;
        match decode_response_v2_for_operation_with_limit(
            &response,
            &request_id,
            operation,
            self.config.max_response_frame_bytes,
        ) {
            Ok(payload) => Ok(payload),
            Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
            Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
        }
    }

    /// Acquire broker-authoritative command-session ownership through protocol-v3.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors. The method does not retry automatically.
    pub async fn acquire_command_session_owner(
        &self,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, ClientError> {
        let request_id = self.next_request_id()?;
        self.acquire_command_session_owner_with_request_id(
            request_id,
            session_id,
            expected_owner_epoch,
            owner_instance_id,
        )
        .await
    }

    /// Acquire command-session ownership with a caller-supplied correlation identity.
    ///
    /// This is the cancellation/recovery form: if the caller cannot know whether a prior owner
    /// acquisition response was lost, retrying with the same `request_id`, session, expected epoch,
    /// and owner-instance identity reproduces the exact protocol-v3 request frame.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors. No automatic retry is performed.
    pub async fn acquire_command_session_owner_with_request_id(
        &self,
        request_id: RequestId,
        session_id: CommandSessionId,
        expected_owner_epoch: SessionOwnerEpoch,
        owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, ClientError> {
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
        let response = self.round_trip_encoded(&frame).await?;
        match decode_owner_acquisition_response_with_limit(
            &response,
            &request_id,
            self.config.max_response_frame_bytes,
        ) {
            Ok(owner_epoch) => Ok(owner_epoch),
            Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
            Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
        }
    }

    /// Execute one owner-aware protocol-v3 mutation without automatic retry.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors including ambiguous commit outcomes.
    pub async fn execute_owned(
        &self,
        identity: &CommandIdentity,
        request: &BrokerRequest,
    ) -> Result<SuccessPayload, ClientError> {
        let owned = OwnerIdentifiedBrokerRequestV3::new(identity.clone(), request.clone())
            .map_err(ClientError::Protocol)?;
        let request_id = request.request_id().clone();
        let operation = request.operation();
        let frame =
            encode_owner_mutation_request_with_limit(&owned, self.config.max_response_frame_bytes)
                .map_err(ClientError::Protocol)?;
        let response = self.round_trip_encoded(&frame).await?;
        match decode_owner_mutation_response_with_limit(
            &response,
            &request_id,
            operation,
            self.config.max_response_frame_bytes,
        ) {
            Ok(payload) => Ok(payload),
            Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
            Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
        }
    }

    /// Reserve and execute one protocol-v3 mutation using an async facade over the durable store.
    ///
    /// Filesystem locking/fsync remains on Tokio's blocking pool; every Broker network attempt is
    /// native async and exact-frame retry semantics match the synchronous durable client.
    ///
    /// # Errors
    /// Returns durable reservation/acknowledgment failures or the final Broker/client outcome.
    pub async fn execute_durable(
        &self,
        store: &AsyncDurableClientSessionStore,
        request: BrokerRequest,
        policy: DurableRetryPolicy,
    ) -> Result<SuccessPayload, DurableExecutionError> {
        let reserved = store.reserve_command(request).await?;
        self.execute_reserved_durable(store, &reserved, policy)
            .await
    }

    /// Retry the exact in-flight command recovered from async durable local state.
    ///
    /// # Errors
    /// Returns durable state failures or the final Broker/client outcome.
    pub async fn recover_durable_in_flight(
        &self,
        store: &AsyncDurableClientSessionStore,
        policy: DurableRetryPolicy,
    ) -> Result<SuccessPayload, DurableExecutionError> {
        let reserved = store.in_flight().await?.ok_or_else(|| {
            ClientSessionStoreError::InvalidState(
                "cannot recover durable execution without an in-flight command".to_owned(),
            )
        })?;
        self.execute_reserved_durable(store, &reserved, policy)
            .await
    }

    async fn execute_reserved_durable(
        &self,
        store: &AsyncDurableClientSessionStore,
        reserved: &ReservedCommand,
        policy: DurableRetryPolicy,
    ) -> Result<SuccessPayload, DurableExecutionError> {
        let owned = OwnerIdentifiedBrokerRequestV3::new(
            reserved.identity().clone(),
            reserved.request().clone(),
        )
        .map_err(ClientError::Protocol)?;
        let request_id = reserved.request().request_id().clone();
        let operation = reserved.request().operation();
        let frame =
            encode_owner_mutation_request_with_limit(&owned, self.config.max_response_frame_bytes)
                .map_err(ClientError::Protocol)?;

        for attempt in 1..=policy.max_attempts().get() {
            let result = self.round_trip_encoded(&frame).await.and_then(|response| {
                match decode_owner_mutation_response_with_limit(
                    &response,
                    &request_id,
                    operation,
                    self.config.max_response_frame_bytes,
                ) {
                    Ok(payload) => Ok(payload),
                    Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
                    Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
                }
            });
            match result {
                Ok(payload) => {
                    store
                        .acknowledge_in_flight_outcome(reserved.identity().clone())
                        .await?;
                    return Ok(payload);
                }
                Err(ClientError::Broker(error)) => match error.disposition() {
                    BrokerErrorDisposition::Committed => {
                        store
                            .acknowledge_in_flight_outcome(reserved.identity().clone())
                            .await?;
                        return Err(ClientError::Broker(error).into());
                    }
                    BrokerErrorDisposition::Rejected => {
                        store
                            .release_rejected_in_flight(reserved.identity().clone())
                            .await?;
                        return Err(ClientError::Broker(error).into());
                    }
                    BrokerErrorDisposition::Unknown if attempt < policy.max_attempts().get() => {}
                    BrokerErrorDisposition::Unknown => {
                        return Err(ClientError::Broker(error).into());
                    }
                },
                Err(ClientError::Transport(_)) if attempt < policy.max_attempts().get() => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(ClientSessionStoreError::InvalidState(
            "async durable retry loop exhausted without returning a classified outcome".to_owned(),
        )
        .into())
    }

    /// Read current Broker health.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn health(&self) -> Result<BrokerHealth, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self
            .execute(&BrokerRequest::Health(HealthRequest { request_id }))
            .await?;
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
    /// Returns transport/protocol/Broker errors.
    pub async fn ensure_namespace(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<NamespaceResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self
            .execute(&BrokerRequest::EnsureNamespace(EnsureNamespaceRequest {
                request_id,
                namespace_id,
            }))
            .await?;
        namespace_result(payload)
    }

    /// Publish one Task without automatic retry.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn publish_task(
        &self,
        namespace_id: NamespaceId,
        task_id: TaskId,
        objective: TaskObjective,
    ) -> Result<TaskPublishedResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self
            .execute(&BrokerRequest::PublishTask(PublishTaskRequest {
                request_id,
                namespace_id,
                task_id,
                objective,
            }))
            .await?;
        task_published_result(payload)
    }

    /// Idempotently ensure one Consumer Group.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn ensure_consumer_group(
        &self,
        namespace_id: NamespaceId,
        group_id: ConsumerGroupId,
    ) -> Result<ConsumerGroupResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self
            .execute(&BrokerRequest::EnsureConsumerGroup(
                EnsureConsumerGroupRequest {
                    request_id,
                    namespace_id,
                    group_id,
                },
            ))
            .await?;
        consumer_group_result(payload)
    }

    /// Join a Consumer Group.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn join_consumer_group(
        &self,
        group_id: ConsumerGroupId,
        member_id: MemberId,
        capabilities: agent_broker_protocol::DeclaredCapabilities,
    ) -> Result<ConsumerGroupResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self
            .execute(&BrokerRequest::JoinConsumerGroup(
                JoinConsumerGroupRequest {
                    request_id,
                    group_id,
                    member_id,
                    capabilities,
                },
            ))
            .await?;
        consumer_group_result(payload)
    }

    /// Send a fenced Consumer Group heartbeat.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn heartbeat(
        &self,
        group_id: ConsumerGroupId,
        member_id: MemberId,
        expected_generation: Generation,
    ) -> Result<HeartbeatResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self
            .execute(&BrokerRequest::Heartbeat(HeartbeatRequest {
                request_id,
                group_id,
                member_id,
                expected_generation,
            }))
            .await?;
        heartbeat_result(payload)
    }

    /// Leave a Consumer Group using the generation fence.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn leave_consumer_group(
        &self,
        group_id: ConsumerGroupId,
        member_id: MemberId,
        expected_generation: Generation,
    ) -> Result<ConsumerGroupResult, ClientError> {
        let request_id = self.next_request_id()?;
        let payload = self
            .execute(&BrokerRequest::LeaveConsumerGroup(
                LeaveConsumerGroupRequest {
                    request_id,
                    group_id,
                    member_id,
                    expected_generation,
                },
            ))
            .await?;
        consumer_group_result(payload)
    }

    /// Claim the oldest ready Task.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn claim_task(&self, input: ClaimInput) -> Result<TaskClaimResult, ClientError> {
        let request_id = self.next_request_id()?;
        let request = agent_broker_protocol::ClaimTaskRequest {
            request_id,
            group_id: input.group_id,
            member_id: input.member_id,
            expected_term: input.expected_term,
            expected_generation: input.expected_generation,
            lease_id: input.lease_id,
            lease_duration: input.lease_duration,
        };
        task_claim_result(self.execute(&BrokerRequest::ClaimTask(request)).await?)
    }

    /// Renew an active Task lease.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn renew_task_lease(
        &self,
        input: RenewInput,
    ) -> Result<TaskLeaseRenewedResult, ClientError> {
        let request_id = self.next_request_id()?;
        let request = agent_broker_protocol::RenewTaskLeaseRequest {
            request_id,
            task_id: input.task_id,
            group_id: input.group_id,
            member_id: input.member_id,
            expected_term: input.expected_term,
            expected_generation: input.expected_generation,
            expected_lease_epoch: input.expected_lease_epoch,
            lease_id: input.lease_id,
            lease_duration: input.lease_duration,
        };
        task_lease_renewed_result(
            self.execute(&BrokerRequest::RenewTaskLease(request))
                .await?,
        )
    }

    /// Complete an active Task lease.
    ///
    /// # Errors
    /// Returns transport/protocol/Broker errors.
    pub async fn complete_task(
        &self,
        input: CompleteInput,
    ) -> Result<TaskCompletedResult, ClientError> {
        let request_id = self.next_request_id()?;
        let request = agent_broker_protocol::CompleteTaskRequest {
            request_id,
            task_id: input.task_id,
            group_id: input.group_id,
            member_id: input.member_id,
            expected_term: input.expected_term,
            expected_generation: input.expected_generation,
            expected_lease_epoch: input.expected_lease_epoch,
            lease_id: input.lease_id,
            result: input.result,
        };
        task_completed_result(self.execute(&BrokerRequest::CompleteTask(request)).await?)
    }

    pub(crate) async fn round_trip_encoded(&self, frame: &[u8]) -> Result<Vec<u8>, ClientError> {
        let deadline = Instant::now()
            .checked_add(self.config.timeout)
            .ok_or_else(|| {
                ClientError::Transport(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Broker async timeout exceeds the platform instant range",
                ))
            })?;
        let operation = async {
            let stream = TcpStream::connect(self.config.address)
                .await
                .map_err(ClientError::Transport)?;
            stream.set_nodelay(true).map_err(ClientError::Transport)?;
            let (read_half, mut write_half) = stream.into_split();
            write_half
                .write_all(frame)
                .await
                .map_err(ClientError::Transport)?;
            write_half.flush().await.map_err(ClientError::Transport)?;
            let mut reader = BufReader::with_capacity(
                self.config.max_response_frame_bytes.min(64 * 1024),
                read_half,
            );
            read_bounded_frame(&mut reader, self.config.max_response_frame_bytes).await
        };
        match timeout_at(deadline, operation).await {
            Ok(result) => result,
            Err(_elapsed) => Err(ClientError::Transport(io::Error::new(
                io::ErrorKind::TimedOut,
                "Broker async round trip exceeded its end-to-end deadline",
            ))),
        }
    }

    fn next_request_id(&self) -> Result<RequestId, ClientError> {
        let previous = self
            .request_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ClientError::RequestIdExhausted)?;
        RequestId::new(format!("rust-async-client-{}", previous + 1)).map_err(|error| {
            ClientError::Protocol(agent_broker_protocol::ProtocolCodecError::InvalidRequest(
                error.to_string(),
            ))
        })
    }
}

async fn read_bounded_frame<R>(reader: &mut R, max_bytes: usize) -> Result<Vec<u8>, ClientError>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(ClientError::Transport)?;
        if available.is_empty() {
            return Err(ClientError::Transport(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Broker closed the connection before a complete response",
            )));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let actual = frame.len().saturating_add(consumed);
        if actual > max_bytes {
            return Err(ClientError::Protocol(
                agent_broker_protocol::ProtocolCodecError::FrameTooLarge {
                    actual,
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

fn decode_response(
    response: &[u8],
    request_id: &RequestId,
    operation: Operation,
    max_response_frame_bytes: usize,
) -> Result<SuccessPayload, ClientError> {
    match decode_response_for_operation_with_limit(
        response,
        request_id,
        operation,
        max_response_frame_bytes,
    ) {
        Ok(payload) => Ok(payload),
        Err(ResponseDecodeError::Protocol(error)) => Err(ClientError::Protocol(error)),
        Err(ResponseDecodeError::Broker(error)) => Err(ClientError::Broker(error)),
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

    use super::AsyncBrokerClient;
    use crate::{BrokerClientConfig, ClientError};

    #[test]
    fn async_client_rejects_unrepresentable_timeout_without_panicking() {
        let config = BrokerClientConfig {
            timeout: Duration::MAX,
            ..BrokerClientConfig::default()
        };
        assert!(matches!(
            AsyncBrokerClient::new(config),
            Err(ClientError::Transport(error)) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }
}
