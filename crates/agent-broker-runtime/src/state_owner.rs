use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread;

use agent_broker_application::{BrokerError, ConsensusAdapter};
use agent_broker_domain::{ConsumerGroupDirectory, TimestampMs};
use agent_broker_protocol::{
    BrokerRequest, BrokerRequestDispatcher, BrokerResponse, BrokerWireRequest,
};

use crate::RuntimeError;
use crate::standalone_maintenance::{
    LeaderMaintenanceResult, MaintenanceRunError, StandaloneMaintenancePolicy,
    StandaloneMaintenanceResult, run_once, run_once_if_authoritative,
};

struct DispatchJob {
    request: BrokerWireRequest,
    observed_at_ms: TimestampMs,
    reply: SyncSender<BrokerResponse>,
}

struct MaintenanceJob {
    policy: StandaloneMaintenancePolicy,
    now_ms: TimestampMs,
    reply: SyncSender<Result<StandaloneMaintenanceResult, agent_broker_application::BrokerError>>,
}

struct LeaderMaintenanceJob {
    policy: StandaloneMaintenancePolicy,
    now_ms: TimestampMs,
    reply: SyncSender<Result<LeaderMaintenanceResult, agent_broker_application::BrokerError>>,
}

struct GroupDirectoryJob {
    reply: SyncSender<Result<ConsumerGroupDirectory, BrokerError>>,
}

enum StateOwnerJob {
    Dispatch(DispatchJob),
    Maintenance(MaintenanceJob),
    LeaderMaintenance(LeaderMaintenanceJob),
    GroupDirectory(GroupDirectoryJob),
}

/// Cloneable bounded submission handle to the single mutable Broker state owner.
#[derive(Clone)]
pub struct StateOwnerHandle {
    sender: SyncSender<StateOwnerJob>,
    load: Arc<StateOwnerLoadCounters>,
    capacity: usize,
}

#[derive(Debug, Default)]
struct StateOwnerLoadCounters {
    active_jobs: AtomicUsize,
    queued_jobs: AtomicUsize,
}

/// Read-only instantaneous load at the single mutable state-owner boundary.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StateOwnerLoad {
    pub active_jobs: usize,
    pub queued_jobs: usize,
    pub capacity: usize,
}

impl StateOwnerHandle {
    /// Spawn one dedicated state-owner thread around a dispatcher.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfiguration`] when the in-flight queue bound is zero.
    pub fn spawn<C>(
        mut dispatcher: BrokerRequestDispatcher<C>,
        max_inflight_requests: usize,
    ) -> Result<Self, RuntimeError>
    where
        C: ConsensusAdapter + Send + 'static,
    {
        if max_inflight_requests == 0 {
            return Err(RuntimeError::InvalidConfiguration(
                "max_inflight_requests must be positive",
            ));
        }
        let (sender, receiver) = mpsc::sync_channel::<StateOwnerJob>(max_inflight_requests);
        let load = Arc::new(StateOwnerLoadCounters::default());
        let owner_load = Arc::clone(&load);
        thread::Builder::new()
            .name("agent-broker-state-owner".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    owner_load.queued_jobs.fetch_sub(1, Ordering::AcqRel);
                    owner_load.active_jobs.store(1, Ordering::Release);
                    match job {
                        StateOwnerJob::Dispatch(job) => {
                            let request_id = job.request.request_id().clone();
                            let result = match job.request {
                                BrokerWireRequest::V1(request) => {
                                    dispatcher.dispatch(request, job.observed_at_ms)
                                }
                                BrokerWireRequest::V2(request) => {
                                    let (identity, request) = request.into_parts();
                                    dispatcher.dispatch_identified(
                                        identity,
                                        request,
                                        job.observed_at_ms,
                                    )
                                }
                                BrokerWireRequest::V3(request) => match request {
                                    agent_broker_protocol::BrokerRequestV3::AcquireOwner(
                                        request,
                                    ) => dispatcher.dispatch_owner_acquisition(
                                        request.session_id().clone(),
                                        request.expected_owner_epoch(),
                                        request.owner_instance_id().clone(),
                                    ),
                                    agent_broker_protocol::BrokerRequestV3::Mutation(request) => {
                                        dispatcher.dispatch_identified(
                                            request.identity().clone(),
                                            request.request().clone(),
                                            job.observed_at_ms,
                                        )
                                    }
                                },
                            };
                            let response = match result {
                                Ok(result) => BrokerResponse::success(request_id, result),
                                Err(error) => BrokerResponse::error(request_id, error),
                            };
                            let _ = job.reply.send(response);
                        }
                        StateOwnerJob::Maintenance(job) => {
                            let result = run_once(&mut dispatcher, job.policy, job.now_ms);
                            let _ = job.reply.send(result);
                        }
                        StateOwnerJob::LeaderMaintenance(job) => {
                            let result =
                                run_once_if_authoritative(&mut dispatcher, job.policy, job.now_ms);
                            let _ = job.reply.send(result);
                        }
                        StateOwnerJob::GroupDirectory(job) => {
                            let result = dispatcher.application_service_mut().group_directory();
                            let _ = job.reply.send(result);
                        }
                    }
                    owner_load.active_jobs.store(0, Ordering::Release);
                }
            })
            .map_err(|error| RuntimeError::io("Broker state-owner thread spawn failed", error))?;
        Ok(Self {
            sender,
            load,
            capacity: max_inflight_requests,
        })
    }

    /// Return instantaneous active/queued work counts for bounded overload and readiness checks.
    #[must_use]
    pub fn load(&self) -> StateOwnerLoad {
        StateOwnerLoad {
            active_jobs: self.load.active_jobs.load(Ordering::Acquire),
            queued_jobs: self.load.queued_jobs.load(Ordering::Acquire),
            capacity: self.capacity,
        }
    }

    /// Return one authoritative read-only Consumer Group directory through the single state-owner
    /// ordering boundary.
    ///
    /// The outer [`Result`] describes runtime queue/owner availability. The inner [`Result`]
    /// preserves Broker read-authority errors such as cluster follower/quorum rejection without
    /// flattening them into runtime failures.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the bounded state-owner queue cannot accept the read or the
    /// owner thread drops the reply.
    pub fn group_directory(
        &self,
    ) -> Result<Result<ConsumerGroupDirectory, BrokerError>, RuntimeError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.try_send(StateOwnerJob::GroupDirectory(GroupDirectoryJob { reply }))?;
        receiver
            .recv()
            .map_err(|_| RuntimeError::StateOwnerReplyDropped)
    }

    /// Dispatch one request through the single state owner and wait for its typed response.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the state-owner thread is no longer available.
    pub fn dispatch(
        &self,
        request: BrokerRequest,
        observed_at_ms: TimestampMs,
    ) -> Result<BrokerResponse, RuntimeError> {
        self.dispatch_wire(BrokerWireRequest::V1(request), observed_at_ms)
    }

    /// Dispatch one already-versioned wire request through the single state owner.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the state-owner thread is no longer available.
    pub fn dispatch_wire(
        &self,
        request: BrokerWireRequest,
        observed_at_ms: TimestampMs,
    ) -> Result<BrokerResponse, RuntimeError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.try_send(StateOwnerJob::Dispatch(DispatchJob {
            request,
            observed_at_ms,
            reply,
        }))?;
        receiver
            .recv()
            .map_err(|_| RuntimeError::StateOwnerReplyDropped)
    }

    /// Run one bounded maintenance tick through the same single mutable state owner as client
    /// requests.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenanceRunError`] when the state owner is unavailable or a maintenance
    /// proposal is rejected by application/consensus.
    pub fn run_maintenance(
        &self,
        policy: StandaloneMaintenancePolicy,
        now_ms: TimestampMs,
    ) -> Result<StandaloneMaintenanceResult, MaintenanceRunError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.try_send(StateOwnerJob::Maintenance(MaintenanceJob {
            policy,
            now_ms,
            reply,
        }))?;
        receiver
            .recv()
            .map_err(|_| RuntimeError::StateOwnerReplyDropped)?
            .map_err(MaintenanceRunError::from)
    }

    /// Run one bounded maintenance tick only when consensus reports this process as authoritative.
    ///
    /// # Errors
    ///
    /// Returns [`MaintenanceRunError`] when the state owner is unavailable, authority cannot be
    /// determined, or an authoritative maintenance proposal fails.
    pub fn run_leader_maintenance(
        &self,
        policy: StandaloneMaintenancePolicy,
        now_ms: TimestampMs,
    ) -> Result<LeaderMaintenanceResult, MaintenanceRunError> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.try_send(StateOwnerJob::LeaderMaintenance(LeaderMaintenanceJob {
            policy,
            now_ms,
            reply,
        }))?;
        receiver
            .recv()
            .map_err(|_| RuntimeError::StateOwnerReplyDropped)?
            .map_err(MaintenanceRunError::from)
    }

    fn try_send(&self, job: StateOwnerJob) -> Result<(), RuntimeError> {
        self.load.queued_jobs.fetch_add(1, Ordering::AcqRel);
        match self.sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.load.queued_jobs.fetch_sub(1, Ordering::AcqRel);
                Err(RuntimeError::StateOwnerSaturated)
            }
            Err(TrySendError::Disconnected(_)) => {
                self.load.queued_jobs.fetch_sub(1, Ordering::AcqRel);
                Err(RuntimeError::StateOwnerStopped)
            }
        }
    }
}
