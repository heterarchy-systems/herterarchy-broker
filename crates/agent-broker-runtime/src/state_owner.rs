use std::sync::mpsc::{self, SyncSender};
use std::thread;

use agent_broker_application::ConsensusAdapter;
use agent_broker_domain::TimestampMs;
use agent_broker_protocol::{BrokerRequest, BrokerRequestDispatcher, BrokerResponse};

use crate::RuntimeError;
use crate::standalone_maintenance::{
    MaintenanceRunError, StandaloneMaintenancePolicy, StandaloneMaintenanceResult, run_once,
};

struct DispatchJob {
    request: BrokerRequest,
    observed_at_ms: TimestampMs,
    reply: mpsc::Sender<BrokerResponse>,
}

struct MaintenanceJob {
    policy: StandaloneMaintenancePolicy,
    now_ms: TimestampMs,
    reply: mpsc::Sender<Result<StandaloneMaintenanceResult, agent_broker_application::BrokerError>>,
}

enum StateOwnerJob {
    Dispatch(DispatchJob),
    Maintenance(MaintenanceJob),
}

/// Cloneable bounded submission handle to the single mutable Broker state owner.
#[derive(Clone)]
pub struct StateOwnerHandle {
    sender: SyncSender<StateOwnerJob>,
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
        thread::Builder::new()
            .name("agent-broker-state-owner".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    match job {
                        StateOwnerJob::Dispatch(job) => {
                            let request_id = job.request.request_id().clone();
                            let response =
                                match dispatcher.dispatch(job.request, job.observed_at_ms) {
                                    Ok(result) => BrokerResponse::success(request_id, result),
                                    Err(error) => BrokerResponse::error(request_id, error),
                                };
                            let _ = job.reply.send(response);
                        }
                        StateOwnerJob::Maintenance(job) => {
                            let result = run_once(&mut dispatcher, job.policy, job.now_ms);
                            let _ = job.reply.send(result);
                        }
                    }
                }
            })
            .map_err(|error| RuntimeError::io("Broker state-owner thread spawn failed", error))?;
        Ok(Self { sender })
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
        let (reply, receiver) = mpsc::channel();
        self.sender
            .send(StateOwnerJob::Dispatch(DispatchJob {
                request,
                observed_at_ms,
                reply,
            }))
            .map_err(|_| RuntimeError::StateOwnerStopped)?;
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
        let (reply, receiver) = mpsc::channel();
        self.sender
            .send(StateOwnerJob::Maintenance(MaintenanceJob {
                policy,
                now_ms,
                reply,
            }))
            .map_err(|_| RuntimeError::StateOwnerStopped)?;
        receiver
            .recv()
            .map_err(|_| RuntimeError::StateOwnerReplyDropped)?
            .map_err(MaintenanceRunError::from)
    }
}
