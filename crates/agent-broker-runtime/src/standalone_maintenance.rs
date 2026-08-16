use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use agent_broker_application::{BrokerError, BrokerErrorCode, ConsensusAdapter};
use agent_broker_domain::{PruneTaskLimit, ReapMemberLimit, TimestampMs};
use agent_broker_protocol::BrokerRequestDispatcher;

use crate::clock::system_clock_ms;
use crate::{RuntimeError, StateOwnerHandle};

const DEFAULT_COMPLETED_TASK_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;
const DEFAULT_MEMBER_TIMEOUT_MS: u64 = 45_000;
const DEFAULT_INTERVAL_MS: u64 = 5_000;
const DEFAULT_PRUNE_BATCH: usize = 1_024;
const DEFAULT_MAX_PRUNE_BATCHES_PER_TICK: usize = 4;
const DEFAULT_REAP_BATCH: usize = 1_024;
const DEFAULT_MAX_REAP_BATCHES_PER_TICK: usize = 4;

/// Standalone-only maintenance policy matching the Python reference defaults and safety bounds.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct StandaloneMaintenancePolicy {
    completed_task_retention_ms: u64,
    member_timeout_ms: u64,
    interval_ms: u64,
    prune_batch: usize,
    max_prune_batches_per_tick: usize,
    reap_batch: usize,
    max_reap_batches_per_tick: usize,
}

impl Default for StandaloneMaintenancePolicy {
    fn default() -> Self {
        Self {
            completed_task_retention_ms: DEFAULT_COMPLETED_TASK_RETENTION_MS,
            member_timeout_ms: DEFAULT_MEMBER_TIMEOUT_MS,
            interval_ms: DEFAULT_INTERVAL_MS,
            prune_batch: DEFAULT_PRUNE_BATCH,
            max_prune_batches_per_tick: DEFAULT_MAX_PRUNE_BATCHES_PER_TICK,
            reap_batch: DEFAULT_REAP_BATCH,
            max_reap_batches_per_tick: DEFAULT_MAX_REAP_BATCHES_PER_TICK,
        }
    }
}

impl StandaloneMaintenancePolicy {
    /// Construct a validated maintenance policy using millisecond durations.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfiguration`] when interval or batch bounds violate the
    /// Python standalone maintenance contract.
    pub fn new(
        completed_task_retention_ms: u64,
        member_timeout_ms: u64,
        interval_ms: u64,
        prune_batch: usize,
        max_prune_batches_per_tick: usize,
        reap_batch: usize,
        max_reap_batches_per_tick: usize,
    ) -> Result<Self, RuntimeError> {
        if !(100..=3_600_000).contains(&interval_ms) {
            return Err(RuntimeError::InvalidConfiguration(
                "maintenance interval_ms must be between 100 and 3600000",
            ));
        }
        if !(1..=4_096).contains(&prune_batch) {
            return Err(RuntimeError::InvalidConfiguration(
                "maintenance prune_batch must be between 1 and 4096",
            ));
        }
        if !(1..=64).contains(&max_prune_batches_per_tick) {
            return Err(RuntimeError::InvalidConfiguration(
                "max_prune_batches_per_tick must be between 1 and 64",
            ));
        }
        if !(1..=4_096).contains(&reap_batch) {
            return Err(RuntimeError::InvalidConfiguration(
                "maintenance reap_batch must be between 1 and 4096",
            ));
        }
        if !(1..=64).contains(&max_reap_batches_per_tick) {
            return Err(RuntimeError::InvalidConfiguration(
                "max_reap_batches_per_tick must be between 1 and 64",
            ));
        }
        Ok(Self {
            completed_task_retention_ms,
            member_timeout_ms,
            interval_ms,
            prune_batch,
            max_prune_batches_per_tick,
            reap_batch,
            max_reap_batches_per_tick,
        })
    }

    #[must_use]
    pub const fn interval(self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    #[must_use]
    pub const fn completed_task_retention_ms(self) -> u64 {
        self.completed_task_retention_ms
    }

    #[must_use]
    pub const fn member_timeout_ms(self) -> u64 {
        self.member_timeout_ms
    }

    #[must_use]
    pub const fn prune_batch(self) -> usize {
        self.prune_batch
    }

    #[must_use]
    pub const fn max_prune_batches_per_tick(self) -> usize {
        self.max_prune_batches_per_tick
    }

    #[must_use]
    pub const fn reap_batch(self) -> usize {
        self.reap_batch
    }

    #[must_use]
    pub const fn max_reap_batches_per_tick(self) -> usize {
        self.max_reap_batches_per_tick
    }
}

/// Counts of durable maintenance mutations accepted during one tick.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub struct StandaloneMaintenanceResult {
    pub pruned_completed_tasks: usize,
    pub reaped_stale_members: usize,
}

/// Maintenance submission failure from either runtime ownership or Broker consensus/application.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MaintenanceRunError {
    Runtime(String),
    Broker(BrokerError),
}

impl fmt::Display for MaintenanceRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(message) => formatter.write_str(message),
            Self::Broker(error) => error.fmt(formatter),
        }
    }
}

impl Error for MaintenanceRunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Broker(error) => Some(error),
            Self::Runtime(_) => None,
        }
    }
}

impl From<RuntimeError> for MaintenanceRunError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error.to_string())
    }
}

impl From<BrokerError> for MaintenanceRunError {
    fn from(error: BrokerError) -> Self {
        Self::Broker(error)
    }
}

pub(crate) fn run_once<C>(
    dispatcher: &mut BrokerRequestDispatcher<C>,
    policy: StandaloneMaintenancePolicy,
    now_ms: TimestampMs,
) -> Result<StandaloneMaintenanceResult, BrokerError>
where
    C: ConsensusAdapter,
{
    let service = dispatcher.application_service_mut();
    let reap_limit = ReapMemberLimit::new(policy.reap_batch())
        .map_err(|error| BrokerError::new(BrokerErrorCode::InternalError, error.to_string()))?;
    let prune_limit = PruneTaskLimit::new(policy.prune_batch())
        .map_err(|error| BrokerError::new(BrokerErrorCode::InternalError, error.to_string()))?;
    let stale_before_ms = TimestampMs::new(now_ms.get().saturating_sub(policy.member_timeout_ms()));
    let completed_before_ms = TimestampMs::new(
        now_ms
            .get()
            .saturating_sub(policy.completed_task_retention_ms()),
    );

    let mut reaped_stale_members = 0_usize;
    for _ in 0..policy.max_reap_batches_per_tick() {
        let result = service.reap_stale_members(stale_before_ms, reap_limit)?;
        reaped_stale_members = reaped_stale_members.saturating_add(result.reaped_count);
        if result.reaped_count < policy.reap_batch() {
            break;
        }
    }

    let mut pruned_completed_tasks = 0_usize;
    for _ in 0..policy.max_prune_batches_per_tick() {
        let result = service.prune_completed_tasks(completed_before_ms, prune_limit)?;
        pruned_completed_tasks = pruned_completed_tasks.saturating_add(result.pruned_count);
        if result.pruned_count < policy.prune_batch() {
            break;
        }
    }
    Ok(StandaloneMaintenanceResult {
        pruned_completed_tasks,
        reaped_stale_members,
    })
}

/// Periodic standalone maintenance thread. Future Raft mode must only run this on the leader.
pub struct StandaloneMaintenanceRunner {
    state_owner: StateOwnerHandle,
    policy: StandaloneMaintenancePolicy,
}

impl StandaloneMaintenanceRunner {
    #[must_use]
    pub const fn new(state_owner: StateOwnerHandle, policy: StandaloneMaintenancePolicy) -> Self {
        Self {
            state_owner,
            policy,
        }
    }

    /// Spawn periodic maintenance until `stop` becomes true.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] if the maintenance thread cannot be created.
    pub fn spawn(self, stop: Arc<AtomicBool>) -> Result<thread::JoinHandle<()>, RuntimeError> {
        thread::Builder::new()
            .name("agent-broker-standalone-maintenance".to_owned())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    match system_clock_ms() {
                        Ok(now_ms) => {
                            if let Err(error) =
                                self.state_owner.run_maintenance(self.policy, now_ms)
                            {
                                eprintln!("agentbrokerd maintenance failed: {error}");
                            }
                        }
                        Err(error) => eprintln!("agentbrokerd maintenance clock failed: {error}"),
                    }
                    thread::sleep(self.policy.interval());
                }
            })
            .map_err(|error| RuntimeError::io("Broker maintenance thread spawn failed", error))
    }
}
