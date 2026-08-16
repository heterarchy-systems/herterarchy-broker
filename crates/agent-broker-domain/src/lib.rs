#![forbid(unsafe_code)]
//! Provider-independent domain primitives shared by standalone and future HA Agent Broker modes.

pub mod checkpoint;
pub mod commands;
pub mod fencing;
pub mod group;
pub mod identifiers;
pub mod policy;
pub mod quorum;
pub mod results;
pub mod state_machine;
pub mod task;

pub use checkpoint::{
    BrokerCheckpoint, CheckpointError, ConsumerGroupCheckpoint, MemberCheckpoint,
    NamespaceCheckpoint, TaskCheckpoint, TaskCheckpointState,
};
pub use fencing::{FencingValueError, Generation, LeaseEpoch, Revision, Term};
pub use group::{
    Capabilities, CapabilitiesError, Capability, ConsumerGroup, ConsumerGroupError,
    HeartbeatOutcome, JoinOutcome, Member,
};
pub use identifiers::{ConsumerGroupId, IdentifierError, LeaseId, MemberId, NamespaceId, TaskId};
pub use policy::{
    BrokerCapacityPolicy, LeaseDurationMs, PolicyError, PruneTaskLimit, ReapMemberLimit,
};
pub use quorum::{QuorumPolicy, QuorumPolicyError};
pub use state_machine::{BrokerState, BrokerStateMachine, Namespace, StateMachineError};
pub use task::{
    CompletedTask, CompletionOutcome, LeaseFence, LeaseGrant, LeasedTask, QueuedTask, Task,
    TaskObjective, TaskResult, TaskState, TaskStatus, TaskTextError, TaskTransitionError,
    TimestampMs,
};
