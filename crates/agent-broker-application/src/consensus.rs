use agent_broker_domain::commands::BrokerCommand;
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{ConsumerGroupDirectory, Revision, Term};

use crate::{
    BrokerError, BrokerErrorCode, CommandIdentity, CommandSessionId, SessionOwnerEpoch,
    SessionOwnerInstanceId,
};

/// Provider-neutral proposal boundary between Broker application use cases and consensus/storage.
///
/// Standalone and future Raft implementations must expose identical committed command semantics
/// through this interface. Provider runtimes never implement this trait.
pub trait ConsensusAdapter {
    /// Return the current authoritative Broker term.
    fn term(&self) -> Term;

    /// Return the current committed Broker revision.
    fn revision(&self) -> Revision;

    /// Return one side-effect-free directory of current Consumer Groups.
    ///
    /// Replicated implementations must establish read authority before returning this data and
    /// must never expose a follower-local stale projection as authoritative state.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when this adapter cannot establish an authoritative read.
    fn group_directory(&mut self) -> Result<ConsumerGroupDirectory, BrokerError> {
        Err(BrokerError::new(
            BrokerErrorCode::InternalError,
            "Consumer Group directory reads are unavailable for this consensus adapter",
        ))
    }

    /// Return whether this adapter currently owns authority to initiate maintenance mutations.
    ///
    /// Standalone and one-node adapters are always authoritative. Replicated adapters must override
    /// this method and derive the answer from consensus leader state rather than local process
    /// liveness or an application-side lease.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when current maintenance authority cannot be determined.
    fn maintenance_authority(&mut self) -> Result<bool, BrokerError> {
        Ok(true)
    }

    /// Propose one deterministic Broker command and return only after it is authoritative for the
    /// adapter's durability/consensus contract.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when validation, persistence, consensus, or fencing rejects the
    /// proposal. An adapter must not acknowledge a mutation before its own authority contract is
    /// satisfied.
    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError>;

    /// Propose a mutation with a stable session/sequence identity that permits safe replay after an
    /// ambiguous transport or consensus response timeout.
    ///
    /// Implementations that cannot durably preserve committed response identity must reject this
    /// path rather than silently falling back to ordinary at-least-once mutation semantics.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the adapter does not support durable identified proposals or
    /// when the implementation rejects/fails the consensus mutation.
    fn propose_identified(
        &mut self,
        _identity: CommandIdentity,
        _command: BrokerCommand,
    ) -> Result<BrokerMutationResult, BrokerError> {
        Err(BrokerError::new(
            BrokerErrorCode::InvalidRequest,
            "identified consensus proposals are not supported by this adapter",
        ))
    }

    /// Acquire the broker-authoritative owner incarnation for one command session.
    ///
    /// A missing session may be bootstrapped only at owner epoch 1. For an existing session,
    /// `expected_owner_epoch` must match the committed epoch. A larger client-supplied epoch is not
    /// itself authority to take ownership; the transition must be durably committed by consensus.
    /// Retrying the same acquisition with the same owner-instance identity after response loss must
    /// return the already committed epoch instead of incrementing it again.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when durable session ownership is unsupported, the bootstrap/current
    /// expected epoch is stale, session capacity is exhausted, or the transition fails
    /// consensus/persistence.
    fn acquire_command_session_owner(
        &mut self,
        _session_id: CommandSessionId,
        _expected_owner_epoch: SessionOwnerEpoch,
        _owner_instance_id: SessionOwnerInstanceId,
    ) -> Result<SessionOwnerEpoch, BrokerError> {
        Err(BrokerError::new(
            BrokerErrorCode::InvalidRequest,
            "command-session owner acquisition is not supported by this adapter",
        ))
    }
}
