use agent_broker_domain::commands::BrokerCommand;
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{Revision, Term};

use crate::BrokerError;

/// Provider-neutral proposal boundary between Broker application use cases and consensus/storage.
///
/// Standalone and future Raft implementations must expose identical committed command semantics
/// through this interface. Provider runtimes never implement this trait.
pub trait ConsensusAdapter {
    /// Return the current authoritative Broker term.
    fn term(&self) -> Term;

    /// Return the current committed Broker revision.
    fn revision(&self) -> Revision;

    /// Propose one deterministic Broker command and return only after it is authoritative for the
    /// adapter's durability/consensus contract.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when validation, persistence, consensus, or fencing rejects the
    /// proposal. An adapter must not acknowledge a mutation before its own authority contract is
    /// satisfied.
    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError>;
}
