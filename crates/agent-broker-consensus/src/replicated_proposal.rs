use agent_broker_application::{
    CommandIdentity, CommandSequence, CommandSessionId, SessionOwnerEpoch, SessionOwnerInstanceId,
};
use agent_broker_domain::commands::BrokerCommand;
use serde::{Deserialize, Serialize};

use crate::{ReplicatedBrokerCommandV1, ReplicatedCommandError};

/// Versioned `OpenRaft` application-data envelope. Legacy proposals intentionally carry no identity.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedBrokerProposalV1 {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity: Option<ReplicatedCommandIdentityV1>,
    command: ReplicatedBrokerCommandV1,
}

impl ReplicatedBrokerProposalV1 {
    #[must_use]
    pub const fn legacy(command: ReplicatedBrokerCommandV1) -> Self {
        Self {
            identity: None,
            command,
        }
    }

    #[must_use]
    pub fn identified(identity: &CommandIdentity, command: ReplicatedBrokerCommandV1) -> Self {
        Self {
            identity: Some(identity.into()),
            command,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> Option<&ReplicatedCommandIdentityV1> {
        self.identity.as_ref()
    }

    #[must_use]
    pub const fn command(&self) -> &ReplicatedBrokerCommandV1 {
        &self.command
    }

    /// Recover validated application identity and the replicated Broker command.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicatedCommandError`] when the serialized identity violates the stable
    /// command-session contract.
    pub fn into_parts(
        self,
    ) -> Result<(Option<CommandIdentity>, ReplicatedBrokerCommandV1), ReplicatedCommandError> {
        let identity = self.identity.map(CommandIdentity::try_from).transpose()?;
        Ok((identity, self.command))
    }
}

impl TryFrom<BrokerCommand> for ReplicatedBrokerProposalV1 {
    type Error = ReplicatedCommandError;

    fn try_from(command: BrokerCommand) -> Result<Self, Self::Error> {
        Ok(Self::legacy(ReplicatedBrokerCommandV1::try_from(command)?))
    }
}

/// Serializable representation of the provider-neutral command session/sequence identity.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedCommandIdentityV1 {
    session_id: String,
    #[serde(default = "initial_owner_epoch")]
    owner_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_instance_id: Option<String>,
    sequence: u64,
}

impl ReplicatedCommandIdentityV1 {
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub const fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    #[must_use]
    pub fn owner_instance_id(&self) -> Option<&str> {
        self.owner_instance_id.as_deref()
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl From<&CommandIdentity> for ReplicatedCommandIdentityV1 {
    fn from(identity: &CommandIdentity) -> Self {
        Self {
            session_id: identity.session_id().as_str().to_owned(),
            owner_epoch: identity.owner_epoch().get(),
            owner_instance_id: identity
                .owner_instance_id()
                .map(|owner_instance_id| owner_instance_id.as_str().to_owned()),
            sequence: identity.sequence().get(),
        }
    }
}

impl TryFrom<ReplicatedCommandIdentityV1> for CommandIdentity {
    type Error = ReplicatedCommandError;

    fn try_from(identity: ReplicatedCommandIdentityV1) -> Result<Self, Self::Error> {
        let session_id = CommandSessionId::new(identity.session_id).map_err(validation_error)?;
        let owner_epoch = SessionOwnerEpoch::new(identity.owner_epoch).map_err(validation_error)?;
        let owner_instance_id = identity
            .owner_instance_id
            .map(SessionOwnerInstanceId::new)
            .transpose()
            .map_err(validation_error)?;
        let sequence = CommandSequence::new(identity.sequence).map_err(validation_error)?;
        Ok(match owner_instance_id {
            Some(owner_instance_id) => {
                Self::new_with_owner(session_id, owner_epoch, owner_instance_id, sequence)
            }
            None => Self::new_with_owner_epoch(session_id, owner_epoch, sequence),
        })
    }
}

const fn initial_owner_epoch() -> u64 {
    1
}

fn validation_error(error: impl std::fmt::Display) -> ReplicatedCommandError {
    ReplicatedCommandError::new(format!("invalid replicated command identity: {error}"))
}
