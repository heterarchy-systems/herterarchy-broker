use agent_broker_application::{BrokerError, BrokerErrorCode, ConsensusAdapter};
use agent_broker_domain::commands::BrokerCommand;
use agent_broker_domain::results::BrokerMutationResult;
use agent_broker_domain::{BrokerStateMachine, ConsumerGroupDirectory, Revision, Term};
use agent_broker_storage::{BrokerStateRepository, RepositoryError};

const FAIL_STOPPED_MESSAGE: &str = "Standalone Broker is fail-stopped after a durability failure.";

/// Single-node commit adapter preserving the same durability/fencing contract as future consensus.
#[derive(Debug)]
pub struct StandaloneConsensusAdapter<R> {
    repository: R,
    machine: BrokerStateMachine,
    poisoned: bool,
}

impl<R: BrokerStateRepository> StandaloneConsensusAdapter<R> {
    /// Load durable state and construct the single-owner standalone consensus path.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerErrorCode::PersistenceError`] when durable recovery fails or the recovered
    /// checkpoint cannot reconstruct the deterministic state machine.
    pub fn new(mut repository: R) -> Result<Self, BrokerError> {
        let checkpoint = repository
            .load()
            .map_err(|error| persistence_error(&error))?;
        let machine = BrokerStateMachine::from_checkpoint(checkpoint).map_err(|error| {
            BrokerError::new(
                BrokerErrorCode::PersistenceError,
                format!("durable Broker checkpoint is invalid: {error}"),
            )
        })?;
        Ok(Self {
            repository,
            machine,
            poisoned: false,
        })
    }

    /// Return whether a durability failure has permanently fail-stopped this adapter instance.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Borrow the current in-process state for diagnostics/read-model construction.
    #[must_use]
    pub const fn state_machine(&self) -> &BrokerStateMachine {
        &self.machine
    }
}

impl<R: BrokerStateRepository> ConsensusAdapter for StandaloneConsensusAdapter<R> {
    fn term(&self) -> Term {
        self.machine.state().term()
    }

    fn revision(&self) -> Revision {
        self.machine.state().revision()
    }

    fn group_directory(&mut self) -> Result<ConsumerGroupDirectory, BrokerError> {
        if self.poisoned {
            return Err(BrokerError::new(
                BrokerErrorCode::PersistenceError,
                FAIL_STOPPED_MESSAGE,
            ));
        }
        Ok(self.machine.state().group_directory())
    }

    fn propose(&mut self, command: BrokerCommand) -> Result<BrokerMutationResult, BrokerError> {
        if self.poisoned {
            return Err(BrokerError::new(
                BrokerErrorCode::PersistenceError,
                FAIL_STOPPED_MESSAGE,
            ));
        }

        let before_revision = self.machine.state().revision();
        let applied = self.machine.apply(command).map_err(BrokerError::from)?;
        if self.machine.state().revision() == before_revision {
            return Ok(applied.result);
        }

        if let Err(error) = self
            .repository
            .commit(self.machine.state(), &applied.changes)
        {
            self.poisoned = true;
            return Err(persistence_error(&error));
        }
        Ok(applied.result)
    }
}

fn persistence_error(error: &RepositoryError) -> BrokerError {
    BrokerError::new(BrokerErrorCode::PersistenceError, error.to_string())
}
