use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use agent_broker_domain::{BrokerState, Revision, Term};

/// Lock-free read-only projection of the committed Broker state owned by `OpenRaft`.
///
/// The Raft state machine remains the sole mutable owner. These atomics expose only scalar
/// observability needed by the synchronous `ConsensusAdapter` facade; callers can never mutate
/// authoritative state through this projection.
#[derive(Debug)]
pub(crate) struct RaftBrokerObservation {
    term: AtomicU64,
    revision: AtomicU64,
    has_applied_index: AtomicBool,
    applied_index: AtomicU64,
}

impl RaftBrokerObservation {
    pub(crate) fn new(state: &BrokerState, applied_index: Option<u64>) -> Self {
        Self {
            term: AtomicU64::new(state.term().get()),
            revision: AtomicU64::new(state.revision().get()),
            has_applied_index: AtomicBool::new(applied_index.is_some()),
            applied_index: AtomicU64::new(applied_index.unwrap_or(0)),
        }
    }

    pub(crate) fn update(&self, state: &BrokerState, applied_index: Option<u64>) {
        // Store data first and the applied pointer last. Readers which observe the new applied
        // pointer are therefore guaranteed to observe at least the corresponding term/revision.
        self.term.store(state.term().get(), Ordering::Release);
        self.revision
            .store(state.revision().get(), Ordering::Release);
        self.has_applied_index.store(false, Ordering::Release);
        if let Some(index) = applied_index {
            self.applied_index.store(index, Ordering::Release);
            self.has_applied_index.store(true, Ordering::Release);
        }
    }

    pub(crate) fn term_value(&self) -> u64 {
        self.term.load(Ordering::Acquire)
    }

    pub(crate) fn revision(&self) -> Revision {
        Revision::new(self.revision.load(Ordering::Acquire))
    }

    pub(crate) fn applied_index(&self) -> Option<u64> {
        if self.has_applied_index.load(Ordering::Acquire) {
            Some(self.applied_index.load(Ordering::Acquire))
        } else {
            None
        }
    }

    pub(crate) fn validated_term(&self) -> Result<Term, agent_broker_domain::FencingValueError> {
        Term::new(self.term_value())
    }
}
