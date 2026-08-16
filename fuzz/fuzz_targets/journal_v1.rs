#![no_main]

use agent_broker_domain::{BrokerCheckpoint, Revision, Term};
use agent_broker_storage::{apply_journal_mutation, decode_journal_mutation};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(mutation) = decode_journal_mutation(data) else {
        return;
    };
    let mut checkpoint = BrokerCheckpoint {
        term: Term::INITIAL,
        revision: Revision::new(0),
        namespaces: Vec::new(),
        tasks: Vec::new(),
        groups: Vec::new(),
    };
    let _ = apply_journal_mutation(&mut checkpoint, mutation);
});
