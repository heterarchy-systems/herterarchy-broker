#![no_main]

use agent_broker_storage::decode_snapshot;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_snapshot(data);
});
