#![no_main]

use agent_broker_protocol::{
    Operation, RequestId, decode_owner_acquisition_response_with_limit,
    decode_owner_mutation_response_with_limit, decode_request_v3_with_limit,
};
use libfuzzer_sys::fuzz_target;

const MAX_FRAME_BYTES: usize = 128 * 1024;
const OPERATIONS: [Operation; 9] = [
    Operation::EnsureNamespace,
    Operation::PublishTask,
    Operation::EnsureConsumerGroup,
    Operation::JoinConsumerGroup,
    Operation::Heartbeat,
    Operation::LeaveConsumerGroup,
    Operation::ClaimTask,
    Operation::RenewTaskLease,
    Operation::CompleteTask,
];

fuzz_target!(|data: &[u8]| {
    let _ = decode_request_v3_with_limit(data, MAX_FRAME_BYTES);

    let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    let Some(request_id) = value.get("request_id").and_then(serde_json::Value::as_str) else {
        return;
    };
    let Ok(request_id) = RequestId::new(request_id) else {
        return;
    };

    let _ = decode_owner_acquisition_response_with_limit(data, &request_id, MAX_FRAME_BYTES);
    for operation in OPERATIONS {
        let _ = decode_owner_mutation_response_with_limit(
            data,
            &request_id,
            operation,
            MAX_FRAME_BYTES,
        );
    }
});
