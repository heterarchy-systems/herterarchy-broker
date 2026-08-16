#![no_main]

use agent_broker_protocol::{Operation, RequestId, decode_request, decode_response_for_operation};
use libfuzzer_sys::fuzz_target;

const OPERATIONS: [Operation; 10] = [
    Operation::Health,
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
    let _ = decode_request(data);

    let Ok(value) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    let Some(request_id) = value.get("request_id").and_then(serde_json::Value::as_str) else {
        return;
    };
    let Ok(request_id) = RequestId::new(request_id) else {
        return;
    };
    for operation in OPERATIONS {
        let _ = decode_response_for_operation(data, &request_id, operation);
    }
});
