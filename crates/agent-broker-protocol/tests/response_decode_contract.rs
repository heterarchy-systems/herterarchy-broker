use std::error::Error;

use agent_broker_application::BrokerErrorCode;
use agent_broker_protocol::{
    Operation, RequestId, ResponseDecodeError, SuccessPayload, decode_response_for_operation,
};

const RESPONSE_CORPUS: &[u8] =
    include_bytes!("../../../compatibility/wire-v1/response_frames.ndjson");

#[test]
fn python_success_response_corpus_decodes_to_typed_operation_payloads() -> Result<(), Box<dyn Error>>
{
    let operations = [
        ("resp-health", Operation::Health),
        ("resp-namespace", Operation::EnsureNamespace),
        ("resp-publish", Operation::PublishTask),
        ("resp-group", Operation::EnsureConsumerGroup),
        ("resp-heartbeat", Operation::Heartbeat),
        ("resp-claim-empty", Operation::ClaimTask),
        ("resp-renew", Operation::RenewTaskLease),
        ("resp-complete", Operation::CompleteTask),
    ];
    let frames = RESPONSE_CORPUS
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|frame| !frame.is_empty())
        .collect::<Vec<_>>();
    for ((request_id, operation), frame) in operations.into_iter().zip(frames) {
        let payload =
            decode_response_for_operation(frame, &RequestId::new(request_id)?, operation)?;
        match (operation, payload) {
            (Operation::Health, SuccessPayload::Health { .. })
            | (Operation::EnsureNamespace, SuccessPayload::Namespace { .. })
            | (Operation::PublishTask, SuccessPayload::TaskPublished { .. })
            | (Operation::EnsureConsumerGroup, SuccessPayload::ConsumerGroup { .. })
            | (Operation::Heartbeat, SuccessPayload::Heartbeat { .. })
            | (Operation::ClaimTask, SuccessPayload::TaskClaimed { .. })
            | (Operation::RenewTaskLease, SuccessPayload::TaskLeaseRenewed { .. })
            | (Operation::CompleteTask, SuccessPayload::TaskCompleted { .. }) => {}
            _ => return Err(format!("response payload does not match {operation}").into()),
        }
    }
    Ok(())
}

#[test]
fn python_error_response_maps_to_stable_typed_broker_error() -> Result<(), Box<dyn Error>> {
    let frame = RESPONSE_CORPUS
        .split_inclusive(|byte| *byte == b'\n')
        .nth(8)
        .ok_or("missing Python error corpus frame")?;
    let result = decode_response_for_operation(
        frame,
        &RequestId::new("resp-error")?,
        Operation::CompleteTask,
    );
    let Err(ResponseDecodeError::Broker(error)) = result else {
        return Err("expected a typed Broker error".into());
    };
    assert_eq!(error.code(), BrokerErrorCode::StaleFence);
    assert_eq!(error.message(), "stale lease");
    Ok(())
}

#[test]
fn response_decoder_rejects_correlation_and_shape_drift() -> Result<(), Box<dyn Error>> {
    let request_id = RequestId::new("resp-health")?;
    let mismatched = decode_response_for_operation(
        br#"{"ok":true,"request_id":"other","result":{"protocol_version":1,"revision":7,"term":2},"version":1}\n"#,
        &request_id,
        Operation::Health,
    );
    assert!(matches!(mismatched, Err(ResponseDecodeError::Protocol(_))));

    let extra_result_field = decode_response_for_operation(
        br#"{"ok":true,"request_id":"resp-health","result":{"extra":1,"protocol_version":1,"revision":7,"term":2},"version":1}\n"#,
        &request_id,
        Operation::Health,
    );
    assert!(matches!(
        extra_result_field,
        Err(ResponseDecodeError::Protocol(_))
    ));
    Ok(())
}
