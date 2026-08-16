use std::error::Error;

use agent_broker_protocol::{BrokerRequest, HealthRequest, Operation, PROTOCOL_VERSION, RequestId};

#[test]
fn protocol_version_and_operation_names_match_python_reference() -> Result<(), Box<dyn Error>> {
    assert_eq!(PROTOCOL_VERSION, 1);
    let expected = [
        (Operation::Health, "health"),
        (Operation::EnsureNamespace, "namespace.ensure"),
        (Operation::PublishTask, "task.publish"),
        (Operation::EnsureConsumerGroup, "group.ensure"),
        (Operation::JoinConsumerGroup, "group.join"),
        (Operation::Heartbeat, "group.heartbeat"),
        (Operation::LeaveConsumerGroup, "group.leave"),
        (Operation::ClaimTask, "task.claim"),
        (Operation::RenewTaskLease, "task.renew"),
        (Operation::CompleteTask, "task.complete"),
    ];

    for (operation, wire_name) in expected {
        assert_eq!(operation.as_str(), wire_name);
        assert_eq!(Operation::try_from(wire_name)?, operation);
    }
    assert!(Operation::try_from("task.unknown").is_err());
    Ok(())
}

#[test]
fn request_id_validation_matches_python_reference_pattern() -> Result<(), Box<dyn Error>> {
    let request_id = RequestId::new("A0_.:-request")?;
    assert_eq!(request_id.as_str(), "A0_.:-request");

    let max_length = format!("a{}", "x".repeat(127));
    assert_eq!(RequestId::new(max_length.clone())?.as_str(), max_length);

    assert!(RequestId::new("").is_err());
    assert!(RequestId::new("-starts-with-punctuation").is_err());
    assert!(RequestId::new("contains space").is_err());
    assert!(RequestId::new("한글").is_err());
    assert!(RequestId::new(format!("a{}", "x".repeat(128))).is_err());
    Ok(())
}

#[test]
fn broker_request_exposes_stable_operation_and_correlation_identity() -> Result<(), Box<dyn Error>>
{
    let request_id = RequestId::new("request-1")?;
    let request = BrokerRequest::Health(HealthRequest {
        request_id: request_id.clone(),
    });

    assert_eq!(request.operation(), Operation::Health);
    assert_eq!(request.request_id(), &request_id);
    Ok(())
}
