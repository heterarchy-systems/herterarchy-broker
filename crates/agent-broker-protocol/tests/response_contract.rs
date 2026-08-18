use std::error::Error;

use agent_broker_application::{
    BrokerError, BrokerErrorCode, BrokerErrorDisposition, BrokerHealth,
};
use agent_broker_domain::results::{MutationMetadata, TaskClaimResult};
use agent_broker_domain::{Generation, Revision, Term};
use agent_broker_protocol::{
    BrokerResponse, DispatchResult, ErrorPayload, PROTOCOL_VERSION, RequestId, SuccessPayload,
};

#[test]
fn health_dispatch_result_maps_to_python_success_payload_shape() -> Result<(), Box<dyn Error>> {
    let response = BrokerResponse::success(
        RequestId::new("response-health")?,
        DispatchResult::Health(BrokerHealth {
            term: Term::INITIAL,
            revision: Revision::new(7),
            protocol_version: PROTOCOL_VERSION,
        }),
    );

    let BrokerResponse::Success { request_id, result } = response else {
        return Err("expected success response".into());
    };
    assert_eq!(request_id.as_str(), "response-health");
    assert_eq!(
        result,
        SuccessPayload::Health {
            protocol_version: 1,
            term: Term::INITIAL,
            revision: Revision::new(7),
        }
    );
    Ok(())
}

#[test]
fn empty_claim_preserves_none_fields_required_by_python_wire_contract() -> Result<(), Box<dyn Error>>
{
    let payload = SuccessPayload::from(DispatchResult::TaskClaimed(TaskClaimResult {
        metadata: MutationMetadata {
            term: Term::INITIAL,
            revision: Revision::new(9),
        },
        task_id: None,
        objective: None,
        task_revision: None,
        lease_id: None,
        lease_epoch: None,
        lease_expires_at_ms: None,
        generation: Generation::new(3),
    }));

    let SuccessPayload::TaskClaimed {
        task_id,
        objective,
        task_revision,
        lease_id,
        lease_epoch,
        lease_expires_at_ms,
        generation,
        ..
    } = payload
    else {
        return Err("expected task claim payload".into());
    };
    assert!(task_id.is_none());
    assert!(objective.is_none());
    assert!(task_revision.is_none());
    assert!(lease_id.is_none());
    assert!(lease_epoch.is_none());
    assert!(lease_expires_at_ms.is_none());
    assert_eq!(generation, Generation::new(3));
    Ok(())
}

#[test]
fn broker_error_maps_to_stable_protocol_error_payload() -> Result<(), Box<dyn Error>> {
    let response = BrokerResponse::error(
        RequestId::new("response-error")?,
        BrokerError::new(BrokerErrorCode::StaleFence, "stale lease"),
    );

    let BrokerResponse::Error { request_id, error } = response else {
        return Err("expected error response".into());
    };
    assert_eq!(request_id.as_str(), "response-error");
    assert_eq!(
        error,
        ErrorPayload {
            code: BrokerErrorCode::StaleFence,
            message: "stale lease".to_owned(),
            disposition: BrokerErrorDisposition::Unknown,
        }
    );
    assert_eq!(error.code.as_str(), "STALE_FENCE");
    Ok(())
}
