use std::error::Error;

use agent_broker_application::BrokerErrorCode;
use agent_broker_domain::{
    ConsumerGroupId, Generation, LeaseEpoch, LeaseId, MemberId, NamespaceId, Revision, TaskId,
    TaskObjective, TaskStatus, Term, TimestampMs,
};
use agent_broker_protocol::{
    BrokerRequest, BrokerResponse, ErrorPayload, HealthRequest, MAX_ERROR_MESSAGE_BYTES,
    ProtocolCodecError, PublishTaskRequest, RequestId, SuccessPayload, decode_request,
    decode_request_with_limit, encode_request, encode_request_with_limit, encode_response,
};

const REQUEST_CORPUS: &[u8] =
    include_bytes!("../../../compatibility/wire-v1/request_frames.ndjson");
const RESPONSE_CORPUS: &[u8] =
    include_bytes!("../../../compatibility/wire-v1/response_frames.ndjson");

fn request_id(value: &str) -> Result<RequestId, Box<dyn Error>> {
    Ok(RequestId::new(value)?)
}

fn term() -> Result<Term, Box<dyn Error>> {
    Ok(Term::new(2)?)
}

fn success(request_id: RequestId, result: SuccessPayload) -> BrokerResponse {
    BrokerResponse::Success { request_id, result }
}

fn reference_responses() -> Result<Vec<BrokerResponse>, Box<dyn Error>> {
    let mut responses = basic_reference_responses()?;
    responses.extend(membership_reference_responses()?);
    responses.extend(lease_reference_responses()?);
    responses.push(BrokerResponse::Error {
        request_id: request_id("resp-error")?,
        error: ErrorPayload {
            code: BrokerErrorCode::StaleFence,
            message: "stale lease".to_owned(),
        },
    });
    Ok(responses)
}

fn basic_reference_responses() -> Result<Vec<BrokerResponse>, Box<dyn Error>> {
    Ok(vec![
        success(
            request_id("resp-health")?,
            SuccessPayload::Health {
                protocol_version: 1,
                term: term()?,
                revision: Revision::new(7),
            },
        ),
        success(
            request_id("resp-namespace")?,
            SuccessPayload::Namespace {
                term: term()?,
                revision: Revision::new(7),
                namespace_id: NamespaceId::new("project-a")?,
                namespace_revision: Revision::new(3),
            },
        ),
        success(
            request_id("resp-publish")?,
            SuccessPayload::TaskPublished {
                term: term()?,
                revision: Revision::new(7),
                task_id: TaskId::new("task-1")?,
                task_revision: Revision::new(4),
                status: TaskStatus::Queued,
            },
        ),
    ])
}

fn membership_reference_responses() -> Result<Vec<BrokerResponse>, Box<dyn Error>> {
    Ok(vec![
        success(
            request_id("resp-group")?,
            SuccessPayload::ConsumerGroup {
                term: term()?,
                revision: Revision::new(7),
                group_id: ConsumerGroupId::new("engineering")?,
                generation: Generation::new(5),
                group_revision: Revision::new(6),
                member_count: 2,
            },
        ),
        success(
            request_id("resp-heartbeat")?,
            SuccessPayload::Heartbeat {
                term: term()?,
                revision: Revision::new(7),
                group_id: ConsumerGroupId::new("engineering")?,
                member_id: MemberId::new("worker-a")?,
                generation: Generation::new(5),
                member_revision: Revision::new(8),
            },
        ),
    ])
}

fn lease_reference_responses() -> Result<Vec<BrokerResponse>, Box<dyn Error>> {
    Ok(vec![
        success(
            request_id("resp-claim-empty")?,
            SuccessPayload::TaskClaimed {
                term: term()?,
                revision: Revision::new(7),
                task_id: None,
                objective: None,
                task_revision: None,
                lease_id: None,
                lease_epoch: None,
                lease_expires_at_ms: None,
                generation: Generation::new(5),
            },
        ),
        success(
            request_id("resp-renew")?,
            SuccessPayload::TaskLeaseRenewed {
                term: term()?,
                revision: Revision::new(7),
                task_id: TaskId::new("task-1")?,
                task_revision: Revision::new(9),
                lease_id: LeaseId::new("lease-1")?,
                lease_epoch: LeaseEpoch::new(2),
                lease_expires_at_ms: TimestampMs::new(35_000),
                generation: Generation::new(5),
            },
        ),
        success(
            request_id("resp-complete")?,
            SuccessPayload::TaskCompleted {
                term: term()?,
                revision: Revision::new(7),
                task_id: TaskId::new("task-1")?,
                task_revision: Revision::new(10),
                status: TaskStatus::Completed,
            },
        ),
    ])
}

#[test]
fn python_request_corpus_round_trips_byte_for_byte() -> Result<(), Box<dyn Error>> {
    let mut frame_count = 0;
    for frame in REQUEST_CORPUS.split_inclusive(|byte| *byte == b'\n') {
        if frame.is_empty() {
            continue;
        }
        let request = decode_request(frame)?;
        let encoded = encode_request(&request)?;
        assert_eq!(encoded, frame);
        frame_count += 1;
    }
    assert_eq!(frame_count, 10);
    Ok(())
}

#[test]
fn rust_response_encoder_matches_python_corpus_byte_for_byte() -> Result<(), Box<dyn Error>> {
    let expected = RESPONSE_CORPUS
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|frame| !frame.is_empty())
        .collect::<Vec<_>>();
    let responses = reference_responses()?;
    assert_eq!(responses.len(), expected.len());
    for (response, expected_frame) in responses.iter().zip(expected) {
        assert_eq!(encode_response(response)?, expected_frame);
    }
    Ok(())
}

#[test]
fn strict_request_decoder_rejects_schema_and_type_drift() {
    let invalid_frames: [&[u8]; 7] = [
        br#"{"version":1,"request_id":"x","operation":"health","payload":{},"extra":1}\n"#,
        br#"{"version":1,"request_id":"x","operation":"namespace.ensure","payload":{"namespace_id":"project-a","extra":1}}\n"#,
        br#"{"version":1,"request_id":"x","operation":"namespace.ensure"}\n"#,
        br#"{"version":2,"request_id":"x","operation":"health","payload":{}}\n"#,
        br#"{"version":1,"request_id":"x","operation":"unknown","payload":{}}\n"#,
        br#"{"version":1,"request_id":"x","operation":"group.heartbeat","payload":{"group_id":"engineering","member_id":"worker-a","expected_generation":true}}\n"#,
        br#"{"version":1,"request_id":"?","operation":"health","payload":{}}\n"#,
    ];
    for frame in invalid_frames {
        assert!(matches!(
            decode_request(frame),
            Err(ProtocolCodecError::InvalidRequest(_))
        ));
    }
}

#[test]
fn codec_enforces_request_response_bounds() -> Result<(), Box<dyn Error>> {
    let health = BrokerRequest::Health(HealthRequest {
        request_id: request_id("bound-check")?,
    });
    assert!(matches!(
        encode_request_with_limit(&health, 8),
        Err(ProtocolCodecError::FrameTooLarge { .. })
    ));
    assert!(matches!(
        decode_request_with_limit(br#"{"version":1}\n"#, 4),
        Err(ProtocolCodecError::FrameTooLarge { .. })
    ));

    let error_response = BrokerResponse::Error {
        request_id: request_id("large-error")?,
        error: ErrorPayload {
            code: BrokerErrorCode::InvalidRequest,
            message: "x".repeat(MAX_ERROR_MESSAGE_BYTES + 1),
        },
    };
    assert!(matches!(
        encode_response(&error_response),
        Err(ProtocolCodecError::ErrorMessageTooLarge { .. })
    ));
    Ok(())
}

#[test]
fn codec_preserves_utf8_without_ascii_escaping() -> Result<(), Box<dyn Error>> {
    let request = BrokerRequest::PublishTask(PublishTaskRequest {
        request_id: request_id("utf8")?,
        namespace_id: NamespaceId::new("project-a")?,
        task_id: TaskId::new("task-utf8")?,
        objective: TaskObjective::new("한글 ✓")?,
    });
    let encoded = encode_request(&request)?;
    assert!(
        encoded
            .windows("한글 ✓".len())
            .any(|window| window == "한글 ✓".as_bytes())
    );
    assert_eq!(decode_request(&encoded)?, request);
    Ok(())
}
