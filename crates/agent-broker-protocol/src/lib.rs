#![forbid(unsafe_code)]
//! Provider-neutral Agent Broker protocol contract.
//!
//! Wire codecs live at this boundary rather than in the Broker domain. The current migration keeps
//! the protocol model dependency-light; JSON serialization is added with `serde` only after the
//! dependency is available through the approved supply-chain path.

mod dispatch;
mod request;
mod response;
mod wire_v1;
mod wire_v2;
mod wire_v3;

pub use dispatch::{BrokerRequestDispatcher, DispatchResult};

pub use request::{
    BrokerRequest, ClaimTaskRequest, CompleteTaskRequest, DeclaredCapabilities,
    EnsureConsumerGroupRequest, EnsureNamespaceRequest, HealthRequest, HeartbeatRequest,
    JoinConsumerGroupRequest, LeaveConsumerGroupRequest, Operation, OperationParseError,
    PROTOCOL_VERSION, PublishTaskRequest, RenewTaskLeaseRequest, RequestId, RequestIdError,
};
pub use response::{BrokerResponse, ErrorPayload, SuccessPayload};
pub use wire_v1::{
    DEFAULT_MAX_FRAME_BYTES, MAX_ERROR_MESSAGE_BYTES, ProtocolCodecError, ResponseDecodeError,
    decode_request, decode_request_with_limit, decode_response_for_operation,
    decode_response_for_operation_with_limit, encode_request, encode_request_with_limit,
    encode_response, encode_response_with_limit,
};
pub use wire_v2::{
    BrokerWireRequest, IdentifiedBrokerRequest, PROTOCOL_VERSION_V2,
    decode_response_v2_for_operation, decode_response_v2_for_operation_with_limit,
    decode_wire_request, decode_wire_request_with_limit, encode_identified_request,
    encode_identified_request_with_limit, encode_response_v2, encode_response_v2_with_limit,
    request_version_hint,
};
pub use wire_v3::{
    ACQUIRE_COMMAND_SESSION_OWNER_OPERATION, BrokerRequestV3, OwnerAcquisitionRequestV3,
    OwnerIdentifiedBrokerRequestV3, PROTOCOL_VERSION_V3,
    decode_owner_acquisition_response_with_limit, decode_owner_mutation_response_with_limit,
    decode_request_v3_with_limit, encode_owner_acquisition_request_with_limit,
    encode_owner_mutation_request_with_limit, encode_response_v3_with_limit,
};
