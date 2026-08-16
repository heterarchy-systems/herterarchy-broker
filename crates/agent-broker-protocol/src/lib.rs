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
