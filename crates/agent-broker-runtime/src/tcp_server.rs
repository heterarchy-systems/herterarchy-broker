use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use agent_broker_application::{BrokerError, BrokerErrorCode, BrokerErrorDisposition};
use agent_broker_protocol::{
    BrokerResponse, PROTOCOL_VERSION_V2, PROTOCOL_VERSION_V3, RequestId,
    decode_wire_request_with_limit, encode_response_v2_with_limit, encode_response_v3_with_limit,
    encode_response_with_limit, request_version_hint,
};

use crate::clock::system_clock_ms;
use crate::{RuntimeError, StateOwnerHandle};

const DEFAULT_MAX_FRAME_BYTES: usize = 128 * 1024;
const MIN_FRAME_BYTES: usize = 4_096;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 256;
const MAX_CONNECTIONS: usize = 4_096;
const DEFAULT_CONNECTION_IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONNECTION_IO_TIMEOUT: Duration = Duration::from_mins(5);
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// TCP framing/resource policy for the Broker protocol boundary.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrokerServerConfig {
    pub address: SocketAddr,
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub connection_io_timeout: Duration,
}

/// Read-only bounded Broker listener load for operations/readiness composition.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrokerServerLoad {
    pub serving: bool,
    pub active_connections: usize,
    pub max_connections: usize,
}

/// Cloneable observer for Broker TCP listener lifecycle and connection pressure.
#[derive(Debug, Clone)]
pub struct BrokerServerObserver {
    serving: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
    max_connections: usize,
}

impl BrokerServerObserver {
    /// Read the current listener lifecycle and bounded connection load without queueing work.
    #[must_use]
    pub fn load(&self) -> BrokerServerLoad {
        BrokerServerLoad {
            serving: self.serving.load(Ordering::Acquire),
            active_connections: self.active_connections.load(Ordering::Acquire),
            max_connections: self.max_connections,
        }
    }
}

/// Listener exposure policy.
///
/// Local-only binding remains the default. Container composition can explicitly permit a bridge
/// listener while constraining host publication separately.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
pub enum BrokerBindPolicy {
    /// Require a loopback listener address.
    #[default]
    LocalOnly,
    /// Permit the configured listener address after resource validation.
    ContainerBridge,
}

impl Default for BrokerServerConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8_811),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            connection_io_timeout: DEFAULT_CONNECTION_IO_TIMEOUT,
        }
    }
}

impl BrokerServerConfig {
    /// Validate loopback and bounded resource constraints.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfiguration`] for non-loopback bind addresses or bounds
    /// outside the protocol-v1 server contract.
    pub fn validate(self) -> Result<Self, RuntimeError> {
        self.validate_with_policy(BrokerBindPolicy::LocalOnly)
    }

    /// Validate bounded resources and listener exposure.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfiguration`] when a non-local address is used without an
    /// explicit bridge policy or when configured resource bounds are invalid.
    pub fn validate_with_policy(self, bind_policy: BrokerBindPolicy) -> Result<Self, RuntimeError> {
        if bind_policy == BrokerBindPolicy::LocalOnly && !self.address.ip().is_loopback() {
            return Err(RuntimeError::InvalidConfiguration(
                "Broker server address must be loopback-only",
            ));
        }
        if !(MIN_FRAME_BYTES..=MAX_FRAME_BYTES).contains(&self.max_frame_bytes) {
            return Err(RuntimeError::InvalidConfiguration(
                "max_frame_bytes must be between 4096 and 1048576",
            ));
        }
        if !(1..=MAX_CONNECTIONS).contains(&self.max_connections) {
            return Err(RuntimeError::InvalidConfiguration(
                "max_connections must be between 1 and 4096",
            ));
        }
        if self.connection_io_timeout.is_zero()
            || self.connection_io_timeout > MAX_CONNECTION_IO_TIMEOUT
        {
            return Err(RuntimeError::InvalidConfiguration(
                "connection_io_timeout must be between 1ns and 300s",
            ));
        }
        Ok(self)
    }
}

/// Bound TCP server that delegates all state mutation to one state-owner thread.
pub struct TcpBrokerServer {
    listener: TcpListener,
    config: BrokerServerConfig,
    state_owner: StateOwnerHandle,
    fallback_request_id: RequestId,
    observer: BrokerServerObserver,
}

impl TcpBrokerServer {
    /// Bind the loopback listener and switch it to stop-aware nonblocking accept polling.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for invalid configuration, bind failure, or listener setup failure.
    pub fn bind(
        config: BrokerServerConfig,
        state_owner: StateOwnerHandle,
    ) -> Result<Self, RuntimeError> {
        Self::bind_with_policy(config, state_owner, BrokerBindPolicy::LocalOnly)
    }

    /// Bind the listener using an explicit exposure policy.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for invalid configuration, bind failure, or listener setup failure.
    pub fn bind_with_policy(
        config: BrokerServerConfig,
        state_owner: StateOwnerHandle,
        bind_policy: BrokerBindPolicy,
    ) -> Result<Self, RuntimeError> {
        let config = config.validate_with_policy(bind_policy)?;
        let listener = TcpListener::bind(config.address)
            .map_err(|error| RuntimeError::io("Broker TCP bind failed", error))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| RuntimeError::io("Broker TCP nonblocking setup failed", error))?;
        let fallback_request_id = RequestId::new("unknown").map_err(|_| {
            RuntimeError::InvalidConfiguration("internal fallback request_id is invalid")
        })?;
        let observer = BrokerServerObserver {
            serving: Arc::new(AtomicBool::new(false)),
            active_connections: Arc::new(AtomicUsize::new(0)),
            max_connections: config.max_connections,
        };
        Ok(Self {
            listener,
            config,
            state_owner,
            fallback_request_id,
            observer,
        })
    }

    /// Return the actual bound address, including an ephemeral port selected from port zero.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the listener address cannot be queried.
    pub fn local_addr(&self) -> Result<SocketAddr, RuntimeError> {
        self.listener
            .local_addr()
            .map_err(|error| RuntimeError::io("Broker TCP local address read failed", error))
    }

    /// Create a cloneable read-only observer for runtime operations/readiness composition.
    #[must_use]
    pub fn observer(&self) -> BrokerServerObserver {
        self.observer.clone()
    }

    /// Serve connections until the supplied stop flag becomes true.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when accept or connection-thread creation fails.
    pub fn serve_until(&self, stop: &AtomicBool) -> Result<(), RuntimeError> {
        self.observer.serving.store(true, Ordering::Release);
        let _serving_guard = ServingGuard(&self.observer.serving);
        let active = Arc::clone(&self.observer.active_connections);
        while !stop.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _peer)) => {
                    if active.fetch_add(1, Ordering::AcqRel) >= self.config.max_connections {
                        active.fetch_sub(1, Ordering::AcqRel);
                        drop(stream);
                        continue;
                    }
                    let state_owner = self.state_owner.clone();
                    let active_guard = Arc::clone(&active);
                    let fallback_request_id = self.fallback_request_id.clone();
                    let max_frame_bytes = self.config.max_frame_bytes;
                    let connection_io_timeout = self.config.connection_io_timeout;
                    let spawn = thread::Builder::new()
                        .name("agent-broker-connection".to_owned())
                        .spawn(move || {
                            let _guard = ConnectionCountGuard(active_guard);
                            if let Err(error) = handle_connection(
                                stream,
                                &state_owner,
                                &fallback_request_id,
                                max_frame_bytes,
                                connection_io_timeout,
                            ) {
                                eprintln!("agentbrokerd connection failed: {error}");
                            }
                        });
                    if let Err(error) = spawn {
                        active.fetch_sub(1, Ordering::AcqRel);
                        return Err(RuntimeError::io(
                            "Broker connection thread spawn failed",
                            error,
                        ));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(ACCEPT_POLL_INTERVAL);
                }
                Err(error) => return Err(RuntimeError::io("Broker TCP accept failed", error)),
            }
        }
        Ok(())
    }
}

struct ServingGuard<'a>(&'a AtomicBool);

impl Drop for ServingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct ConnectionCountGuard(Arc<AtomicUsize>);

impl Drop for ConnectionCountGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) enum FrameRead {
    Eof,
    Frame(Vec<u8>),
    TooLarge,
}

fn handle_connection(
    stream: TcpStream,
    state_owner: &StateOwnerHandle,
    fallback_request_id: &RequestId,
    max_frame_bytes: usize,
    connection_io_timeout: Duration,
) -> Result<(), RuntimeError> {
    configure_connection(&stream, connection_io_timeout)?;
    let mut reader = BufReader::new(stream);
    loop {
        match read_request_frame(&mut reader, max_frame_bytes)? {
            FrameRead::Eof => return Ok(()),
            FrameRead::TooLarge => {
                let frame = encode_error(
                    fallback_request_id.clone(),
                    BrokerErrorCode::InvalidRequest,
                    "Request frame exceeds the configured byte limit.",
                    max_frame_bytes,
                )?;
                write_frame(reader.get_mut(), &frame)?;
                return Ok(());
            }
            FrameRead::Frame(frame) => {
                let request = match decode_wire_request_with_limit(&frame, max_frame_bytes) {
                    Ok(request) => request,
                    Err(error) => {
                        let response = encode_error(
                            fallback_request_id.clone(),
                            BrokerErrorCode::InvalidRequest,
                            &error.to_string(),
                            max_frame_bytes,
                        )?;
                        let response = if request_version_hint(&frame) == Some(PROTOCOL_VERSION_V2)
                        {
                            encode_error_v2(
                                fallback_request_id.clone(),
                                BrokerErrorCode::InvalidRequest,
                                &error.to_string(),
                                max_frame_bytes,
                            )?
                        } else if request_version_hint(&frame) == Some(PROTOCOL_VERSION_V3) {
                            encode_error_v3(
                                fallback_request_id.clone(),
                                BrokerErrorCode::InvalidRequest,
                                &error.to_string(),
                                max_frame_bytes,
                            )?
                        } else {
                            response
                        };
                        write_frame(reader.get_mut(), &response)?;
                        continue;
                    }
                };
                let request_id = request.request_id().clone();
                let protocol_version = request.protocol_version();
                let response = match system_clock_ms() {
                    Ok(observed_at_ms) => {
                        match state_owner.dispatch_wire(request, observed_at_ms) {
                            Ok(response) => response,
                            Err(RuntimeError::StateOwnerSaturated) => BrokerResponse::error(
                                request_id,
                                BrokerError::new(
                                    BrokerErrorCode::CapacityExceeded,
                                    "Broker request queue is saturated.",
                                )
                                .with_disposition(BrokerErrorDisposition::Rejected),
                            ),
                            Err(_) => BrokerResponse::error(
                                request_id,
                                BrokerError::new(
                                    BrokerErrorCode::InternalError,
                                    "Broker request failed unexpectedly.",
                                ),
                            ),
                        }
                    }
                    Err(_) => BrokerResponse::error(
                        request_id,
                        BrokerError::new(
                            BrokerErrorCode::InternalError,
                            "Broker request failed unexpectedly.",
                        ),
                    ),
                };
                let encoded = match protocol_version {
                    PROTOCOL_VERSION_V2 => {
                        encode_response_v2_with_limit(&response, max_frame_bytes)?
                    }
                    PROTOCOL_VERSION_V3 => {
                        encode_response_v3_with_limit(&response, max_frame_bytes)?
                    }
                    _ => encode_response_with_limit(&response, max_frame_bytes)?,
                };
                write_frame(reader.get_mut(), &encoded)?;
            }
        }
    }
}

pub(crate) fn configure_connection(
    stream: &TcpStream,
    connection_io_timeout: Duration,
) -> Result<(), RuntimeError> {
    stream
        .set_nonblocking(false)
        .map_err(|error| RuntimeError::io("Broker connection blocking-mode setup failed", error))?;
    stream
        .set_nodelay(true)
        .map_err(|error| RuntimeError::io("Broker TCP_NODELAY setup failed", error))?;
    stream
        .set_read_timeout(Some(connection_io_timeout))
        .map_err(|error| RuntimeError::io("Broker connection read-timeout setup failed", error))?;
    stream
        .set_write_timeout(Some(connection_io_timeout))
        .map_err(|error| RuntimeError::io("Broker connection write-timeout setup failed", error))?;
    Ok(())
}

pub(crate) fn read_request_frame(
    reader: &mut BufReader<TcpStream>,
    max_frame_bytes: usize,
) -> Result<FrameRead, RuntimeError> {
    let mut frame = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| RuntimeError::io("Broker request read failed", error))?;
        if available.is_empty() {
            return Ok(FrameRead::Eof);
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if frame.len().saturating_add(consumed) > max_frame_bytes {
            return Ok(FrameRead::TooLarge);
        }
        frame.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(FrameRead::Frame(frame));
        }
    }
}

pub(crate) fn write_frame(stream: &mut TcpStream, frame: &[u8]) -> Result<(), RuntimeError> {
    stream
        .write_all(frame)
        .map_err(|error| RuntimeError::io("Broker response write failed", error))?;
    stream
        .flush()
        .map_err(|error| RuntimeError::io("Broker response flush failed", error))
}

fn encode_error(
    request_id: RequestId,
    code: BrokerErrorCode,
    message: &str,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let response = BrokerResponse::error(request_id, BrokerError::new(code, message));
    encode_response_with_limit(&response, max_frame_bytes).map_err(RuntimeError::from)
}

fn encode_error_v2(
    request_id: RequestId,
    code: BrokerErrorCode,
    message: &str,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let response = BrokerResponse::error(request_id, BrokerError::new(code, message));
    encode_response_v2_with_limit(&response, max_frame_bytes).map_err(RuntimeError::from)
}

fn encode_error_v3(
    request_id: RequestId,
    code: BrokerErrorCode,
    message: &str,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, RuntimeError> {
    let response = BrokerResponse::error(request_id, BrokerError::new(code, message));
    encode_response_v3_with_limit(&response, max_frame_bytes).map_err(RuntimeError::from)
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    use super::{BufReader, configure_connection, read_request_frame};

    #[test]
    fn idle_connection_read_is_bounded_by_configured_timeout()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let client = TcpStream::connect(listener.local_addr()?)?;
        let (server, _peer) = listener.accept()?;
        let timeout = Duration::from_millis(50);
        configure_connection(&server, timeout)?;
        assert_eq!(server.read_timeout()?, Some(timeout));
        assert_eq!(server.write_timeout()?, Some(timeout));

        let mut reader = BufReader::new(server);
        let result = read_request_frame(&mut reader, 4_096);
        assert!(result.is_err());
        drop(client);
        Ok(())
    }
}
