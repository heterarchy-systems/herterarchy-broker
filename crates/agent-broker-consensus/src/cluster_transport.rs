use std::future::Future;
use std::io::Cursor;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use openraft::error::{
    Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, Raft, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, SnapshotMeta, Vote};
use rustls::{ClientConnection, ServerConnection, StreamOwned};
use serde::{Deserialize, Serialize};

use crate::cluster_tls::{LoadedClusterRaftTls, peer_server_name};
use crate::raft_type_config::{AgentBrokerRaftNodeId, AgentBrokerRaftTypeConfig};

const MAX_RAFT_FRAME_BYTES: usize = 256 * 1024 * 1024;
const MAX_RAFT_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
const SNAPSHOT_IO_CHUNK_BYTES: usize = 64 * 1024;
const RPC_IO_TIMEOUT: Duration = Duration::from_secs(30);
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const RPC_CONNECTION_WORKERS: usize = 8;
const RPC_CONNECTION_QUEUE_CAPACITY: usize = 64;
const TLS_HANDSHAKE_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Debug, Clone)]
pub(crate) struct TcpRaftNetworkFactory {
    source_node_id: AgentBrokerRaftNodeId,
    connect_timeout: Duration,
    tls: Arc<LoadedClusterRaftTls>,
}

impl TcpRaftNetworkFactory {
    pub(crate) fn new(
        source_node_id: AgentBrokerRaftNodeId,
        connect_timeout: Duration,
        tls: Arc<LoadedClusterRaftTls>,
    ) -> Self {
        Self {
            source_node_id,
            connect_timeout,
            tls,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TcpRaftNetwork {
    target: AgentBrokerRaftNodeId,
    node: BasicNode,
    source_node_id: AgentBrokerRaftNodeId,
    connect_timeout: Duration,
    tls: Arc<LoadedClusterRaftTls>,
}

impl RaftNetworkFactory<AgentBrokerRaftTypeConfig> for TcpRaftNetworkFactory {
    type Network = TcpRaftNetwork;

    async fn new_client(
        &mut self,
        target: AgentBrokerRaftNodeId,
        node: &BasicNode,
    ) -> Self::Network {
        TcpRaftNetwork {
            target,
            node: node.clone(),
            source_node_id: self.source_node_id,
            connect_timeout: self.connect_timeout,
            tls: Arc::clone(&self.tls),
        }
    }
}

impl RaftNetwork<AgentBrokerRaftTypeConfig> for TcpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<AgentBrokerRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<AgentBrokerRaftNodeId>,
        RPCError<AgentBrokerRaftNodeId, BasicNode, RaftError<AgentBrokerRaftNodeId>>,
    > {
        let response = self
            .request(ClusterRpcRequest::AppendEntries(rpc), &option)
            .await
            .map_err(|error| unreachable_rpc_error(&error))?;
        match response {
            ClusterRpcResponse::AppendEntries(Ok(response)) => Ok(response),
            ClusterRpcResponse::AppendEntries(Err(error)) => Err(RemoteError::new_with_node(
                self.target,
                self.node.clone(),
                RaftError::Fatal(error),
            )
            .into()),
            other => Err(protocol_rpc_error("append_entries", &other)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<AgentBrokerRaftNodeId>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<AgentBrokerRaftNodeId>,
        RPCError<AgentBrokerRaftNodeId, BasicNode, RaftError<AgentBrokerRaftNodeId>>,
    > {
        let response = self
            .request(ClusterRpcRequest::Vote(rpc), &option)
            .await
            .map_err(|error| unreachable_rpc_error(&error))?;
        match response {
            ClusterRpcResponse::Vote(Ok(response)) => Ok(response),
            ClusterRpcResponse::Vote(Err(error)) => Err(RemoteError::new_with_node(
                self.target,
                self.node.clone(),
                RaftError::Fatal(error),
            )
            .into()),
            other => Err(protocol_rpc_error("vote", &other)),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<AgentBrokerRaftNodeId>,
        snapshot: Snapshot<AgentBrokerRaftTypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<
        SnapshotResponse<AgentBrokerRaftNodeId>,
        StreamingError<AgentBrokerRaftTypeConfig, Fatal<AgentBrokerRaftNodeId>>,
    > {
        let data = (*snapshot.snapshot).into_inner();
        let response = self
            .snapshot_request(vote, snapshot.meta, data)
            .await
            .map_err(|error| StreamingError::Unreachable(Unreachable::new(&error)))?;
        match response {
            ClusterRpcResponse::FullSnapshot(Ok(response)) => Ok(response),
            ClusterRpcResponse::FullSnapshot(Err(error)) => Err(StreamingError::RemoteError(
                RemoteError::new_with_node(self.target, self.node.clone(), error),
            )),
            other => {
                let error = io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected Raft snapshot RPC response: {other:?}"),
                );
                Err(StreamingError::Unreachable(Unreachable::new(&error)))
            }
        }
    }
}

impl TcpRaftNetwork {
    async fn request(
        &self,
        request: ClusterRpcRequest,
        option: &RPCOption,
    ) -> io::Result<ClusterRpcResponse> {
        let address = self.node.addr.clone();
        let source_node_id = self.source_node_id;
        let target = self.target;
        let tls = Arc::clone(&self.tls);
        let io_timeout = bounded_rpc_io_timeout(option);
        let connect_timeout = self.connect_timeout.min(io_timeout);
        tokio::task::spawn_blocking(move || {
            blocking_request(
                &address,
                source_node_id,
                target,
                connect_timeout,
                io_timeout,
                tls.as_ref(),
                &request,
            )
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn snapshot_request(
        &self,
        vote: Vote<AgentBrokerRaftNodeId>,
        meta: SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>,
        data: Vec<u8>,
    ) -> io::Result<ClusterRpcResponse> {
        let address = self.node.addr.clone();
        let source_node_id = self.source_node_id;
        let target = self.target;
        let tls = Arc::clone(&self.tls);
        let connect_timeout = self.connect_timeout;
        let snapshot = SnapshotRequest { vote, meta, data };
        tokio::task::spawn_blocking(move || {
            blocking_snapshot_request(
                &address,
                source_node_id,
                target,
                connect_timeout,
                tls.as_ref(),
                snapshot,
            )
        })
        .await
        .map_err(io::Error::other)?
    }
}

#[derive(Debug)]
pub(crate) struct RaftRpcServerHandle {
    local_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    load: Arc<RaftRpcLoad>,
    accept_thread: Option<JoinHandle<io::Result<()>>>,
    worker_threads: Vec<JoinHandle<io::Result<()>>>,
}

#[derive(Debug, Default)]
struct RaftRpcLoad {
    queued_connections: AtomicUsize,
    active_connections: AtomicUsize,
}

impl RaftRpcServerHandle {
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn queued_connections(&self) -> usize {
        self.load.queued_connections.load(Ordering::Acquire)
    }

    pub(crate) fn active_connections(&self) -> usize {
        self.load.active_connections.load(Ordering::Acquire)
    }

    pub(crate) fn stop(mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::Release);
        let accept_result = match self.accept_thread.take() {
            Some(thread) => join_rpc_thread(thread, "Raft RPC accept thread panicked"),
            None => Ok(()),
        };
        let mut worker_result = Ok(());
        for thread in self.worker_threads.drain(..) {
            if let Err(error) = join_rpc_thread(thread, "Raft RPC worker thread panicked") {
                worker_result = Err(error);
            }
        }
        accept_result.and(worker_result)
    }
}

pub(crate) fn start_raft_rpc_server(
    raft: &Raft<AgentBrokerRaftTypeConfig>,
    bind_addr: SocketAddr,
    tls: &Arc<LoadedClusterRaftTls>,
) -> io::Result<RaftRpcServerHandle> {
    let listener = TcpListener::bind(bind_addr)?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let stop = Arc::new(AtomicBool::new(false));
    let load = Arc::new(RaftRpcLoad::default());
    let (connections, connection_receiver) =
        mpsc::sync_channel::<TcpStream>(RPC_CONNECTION_QUEUE_CAPACITY);
    let connection_receiver = Arc::new(Mutex::new(connection_receiver));
    let runtime = tokio::runtime::Handle::current();
    let mut worker_threads: Vec<JoinHandle<io::Result<()>>> =
        Vec::with_capacity(RPC_CONNECTION_WORKERS);
    for worker_index in 0..RPC_CONNECTION_WORKERS {
        let worker_raft = raft.clone();
        let worker_runtime = runtime.clone();
        let worker_receiver = Arc::clone(&connection_receiver);
        let worker_load = Arc::clone(&load);
        let worker_stop = Arc::clone(&stop);
        let worker_tls = Arc::clone(tls);
        let thread = match thread::Builder::new()
            .name(format!("agent-broker-raft-rpc-{worker_index}"))
            .spawn(move || {
                serve_rpc_connections(
                    &worker_receiver,
                    &worker_raft,
                    &worker_runtime,
                    worker_load.as_ref(),
                    worker_stop.as_ref(),
                    worker_tls.as_ref(),
                )
            }) {
            Ok(thread) => thread,
            Err(error) => {
                drop(connections);
                for thread in worker_threads {
                    let _join_result = thread.join();
                }
                return Err(error);
            }
        };
        worker_threads.push(thread);
    }
    let thread_stop = Arc::clone(&stop);
    let accept_load = Arc::clone(&load);
    let accept_connections = connections.clone();
    let accept_thread = match thread::Builder::new()
        .name("agent-broker-raft-rpc".to_owned())
        .spawn(move || {
            serve_rpc(
                &listener,
                &accept_connections,
                accept_load.as_ref(),
                thread_stop.as_ref(),
            )
        }) {
        Ok(thread) => thread,
        Err(error) => {
            drop(connections);
            for thread in worker_threads {
                let _join_result = thread.join();
            }
            return Err(error);
        }
    };
    drop(connections);
    Ok(RaftRpcServerHandle {
        local_addr,
        stop,
        load,
        accept_thread: Some(accept_thread),
        worker_threads,
    })
}

fn serve_rpc(
    listener: &TcpListener,
    connections: &SyncSender<TcpStream>,
    load: &RaftRpcLoad,
    stop: &AtomicBool,
) -> io::Result<()> {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                load.queued_connections.fetch_add(1, Ordering::AcqRel);
                match connections.try_send(stream) {
                    Ok(()) => {}
                    Err(TrySendError::Full(stream)) => {
                        load.queued_connections.fetch_sub(1, Ordering::AcqRel);
                        let _shutdown_result = stream.shutdown(std::net::Shutdown::Both);
                    }
                    Err(TrySendError::Disconnected(_stream)) => {
                        load.queued_connections.fetch_sub(1, Ordering::AcqRel);
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "Raft RPC connection workers stopped unexpectedly",
                        ));
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn serve_rpc_connections(
    receiver: &Mutex<Receiver<TcpStream>>,
    raft: &Raft<AgentBrokerRaftTypeConfig>,
    runtime: &tokio::runtime::Handle,
    load: &RaftRpcLoad,
    stop: &AtomicBool,
    tls: &LoadedClusterRaftTls,
) -> io::Result<()> {
    loop {
        let stream = receiver
            .lock()
            .map_err(|_| io::Error::other("Raft RPC connection queue mutex was poisoned"))?
            .recv();
        let Ok(stream) = stream else {
            return Ok(());
        };
        load.queued_connections.fetch_sub(1, Ordering::AcqRel);
        if stop.load(Ordering::Acquire) {
            let _shutdown_result = stream.shutdown(std::net::Shutdown::Both);
            continue;
        }
        load.active_connections.fetch_add(1, Ordering::AcqRel);
        if let Err(error) = handle_connection(stream, raft, runtime, tls) {
            eprintln!("agent-broker Raft RPC connection failed: {error}");
        }
        load.active_connections.fetch_sub(1, Ordering::AcqRel);
    }
}

fn join_rpc_thread(
    thread: JoinHandle<io::Result<()>>,
    panic_message: &'static str,
) -> io::Result<()> {
    match thread.join() {
        Ok(result) => result,
        Err(_) => Err(io::Error::other(panic_message)),
    }
}

fn handle_connection(
    stream: TcpStream,
    raft: &Raft<AgentBrokerRaftTypeConfig>,
    runtime: &tokio::runtime::Handle,
    tls: &LoadedClusterRaftTls,
) -> io::Result<()> {
    // The listener is nonblocking so shutdown can poll the stop flag. On some Unix platforms an
    // accepted socket can inherit nonblocking mode, which would turn a bounded blocking read into
    // immediate EAGAIN and defeat both timeout semantics and slow-peer backpressure accounting.
    let mut stream = accept_tls_peer(stream, tls)?;
    let frame = read_frame(&mut stream)?;
    let envelope: ClusterRpcEnvelope = serde_json::from_slice(&frame).map_err(invalid_data)?;
    verify_claimed_peer_certificate(
        tls,
        envelope.source_node_id,
        stream.conn.peer_certificates(),
    )?;
    let response = match envelope.request {
        ClusterRpcRequest::AppendEntries(request) => ClusterRpcResponse::AppendEntries(
            runtime
                .block_on(raft.append_entries(request))
                .map_err(raft_error_to_fatal),
        ),
        ClusterRpcRequest::Vote(request) => ClusterRpcResponse::Vote(
            runtime
                .block_on(raft.vote(request))
                .map_err(raft_error_to_fatal),
        ),
        ClusterRpcRequest::FullSnapshot {
            vote,
            meta,
            data_len,
        } => {
            let data = read_snapshot_body(&mut stream, data_len)?;
            let snapshot = Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            };
            ClusterRpcResponse::FullSnapshot(
                runtime.block_on(raft.install_full_snapshot(vote, snapshot)),
            )
        }
    };
    let encoded = serde_json::to_vec(&response).map_err(invalid_data)?;
    write_frame(&mut stream, &encoded)
}

fn blocking_request(
    address: &str,
    source_node_id: AgentBrokerRaftNodeId,
    target_node_id: AgentBrokerRaftNodeId,
    connect_timeout: Duration,
    io_timeout: Duration,
    tls: &LoadedClusterRaftTls,
    request: &ClusterRpcRequest,
) -> io::Result<ClusterRpcResponse> {
    let mut stream = connect_tls_peer(address, target_node_id, connect_timeout, io_timeout, tls)?;
    let envelope = ClusterRpcEnvelope {
        source_node_id,
        request,
    };
    let encoded = serde_json::to_vec(&envelope).map_err(invalid_data)?;
    write_frame(&mut stream, &encoded)?;
    let response = read_frame(&mut stream)?;
    serde_json::from_slice(&response).map_err(invalid_data)
}

fn bounded_rpc_io_timeout(option: &RPCOption) -> Duration {
    let hard_ttl = option.hard_ttl();
    if hard_ttl.is_zero() {
        return Duration::from_millis(1);
    }
    hard_ttl.min(RPC_IO_TIMEOUT)
}

fn blocking_snapshot_request(
    address: &str,
    source_node_id: AgentBrokerRaftNodeId,
    target_node_id: AgentBrokerRaftNodeId,
    connect_timeout: Duration,
    tls: &LoadedClusterRaftTls,
    snapshot: SnapshotRequest,
) -> io::Result<ClusterRpcResponse> {
    let mut stream = connect_tls_peer(
        address,
        target_node_id,
        connect_timeout,
        RPC_IO_TIMEOUT,
        tls,
    )?;
    write_snapshot_request(
        &mut stream,
        source_node_id,
        snapshot.vote,
        snapshot.meta,
        &snapshot.data,
    )?;
    let response = read_frame(&mut stream)?;
    serde_json::from_slice(&response).map_err(invalid_data)
}

struct SnapshotRequest {
    vote: Vote<AgentBrokerRaftNodeId>,
    meta: SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>,
    data: Vec<u8>,
}

fn connect_tls_peer(
    address: &str,
    target_node_id: AgentBrokerRaftNodeId,
    connect_timeout: Duration,
    io_timeout: Duration,
    tls: &LoadedClusterRaftTls,
) -> io::Result<StreamOwned<ClientConnection, TcpStream>> {
    let mut socket = connect_peer(address, connect_timeout)?;
    socket.set_nodelay(true)?;
    let server_name = peer_server_name(target_node_id).map_err(|_error| {
        io::Error::new(io::ErrorKind::InvalidInput, "Raft TLS peer name invalid")
    })?;
    let mut connection = ClientConnection::new(tls.client_config(), server_name)
        .map_err(|_error| io::Error::other("Raft TLS client configuration failed"))?;
    complete_client_tls_handshake(&mut connection, &mut socket, tls.handshake_timeout())?;
    verify_pinned_peer_certificate(tls, target_node_id, connection.peer_certificates())?;
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(io_timeout))?;
    socket.set_write_timeout(Some(io_timeout))?;
    Ok(StreamOwned::new(connection, socket))
}

fn accept_tls_peer(
    mut socket: TcpStream,
    tls: &LoadedClusterRaftTls,
) -> io::Result<StreamOwned<ServerConnection, TcpStream>> {
    socket.set_nodelay(true)?;
    let mut connection = ServerConnection::new(tls.server_config())
        .map_err(|_error| io::Error::other("Raft TLS server configuration failed"))?;
    complete_server_tls_handshake(&mut connection, &mut socket, tls.handshake_timeout())?;
    socket.set_nonblocking(false)?;
    socket.set_read_timeout(Some(RPC_IO_TIMEOUT))?;
    socket.set_write_timeout(Some(RPC_IO_TIMEOUT))?;
    Ok(StreamOwned::new(connection, socket))
}

fn complete_client_tls_handshake(
    connection: &mut ClientConnection,
    socket: &mut TcpStream,
    timeout: Duration,
) -> io::Result<()> {
    socket.set_nonblocking(true)?;
    let deadline = Instant::now() + timeout;
    while connection.is_handshaking() {
        match connection.complete_io(socket) {
            Ok(_progress) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(_error) => return Err(io::Error::other("Raft TLS client handshake failed")),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Raft TLS client handshake timed out",
            ));
        }
        if connection.is_handshaking() {
            thread::sleep(TLS_HANDSHAKE_POLL_INTERVAL);
        }
    }
    Ok(())
}

fn complete_server_tls_handshake(
    connection: &mut ServerConnection,
    socket: &mut TcpStream,
    timeout: Duration,
) -> io::Result<()> {
    socket.set_nonblocking(true)?;
    let deadline = Instant::now() + timeout;
    while connection.is_handshaking() {
        match connection.complete_io(socket) {
            Ok(_progress) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(_error) => return Err(io::Error::other("Raft TLS server handshake failed")),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Raft TLS server handshake timed out",
            ));
        }
        if connection.is_handshaking() {
            thread::sleep(TLS_HANDSHAKE_POLL_INTERVAL);
        }
    }
    Ok(())
}

fn verify_pinned_peer_certificate(
    tls: &LoadedClusterRaftTls,
    node_id: AgentBrokerRaftNodeId,
    peer_certificates: Option<&[rustls::pki_types::CertificateDer<'_>]>,
) -> io::Result<()> {
    if tls.pinned_peer_certificate(node_id).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Raft TLS peer identity is unknown",
        ));
    }
    let actual = peer_certificates
        .and_then(|certificates| certificates.first())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Raft TLS peer certificate is missing",
            )
        })?;
    if !tls.matches_pinned_peer_certificate(node_id, actual) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Raft TLS peer certificate does not match configured node identity",
        ));
    }
    Ok(())
}

fn verify_claimed_peer_certificate(
    tls: &LoadedClusterRaftTls,
    claimed_node_id: AgentBrokerRaftNodeId,
    peer_certificates: Option<&[rustls::pki_types::CertificateDer<'_>]>,
) -> io::Result<()> {
    verify_pinned_peer_certificate(tls, claimed_node_id, peer_certificates)
}

fn connect_peer(address: &str, timeout: Duration) -> io::Result<TcpStream> {
    let socket_addr = address.parse::<SocketAddr>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Raft peer address must be a pre-resolved IP socket address",
        )
    })?;
    TcpStream::connect_timeout(&socket_addr, timeout)
}

fn write_snapshot_request(
    stream: &mut impl Write,
    source_node_id: AgentBrokerRaftNodeId,
    vote: Vote<AgentBrokerRaftNodeId>,
    meta: SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>,
    data: &[u8],
) -> io::Result<()> {
    if data.len() > MAX_RAFT_SNAPSHOT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft snapshot exceeds the configured byte limit",
        ));
    }
    let data_len = u64::try_from(data.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft snapshot length cannot be represented as u64",
        )
    })?;
    let header = ClusterRpcEnvelope {
        source_node_id,
        request: ClusterRpcRequest::FullSnapshot {
            vote,
            meta,
            data_len,
        },
    };
    let encoded = serde_json::to_vec(&header).map_err(invalid_data)?;
    write_frame(stream, &encoded)?;
    write_snapshot_body(stream, data)
}

fn raft_error_to_fatal(error: RaftError<AgentBrokerRaftNodeId>) -> Fatal<AgentBrokerRaftNodeId> {
    match error {
        RaftError::Fatal(error) => error,
        RaftError::APIError(never) => match never {},
    }
}

fn write_frame(stream: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_RAFT_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft RPC frame exceeds the configured byte limit",
        ));
    }
    let length = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft RPC frame length cannot be represented as u32",
        )
    })?;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}

fn read_frame(stream: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut length_bytes = [0_u8; 4];
    stream.read_exact(&mut length_bytes)?;
    let length = usize::try_from(u32::from_be_bytes(length_bytes))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid Raft RPC frame length"))?;
    if length > MAX_RAFT_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft RPC frame exceeds the configured byte limit",
        ));
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn write_snapshot_body(stream: &mut impl Write, data: &[u8]) -> io::Result<()> {
    if data.len() > MAX_RAFT_SNAPSHOT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft snapshot exceeds the configured byte limit",
        ));
    }
    for chunk in data.chunks(SNAPSHOT_IO_CHUNK_BYTES) {
        stream.write_all(chunk)?;
    }
    stream.flush()
}

fn read_snapshot_body(stream: &mut impl Read, data_len: u64) -> io::Result<Vec<u8>> {
    let data_len = usize::try_from(data_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft snapshot length cannot be represented as usize",
        )
    })?;
    if data_len > MAX_RAFT_SNAPSHOT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Raft snapshot exceeds the configured byte limit",
        ));
    }
    let mut data = vec![0_u8; data_len];
    read_snapshot_body_into(stream, &mut data)?;
    Ok(data)
}

fn read_snapshot_body_into(stream: &mut impl Read, data: &mut [u8]) -> io::Result<()> {
    for chunk in data.chunks_mut(SNAPSHOT_IO_CHUNK_BYTES) {
        stream.read_exact(chunk)?;
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "rpc", content = "payload", rename_all = "snake_case")]
enum ClusterRpcRequest {
    AppendEntries(AppendEntriesRequest<AgentBrokerRaftTypeConfig>),
    Vote(VoteRequest<AgentBrokerRaftNodeId>),
    FullSnapshot {
        vote: Vote<AgentBrokerRaftNodeId>,
        meta: SnapshotMeta<AgentBrokerRaftNodeId, BasicNode>,
        data_len: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterRpcEnvelope<T = ClusterRpcRequest> {
    source_node_id: AgentBrokerRaftNodeId,
    request: T,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "rpc", content = "payload", rename_all = "snake_case")]
enum ClusterRpcResponse {
    AppendEntries(
        Result<AppendEntriesResponse<AgentBrokerRaftNodeId>, Fatal<AgentBrokerRaftNodeId>>,
    ),
    Vote(Result<VoteResponse<AgentBrokerRaftNodeId>, Fatal<AgentBrokerRaftNodeId>>),
    FullSnapshot(Result<SnapshotResponse<AgentBrokerRaftNodeId>, Fatal<AgentBrokerRaftNodeId>>),
}

fn unreachable_rpc_error(
    error: &io::Error,
) -> RPCError<AgentBrokerRaftNodeId, BasicNode, RaftError<AgentBrokerRaftNodeId>> {
    Unreachable::new(error).into()
}

fn protocol_rpc_error(
    operation: &'static str,
    response: &ClusterRpcResponse,
) -> RPCError<AgentBrokerRaftNodeId, BasicNode, RaftError<AgentBrokerRaftNodeId>> {
    let error = io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected Raft {operation} RPC response: {response:?}"),
    );
    Unreachable::new(&error).into()
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    #[cfg(target_os = "macos")]
    use std::process::Command;

    use openraft::network::RPCOption;
    use openraft::{BasicNode, SnapshotMeta, Vote};

    use super::{
        ClusterRpcEnvelope, ClusterRpcRequest, read_frame, read_snapshot_body_into,
        write_snapshot_request,
    };

    const LARGE_SNAPSHOT_BYTES: usize = 32 * 1024 * 1024;
    const MAX_TRANSFER_RSS_DELTA_BYTES: u64 = 64 * 1024 * 1024;

    #[test]
    fn raft_peer_connect_rejects_unresolved_hostname_before_os_resolution()
    -> Result<(), Box<dyn std::error::Error>> {
        let Err(error) = super::connect_peer("raft-node-1:18811", Duration::from_millis(10)) else {
            return Err("hostname unexpectedly reached the connect path".into());
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    }

    #[test]
    fn ordinary_raft_io_timeout_is_capped_by_openraft_hard_ttl() {
        let short = RPCOption::new(Duration::from_millis(75));
        assert_eq!(
            super::bounded_rpc_io_timeout(&short),
            Duration::from_millis(75)
        );

        let long = RPCOption::new(super::RPC_IO_TIMEOUT + Duration::from_secs(1));
        assert_eq!(super::bounded_rpc_io_timeout(&long), super::RPC_IO_TIMEOUT);

        let zero = RPCOption::new(Duration::ZERO);
        assert_eq!(
            super::bounded_rpc_io_timeout(&zero),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn snapshot_header_is_small_and_does_not_embed_binary_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let header = ClusterRpcRequest::FullSnapshot {
            vote: Vote::new(1, 1),
            meta: SnapshotMeta::<u64, BasicNode>::default(),
            data_len: u64::try_from(LARGE_SNAPSHOT_BYTES)?,
        };
        let encoded = serde_json::to_vec(&header)?;
        assert!(
            encoded.len() < 4_096,
            "snapshot control header unexpectedly grew: {} bytes",
            encoded.len()
        );
        let encoded_text = std::str::from_utf8(&encoded)?;
        assert!(encoded_text.contains("\"data_len\""));
        assert!(!encoded_text.contains("\"data\":"));
        Ok(())
    }

    #[test]
    #[ignore = "release perf gate: allocates and transfers a 32 MiB synthetic snapshot"]
    fn snapshot_binary_transfer_peak_rss_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let data = vec![0xA5_u8; LARGE_SNAPSHOT_BYTES];
        let baseline_rss = current_rss_bytes()?;
        let (resident_sender, resident_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);

        let receiver = thread::spawn(move || -> io::Result<()> {
            let (mut stream, _peer) = listener.accept()?;
            stream.set_read_timeout(Some(super::RPC_IO_TIMEOUT))?;
            let header = read_frame(&mut stream)?;
            let envelope: ClusterRpcEnvelope =
                serde_json::from_slice(&header).map_err(super::invalid_data)?;
            let ClusterRpcRequest::FullSnapshot { data_len, .. } = envelope.request else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected snapshot header",
                ));
            };
            let data_len = usize::try_from(data_len).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "snapshot length overflow")
            })?;
            if data_len != LARGE_SNAPSHOT_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected snapshot length",
                ));
            }
            let mut resident = vec![0x5A_u8; data_len];
            let first_chunk_len = super::SNAPSHOT_IO_CHUNK_BYTES.min(resident.len());
            stream.read_exact(&mut resident[..first_chunk_len])?;
            resident_sender
                .send(())
                .map_err(|_| io::Error::other("snapshot RSS barrier disconnected"))?;
            release_receiver
                .recv()
                .map_err(|_| io::Error::other("snapshot RSS release disconnected"))?;
            read_snapshot_body_into(&mut stream, &mut resident[first_chunk_len..])?;
            std::hint::black_box(&resident);
            Ok(())
        });

        let sender = thread::spawn(move || -> io::Result<()> {
            let mut stream = TcpStream::connect(address)?;
            stream.set_nodelay(true)?;
            stream.set_write_timeout(Some(super::RPC_IO_TIMEOUT))?;
            write_snapshot_request(
                &mut stream,
                1,
                Vote::new(1, 1),
                SnapshotMeta::<u64, BasicNode>::default(),
                &data,
            )
        });

        resident_receiver
            .recv_timeout(super::RPC_IO_TIMEOUT)
            .map_err(|_| io::Error::other("snapshot RSS barrier timed out"))?;
        let sender_finished_before_measurement = sender.is_finished();
        let peak_rss = current_rss_bytes()?;
        let rss_delta = peak_rss.saturating_sub(baseline_rss);
        println!(
            "snapshot_transport_peak_rss baseline_bytes={baseline_rss} peak_bytes={peak_rss} delta_bytes={rss_delta} snapshot_bytes={LARGE_SNAPSHOT_BYTES} budget_bytes={MAX_TRANSFER_RSS_DELTA_BYTES}"
        );
        if rss_delta > MAX_TRANSFER_RSS_DELTA_BYTES {
            return Err(format!(
                "binary snapshot transfer RSS delta {rss_delta} exceeds budget {MAX_TRANSFER_RSS_DELTA_BYTES}"
            )
            .into());
        }
        release_sender.send(())?;
        join_io_thread(sender, "snapshot sender thread panicked")?;
        join_io_thread(receiver, "snapshot receiver thread panicked")?;
        if sender_finished_before_measurement {
            return Err(
                "snapshot sender completed before the RSS barrier; source residency was not proven"
                    .into(),
            );
        }
        Ok(())
    }

    fn join_io_thread(
        handle: thread::JoinHandle<io::Result<()>>,
        panic_message: &'static str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match handle.join() {
            Ok(result) => Ok(result?),
            Err(_) => Err(panic_message.into()),
        }
    }

    #[cfg(target_os = "linux")]
    fn current_rss_bytes() -> io::Result<u64> {
        let status = std::fs::read_to_string("/proc/self/status")?;
        let line = status
            .lines()
            .find(|line| line.starts_with("VmRSS:"))
            .ok_or_else(|| io::Error::other("VmRSS missing from /proc/self/status"))?;
        let kib = line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| io::Error::other("VmRSS value missing"))?
            .parse::<u64>()
            .map_err(io::Error::other)?;
        kib.checked_mul(1_024)
            .ok_or_else(|| io::Error::other("VmRSS byte conversion overflow"))
    }

    #[cfg(target_os = "macos")]
    fn current_rss_bytes() -> io::Result<u64> {
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()?;
        if !output.status.success() {
            return Err(io::Error::other("ps failed while reading current RSS"));
        }
        let kib = std::str::from_utf8(&output.stdout)
            .map_err(io::Error::other)?
            .trim()
            .parse::<u64>()
            .map_err(io::Error::other)?;
        kib.checked_mul(1_024)
            .ok_or_else(|| io::Error::other("RSS byte conversion overflow"))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn current_rss_bytes() -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "snapshot RSS gate supports Linux and macOS",
        ))
    }
}
