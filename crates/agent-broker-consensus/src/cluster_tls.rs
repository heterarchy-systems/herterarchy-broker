use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use agent_broker_application::{BrokerError, BrokerErrorCode};
use rustls::RootCertStore;
use rustls::client::ClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ServerConfig, WebPkiClientVerifier};

use crate::raft_type_config::AgentBrokerRaftNodeId;

const DEFAULT_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const CA_CERTIFICATE_FILE: &str = "ca.pem";

/// File-backed mandatory mTLS configuration for a static Agent Broker Raft cluster.
///
/// The directory convention is deliberately deterministic:
///
/// - `ca.pem`
/// - `node-{id}.pem`
/// - `node-{id}-key.pem`
///
/// Certificate/key contents are loaded only during cluster startup and are never included in
/// `Debug` output or runtime logs by this type.
#[derive(Clone, Eq, PartialEq)]
pub struct ClusterRaftTlsConfig {
    directory: PathBuf,
    handshake_timeout: Duration,
}

impl std::fmt::Debug for ClusterRaftTlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterRaftTlsConfig")
            .field("directory", &self.directory)
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl ClusterRaftTlsConfig {
    /// Create mandatory cluster TLS configuration from a deterministic certificate directory.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the directory path is empty.
    pub fn new(directory: impl Into<PathBuf>) -> Result<Self, BrokerError> {
        let config = Self {
            directory: directory.into(),
            handshake_timeout: DEFAULT_TLS_HANDSHAKE_TIMEOUT,
        };
        config.validate()?;
        Ok(config)
    }

    /// Override the bounded TLS handshake deadline.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] when the timeout is zero or exceeds 30 seconds.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Result<Self, BrokerError> {
        self.handshake_timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub const fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }

    pub(crate) fn validate(&self) -> Result<(), BrokerError> {
        if self.directory.as_os_str().is_empty() {
            return Err(invalid_tls_configuration(
                "cluster Raft TLS directory must not be empty",
            ));
        }
        if self.handshake_timeout.is_zero() || self.handshake_timeout > MAX_TLS_HANDSHAKE_TIMEOUT {
            return Err(invalid_tls_configuration(
                "cluster Raft TLS handshake timeout must be between 1ns and 30s",
            ));
        }
        Ok(())
    }

    pub(crate) fn load(
        &self,
        local_node_id: AgentBrokerRaftNodeId,
        node_ids: impl Iterator<Item = AgentBrokerRaftNodeId>,
    ) -> Result<LoadedClusterRaftTls, BrokerError> {
        self.validate()?;
        let ca_chain = read_certificates(&self.directory.join(CA_CERTIFICATE_FILE))?;
        if ca_chain.is_empty() {
            return Err(invalid_tls_configuration(
                "cluster Raft TLS CA bundle must contain at least one certificate",
            ));
        }

        let mut roots = RootCertStore::empty();
        for certificate in &ca_chain {
            roots.add(certificate.clone()).map_err(|_error| {
                invalid_tls_configuration("cluster Raft TLS CA certificate is invalid")
            })?;
        }

        let local_chain =
            read_certificates(&node_certificate_path(&self.directory, local_node_id))?;
        if local_chain.is_empty() {
            return Err(invalid_tls_configuration(
                "cluster Raft TLS local certificate chain is empty",
            ));
        }
        let local_key = read_private_key(&node_key_path(&self.directory, local_node_id))?;

        let mut pinned_peer_certificates = BTreeMap::new();
        for node_id in node_ids {
            let peer_chain = read_certificates(&node_certificate_path(&self.directory, node_id))?;
            let peer_leaf = peer_chain.into_iter().next().ok_or_else(|| {
                invalid_tls_configuration("cluster Raft TLS peer certificate chain is empty")
            })?;
            if pinned_peer_certificates
                .insert(node_id, peer_leaf)
                .is_some()
            {
                return Err(invalid_tls_configuration(
                    "cluster Raft TLS peer certificate map contains a duplicate node id",
                ));
            }
        }

        if !pinned_peer_certificates.contains_key(&local_node_id) {
            return Err(invalid_tls_configuration(
                "cluster Raft TLS peer certificate map does not contain local node id",
            ));
        }

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let client_config = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_error| {
                invalid_tls_configuration("cluster Raft TLS client protocol configuration failed")
            })?
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(local_chain.clone(), local_key.clone_key())
            .map_err(|_error| {
                invalid_tls_configuration("cluster Raft TLS client identity is invalid")
            })?;

        let client_verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
                .build()
                .map_err(|_error| {
                    invalid_tls_configuration(
                        "cluster Raft TLS client verifier configuration failed",
                    )
                })?;
        let server_config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|_error| {
                invalid_tls_configuration("cluster Raft TLS server protocol configuration failed")
            })?
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(local_chain, local_key)
            .map_err(|_error| {
                invalid_tls_configuration("cluster Raft TLS server identity is invalid")
            })?;

        Ok(LoadedClusterRaftTls {
            local_node_id,
            client_config: Arc::new(client_config),
            server_config: Arc::new(server_config),
            pinned_peer_certificates,
            handshake_timeout: self.handshake_timeout,
        })
    }
}

/// Loaded TLS material. Deliberately omits certificate/key bytes from `Debug`.
#[derive(Clone)]
pub(crate) struct LoadedClusterRaftTls {
    local_node_id: AgentBrokerRaftNodeId,
    client_config: Arc<ClientConfig>,
    server_config: Arc<ServerConfig>,
    pinned_peer_certificates: BTreeMap<AgentBrokerRaftNodeId, CertificateDer<'static>>,
    handshake_timeout: Duration,
}

impl std::fmt::Debug for LoadedClusterRaftTls {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LoadedClusterRaftTls")
            .field("local_node_id", &self.local_node_id)
            .field("peer_count", &self.pinned_peer_certificates.len())
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl LoadedClusterRaftTls {
    #[must_use]
    pub(crate) fn client_config(&self) -> Arc<ClientConfig> {
        Arc::clone(&self.client_config)
    }

    #[must_use]
    pub(crate) fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server_config)
    }

    #[must_use]
    pub(crate) const fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }

    #[must_use]
    pub(crate) fn pinned_peer_certificate(
        &self,
        node_id: AgentBrokerRaftNodeId,
    ) -> Option<&CertificateDer<'static>> {
        self.pinned_peer_certificates.get(&node_id)
    }

    #[must_use]
    pub(crate) fn matches_pinned_peer_certificate(
        &self,
        node_id: AgentBrokerRaftNodeId,
        certificate: &CertificateDer<'_>,
    ) -> bool {
        self.pinned_peer_certificate(node_id)
            .is_some_and(|expected| expected.as_ref() == certificate.as_ref())
    }
}

pub(crate) fn peer_server_name(
    node_id: AgentBrokerRaftNodeId,
) -> Result<ServerName<'static>, BrokerError> {
    ServerName::try_from(format!("node-{node_id}.agent-broker.internal"))
        .map_err(|_error| invalid_tls_configuration("cluster Raft TLS peer server name is invalid"))
}

fn node_certificate_path(directory: &Path, node_id: AgentBrokerRaftNodeId) -> PathBuf {
    directory.join(format!("node-{node_id}.pem"))
}

fn node_key_path(directory: &Path, node_id: AgentBrokerRaftNodeId) -> PathBuf {
    directory.join(format!("node-{node_id}-key.pem"))
}

fn read_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, BrokerError> {
    let file = File::open(path).map_err(|_error| {
        invalid_tls_configuration("cluster Raft TLS certificate file is unavailable")
    })?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_error| invalid_tls_configuration("cluster Raft TLS certificate PEM is invalid"))
}

fn read_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, BrokerError> {
    let file = File::open(path).map_err(|_error| {
        invalid_tls_configuration("cluster Raft TLS identity key file is unavailable")
    })?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|_error| {
            invalid_tls_configuration("cluster Raft TLS identity key PEM is invalid")
        })?
        .ok_or_else(|| {
            invalid_tls_configuration("cluster Raft TLS identity key file contains no key")
        })
}

fn invalid_tls_configuration(message: &'static str) -> BrokerError {
    BrokerError::new(BrokerErrorCode::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        KeyUsagePurpose,
    };
    use tempfile::tempdir;

    use super::{ClusterRaftTlsConfig, read_certificates};

    fn write_tls_fixture(directory: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_key = KeyPair::generate()?;
        let ca_certificate = ca_params.self_signed(&ca_key)?;
        let issuer = Issuer::new(ca_params, ca_key);
        fs::write(directory.join("ca.pem"), ca_certificate.pem())?;
        for node_id in [1_u64, 2, 3] {
            let name = format!("node-{node_id}.agent-broker.internal");
            let mut params = CertificateParams::new(vec![name.clone()])?;
            params
                .distinguished_name
                .push(rcgen::DnType::CommonName, name);
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let key = KeyPair::generate()?;
            let certificate = params.signed_by(&key, &issuer)?;
            fs::write(
                directory.join(format!("node-{node_id}.pem")),
                certificate.pem(),
            )?;
            fs::write(
                directory.join(format!("node-{node_id}-key.pem")),
                key.serialize_pem(),
            )?;
        }
        Ok(())
    }

    #[test]
    fn tls_handshake_timeout_is_explicitly_bounded() -> Result<(), Box<dyn std::error::Error>> {
        let config = ClusterRaftTlsConfig::new("unused-tls")?;
        assert!(
            config
                .clone()
                .with_handshake_timeout(Duration::ZERO)
                .is_err()
        );
        assert!(
            config
                .with_handshake_timeout(Duration::from_secs(31))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn peer_certificate_pin_is_bound_to_exact_node_id() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        write_tls_fixture(directory.path())?;
        let loaded = ClusterRaftTlsConfig::new(directory.path())?.load(1, [1, 2, 3].into_iter())?;
        let node_two = read_certificates(&directory.path().join("node-2.pem"))?
            .into_iter()
            .next()
            .ok_or("node 2 fixture certificate is missing")?;

        assert!(loaded.matches_pinned_peer_certificate(2, &node_two));
        assert!(!loaded.matches_pinned_peer_certificate(3, &node_two));
        assert!(!loaded.matches_pinned_peer_certificate(99, &node_two));
        Ok(())
    }
}
