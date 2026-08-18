use std::error::Error;
use std::fs;
use std::path::Path;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

pub fn write_cluster_tls_fixture(directory: &Path, node_ids: &[u64]) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(directory)?;

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

    for &node_id in node_ids {
        let name = format!("node-{node_id}.agent-broker.internal");
        let mut params = CertificateParams::new(vec![name.clone()])?;
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.use_authority_key_identifier_extension = true;
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
