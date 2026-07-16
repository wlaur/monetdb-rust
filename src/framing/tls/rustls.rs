// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0.  If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 MonetDB Foundation

use std::{fmt, io, sync::Arc};

use rustls::{
    CertificateError, ClientConfig, ClientConnection, DigitallySignedStruct, Error, RootCertStore,
    SignatureScheme, StreamOwned,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
};
use rustls_platform_verifier::BuilderVerifierExt;
use sha2::{Digest, Sha256};

use crate::{
    framing::{
        ServerSock, ServerSockTrait,
        connecting::{ConnectError, ConnectResult},
    },
    parms::{TlsVerify, Validated},
};

pub fn wrap_with_rustls(parms: &Validated, sock: ServerSock) -> ConnectResult<ServerSock> {
    wrap_inner(parms, sock).map_err(|e| ConnectError::TlsError(e.to_string()))
}

fn wrap_inner(
    parms: &Validated,
    sock: ServerSock,
) -> Result<ServerSock, Box<dyn std::error::Error>> {
    let control = sock.control();
    let builder = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13]);
    let builder = match parms.connect_tls_verify {
        TlsVerify::System => builder.with_platform_verifier()?,
        TlsVerify::Cert => builder.with_root_certificates(load_roots(&parms.cert)?),
        TlsVerify::Hash => {
            let algorithms = builder.crypto_provider().signature_verification_algorithms;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(HashVerifier {
                    required_prefix: parms.connect_certhash_digits.clone(),
                    algorithms,
                }))
        }
        TlsVerify::Off => return Err(io::Error::other("TLS verification mode is off").into()),
    };

    let mut config = if parms.connect_clientkey.is_empty() {
        builder.with_no_client_auth()
    } else {
        let certificates = load_certificates(&parms.connect_clientcert)?;
        let key = load_private_key(&parms.connect_clientkey)?;
        builder.with_client_auth_cert(certificates, key)?
    };
    // MonetDB's C MAPI client requires TLS 1.3 and advertises only mapi/9.
    // See clients/mapilib/connect_openssl.c in the MonetDB source tree.
    config.alpn_protocols = vec![b"mapi/9".to_vec()];
    let config = Arc::new(config);

    let server_name = parms.connect_tcp.to_string();
    let server_name = ServerName::try_from(server_name)?;

    let client = ClientConnection::new(config, server_name)?;

    let stream = StreamOwned::new(client, sock);
    let wrapped = StreamWrapper(stream);

    Ok(ServerSock::wrap(wrapped, control))
}

fn load_certificates(
    path: &str,
) -> Result<Vec<CertificateDer<'static>>, Box<dyn std::error::Error>> {
    let certificates = CertificateDer::pem_file_iter(path)?.collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("certificate file {path:?} contains no certificates"),
        )
        .into());
    }
    Ok(certificates)
}

fn load_roots(path: &str) -> Result<RootCertStore, Box<dyn std::error::Error>> {
    let mut roots = RootCertStore::empty();
    for certificate in load_certificates(path)? {
        roots.add(certificate)?;
    }
    Ok(roots)
}

fn load_private_key(
    path: &str,
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    PrivateKeyDer::from_pem_file(path).map_err(Into::into)
}

struct HashVerifier {
    required_prefix: String,
    algorithms: WebPkiSupportedAlgorithms,
}

impl fmt::Debug for HashVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashVerifier")
            .field("required_prefix", &self.required_prefix)
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for HashVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let digest = hex::encode(Sha256::digest(end_entity.as_ref()));
        if digest.starts_with(&self.required_prefix) {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(CertificateError::ApplicationVerificationFailure.into())
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls12_signature(message, certificate, signature, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature(message, certificate, signature, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// We need to wrap the rustls::Stream so we can make it implement ServerSockTrait.
#[derive(Debug)]
struct StreamWrapper(pub StreamOwned<ClientConnection, ServerSock>);

impl io::Read for StreamWrapper {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl io::Write for StreamWrapper {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl ServerSockTrait for StreamWrapper {}
