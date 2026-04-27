use std::{sync::Arc, task::Poll};

use hyper::Uri;
use hyper_util::rt::TokioIo;
use snafu::ResultExt;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use super::{BuildTlsConfigSnafu, InvalidUriSnafu, Result};

/// How the TLS peer certificate is verified for a gRPC connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum VerificationMode {
    /// HTTP endpoint — no TLS at all.
    None,
    /// HTTPS endpoint with a configured CA certificate for peer verification.
    ConfiguredCa,
    /// HTTPS endpoint without a CA — encrypted but peer certificate is not verified.
    Insecure,
}

impl std::fmt::Display for VerificationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::ConfiguredCa => f.write_str("config"),
            Self::Insecure => f.write_str("insecure"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RpcTransportMeta {
    pub(super) endpoint: String,
    pub(super) transport: &'static str,
    pub(super) verify_mode: VerificationMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparedRpcEndpoint {
    pub(super) normalized_url: String,
    pub(super) use_tls: bool,
}

pub(super) fn prepare_rpc_endpoint(url: &str) -> PreparedRpcEndpoint {
    let normalized_url = normalize_rpc_endpoint(url);
    let use_tls = normalized_url.starts_with("https://");
    PreparedRpcEndpoint {
        normalized_url,
        use_tls,
    }
}

impl PreparedRpcEndpoint {
    pub(super) fn meta(&self, rpc_ca_cert_pem: Option<&[u8]>) -> RpcTransportMeta {
        let verify_mode = if !self.use_tls {
            VerificationMode::None
        } else if rpc_ca_cert_pem.is_some() {
            VerificationMode::ConfiguredCa
        } else {
            VerificationMode::Insecure
        };

        RpcTransportMeta {
            endpoint: self.normalized_url.clone(),
            transport: if self.use_tls { "https" } else { "http" },
            verify_mode,
        }
    }
}

/// Classifies a connect error into a log category string.
///
/// This is a heuristic based on keyword matching against the formatted tonic/hyper/rustls error
/// message. It is not driven by a stable error API and may need updating if upstream crates
/// change their error text.
pub(super) fn classify_connect_error(err: &tonic::transport::Error) -> &'static str {
    let message = err.to_string().to_lowercase();
    if message.contains("certificate")
        || message.contains("tls")
        || message.contains("unknown issuer")
        || message.contains("hostname")
    {
        "tls_verify_error"
    } else {
        "transport_error"
    }
}

/// Returns a hint string when the error pattern suggests an HTTP/HTTPS scheme mismatch.
///
/// This is a heuristic based on keyword matching against the formatted tonic/hyper/rustls error
/// message. It is not driven by a stable error API and may need updating if upstream crates
/// change their error text.
pub(super) fn possible_scheme_mismatch_hint(
    transport: &str,
    err: &tonic::transport::Error,
) -> Option<&'static str> {
    let message = err.to_string().to_lowercase();
    if transport == "http"
        && (message.contains("h2 protocol error")
            || message.contains("invalid size")
            || message.contains("wrong version number"))
    {
        Some("endpoint scheme may not match taosx grpc tls setting")
    } else {
        None
    }
}

/// An rpc endpoint that is ready to connect.
///
/// For HTTP and HTTPS-with-CA, wraps a standard tonic [`Endpoint`]. For
/// HTTPS-without-CA the endpoint is stored along with an insecure-TLS connector
/// that skips peer certificate verification.
#[derive(Clone)]
pub(super) enum BuiltRpcEndpoint {
    Standard(Endpoint),
    Insecure { endpoint: Endpoint },
}

impl BuiltRpcEndpoint {
    #[cfg(test)]
    pub(super) fn uri(&self) -> &tonic::transport::Uri {
        match self {
            Self::Standard(ep) => ep.uri(),
            Self::Insecure { endpoint } => endpoint.uri(),
        }
    }

    pub(super) async fn connect(self) -> std::result::Result<Channel, tonic::transport::Error> {
        match self {
            Self::Standard(ep) => ep.connect().await,
            Self::Insecure { endpoint } => {
                endpoint
                    .connect_with_connector(InsecureGrpcConnector::new())
                    .await
            }
        }
    }
}

pub(super) fn build_rpc_endpoint(
    prepared: &PreparedRpcEndpoint,
    rpc_ca_cert_pem: Option<&[u8]>,
) -> Result<BuiltRpcEndpoint> {
    let endpoint =
        Channel::from_shared(prepared.normalized_url.clone()).context(InvalidUriSnafu {
            url: prepared.normalized_url.clone(),
        })?;
    if !prepared.use_tls {
        return Ok(BuiltRpcEndpoint::Standard(endpoint));
    }
    if let Some(ca_cert) = rpc_ca_cert_pem {
        // HTTPS with configured CA: verify the peer certificate with the preloaded CA PEM.
        let tls = ClientTlsConfig::new()
            .with_native_roots()
            .ca_certificate(Certificate::from_pem(ca_cert));
        return endpoint
            .tls_config(tls)
            .context(BuildTlsConfigSnafu)
            .map(BuiltRpcEndpoint::Standard);
    }
    // HTTPS without CA: connect using insecure TLS (encrypted, no cert verification).
    // Keep the request origin as HTTPS so tonic emits the correct scheme while the custom
    // connector performs the TLS handshake itself.
    let origin = Channel::from_shared(prepared.normalized_url.clone())
        .context(InvalidUriSnafu {
            url: prepared.normalized_url.clone(),
        })?
        .uri()
        .clone();
    let endpoint = Channel::from_shared(prepared.normalized_url.replacen("https://", "http://", 1))
        .context(InvalidUriSnafu {
            url: prepared.normalized_url.clone(),
        })?
        .origin(origin);
    Ok(BuiltRpcEndpoint::Insecure { endpoint })
}

fn normalize_rpc_endpoint(url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_string()
    } else {
        format!("http://{url}")
    }
}

/// A tower [`Service`] that establishes a TLS connection without verifying the peer certificate.
///
/// Used when HTTPS is requested but no CA is configured. The connection is still encrypted;
/// only peer certificate verification is skipped.
#[derive(Clone)]
struct InsecureGrpcConnector {
    tls: TlsConnector,
}

impl std::fmt::Debug for InsecureGrpcConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsecureGrpcConnector")
            .finish_non_exhaustive()
    }
}

impl InsecureGrpcConnector {
    fn new() -> Self {
        let mut config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifyCertVerifier))
            .with_no_client_auth();
        // Advertise HTTP/2 so tonic negotiates the correct protocol.
        config.alpn_protocols = vec![b"h2".to_vec()];
        Self {
            tls: TlsConnector::from(Arc::new(config)),
        }
    }
}

impl tower::Service<Uri> for InsecureGrpcConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Self::Response, Self::Error>>
                + Send,
        >,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let tls = self.tls.clone();
        Box::pin(async move {
            let host = uri.host().ok_or("missing host in uri")?.to_string();
            let port = uri.port_u16().unwrap_or(443);
            let domain = ServerName::try_from(host.clone())
                .map_err(|e| format!("invalid server name '{host}': {e}"))?
                .to_owned();
            let tcp = TcpStream::connect((host.as_str(), port)).await?;
            let tls_stream = tls.connect(domain, tcp).await?;
            Ok(TokioIo::new(tls_stream))
        })
    }
}

/// A rustls certificate verifier that accepts any server certificate.
///
/// Used when HTTPS is requested but no CA is configured. The connection is still
/// encrypted; only peer certificate verification is skipped.
#[derive(Debug)]
struct NoVerifyCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifyCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verification_mode_display_values() {
        assert_eq!(VerificationMode::None.to_string(), "none");
        assert_eq!(VerificationMode::ConfiguredCa.to_string(), "config");
        assert_eq!(VerificationMode::Insecure.to_string(), "insecure");
    }

    #[test]
    fn normalize_rpc_endpoint_defaults_to_http_when_missing_scheme() {
        let prepared = prepare_rpc_endpoint("localhost:6055");
        assert_eq!(prepared.normalized_url, "http://localhost:6055");
        assert!(!prepared.use_tls);
    }

    #[test]
    fn normalize_rpc_endpoint_preserves_https_scheme() {
        let prepared = prepare_rpc_endpoint("https://localhost:6055");
        assert_eq!(prepared.normalized_url, "https://localhost:6055");
        assert!(prepared.use_tls);
    }

    #[test]
    fn normalize_rpc_endpoint_preserves_http_scheme() {
        let prepared = prepare_rpc_endpoint("http://localhost:6055");
        assert_eq!(prepared.normalized_url, "http://localhost:6055");
        assert!(!prepared.use_tls);
    }

    #[test]
    fn build_rpc_endpoint_preserves_http_scheme() {
        let prepared = prepare_rpc_endpoint("http://localhost:6055");
        let built = build_rpc_endpoint(&prepared, None).expect("build endpoint");
        assert_eq!(built.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn build_rpc_endpoint_preserves_https_scheme_with_ca_pem() {
        let prepared = prepare_rpc_endpoint("https://localhost:6055");
        let ca_pem = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/tls/ca.pem"),
        )
        .expect("read ca");
        let built = build_rpc_endpoint(&prepared, Some(ca_pem.as_slice())).expect("build endpoint");
        assert_eq!(built.uri().scheme_str(), Some("https"));
    }

    #[test]
    fn built_rpc_endpoint_is_cloneable_for_reconnect_retries() {
        let prepared = prepare_rpc_endpoint("https://localhost:6055");
        let built = build_rpc_endpoint(&prepared, None).expect("build endpoint");
        let cloned = built.clone();
        assert_eq!(cloned.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn build_rpc_endpoint_rewrites_insecure_connector_uri_to_http_without_ca() {
        let prepared = prepare_rpc_endpoint("https://localhost:6055");
        let built = build_rpc_endpoint(&prepared, None).expect("build endpoint");
        assert_eq!(built.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn prepared_rpc_endpoint_reports_http_without_ca() {
        let prepared = prepare_rpc_endpoint("localhost:6055");
        let meta = prepared.meta(None);
        assert_eq!(meta.endpoint, "http://localhost:6055");
        assert_eq!(meta.transport, "http");
        assert_eq!(meta.verify_mode, VerificationMode::None);
    }

    #[test]
    fn prepared_rpc_endpoint_reports_https_without_ca_as_insecure() {
        let prepared = prepare_rpc_endpoint("https://localhost:6055");
        let meta = prepared.meta(None);
        assert_eq!(meta.endpoint, "https://localhost:6055");
        assert_eq!(meta.transport, "https");
        assert_eq!(meta.verify_mode, VerificationMode::Insecure);
    }

    #[test]
    fn prepared_rpc_endpoint_reports_https_with_configured_ca() {
        let prepared = prepare_rpc_endpoint("https://localhost:6055");
        let meta = prepared.meta(Some(b"pem"));
        assert_eq!(meta.verify_mode, VerificationMode::ConfiguredCa);
    }
}
