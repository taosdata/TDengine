pub mod agent;
pub mod rebalance;
pub mod task;
pub mod xnode;

use std::{path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    extract::rejection::{JsonRejection, PathRejection, QueryRejection},
    http::{Method, Request, Response, StatusCode, Uri},
    response::IntoResponse,
    routing::{delete, get, post},
    serve::Listener,
};
use rustls::{RootCertStore, ServerConfig};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream, lookup_host},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tower_http::{classify::ServerErrorsFailureClass, trace::TraceLayer};
use tracing::{Span, instrument};

use crate::{
    HttpsConfig,
    api::{agent::*, rebalance::*, task::*, xnode::*},
    controller::{self, Controller},
};

static DEFAULT_LISTEN: &str = "0.0.0.0:6051";

#[derive(Debug, PartialEq, Eq)]
struct AccessLogFields {
    method: String,
    path: String,
}

#[derive(Debug, PartialEq, Eq)]
struct ResponseLogFields {
    status: u16,
    latency_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct FailureLogFields {
    failure: String,
    latency_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
struct ApiErrorLogFields {
    status: u16,
    error_kind: &'static str,
    error: String,
}

fn build_access_log_fields(method: &Method, uri: &Uri) -> AccessLogFields {
    AccessLogFields {
        method: method.to_string(),
        path: uri.path().to_string(),
    }
}

fn build_response_log_fields(status: StatusCode, latency: Duration) -> ResponseLogFields {
    ResponseLogFields {
        status: status.as_u16(),
        latency_ms: latency.as_millis() as u64,
    }
}

fn should_log_http_response(status: StatusCode) -> bool {
    !status.is_server_error()
}

fn build_failure_log_fields(
    failure: &ServerErrorsFailureClass,
    latency: Duration,
) -> FailureLogFields {
    FailureLogFields {
        failure: match failure {
            ServerErrorsFailureClass::StatusCode(status) => status.to_string(),
            ServerErrorsFailureClass::Error(error) => error.to_string(),
        },
        latency_ms: latency.as_millis() as u64,
    }
}

fn error_status_code(error: &Error) -> StatusCode {
    match error {
        Error::Controller { source } => source.status_code(),
        Error::Cancelled => StatusCode::SERVICE_UNAVAILABLE,
        Error::JsonRejection { source } => source.status(),
        Error::PathRejection { source } => source.status(),
        Error::QueryRejection { source } => source.status(),
    }
}

fn error_kind(error: &Error) -> &'static str {
    match error {
        Error::Controller { .. } => "controller",
        Error::Cancelled => "cancelled",
        Error::JsonRejection { .. } => "json_rejection",
        Error::PathRejection { .. } => "path_rejection",
        Error::QueryRejection { .. } => "query_rejection",
    }
}

fn build_api_error_log_fields(error: &Error) -> ApiErrorLogFields {
    ApiErrorLogFields {
        status: error_status_code(error).as_u16(),
        error_kind: error_kind(error),
        error: error.to_string(),
    }
}

fn make_http_span<B>(request: &Request<B>) -> Span {
    let fields = build_access_log_fields(request.method(), request.uri());
    tracing::info_span!("xnoded_http", method = %fields.method, path = %fields.path)
}

fn log_http_request<B>(_request: &Request<B>, span: &Span) {
    let _guard = span.enter();
    tracing::info!("http request");
}

fn log_http_response<B>(response: &Response<B>, latency: Duration, span: &Span) {
    if !should_log_http_response(response.status()) {
        return;
    }
    let _guard = span.enter();
    let fields = build_response_log_fields(response.status(), latency);
    tracing::info!(
        status = fields.status,
        latency_ms = fields.latency_ms,
        "http response"
    );
}

fn log_http_failure(failure: ServerErrorsFailureClass, latency: Duration, span: &Span) {
    let _guard = span.enter();
    let fields = build_failure_log_fields(&failure, latency);
    tracing::warn!(
        latency_ms = fields.latency_ms,
        failure = %fields.failure,
        "http failure"
    );
}

#[instrument(skip_all)]
pub async fn start_http(
    listen: Option<String>,
    https_config: HttpsConfig,
    controller: Arc<Controller>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let _guard = cancel.clone().drop_guard();
    let app = axum::Router::new()
        .route("/xnode", post(create_xnode))
        .route("/xnode/{id}", delete(delete_xnode))
        .route("/xnode/drain/{id}", post(drain_xnode))
        .route("/xnode/{id}", get(xnode_status))
        .route("/task/{id}", get(task_status))
        .route("/task/check", post(check_task))
        .route("/task/{id}", post(start_task))
        .route("/task/{id}", delete(stop_task))
        .route("/task/drop/{id}", delete(drop_task))
        .route(
            "/rebalance/manual/{tid}/{jid}/{xid}",
            post(rebalance_manual),
        )
        .route("/rebalance/auto", post(rebalance_auto))
        .route("/agent", post(add_agent))
        .route("/agent/{id}", delete(del_agent))
        .route("/agents", get(get_agent))
        .with_state(controller)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(make_http_span)
                .on_request(log_http_request)
                .on_response(log_http_response)
                .on_body_chunk(())
                .on_eos(())
                .on_failure(log_http_failure),
        );

    let is_unix = listen
        .as_deref()
        .is_some_and(|p| PathBuf::from(p).parent().is_some_and(|d| d.exists()));

    if https_config.enabled && is_unix {
        anyhow::bail!(
            "HTTPS is not supported on Unix socket listeners; \
             set XNODED_ENABLE_TLS=false or use a TCP address for XNODED_LISTEN"
        );
    }

    let listener = if https_config.enabled {
        let tls_acceptor = build_tls_acceptor(&https_config)
            .await
            .context("build TLS acceptor error")?;
        let addr = listen.as_deref().unwrap_or(DEFAULT_LISTEN);
        build_tls_tcp_listener(addr, tls_acceptor).await?
    } else {
        match &listen {
            Some(path) if PathBuf::from(path).parent().is_some_and(|p| p.exists()) => {
                build_unix_listener(path).await?
            }
            Some(path) if lookup_host(path).await.is_ok() => build_tcp_listener(path).await?,
            _ => build_tcp_listener(DEFAULT_LISTEN).await?,
        }
    };

    tracing::info!(
        "start listen on {} ({})",
        listen.as_deref().unwrap_or(DEFAULT_LISTEN),
        if https_config.enabled {
            "HTTPS"
        } else {
            "HTTP"
        },
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
        .context("serve error")?;

    Ok(())
}

enum ServeListener {
    Tcp(TcpListener),
    TlsTcp(TcpListener, TlsAcceptor),
    #[cfg(unix)]
    UnixSocket(tokio::net::UnixListener),
}

enum ServeStream {
    Tcp(TcpStream),
    TlsTcp(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    #[cfg(unix)]
    UnixSocket(tokio::net::UnixStream),
}

#[derive(Debug)]
enum ServeAddr {
    Tcp(std::net::SocketAddr),
    #[cfg(unix)]
    UnixSocket(tokio::net::unix::SocketAddr),
}

impl std::fmt::Display for ServeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeAddr::Tcp(addr) => write!(f, "{addr}"),
            #[cfg(unix)]
            ServeAddr::UnixSocket(addr) => write!(f, "{addr:?}"),
        }
    }
}

impl AsyncRead for ServeStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.as_mut();
        match this.get_mut() {
            ServeStream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
            ServeStream::TlsTcp(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ServeStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.as_mut();
        match this.get_mut() {
            ServeStream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
            ServeStream::TlsTcp(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.as_mut();
        match this.get_mut() {
            ServeStream::Tcp(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            ServeStream::TlsTcp(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            ServeStream::Tcp(stream) => stream.is_write_vectored(),
            ServeStream::TlsTcp(stream) => stream.is_write_vectored(),
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.as_mut();
        match this.get_mut() {
            ServeStream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
            ServeStream::TlsTcp(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.as_mut();
        match this.get_mut() {
            ServeStream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
            ServeStream::TlsTcp(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

/// Sleeps briefly after a listener `accept` error to prevent busy-looping on
/// resource-exhaustion conditions such as EMFILE / ENFILE.
///
/// Per-connection errors (ECONNRESET, ECONNABORTED, etc.) and `EINTR` are
/// transient and safe to retry immediately, so no delay is added for those.
async fn accept_backoff(e: &std::io::Error) {
    if !matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::Interrupted
    ) {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

impl Listener for ServeListener {
    type Io = ServeStream;

    type Addr = ServeAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self {
            ServeListener::Tcp(listener) => loop {
                // Use the inherent tokio method directly so errors surface here
                // rather than being swallowed by axum's blanket Listener impl.
                match TcpListener::accept(listener).await {
                    Ok((stream, addr)) => return (ServeStream::Tcp(stream), ServeAddr::Tcp(addr)),
                    Err(e) => {
                        tracing::warn!("TCP accept error, retrying: {e:#}");
                        accept_backoff(&e).await;
                    }
                }
            },
            ServeListener::TlsTcp(listener, acceptor) => loop {
                let (tcp_stream, addr) = match TcpListener::accept(listener).await {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!("TCP accept error, retrying: {e:#}");
                        accept_backoff(&e).await;
                        continue;
                    }
                };
                match acceptor.accept(tcp_stream).await {
                    Ok(tls_stream) => {
                        return (
                            ServeStream::TlsTcp(Box::new(tls_stream)),
                            ServeAddr::Tcp(addr),
                        );
                    }
                    Err(e) => {
                        tracing::warn!("TLS handshake failed from {addr}: {e:#}");
                        // Try the next connection instead of returning a broken stream.
                    }
                }
            },
            #[cfg(unix)]
            ServeListener::UnixSocket(listener) => loop {
                match tokio::net::UnixListener::accept(listener).await {
                    Ok((stream, addr)) => {
                        return (ServeStream::UnixSocket(stream), ServeAddr::UnixSocket(addr));
                    }
                    Err(e) => {
                        tracing::warn!("Unix socket accept error, retrying: {e:#}");
                        accept_backoff(&e).await;
                    }
                }
            },
        }
    }

    fn local_addr(&self) -> tokio::io::Result<Self::Addr> {
        match self {
            ServeListener::Tcp(listener) | ServeListener::TlsTcp(listener, _) => {
                listener.local_addr().map(ServeAddr::Tcp)
            }
            #[cfg(unix)]
            ServeListener::UnixSocket(listener) => listener.local_addr().map(ServeAddr::UnixSocket),
        }
    }
}

#[instrument(skip_all)]
async fn build_tcp_listener(addr: &str) -> anyhow::Result<ServeListener> {
    let listener = TcpListener::bind(addr)
        .await
        .context("bind http listener error")?;
    Ok(ServeListener::Tcp(listener))
}

#[instrument(skip_all)]
#[cfg(unix)]
async fn build_unix_listener(path: &str) -> anyhow::Result<ServeListener> {
    let socket = tokio::net::UnixSocket::new_stream().context("build unix socket error")?;
    socket
        .bind(path)
        .with_context(|| format!("unix socket bind path {path} error"))?;
    let listener = socket.listen(1024).context("unix socket listen error")?;
    Ok(ServeListener::UnixSocket(listener))
}

#[cfg(not(unix))]
async fn build_unix_listener(path: &str) -> anyhow::Result<ServeListener> {
    Err(anyhow::anyhow!(
        "unix socket is not supported on this platform: {path}"
    ))
}

/// Builds a rustls `TlsAcceptor` from the certificate and private key paths in
/// `https_config`.  Returns an error with context if any file is missing or
/// the certificate/key are invalid.
pub async fn build_tls_acceptor(https_config: &HttpsConfig) -> anyhow::Result<TlsAcceptor> {
    let cert_path = https_config
        .certificate
        .as_ref()
        .context("XNODED_TLS_SVR_CERT_PATH is required when TLS is enabled")?;
    let key_path = https_config
        .certificate_key
        .as_ref()
        .context("XNODED_TLS_SVR_KEY_PATH is required when TLS is enabled")?;

    let cert_bytes = tokio::fs::read(cert_path)
        .await
        .with_context(|| format!("failed to open certificate file: {}", cert_path.display()))?;
    let key_bytes = tokio::fs::read(key_path)
        .await
        .with_context(|| format!("failed to open private key file: {}", key_path.display()))?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_bytes.as_slice())
        .collect::<Result<_, _>>()
        .with_context(|| format!("failed to parse certificate file: {}", cert_path.display()))?;

    let private_key = rustls_pemfile::private_key(&mut key_bytes.as_slice())
        .with_context(|| format!("failed to read private key file: {}", key_path.display()))?
        .with_context(|| format!("no private key found in file: {}", key_path.display()))?;

    let mut config = match https_config.ca_path.as_ref() {
        Some(ca_path) => {
            let client_roots = load_client_ca_roots(ca_path).await?;
            let client_verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(client_roots))
                    .build()
                    .context("invalid TLS client CA certificate")?;

            ServerConfig::builder()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs, private_key)
                .context("invalid TLS certificate or private key")?
        }
        None => ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .context("invalid TLS certificate or private key")?,
    };

    // Enable HTTP/1.1 and HTTP/2 ALPN so axum/hyper can negotiate the protocol.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(config)))
}

async fn load_client_ca_roots(ca_path: &std::path::Path) -> anyhow::Result<RootCertStore> {
    let ca_bytes = tokio::fs::read(ca_path)
        .await
        .with_context(|| format!("failed to open CA file: {}", ca_path.display()))?;
    let ca_certs: Vec<_> = rustls_pemfile::certs(&mut ca_bytes.as_slice())
        .collect::<Result<_, _>>()
        .with_context(|| format!("failed to parse CA file: {}", ca_path.display()))?;
    if ca_certs.is_empty() {
        anyhow::bail!("no valid CA cert found in file: {}", ca_path.display());
    }

    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(ca_certs);
    Ok(roots)
}

#[instrument(skip_all)]
async fn build_tls_tcp_listener(
    addr: &str,
    acceptor: TlsAcceptor,
) -> anyhow::Result<ServeListener> {
    let listener = TcpListener::bind(addr)
        .await
        .context("bind HTTPS listener error")?;
    Ok(ServeListener::TlsTcp(listener, acceptor))
}

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(transparent)]
    Controller { source: controller::Error },
    #[snafu(display("cancelled"))]
    Cancelled,
    #[snafu(transparent)]
    JsonRejection { source: JsonRejection },
    #[snafu(transparent)]
    PathRejection { source: PathRejection },
    #[snafu(transparent)]
    QueryRejection { source: QueryRejection },
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let fields = build_api_error_log_fields(&self);
        let code = error_status_code(&self);
        let message = format!("{:#}", anyhow::Error::new(self));
        tracing::error!(
            status = fields.status,
            error_kind = fields.error_kind,
            error = %fields.error,
            "http response error"
        );
        (code, message).into_response()
    }
}

type RawResult<T> = std::result::Result<T, Error>;

type JsonResult<T> = std::result::Result<Data<T>, Error>;

pub struct Data<T>(T);

impl<T> IntoResponse for Data<T>
where
    T: serde::Serialize,
{
    fn into_response(self) -> axum::response::Response {
        match serde_json::to_string(&self.0) {
            Ok(data) => (StatusCode::OK, data),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to serialize data: {e}"),
            ),
        }
        .into_response()
    }
}

macro_rules! call {
    ($controller:expr, $method:ident($($args:expr),*)) => {{
        let cancel = $controller.cancel();
        match cancel.run_until_cancelled($controller.$method($($args),*)).await {
            Some(res) => res?,
            None => return CancelledSnafu.fail(),
        };
        Ok(())
    }};
    (spawn, $controller:expr, $method:ident($($args:expr),*)) => {{
        let cancel = $controller.cancel().child_token();
        let ctl = $controller.clone();
        tokio::spawn(async move {
            match cancel.run_until_cancelled(ctl.$method($($args),*)).await {
                Some(Ok(_)) => {}
                Some(Err(e)) => {
                    tracing::error!("{:#}", anyhow::Error::new(e));
                }
                None => {
                    tracing::warn!("request cancelled");
                }
            }
        });
        Ok(())
    }};
    (json, $controller:expr, $method:ident($($args:expr),*)) => {{
        let cancel = $controller.cancel();
        let res = match cancel.run_until_cancelled($controller.$method($($args),*)).await {
            Some(res) => res?,
            None => return CancelledSnafu.fail(),
        };
        Ok(Data(res))
    }};
}

pub(crate) use call;

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use super::*;
    use tracing::{
        Subscriber,
        field::{Field, Visit},
    };
    use tracing_subscriber::{
        layer::{Context, Layer, SubscriberExt},
        registry::Registry,
    };

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct RecordedEvent {
        fields: BTreeMap<String, String>,
    }

    #[derive(Default)]
    struct EventVisitor {
        fields: BTreeMap<String, String>,
    }

    impl Visit for EventVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    #[derive(Clone, Default)]
    struct RecordingLayer {
        events: Arc<Mutex<Vec<RecordedEvent>>>,
    }

    impl<S> Layer<S> for RecordingLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = EventVisitor::default();
            event.record(&mut visitor);
            self.events
                .lock()
                .expect("lock events")
                .push(RecordedEvent {
                    fields: visitor.fields,
                });
        }
    }

    fn captured_events(emit: impl FnOnce()) -> Vec<RecordedEvent> {
        let layer = RecordingLayer::default();
        let events = Arc::clone(&layer.events);
        let subscriber = Registry::default().with(layer);

        tracing::subscriber::with_default(subscriber, emit);

        events.lock().expect("lock events").clone()
    }

    #[test]
    fn access_log_fields_include_method_and_path() {
        let fields = build_access_log_fields(
            &axum::http::Method::GET,
            &"/xnode/15?verbose=true".parse().unwrap(),
        );

        assert_eq!(fields.method, "GET");
        assert_eq!(fields.path, "/xnode/15");
    }

    #[test]
    fn access_log_fields_strip_query_string() {
        let fields = build_access_log_fields(
            &axum::http::Method::DELETE,
            &"/agent/12?token=secret".parse().unwrap(),
        );

        assert_eq!(fields.method, "DELETE");
        assert_eq!(fields.path, "/agent/12");
    }

    #[test]
    fn api_error_into_response_uses_controller_status() {
        let source = controller::Error::NoAvailableXnode;
        let err = Error::Controller { source };

        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn response_log_fields_include_status_and_latency() {
        let fields = build_response_log_fields(StatusCode::OK, Duration::from_millis(42));

        assert_eq!(
            fields,
            ResponseLogFields {
                status: 200,
                latency_ms: 42,
            }
        );
    }

    #[test]
    fn failure_log_fields_include_failure_and_latency() {
        let fields = build_failure_log_fields(
            &ServerErrorsFailureClass::StatusCode(StatusCode::BAD_REQUEST),
            Duration::from_millis(100),
        );

        assert_eq!(fields.latency_ms, 100);
        assert_eq!(fields.failure, "400 Bad Request");
    }

    #[test]
    fn log_http_response_emits_event_for_non_server_errors() {
        let events = captured_events(|| {
            let span = tracing::info_span!("xnoded_http", method = "GET", path = "/xnode/15");
            let response = Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(())
                .unwrap();

            log_http_response(&response, Duration::from_millis(42), &span);
        });

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].fields.get("message"),
            Some(&"http response".into())
        );
        assert_eq!(events[0].fields.get("status"), Some(&"404".into()));
        assert_eq!(events[0].fields.get("latency_ms"), Some(&"42".into()));
    }

    #[test]
    fn log_http_response_skips_server_error_statuses() {
        let events = captured_events(|| {
            let span = tracing::info_span!("xnoded_http", method = "GET", path = "/xnode/15");
            let response = Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(())
                .unwrap();

            log_http_response(&response, Duration::from_millis(42), &span);
        });

        assert!(events.is_empty());
    }

    #[test]
    fn log_http_request_uses_span_for_method_and_path() {
        let events = captured_events(|| {
            let request = Request::builder()
                .method(Method::GET)
                .uri("/xnode/15?verbose=true")
                .body(())
                .unwrap();
            let span = make_http_span(&request);

            log_http_request(&request, &span);
        });

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].fields.get("message"),
            Some(&"http request".into())
        );
        assert!(!events[0].fields.contains_key("method"));
        assert!(!events[0].fields.contains_key("path"));
    }

    #[test]
    fn api_error_log_fields_use_structured_metadata() {
        let fields = build_api_error_log_fields(&Error::Controller {
            source: controller::Error::NoAvailableXnode,
        });

        assert_eq!(fields.status, StatusCode::NOT_FOUND.as_u16());
        assert_eq!(fields.error_kind, "controller");
        assert!(!fields.error.starts_with("HTTP response error:"));
    }

    #[test]
    fn data_into_response_success_and_failure_status() {
        let ok = Data(serde_json::json!({ "key": 1 }));
        let resp = ok.into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        struct Failing;

        impl serde::Serialize for Failing {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("serialize failed"))
            }
        }

        let failing = Data(Failing);
        let resp = failing.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn build_tcp_listener_binds_address() {
        let listener = build_tcp_listener("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        match addr {
            ServeAddr::Tcp(a) => {
                assert!(a.port() != 0);
            }
            #[cfg(unix)]
            ServeAddr::UnixSocket(_) => panic!("expected tcp listener"),
        }
    }

    #[tokio::test]
    async fn build_tls_acceptor_errors_when_cert_missing() {
        let cfg = crate::HttpsConfig {
            enabled: true,
            ca_path: None,
            certificate: None,
            certificate_key: Some(std::path::PathBuf::from("/some/key.pem")),
        };
        let err = build_tls_acceptor(&cfg)
            .await
            .err()
            .expect("expected error when certificate is missing");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("XNODED_TLS_SVR_CERT_PATH"),
            "expected certificate path in error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn build_tls_acceptor_errors_when_key_missing() {
        let cfg = crate::HttpsConfig {
            enabled: true,
            ca_path: None,
            certificate: Some(std::path::PathBuf::from("/some/cert.pem")),
            certificate_key: None,
        };
        let err = build_tls_acceptor(&cfg)
            .await
            .err()
            .expect("expected error when certificate key is missing");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("XNODED_TLS_SVR_KEY_PATH"),
            "expected key path in error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn build_tls_acceptor_errors_when_cert_file_not_found() {
        let cfg = crate::HttpsConfig {
            enabled: true,
            ca_path: None,
            certificate: Some(std::path::PathBuf::from("/nonexistent/cert.pem")),
            certificate_key: Some(std::path::PathBuf::from("/nonexistent/key.pem")),
        };
        let err = build_tls_acceptor(&cfg)
            .await
            .err()
            .expect("expected error when certificate file is not found");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("certificate file"),
            "expected certificate file error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn build_tls_acceptor_supports_optional_client_ca() {
        crate::install_rustls_provider().expect("install rustls provider");
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .to_path_buf();
        let cfg = crate::HttpsConfig {
            enabled: true,
            ca_path: Some(repo_root.join("tests/tls/ca.pem")),
            certificate: Some(repo_root.join("tests/tls/server.pem")),
            certificate_key: Some(repo_root.join("tests/tls/server.key")),
        };

        build_tls_acceptor(&cfg)
            .await
            .expect("build TLS acceptor with client CA");
    }
}
