pub mod rebalance;
pub mod task;
pub mod xnode;

use std::{path::PathBuf, pin::Pin, sync::Arc};

use anyhow::Context;
use axum::{
    extract::rejection::{JsonRejection, PathRejection, QueryRejection},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    serve::Listener,
};
use futures::FutureExt;
#[cfg(unix)]
use tokio::net::{UnixListener, UnixSocket, UnixStream};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream, lookup_host},
};
use tokio_util::sync::CancellationToken;
use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest, DefaultOnResponse};
use tracing::{Level, instrument};

use crate::{
    api::{rebalance::*, task::*, xnode::*},
    controller::{self, Controller},
};

static DEFAULT_LISTEN: &str = "0.0.0.0:6051";

#[instrument(skip_all)]
pub async fn start_http(
    listen: Option<String>,
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
        .with_state(controller)
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO))
                .on_body_chunk(())
                .on_eos(())
                .on_failure(()),
        );

    let listener = match &listen {
        Some(path) if PathBuf::from(path).parent().is_some_and(|p| p.exists()) => {
            build_unix_listener(path).await?
        }
        Some(path) if lookup_host(path).await.is_ok() => build_tcp_listener(path).await?,
        _ => build_tcp_listener(DEFAULT_LISTEN).await?,
    };

    tracing::info!(
        "start listen on {}",
        listen.as_deref().unwrap_or(DEFAULT_LISTEN)
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(cancel.cancelled_owned())
        .await
        .context("serve error")?;

    Ok(())
}

enum ServeListener {
    Tcp(TcpListener),
    #[cfg(unix)]
    UnixSocket(UnixListener),
}

enum ServeStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    UnixSocket(UnixStream),
}

#[derive(Debug)]
#[allow(dead_code)]
enum ServeAddr {
    Tcp(std::net::SocketAddr),
    #[cfg(unix)]
    UnixSocket(tokio::net::unix::SocketAddr),
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
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            ServeStream::Tcp(stream) => stream.is_write_vectored(),
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
            #[cfg(unix)]
            ServeStream::UnixSocket(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl Listener for ServeListener {
    type Io = ServeStream;

    type Addr = ServeAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self {
            ServeListener::Tcp(listener) => {
                listener
                    .accept()
                    .map(|(stream, addr)| (ServeStream::Tcp(stream), ServeAddr::Tcp(addr)))
                    .await
            }
            #[cfg(unix)]
            ServeListener::UnixSocket(listener) => {
                listener
                    .accept()
                    .map(|(stream, addr)| {
                        (ServeStream::UnixSocket(stream), ServeAddr::UnixSocket(addr))
                    })
                    .await
            }
        }
    }

    fn local_addr(&self) -> tokio::io::Result<Self::Addr> {
        match self {
            ServeListener::Tcp(listener) => listener.local_addr().map(ServeAddr::Tcp),
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
    let socket = UnixSocket::new_stream().context("build unix socket error")?;
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
        let code = match &self {
            Error::Controller { source } => source.status_code(),
            Error::Cancelled => StatusCode::SERVICE_UNAVAILABLE,
            Error::JsonRejection { source } => source.status(),
            Error::PathRejection { source } => source.status(),
            Error::QueryRejection { source } => source.status(),
        };

        let message = format!("{:#}", anyhow::Error::new(self));
        tracing::error!(code = code.as_u16(), "HTTP response error: {message}");
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
