use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

pub type InterceptFn = Arc<dyn Fn(&Message, &mut ProxyContext) -> ProxyAction + Send + Sync>;

#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum ProxyAction {
    Restart,
    Forward,
}

#[derive(Debug, Clone)]
pub struct ProxyContext {
    pub req_count: usize,
}

pub struct WsProxy {
    stop: Arc<Notify>,
}

impl WsProxy {
    pub async fn start(listen_addr: &str, backend_url: &str, intercept_fn: InterceptFn) -> Self {
        tracing::info!("starting WebSocket proxy on {listen_addr}, backend: {backend_url}");

        let listen_addr = listen_addr.parse().unwrap();
        let backend_url = backend_url.to_string();
        let running = Arc::new(AtomicBool::new(true));
        let ctx = Arc::new(Mutex::new(ProxyContext { req_count: 0 }));
        let stop_notify = Arc::new(Notify::new());
        let stop = stop_notify.clone();

        tokio::spawn(async move {
            loop {
                run_proxy_server(
                    listen_addr,
                    backend_url.clone(),
                    intercept_fn.clone(),
                    running.clone(),
                    ctx.clone(),
                    stop_notify.clone(),
                )
                .await;

                if running.load(Ordering::Relaxed) {
                    tracing::info!("stopping WebSocket proxy...");
                    break;
                }

                tracing::info!("restarting WebSocket proxy...");
                running.store(true, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });

        Self { stop }
    }
}

impl Drop for WsProxy {
    fn drop(&mut self) {
        tracing::info!("dropping WebSocket proxy");
        self.stop.notify_waiters();
    }
}

async fn run_proxy_server(
    listen_addr: SocketAddr,
    backend_url: String,
    intercept_fn: InterceptFn,
    running: Arc<AtomicBool>,
    ctx: Arc<Mutex<ProxyContext>>,
    stop_notify: Arc<Notify>,
) {
    let listener = match TcpListener::bind(listen_addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!("failed to bind to {listen_addr}: {err}");
            return;
        }
    };

    loop {
        tokio::select! {
            Ok((stream, _)) = listener.accept() => {
                let backend_url = backend_url.clone();
                let intercept_fn = intercept_fn.clone();
                let running = running.clone();
                let ctx = ctx.clone();
                let stop_notify = stop_notify.clone();

                tokio::spawn(async move {
                    let client = match accept_async(stream).await {
                        Ok(ws) => ws,
                        Err(err) => {
                            tracing::error!("failed to accept WebSocket connection: {err}");
                            return;
                        }
                    };
                    let (backend, _) = match connect_async(backend_url).await {
                        Ok(ws) => ws,
                        Err(err) => {
                            tracing::error!("failed to connect to backend: {err}");
                            return;
                        }
                    };

                    let (mut client_sink, mut client_stream) = client.split();
                    let (mut backend_sink, mut backend_stream) = backend.split();

                    let req_to_backend = async {
                        while let Some(Ok(msg)) = client_stream.next().await {
                            tracing::trace!("received message from client: {msg:?}");
                            let mut ctx = ctx.lock().await;
                            match intercept_fn(&msg, &mut ctx) {
                                ProxyAction::Restart => {
                                    running.store(false, Ordering::Relaxed);
                                    stop_notify.notify_waiters();
                                    break;
                                }
                                ProxyAction::Forward => {
                                    if let Err(err) = backend_sink.send(msg).await {
                                        tracing::error!("failed to send message to backend: {err:?}");
                                        break;
                                    }
                                }
                            }
                        }
                    };

                    let resp_to_client = async {
                        while let Some(Ok(msg)) = backend_stream.next().await {
                            tracing::trace!("received message from backend: {msg:?}");
                            if let Err(err) = client_sink.send(msg).await {
                                tracing::error!("failed to send message to client: {err:?}");
                                break;
                            }
                        }
                    };

                    tokio::select! {
                        _ = req_to_backend => {},
                        _ = resp_to_client => {},
                    }
                });
            }
            _ = stop_notify.notified() => {
                break;
            }
        }
    }
}
