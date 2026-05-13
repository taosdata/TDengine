use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use taos_query::prelude::RawError;

use taos_query::util::generate_req_id;
use taos_query::RawResult;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::time;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error as WsError;
use tracing::Instrument;

use crate::consumer::messages::{ReqId, TmqInit, TmqRecv, TmqRecvData, TmqSend, WsMessage};
use crate::consumer::{WsTmqAgent, WsTmqError, WsTmqSender};
use crate::query::asyn::{is_support_binary_sql, WS_ERROR_NO};
use crate::query::messages::ToMessage;
use crate::query::WsConnReq;
use crate::{
    handle_disconnect_error, EndpointType, TaosBuilder, WsStream, WsStreamReader, WsStreamSender,
};

#[derive(Debug, Clone)]
struct MessageCache {
    message: Arc<Mutex<Option<(ReqId, Message)>>>,
}

impl MessageCache {
    fn new() -> Self {
        Self {
            message: Arc::default(),
        }
    }

    async fn insert(&self, req_id: ReqId, message: Message) {
        let message = (req_id, message);
        tracing::trace!("insert message into cache, message: {message:?}");
        *self.message.lock().await = Some(message);
    }

    async fn remove(&self) {
        let mut guard = self.message.lock().await;
        tracing::trace!("remove message from cache, message: {:?}", *guard);
        *guard = None;
    }

    async fn req_id(&self) -> Option<ReqId> {
        let guard = self.message.lock().await;
        guard.as_ref().map(|(req_id, _)| *req_id)
    }

    async fn message(&self) -> Option<Message> {
        let guard = self.message.lock().await;
        guard.as_ref().map(|(_, msg)| msg.clone())
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run(
    builder: TaosBuilder,
    mut ws_stream: WsStream,
    tmq_sender: WsTmqSender,
    poll_cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    message_reader: flume::Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
    tmq_conf: TmqInit,
    topics: Arc<RwLock<Vec<String>>>,
    support_fetch_raw: Arc<AtomicBool>,
) {
    let cache = MessageCache::new();

    loop {
        let (ws_stream_tx, ws_stream_rx) = ws_stream.split();
        let (err_tx, mut err_rx) = mpsc::channel(2);
        let (close_tx, close_rx) = watch::channel(false);

        let send_handle = tokio::spawn(
            send_messages(
                ws_stream_tx,
                message_reader.clone(),
                close_rx.clone(),
                err_tx.clone(),
                cache.clone(),
            )
            .in_current_span(),
        );

        let recv_handle = tokio::spawn(
            read_messages(
                ws_stream_rx,
                tmq_sender.clone(),
                poll_cache_sender.clone(),
                close_rx,
                err_tx,
                cache.clone(),
            )
            .in_current_span(),
        );

        tokio::select! {
            err = err_rx.recv() => {
                if let Some(err) = err {
                    tracing::error!("WebSocket error: {err}");
                    let _ = close_tx.send(true);
                    if !is_disconnect_error(&err) {
                        tracing::error!("non-disconnect error detected, cleaning up all pending queries");
                        cleanup_after_disconnect(tmq_sender.queries.clone()).await;
                        return;
                    }
                    tracing::warn!("disconnect error detected, attempting to reconnect");
                }
            }
            _ = close_reader.changed() => {
                tracing::info!("WebSocket received close signal");
                let _ = close_tx.send(true);
                return;
            }
        }

        if let Err(err) = send_handle.await {
            tracing::error!("send messages task failed: {err:?}");
        }
        if let Err(err) = recv_handle.await {
            tracing::error!("read messages task failed: {err:?}");
        }

        tracing::warn!("WebSocket disconnected, starting to reconnect");

        let res = if cache.message().await.is_some() {
            let cb = send_subscribe_request(
                builder.build_conn_request(),
                tmq_conf.clone(),
                topics.clone(),
                builder.conn_timeout,
            );
            builder.connect_with_cb(EndpointType::Tmq, cb).await
        } else {
            builder.connect_with_ty(EndpointType::Tmq).await
        };

        match res {
            Ok((ws, ver)) => {
                ws_stream = ws;
                support_fetch_raw.store(is_fetch_raw_supported(&ver), Ordering::Relaxed);
            }
            Err(err) => {
                tracing::error!("WebSocket reconnection failed: {err}");
                cleanup_after_disconnect(tmq_sender.queries.clone()).await;
                return;
            }
        }

        tracing::info!("WebSocket reconnected successfully");

        cleanup_after_reconnect(tmq_sender.queries.clone(), cache.clone()).await;
    }
}

async fn send_messages(
    mut ws_stream_sender: WsStreamSender,
    message_reader: flume::Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
    err_sender: mpsc::Sender<WsTmqError>,
    cache: MessageCache,
) {
    tracing::trace!("start sending messages to WebSocket stream");

    if let Some(message) = cache.message().await {
        if let Err(err) = ws_stream_sender.send(message).await {
            tracing::error!("failed to send poll message: {err}");
            let _ = err_sender.send(err.into()).await;
        }
    }

    let mut interval = time::interval(Duration::from_secs(29));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(err) = send_ping_message(&mut ws_stream_sender).await {
                    tracing::error!("failed to send WebSocket ping message: {err}");
                    let _ = err_sender.send(err).await;
                    break;
                }
            }
            _ = close_reader.changed() => {
                tracing::trace!("WebSocket sender received close signal");
                send_close_message(&mut ws_stream_sender).await;
                break;
            }
            message = message_reader.recv_async() => {
                match message {
                    Ok(message) => {
                        let req_id = message.req_id();
                        let should_cache = message.should_cache();
                        let message = message.into_message();
                        if should_cache {
                            cache.insert(req_id, message.clone()).await;
                        }
                        if let Err(err) = ws_stream_sender.send(message).await {
                            tracing::error!("WebSocket sender error: {err:?}");
                            let _ = err_sender.send(err.into()).await;
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::error!("failed to receive message from channel: {err}");
                        let _ = err_sender.send(WsTmqError::ChannelClosedError).await;
                        break;
                    }
                }
            }
        }
    }

    tracing::trace!("stop sending messages to WebSocket stream");
}

async fn send_ping_message(ws_stream_sender: &mut WsStreamSender) -> Result<(), WsTmqError> {
    ws_stream_sender
        .send(Message::Ping(b"TAOS".to_vec()))
        .await
        .map_err(Into::<WsTmqError>::into)?;

    Ok(())
}

async fn send_close_message(ws_stream_sender: &mut WsStreamSender) {
    if let Err(err) = ws_stream_sender.send(Message::Close(None)).await {
        tracing::error!("failed to send close message: {err:?}");
    }
    if let Err(err) = ws_stream_sender.close().await {
        tracing::error!("failed to close WebSocket stream: {err:?}");
    }
}

async fn read_messages(
    mut ws_stream_reader: WsStreamReader,
    tmq_sender: WsTmqSender,
    poll_cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    mut close_reader: watch::Receiver<bool>,
    err_sender: mpsc::Sender<WsTmqError>,
    cache: MessageCache,
) {
    tracing::trace!("start reading messages from WebSocket stream");

    let (message_tx, message_rx) = mpsc::channel(64);

    let message_handle = tokio::spawn(
        handle_messages(
            message_rx,
            tmq_sender.clone(),
            poll_cache_sender,
            err_sender.clone(),
            cache,
        )
        .in_current_span(),
    );

    let mut closed_normally = false;

    loop {
        tokio::select! {
            res = ws_stream_reader.try_next() => {
                match res {
                    Ok(Some(message)) => {
                        if let Err(err) = message_tx.send(message).await {
                            tracing::error!("failed to send message to handler, err: {err:?}");
                            let _ = err_sender.send(WsError::ConnectionClosed.into()).await;
                            break;
                        }
                    }
                    Ok(None) => {
                        tracing::info!("WebSocket stream closed by peer");
                        closed_normally = true;
                        break;
                    }
                    Err(err) => {
                        tracing::error!("WebSocket reader error: {err:?}");
                        let _ = err_sender.send(err.into()).await;
                        break;
                    }
                }
            }
            _ = close_reader.changed() => {
                tracing::trace!("WebSocket reader received close signal");
                closed_normally = true;
                break;
            }
        }
    }

    drop(message_tx);

    if let Err(err) = message_handle.await {
        if err.is_cancelled() {
            tracing::trace!("handle messages task was cancelled");
        } else {
            tracing::error!("handle messages task panicked: {err:?}");
        }
    }

    if closed_normally {
        cleanup_after_disconnect(tmq_sender.queries.clone()).await;
    }

    tracing::trace!("stop reading messages from WebSocket stream");
}

async fn handle_messages(
    mut message_reader: mpsc::Receiver<Message>,
    tmq_sender: WsTmqSender,
    poll_cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    err_sender: mpsc::Sender<WsTmqError>,
    cache: MessageCache,
) {
    while let Some(message) = message_reader.recv().await {
        parse_message(
            message,
            tmq_sender.clone(),
            poll_cache_sender.clone(),
            err_sender.clone(),
            cache.clone(),
        )
        .await;
    }
}

async fn parse_message(
    message: Message,
    tmq_sender: WsTmqSender,
    poll_cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    err_sender: mpsc::Sender<WsTmqError>,
    cache: MessageCache,
) {
    match message {
        Message::Text(text) => {
            parse_text_message(
                text,
                tmq_sender.queries.clone(),
                poll_cache_sender,
                err_sender,
                cache,
            )
            .await;
        }
        Message::Binary(data) => parse_binary_message(data, tmq_sender.queries.clone()).await,
        Message::Ping(data) => {
            tokio::spawn(async move {
                let _ = tmq_sender
                    .sender
                    .send_async(WsMessage::Raw(Message::Pong(data)))
                    .await;
            });
        }
        Message::Close(_) => {
            // taosAdapter should never send a close frame to the client.
            // Therefore all close frames should be treated as errors.
            tracing::warn!("received unexpected close message, this should not happen");
        }
        Message::Pong(_) => tracing::trace!("received pong message, do nothing"),
        Message::Frame(_) => {
            tracing::warn!("received unexpected frame message, do nothing");
            tracing::trace!("frame message: {message:?}");
        }
    }
}

async fn parse_text_message(
    text: String,
    queries: WsTmqAgent,
    poll_cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    err_sender: mpsc::Sender<WsTmqError>,
    cache: MessageCache,
) {
    tracing::trace!("received text message, text: {text}");

    let resp = match serde_json::from_str::<TmqRecv>(&text) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!("failed to deserialize json text: {text}, err: {err:?}");
            return;
        }
    };

    let (req_id, data, ok) = resp.ok();

    match &data {
        TmqRecvData::Version { .. } => {}
        TmqRecvData::FetchBlock { .. }
        | TmqRecvData::Bytes(..)
        | TmqRecvData::FetchRawData { .. }
        | TmqRecvData::Block(..)
        | TmqRecvData::Close => unreachable!("unexpected data type: {:?}", data),
        TmqRecvData::Poll(_) => {
            cache.remove().await;

            let data = match queries.remove(&req_id) {
                Some((_, sender)) => {
                    #[cfg(test)]
                    #[allow(static_mut_refs)]
                    {
                        if std::env::var("TEST_POLLING_LOST").is_ok() {
                            static mut POLLING_LOST_IDX: u64 = 0;
                            unsafe {
                                POLLING_LOST_IDX += 1;
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                tracing::warn!(lost = POLLING_LOST_IDX, "polling lost");
                            }
                        }
                    }

                    match sender.send(ok.map(|_| data)).await {
                        Ok(_) => None,
                        Err(err) => match err.0 {
                            Ok(data) => Some(data),
                            Err(err) => {
                                tracing::warn!("poll message received, err: {err:?}");
                                None
                            }
                        },
                    }
                }
                None => Some(data),
            };

            tracing::trace!("poll end: {data:?}");
            if let Err(err) = poll_cache_sender.send(data).await {
                tracing::error!("poll end notification failed, break the connection, err: {err:?}");
                let _ = err_sender.send(WsTmqError::ChannelClosedError).await;
            }
        }
        _ => {
            if let Some((_, sender)) = queries.remove(&req_id) {
                if let Err(err) = sender.send(ok.map(|_| data)).await {
                    tracing::warn!("failed to send data, req_id: 0x{req_id:x}, err: {err:?}");
                }
            } else {
                tracing::warn!("no sender found for req_id: 0x{req_id:x}, the message may be lost");
            }
        }
    }
}

async fn parse_binary_message(data: Vec<u8>, queries: WsTmqAgent) {
    use taos_query::util::InlinableRead;

    let mut slice = data.as_slice();

    let timing = slice.read_u64().unwrap();
    let bytes = if timing != u64::MAX {
        slice[16..].to_vec()
    } else {
        let bytes = slice[26..].to_vec();
        let _action = slice.read_u64().unwrap();
        let _version = slice.read_u16().unwrap();
        let _time = slice.read_u64().unwrap();
        bytes
    };

    let req_id = slice.read_u64().unwrap();

    if let Some((_, sender)) = queries.remove(&req_id) {
        if let Err(err) = sender.send(Ok(TmqRecvData::Bytes(bytes.into()))).await {
            tracing::warn!("failed to send data, req_id: 0x{req_id:x}, err: {err:?}");
        }
    } else {
        tracing::warn!("no sender found for req_id: 0x{req_id:x}, the message may be lost");
    }
}

fn send_subscribe_request(
    conn_req: WsConnReq,
    tmq_conf: TmqInit,
    topics: Arc<RwLock<Vec<String>>>,
    conn_timeout: Duration,
) -> impl for<'a> Fn(&'a mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<()>> + Send + 'a>> {
    move |ws_stream| {
        let conn_req = conn_req.clone();
        let tmq_conf = tmq_conf.clone();
        let topics = topics.clone();

        Box::pin(async move {
            let topics = topics.read().await.clone();
            let req = TmqSend::Subscribe {
                req_id: generate_req_id(),
                req: tmq_conf.clone().disable_auto_commit(),
                topics: topics.clone(),
                conn: conn_req.clone(),
            };

            if let Err(err) = send_recv(ws_stream, req.to_msg(), conn_timeout).await {
                tracing::error!("subscribe error: {err:?}");
                if tmq_conf.enable_batch_meta.is_none() {
                    return Err(err);
                }

                let code: i32 = err.code().into();
                if code & 0xFFFF == 0xFFFE {
                    let req = TmqSend::Subscribe {
                        req_id: generate_req_id(),
                        req: tmq_conf.disable_batch_meta().disable_auto_commit(),
                        topics,
                        conn: conn_req,
                    };
                    let _ = send_recv(ws_stream, req.to_msg(), conn_timeout).await?;
                } else {
                    return Err(err);
                }
            }

            Ok(())
        })
    }
}

async fn send_recv(
    ws_stream: &mut WsStream,
    message: Message,
    conn_timeout: Duration,
) -> RawResult<TmqRecvData> {
    time::timeout(conn_timeout, ws_stream.send(message))
        .await
        .map_err(|_| {
            RawError::from_code(WS_ERROR_NO::SEND_MESSAGE_TIMEOUT.as_code())
                .context("timeout sending subscribe request")
        })?
        .map_err(handle_disconnect_error)?;

    loop {
        let res = time::timeout(conn_timeout, ws_stream.next())
            .await
            .map_err(|_| {
                RawError::from_code(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code())
                    .context("timeout waiting for subscribe response")
            })?;

        let Some(res) = res else {
            return Err(RawError::from_code(
                WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code(),
            ));
        };

        let message = res.map_err(handle_disconnect_error)?;
        tracing::trace!("send_subscribe_request, received message: {message}");

        match message {
            Message::Text(text) => {
                let resp: TmqRecv = serde_json::from_str(&text).map_err(|err| {
                    RawError::any(err)
                        .with_code(WS_ERROR_NO::DE_ERROR.as_code())
                        .context("invalid json response")
                })?;
                let (_, data, ok) = resp.ok();
                ok?;
                match data {
                    TmqRecvData::Subscribe => return Ok(data),
                    TmqRecvData::Version { .. } => {}
                    _ => {
                        return Err(RawError::from_string(format!(
                            "unexpected subscribe response: {data:?}"
                        )))
                    }
                }
            }
            Message::Ping(bytes) => {
                ws_stream
                    .send(Message::Pong(bytes))
                    .await
                    .map_err(handle_disconnect_error)?;
            }
            _ => {
                return Err(RawError::from_string(format!(
                    "unexpected message during subscribe: {message:?}"
                )))
            }
        }
    }
}

#[inline]
pub(super) fn is_fetch_raw_supported(version: &str) -> bool {
    !version.starts_with('2') && is_support_binary_sql(version)
}

fn is_disconnect_error(err: &WsTmqError) -> bool {
    match err {
        WsTmqError::WsError(err) => matches!(
            err,
            WsError::ConnectionClosed
                | WsError::AlreadyClosed
                | WsError::Io(_)
                | WsError::Tls(_)
                | WsError::Protocol(_)
        ),
        _ => false,
    }
}

async fn cleanup_after_disconnect(queries: WsTmqAgent) {
    let keys = queries.iter().map(|r| *r.key()).collect_vec();
    for key in keys {
        if let Some((_, sender)) = queries.remove(&key) {
            let _ = sender
                .send(Err(RawError::new(
                    WS_ERROR_NO::CONN_CLOSED.as_code(),
                    "WebSocket connection is closed",
                )))
                .await;
        }
    }
}

async fn cleanup_after_reconnect(queries: WsTmqAgent, cache: MessageCache) {
    let req_id = cache.req_id().await;
    if let Some(req_id) = req_id {
        let remove_ids: Vec<_> = queries
            .iter()
            .map(|r| *r.key())
            .filter(|&id| id != req_id)
            .collect();
        for id in remove_ids {
            queries.remove(&id);
        }
    } else {
        queries.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use taos_query::AsyncTBuilder;
    use tokio::task::JoinHandle;
    use warp::ws::Message;
    use warp::Filter;

    use crate::TmqBuilder;

    #[tokio::test]
    async fn test_tmq_auto_reconnect() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let poll_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn("ws://127.0.0.1:9988?group.id=10")?;
            let consumer = tmq.build().await?;
            let timeout = Duration::from_secs(10);

            let res = consumer.poll_timeout(timeout).await?;
            assert!(res.is_some());

            let res = consumer.poll_timeout(timeout).await?;
            assert!(res.is_some());

            Ok(())
        });

        let (close_tx, close_rx) = flume::bounded(1);
        let poll_cnt = Arc::new(AtomicUsize::new(0));

        let routes = warp::path!("rest" / "tmq").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                let poll_cnt = poll_cnt.clone();

                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let poll_cnt = poll_cnt;
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::debug!("ws recv message: {message:?}");
                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "version message",
                                    "action": "version",
                                    "req_id": 1001,
                                    "version": "3.0"
                                });
                                let msg = Message::text(data.to_string());
                                let _ = ws_tx.send(msg).await;
                            } else if text.contains("subscribe") {
                                let data = json!({
                                    "code": 0,
                                    "message": "subscribe message",
                                    "action": "subscribe",
                                    "req_id": 1002,
                                });
                                let msg = Message::text(data.to_string());
                                let _ = ws_tx.send(msg).await;
                            } else if text.contains("poll") {
                                let cnt = poll_cnt.fetch_add(1, Ordering::Relaxed);
                                if cnt == 0 {
                                    let _ = close.send_async(()).await;
                                    break;
                                } else if cnt == 1 {
                                    let data = json!({
                                        "code": 0,
                                        "message": "",
                                        "action": "poll",
                                        "req_id": 1,
                                        "timing": 1277505,
                                        "have_message": true,
                                        "topic": "topic_1748505708",
                                        "database": "test_1748505708",
                                        "vgroup_id": 56,
                                        "message_type": 1,
                                        "message_id": 1561,
                                        "offset": 5621
                                    });
                                    let msg = Message::text(data.to_string());
                                    let _ = ws_tx.send(msg).await;
                                } else if cnt == 2 {
                                    let data = json!({
                                        "code": 0,
                                        "message": "",
                                        "action": "poll",
                                        "req_id": 2,
                                        "timing": 1277505,
                                        "have_message": true,
                                        "topic": "topic_1748505708",
                                        "database": "test_1748505708",
                                        "vgroup_id": 56,
                                        "message_type": 1,
                                        "message_id": 1561,
                                        "offset": 5621
                                    });
                                    let msg = Message::text(data.to_string());
                                    let _ = ws_tx.send(msg).await;
                                    let _ = close.send_async(()).await;
                                    break;
                                }
                            }
                        }
                    }
                })
            }
        });

        let close = close_rx.clone();
        let (_, server1) = warp::serve(routes.clone()).bind_with_graceful_shutdown(
            ([127, 0, 0, 1], 9988),
            async move {
                let _ = close.recv_async().await;
                tracing::debug!("server1 shutting down...");
            },
        );

        server1.await;

        tokio::time::sleep(Duration::from_millis(500)).await;

        let (_, server2) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 9988), async move {
                let _ = close_rx.recv_async().await;
                tracing::debug!("server2 shutting down...");
            });

        server2.await;

        poll_handle.await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_failover() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let poll_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn("ws://127.0.0.1:9984,127.0.0.1:9985?group.id=10")?;
            let consumer = tmq.build().await?;
            let timeout = Duration::from_secs(10);
            let res = consumer.poll_timeout(timeout).await?;
            assert!(res.is_some());
            Ok(())
        });

        let (close_tx, close_rx) = flume::bounded(1);
        let poll_cnt = Arc::new(AtomicUsize::new(0));

        let routes = warp::path!("rest" / "tmq").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                let poll_cnt = poll_cnt.clone();

                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let poll_cnt = poll_cnt;
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::debug!("ws recv message: {message:?}");
                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "version message",
                                    "action": "version",
                                    "req_id": 1001,
                                    "version": "3.0"
                                });
                                let msg = Message::text(data.to_string());
                                let _ = ws_tx.send(msg).await;
                            } else if text.contains("subscribe") {
                                let data = json!({
                                    "code": 0,
                                    "message": "subscribe message",
                                    "action": "subscribe",
                                    "req_id": 1002,
                                });
                                let msg = Message::text(data.to_string());
                                let _ = ws_tx.send(msg).await;
                            } else if text.contains("poll") {
                                if poll_cnt.load(Ordering::Relaxed) == 1 {
                                    let data = json!({
                                        "code": 0,
                                        "message": "",
                                        "action": "poll",
                                        "req_id": 1,
                                        "timing": 1277505,
                                        "have_message": true,
                                        "topic": "topic_1748505708",
                                        "database": "test_1748505708",
                                        "vgroup_id": 56,
                                        "message_type": 1,
                                        "message_id": 1561,
                                        "offset": 5621
                                    });
                                    let msg = Message::text(data.to_string());
                                    let _ = ws_tx.send(msg).await;
                                }

                                poll_cnt.fetch_add(1, Ordering::Relaxed);
                                let _ = close.send_async(()).await;
                                break;
                            }
                        }
                    }
                })
            }
        });

        let close = close_rx.clone();
        let (_, server1) = warp::serve(routes.clone()).bind_with_graceful_shutdown(
            ([127, 0, 0, 1], 9984),
            async move {
                let _ = close.recv_async().await;
                tracing::debug!("server1 shutting down...");
            },
        );

        let (_, server2) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 9985), async move {
                let _ = close_rx.recv_async().await;
                tracing::debug!("server2 shutting down...");
            });

        tokio::join!(server1, server2);

        poll_handle.await??;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_subscribe_resp_timeout() -> anyhow::Result<()> {
        use crate::query::asyn::WS_ERROR_NO;
        use crate::TaosBuilder;
        use serde_json::Value;
        use taos_query::prelude::*;
        use taos_query::util::ws_proxy::*;
        use tokio_tungstenite::tungstenite::Message;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let intercept: InterceptFn = {
            Arc::new(move |msg, ctx| {
                if let Message::Text(text) = msg {
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "poll" {
                        return ProxyAction::Restart;
                    } else if action == "subscribe" {
                        ctx.req_count += 1;
                        if ctx.req_count == 2 {
                            tokio::task::block_in_place(|| {
                                std::thread::sleep(std::time::Duration::from_secs(2));
                            });
                        }
                    }
                }

                ProxyAction::Forward
            })
        };

        let _proxy =
            WsProxy::start("127.0.0.1:8907", "ws://localhost:6041/rest/tmq", intercept).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1762848301",
            "drop database if exists test_1762848301",
            "create database test_1762848301",
            "create topic topic_1762848301 as database test_1762848301",
            "use test_1762848301",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values (now, 1)",
        ])
        .await?;

        let tmq = TmqBuilder::from_dsn("ws://localhost:8907?group.id=123424&conn_timeout=1")?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1762848301"]).await?;

        let timeout = Timeout::Duration(std::time::Duration::from_secs(10));
        let err = consumer.recv_timeout(timeout).await.unwrap_err();
        assert_eq!(err.code(), WS_ERROR_NO::CONN_CLOSED.as_code());
        assert!(err.to_string().contains("WebSocket connection is closed"));

        consumer.unsubscribe().await;

        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        taos.exec_many([
            "drop topic if exists topic_1762848301",
            "drop database if exists test_1762848301",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_version_resp_timeout() -> anyhow::Result<()> {
        use crate::TaosBuilder;
        use taos_query::prelude::*;
        use taos_query::util::ws_proxy::*;
        use tokio_tungstenite::tungstenite::Message;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let intercept: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("version") {
                        tokio::task::block_in_place(|| {
                            std::thread::sleep(std::time::Duration::from_millis(1500));
                        });
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy =
            WsProxy::start("127.0.0.1:8908", "ws://localhost:6041/rest/tmq", intercept).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1762850286",
            "drop database if exists test_1762850286",
            "create database test_1762850286",
            "create topic topic_1762850286 as database test_1762850286",
            "use test_1762850286",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values (now, 1)",
        ])
        .await?;

        let tmq = TmqBuilder::from_dsn(
            "ws://localhost:8908?group.id=132324&conn_timeout=1&version_prefer=3.x&auto.offset.reset=earliest",
        )?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1762850286"]).await?;

        let timeout = Timeout::Duration(std::time::Duration::from_secs(5));
        let _ = consumer.recv_timeout(timeout).await?;

        consumer.unsubscribe().await;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic if exists topic_1762850286",
            "drop database if exists test_1762850286",
        ])
        .await?;

        Ok(())
    }
}
