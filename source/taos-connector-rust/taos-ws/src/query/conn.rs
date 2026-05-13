use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use flume::{Receiver, Sender};
use futures::{SinkExt, StreamExt, TryStreamExt};
use taos_query::prelude::RawError;
use taos_query::util::generate_req_id;
use taos_query::RawResult;
use tokio::sync::{mpsc, watch};
use tokio::time;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::Instrument;

use crate::query::asyn::{ConnState, WsQuerySender, WS_ERROR_NO};
use crate::query::messages::{MessageId, ReqId, ToMessage, WsMessage, WsRecv, WsRecvData, WsSend};
use crate::query::{Error, WsConnReq, WsTaos};
use crate::{
    handle_disconnect_error, send_request_with_timeout, TaosBuilder, WsStream, WsStreamReader,
    WsStreamSender,
};

pub fn send_conn_request(
    conn_req: WsConnReq,
    conn_timeout: Duration,
) -> impl for<'a> Fn(&'a mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<()>> + Send + 'a>> {
    move |ws_stream| {
        let conn_req = conn_req.clone();

        Box::pin(async move {
            let req = WsSend::Conn {
                req_id: generate_req_id(),
                req: conn_req,
            };

            send_request_with_timeout(ws_stream, req.to_msg(), conn_timeout).await?;

            loop {
                let res = time::timeout(conn_timeout, ws_stream.next())
                    .await
                    .map_err(|_| {
                        RawError::from_code(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code())
                            .context("timeout waiting for conn response")
                    })?;

                let Some(res) = res else {
                    return Err(RawError::from_code(
                        WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code(),
                    ));
                };

                let message = res.map_err(handle_disconnect_error)?;
                tracing::trace!("send_conn_request, received message: {message}");

                match message {
                    Message::Text(text) => {
                        let resp: WsRecv = serde_json::from_str(&text).map_err(|err| {
                            RawError::any(err)
                                .with_code(WS_ERROR_NO::DE_ERROR.as_code())
                                .context("invalid json response")
                        })?;
                        let (_, data, ok) = resp.ok();
                        ok?;
                        match data {
                            WsRecvData::Conn => return Ok(()),
                            WsRecvData::Version { .. } => {}
                            _ => {
                                return Err(RawError::from_string(format!(
                                    "unexpected conn response: {data:?}"
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
                            "unexpected message during conn: {message:?}"
                        )))
                    }
                }
            }
        })
    }
}

#[derive(Default, Clone)]
struct MessageCache {
    next_msg_id: Arc<AtomicU64>,
    req_to_msg: Arc<scc::HashMap<ReqId, MessageId>>,
    messages: Arc<scc::HashMap<MessageId, Message>>,
}

impl MessageCache {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&self, req_id: ReqId, message: Message) {
        let msg_id = self.next_msg_id.fetch_add(1, Ordering::Relaxed);
        tracing::trace!("insert message into cache, req_id: 0x{req_id:x}, msg_id: {msg_id}");
        let _ = self.req_to_msg.insert(req_id, msg_id);
        let _ = self.messages.insert(msg_id, message);
    }

    fn remove(&self, req_id: &ReqId) {
        tracing::trace!("remove message from cache, req_id: 0x{req_id:x}");
        if let Some((_, msg_id)) = self.req_to_msg.remove(req_id) {
            self.messages.remove(&msg_id);
        }
    }

    fn messages(&self) -> Vec<Message> {
        let mut pairs = Vec::with_capacity(self.messages.len());
        self.messages.scan(|msg_id, msg| {
            pairs.push((*msg_id, msg.clone()));
        });
        pairs.sort_unstable_by_key(|(msg_id, _)| *msg_id);
        pairs.into_iter().map(|(_, msg)| msg).collect()
    }
}

pub(super) async fn run(
    ws_taos: Weak<WsTaos>,
    builder: Arc<TaosBuilder>,
    mut ws_stream: WsStream,
    query_sender: WsQuerySender,
    message_reader: Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
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
                query_sender.clone(),
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
                        if let Some(taos) = ws_taos.upgrade() {
                            taos.set_state(ConnState::Disconnected);
                        }
                        cleanup_after_disconnect(query_sender.clone());
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

        if let Some(taos) = ws_taos.upgrade() {
            taos.set_state(ConnState::Reconnecting);
        }

        if let Err(err) = send_handle.await {
            tracing::error!("send messages task failed: {err:?}");
        }
        if let Err(err) = recv_handle.await {
            tracing::error!("read messages task failed: {err:?}");
        }

        tracing::warn!("WebSocket disconnected, starting to reconnect");

        match builder.connect().await {
            Ok((ws, ver)) => {
                ws_stream = ws;
                query_sender.version_info.update(ver).await;
            }
            Err(err) => {
                tracing::error!("WebSocket reconnection failed: {err}");
                if let Some(taos) = ws_taos.upgrade() {
                    taos.set_state(ConnState::Disconnected);
                }
                cleanup_after_disconnect(query_sender.clone());
                return;
            }
        };

        tracing::info!("WebSocket reconnected successfully");

        if let Some(taos) = ws_taos.upgrade() {
            taos.wait_for_previous_recover_stmt2().await;
            let mut stmt2_req_ids =
                cleanup_stmt2(query_sender.sender.clone(), message_reader.clone());
            taos.clone().recover_stmt2().await;
            stmt2_req_ids.extend(taos.stmt2_req_ids().await);
            cleanup_after_reconnect(query_sender.clone(), cache.clone(), stmt2_req_ids);
        }
    }
}

async fn send_messages(
    mut ws_stream_sender: WsStreamSender,
    message_reader: Receiver<WsMessage>,
    mut close_reader: watch::Receiver<bool>,
    err_sender: mpsc::Sender<Error>,
    cache: MessageCache,
) {
    tracing::trace!("start sending messages to WebSocket stream");

    let mut interval = time::interval(Duration::from_secs(53));

    for message in cache.messages() {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(err) = send_ping_message(&mut ws_stream_sender).await {
                    tracing::error!("failed to send WebSocket ping message: {err}");
                    let _ = err_sender.send(err).await;
                    return;
                }
            }
            _ = close_reader.changed() => {
                tracing::info!("WebSocket sender received close signal");
                send_close_message(&mut ws_stream_sender).await;
                return;
            }
            res = ws_stream_sender.send(message) => {
                if let Err(err) = res {
                    tracing::error!("WebSocket sender error: {err:?}");
                    let _ = err_sender.send(err.into()).await;
                    return;
                }
            }
        }
    }

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
                tracing::info!("WebSocket sender received close signal");
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
                            cache.insert(req_id, message.clone());
                        }
                        if let Err(err) = ws_stream_sender.send(message).await {
                            tracing::error!("WebSocket sender error: {err:?}");
                            let _ = err_sender.send(err.into()).await;
                            break;
                        }
                    }
                    Err(_) => {
                        tracing::info!("message channel closed, client is shutting down");
                        break;
                    }
                }
            }
        }
    }

    tracing::trace!("stop sending messages to WebSocket stream");
}

async fn send_ping_message(ws_stream_sender: &mut WsStreamSender) -> Result<(), Error> {
    ws_stream_sender
        .send(Message::Ping(b"TAOS".to_vec()))
        .await
        .map_err(Into::<Error>::into)?;

    ws_stream_sender.flush().await.map_err(Into::<Error>::into)
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
    query_sender: WsQuerySender,
    mut close_reader: watch::Receiver<bool>,
    err_sender: mpsc::Sender<Error>,
    cache: MessageCache,
) {
    tracing::trace!("start reading messages from WebSocket stream");

    let (message_tx, message_rx) = mpsc::channel(64);

    let message_handle =
        tokio::spawn(handle_messages(message_rx, query_sender.clone(), cache).in_current_span());

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
                tracing::info!("WebSocket reader received close signal");
                break;
            }
        }
    }

    drop(message_tx);

    if let Err(err) = message_handle.await {
        tracing::error!("handle messages task failed: {err:?}");
    }

    tracing::trace!("stop reading messages from WebSocket stream");
}

async fn handle_messages(
    mut message_reader: mpsc::Receiver<Message>,
    query_sender: WsQuerySender,
    cache: MessageCache,
) {
    while let Some(message) = message_reader.recv().await {
        parse_message(message, query_sender.clone(), cache.clone());
    }
}

fn parse_message(message: Message, query_sender: WsQuerySender, cache: MessageCache) {
    match message {
        Message::Text(text) => parse_text_message(text, query_sender, cache),
        Message::Binary(data) => parse_binary_message(data, query_sender, cache),
        Message::Ping(data) => {
            tokio::spawn(async move {
                let _ = query_sender
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

fn parse_text_message(text: String, query_sender: WsQuerySender, cache: MessageCache) {
    tracing::trace!("received text message, text: {text}");

    let resp = match serde_json::from_str::<WsRecv>(&text) {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!("failed to deserialize json text: {text}, err: {err:?}");
            return;
        }
    };

    let (req_id, data, ok) = resp.ok();

    cache.remove(&req_id);

    match &data {
        WsRecvData::Conn
        | WsRecvData::Version { .. }
        | WsRecvData::Block { .. }
        | WsRecvData::BlockNew { .. }
        | WsRecvData::BlockV2 { .. } => unreachable!("unexpected data type: {:?}", data),
        _ => {
            if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
                if let Err(err) = sender.send(ok.map(|_| data)) {
                    tracing::warn!("failed to send data, req_id: 0x{req_id:x}, err: {err:?}");
                }
            } else {
                tracing::warn!("no sender found for req_id: 0x{req_id:x}, the message may be lost");
            }
        }
    }
}

fn parse_binary_message(data: Vec<u8>, query_sender: WsQuerySender, cache: MessageCache) {
    tracing::trace!("received binary message, len: {}", data.len());

    let is_v3 = query_sender.version_info.is_v3();

    tokio::spawn(async move {
        use taos_query::util::InlinableRead;

        let mut slice = data.as_slice();
        let mut is_block_new = false;

        let timing = if is_v3 {
            let timing = slice.read_u64().unwrap();
            if timing == u64::MAX {
                is_block_new = true;
                Duration::ZERO
            } else {
                Duration::from_nanos(timing)
            }
        } else {
            Duration::ZERO
        };

        let (req_id, data) = if is_block_new {
            let _action = slice.read_u64().unwrap();
            let block_version = slice.read_u16().unwrap();
            let timing = Duration::from_nanos(slice.read_u64().unwrap());
            let block_req_id = slice.read_u64().unwrap();
            let block_code = slice.read_u32().unwrap();
            let block_message = slice.read_inlined_str::<4>().unwrap();
            let _res_id = slice.read_u64().unwrap();
            let finished = slice.read_u8().unwrap() == 1;
            let block = if finished {
                Vec::new()
            } else {
                slice.read_inlined_bytes::<4>().unwrap()
            };

            cache.remove(&block_req_id);

            (
                block_req_id,
                WsRecvData::BlockNew {
                    block_version,
                    timing,
                    block_req_id,
                    block_code,
                    block_message,
                    finished,
                    raw: block,
                },
            )
        } else {
            let res_id = slice.read_u64().unwrap();
            if let Some((_, req_id)) = query_sender.results.remove(&res_id) {
                cache.remove(&req_id);

                let data = if is_v3 {
                    WsRecvData::Block {
                        timing,
                        raw: data[16..].to_vec(),
                    }
                } else {
                    WsRecvData::BlockV2 {
                        timing,
                        raw: data[8..].to_vec(),
                    }
                };

                (req_id, data)
            } else {
                tracing::warn!("no request id found for result id: {res_id}");
                return;
            }
        };

        if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
            if let Err(err) = sender.send(Ok(data)) {
                tracing::warn!("failed to send data, req_id: 0x{req_id:x}, err: {err:?}");
            }
        } else {
            tracing::warn!("no sender found for req_id: 0x{req_id:x}, the message may be lost");
        }
    });
}

fn is_disconnect_error(err: &Error) -> bool {
    match err {
        Error::TungsteniteError(err) => matches!(
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

fn cleanup_after_disconnect(query_sender: WsQuerySender) {
    let mut req_ids = Vec::with_capacity(query_sender.queries.len());
    query_sender.queries.scan(|req_id, _| {
        req_ids.push(*req_id);
    });

    for req_id in req_ids {
        if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
            let _ = sender.send(Err(RawError::from_code(WS_ERROR_NO::CONN_CLOSED.as_code())
                .context("WebSocket connection is closed (disconnect)")));
        }
    }
}

fn cleanup_after_reconnect(
    query_sender: WsQuerySender,
    cache: MessageCache,
    stmt2_req_ids: Vec<ReqId>,
) {
    let mut req_ids = HashSet::new();

    query_sender.queries.scan(|req_id, _| {
        if !cache.req_to_msg.contains(req_id) && !stmt2_req_ids.contains(req_id) {
            req_ids.insert(*req_id);
        }
    });

    query_sender.results.scan(|_, req_id| {
        req_ids.insert(*req_id);
    });

    query_sender.results.clear();

    for req_id in req_ids {
        if let Some((_, sender)) = query_sender.queries.remove(&req_id) {
            let _ = sender.send(Err(RawError::from_code(WS_ERROR_NO::CONN_CLOSED.as_code())
                .context("WebSocket connection is closed (reconnect)")));
        }
    }
}

fn cleanup_stmt2(
    message_sender: Sender<WsMessage>,
    message_reader: Receiver<WsMessage>,
) -> Vec<ReqId> {
    let mut dropped_cnt = 0;
    let mut req_ids = Vec::new();
    let mut kept_messages = Vec::new();

    while let Ok(message) = message_reader.try_recv() {
        if message.is_stmt2() {
            dropped_cnt += 1;
            if !message.is_stmt2_close() {
                req_ids.push(message.req_id());
            }
            tracing::trace!("drop stmt2 message: {message:?}");
        } else {
            kept_messages.push(message);
        }
    }

    for message in kept_messages {
        let _ = message_sender.send(message);
    }

    if dropped_cnt > 0 {
        tracing::info!("dropped {dropped_cnt} stmt2 messages during cleanup");
    }

    req_ids
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use serde_json::{json, Value};
    use taos_query::{AsyncQueryable, AsyncTBuilder};
    use tokio::task::JoinHandle;
    use warp::ws::Message;
    use warp::Filter;

    use crate::query::ConnOption;
    use crate::TaosBuilder;

    #[tokio::test]
    async fn test_ws_auto_reconnect() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let query_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn("ws://127.0.0.1:9980")?
                .build()
                .await?;

            let options = &[
                ConnOption {
                    option: 2,
                    value: Some("127.0.0.1".to_string()),
                },
                ConnOption {
                    option: 3,
                    value: Some("test".to_string()),
                },
            ];

            taos.client().options_connection(options).await?;

            let _ = taos.query("insert into meters values(now, 0)").await?;
            let _ = taos.query("insert into meters values(now, 0)").await?;

            Ok(())
        });

        let (close_tx, close_rx) = flume::bounded(1);
        let query_cnt = Arc::new(AtomicUsize::new(0));

        let routes = warp::path("ws").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                let query_cnt = query_cnt.clone();

                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let query_cnt = query_cnt;
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::debug!("ws recv message: {message:?}");
                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            let req = serde_json::from_str::<Value>(text).unwrap();
                            let req_id = req
                                .get("args")
                                .and_then(|v| v.get("req_id"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0);

                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "version",
                                    "req_id": req_id,
                                    "version": "3.0"
                                });
                                let message = Message::text(data.to_string());
                                let _ = ws_tx.send(message).await;
                            } else if text.contains("options_connection") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "options_connection",
                                    "req_id": req_id,
                                    "timing": 99,
                                });
                                let message = Message::text(data.to_string());
                                let _ = ws_tx.send(message).await;
                            } else if text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "conn",
                                    "req_id": req_id
                                });
                                let message = Message::text(data.to_string());
                                let _ = ws_tx.send(message).await;
                            } else if text.contains("query") {
                                let cnt = query_cnt.fetch_add(1, Ordering::Relaxed);
                                if cnt == 0 {
                                    let _ = close.send_async(()).await;
                                    break;
                                } else if cnt == 1 {
                                    let data = json!({
                                        "code": 0,
                                        "message": "",
                                        "action": "binary_query",
                                        "req_id": req_id,
                                        "timing": 4369543,
                                        "id": 0,
                                        "is_update": true,
                                        "affected_rows": 1,
                                        "fields_count": 0,
                                        "fields_names": null,
                                        "fields_types": null,
                                        "fields_lengths": null,
                                        "precision": 0,
                                        "fields_precisions": null,
                                        "fields_scales": null
                                    });
                                    let message = Message::text(data.to_string());
                                    let _ = ws_tx.send(message).await;
                                } else if cnt == 2 {
                                    let data = json!({
                                        "code": 0,
                                        "message": "",
                                        "action": "binary_query",
                                        "req_id": req_id,
                                        "timing": 4369543,
                                        "id": 0,
                                        "is_update": true,
                                        "affected_rows": 1,
                                        "fields_count": 0,
                                        "fields_names": null,
                                        "fields_types": null,
                                        "fields_lengths": null,
                                        "precision": 0,
                                        "fields_precisions": null,
                                        "fields_scales": null
                                    });
                                    let message = Message::text(data.to_string());
                                    let _ = ws_tx.send(message).await;
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
            ([127, 0, 0, 1], 9980),
            async move {
                let _ = close.recv_async().await;
                tracing::debug!("server1 shutting down...");
            },
        );

        server1.await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let (_, server2) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 9980), async move {
                let _ = close_rx.recv_async().await;
                tracing::debug!("server2 shutting down...");
            });

        server2.await;

        query_handle.await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_failover() {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let _handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn("ws://127.0.0.1:9987,127.0.0.1:9986")?
                .build()
                .await?;
            let _ = taos.query("select * from meters").await?;
            Ok(())
        });

        let (close_tx, close_rx) = flume::bounded(1);

        let routes = warp::path("ws").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::debug!("ws recv message: {message:?}");
                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "version",
                                    "req_id": 100,
                                    "version": "3.0"
                                });
                                let message = Message::text(data.to_string());
                                let _ = ws_tx.send(message).await;
                            } else if text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "conn",
                                    "req_id": 100
                                });
                                let message = Message::text(data.to_string());
                                let _ = ws_tx.send(message).await;
                            } else if text.contains("query") {
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
            ([127, 0, 0, 1], 9986),
            async move {
                let _ = close.recv_async().await;
                tracing::debug!("server1 shutting down...");
            },
        );

        let (_, server2) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 9987), async move {
                let _ = close_rx.recv_async().await;
                tracing::debug!("server2 shutting down...");
            });

        tokio::join!(server1, server2);
    }

    #[tokio::test]
    async fn test_connect_failed() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let res = TaosBuilder::from_dsn("ws://127.0.0.1:9982,127.0.0.1:9983")?
            .build()
            .await;

        let errstr = res.unwrap_err().to_string();
        assert!(errstr.contains("IO error: Connection refused"));

        Ok(())
    }

    #[tokio::test]
    async fn test_conn_retries() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .try_init();

        let res = TaosBuilder::from_dsn("ws://127.0.0.1:9978,127.0.0.1:9979?conn_retries=0")?
            .build()
            .await;

        let errstr = res.unwrap_err().to_string();
        assert!(errstr.contains("IO error: Connection refused"));

        let res = TaosBuilder::from_dsn("ws://127.0.0.1:9978,127.0.0.1:9979?conn_retries=1")?
            .build()
            .await;

        let errstr = res.unwrap_err().to_string();
        assert!(errstr.contains("IO error: Connection refused"));

        let res = TaosBuilder::from_dsn("ws://127.0.0.1:9978,127.0.0.1:9979?conn_retries=2")?
            .build()
            .await;

        let errstr = res.unwrap_err().to_string();
        assert!(errstr.contains("IO error: Connection refused"));

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_backoff() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .try_init();

        let res = TaosBuilder::from_dsn(
            "ws://127.0.0.1:9978?conn_retries=10&retry_backoff_ms=200&retry_backoff_max_ms=2000",
        )?
        .build()
        .await;

        let errstr = res.unwrap_err().to_string();
        assert!(errstr.contains("IO error: Connection refused"));

        Ok(())
    }

    #[tokio::test]
    async fn test_wstaos_drop_closes_connection() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .try_init();

        let conn_dropped = Arc::new(AtomicBool::new(false));
        let conn_dropped_clone = conn_dropped.clone();

        let routes = warp::path("ws")
            .and(warp::ws())
            .map(move |ws: warp::ws::Ws| {
                let conn_dropped = conn_dropped_clone.clone();
                ws.on_upgrade(move |ws| async move {
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::debug!("ws recv message: {message:?}");

                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            let req: Value = serde_json::from_str(text).unwrap();
                            let req_id = req
                                .get("args")
                                .and_then(|v| v.get("req_id"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0);

                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "version",
                                    "req_id": req_id,
                                    "version": "3.0"
                                });
                                let _ = ws_tx.send(Message::text(data.to_string())).await;
                            } else if text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "conn",
                                    "req_id": req_id
                                });
                                let _ = ws_tx.send(Message::text(data.to_string())).await;
                            }
                        } else if message.is_close() {
                            tracing::info!("received close message from client");
                            conn_dropped.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                })
            });

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 9984), async move {
                let _ = shutdown_rx.await;
            });

        tokio::spawn(server);
        tokio::time::sleep(Duration::from_millis(100)).await;

        {
            let _taos = TaosBuilder::from_dsn("ws://127.0.0.1:9984")?
                .build()
                .await?;
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(conn_dropped.load(Ordering::Relaxed));
        let _ = shutdown_tx.send(());
        Ok(())
    }
}
