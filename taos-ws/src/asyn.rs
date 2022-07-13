use futures::stream::SplitSink;
use futures::{FutureExt, SinkExt, StreamExt};
use scc::HashMap;
// use std::sync::Mutex;
use taos_query::common::{Field, Precision, Raw};
use taos_query::{AsyncFetchable, AsyncQueryable, DeError, DsnError, IntoDsn};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::{oneshot, watch};

use tokio::time;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::{infra::*, WsInfo};

use std::fmt::Debug;
use std::result::Result as StdResult;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

type WsFetchResult = std::result::Result<WsFetchData, taos_error::Error>;
type FetchSender = std::sync::mpsc::SyncSender<WsFetchResult>;
type FetchReceiver = std::sync::mpsc::Receiver<WsFetchResult>;
// type WsSenderStream = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

type WsSender = tokio::sync::mpsc::Sender<Message>;

pub struct WsAsyncClient {
    req_id: Arc<AtomicU64>,
    ws: WsSender,
    close_signal: watch::Sender<bool>,
    queries:
        Arc<HashMap<ReqId, oneshot::Sender<std::result::Result<WsQueryResp, taos_error::Error>>>>,
    fetches: Arc<HashMap<ResId, FetchSender>>,
}

pub struct ResultSet {
    ws: WsSender,
    fetches: Arc<HashMap<ResId, FetchSender>>,
    receiver: Option<FetchReceiver>,
    args: WsResArgs,
    fields: Option<Vec<Field>>,
    fields_count: usize,
    affected_rows: usize,
    precision: Precision,
}
impl Debug for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultSet")
            .field("ws", &"...")
            .field("fetches", &"...")
            .field("receiver", &self.receiver)
            .field("args", &self.args)
            .field("fields", &self.fields)
            .field("fields_count", &self.fields_count)
            .field("affected_rows", &self.affected_rows)
            .field("precision", &self.precision)
            .finish()
    }
}
pub struct ResultSetRef {
    ws: WsSender,
    fetches: Arc<HashMap<ResId, FetchSender>>,
    receiver: Option<FetchReceiver>,
    args: WsResArgs,
    fields: Option<Vec<Field>>,
    fields_count: usize,
    affected_rows: usize,
    precision: Precision,
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        if self.receiver.is_some() {
            self.fetches.remove(&self.args.id);
            let args = self.args;
            let ws = self.ws.clone();
            tokio::spawn(async move {
                let _ = ws.send(WsSend::Close(args).to_msg()).await;
            });
        }
    }
}

impl Debug for WsAsyncClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("req_id", &self.req_id)
            .field("...", &"...")
            .finish()
    }
}
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Dsn(#[from] DsnError),
    #[error("{0}")]
    FetchError(#[from] oneshot::error::RecvError),
    #[error("{0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error("{0}")]
    StdSendError(#[from] std::sync::mpsc::SendError<tokio_tungstenite::tungstenite::Message>),
    #[error("{0}")]
    RecvError(#[from] std::sync::mpsc::RecvError),
    #[error("{0}")]
    TaosError(#[from] taos_error::Error),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("{0}")]
    WsError(#[from] WsError),
}

impl Error {
    pub const fn errno(&self) -> taos_error::Code {
        match self {
            Error::TaosError(error) => error.code(),
            _ => taos_error::Code::Failed,
        }
    }
    pub fn errstr(&self) -> String {
        match self {
            Error::TaosError(error) => error.message().to_string(),
            _ => format!("{}", self),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

impl Drop for WsAsyncClient {
    fn drop(&mut self) {
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

impl WsAsyncClient {
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///
    pub async fn from_dsn(dsn: impl IntoDsn) -> Result<Self> {
        let dsn = dsn.into_dsn()?;
        let info = WsInfo::from_dsn(dsn)?;
        Self::from_wsinfo(&info).await
    }
    pub(crate) async fn from_wsinfo(info: &WsInfo) -> Result<Self> {
        let (ws, _) = connect_async(dbg!(info.to_query_url())).await?;
        let req_id = 0;
        let (mut sender, mut reader) = ws.split();

        let login = WsSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        sender.send(login.to_msg()).await?;
        if let Some(Ok(message)) = reader.next().await {
            match message {
                Message::Text(text) => {
                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                    let (req_id, data, ok) = v.ok();
                    match data {
                        WsRecvData::Conn => ok?,
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }

        use std::collections::hash_map::RandomState;

        let queries = Arc::new(HashMap::<ReqId, tokio::sync::oneshot::Sender<_>>::new(
            100,
            RandomState::new(),
        ));

        let fetches = Arc::new(HashMap::<ResId, FetchSender>::new(100, RandomState::new()));

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel(100);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(10));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        //
                        // println!("10ms passed");
                    }
                    Some(msg) = msg_recv.recv() => {
                        dbg!(&msg);
                        sender.send(msg).await.unwrap();
                    }
                    _ = rx.changed() => {
                        log::info!("close sender task");
                        break;
                    }
                }
            }
        });

        // message handler for query/fetch/fetch_block
        tokio::spawn(async move {
            let mut bytes: Vec<u8> = Vec::new();
            let mut len_requires = 0usize;
            let mut full = false;
            loop {
                tokio::select! {
                    Some(message) = reader.next() => {
                        match message {
                            Ok(message) => match message {
                                Message::Text(text) => {
                                    dbg!(&text);
                                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                                    let (req_id, data, ok) = v.ok();
                                    match data {
                                        WsRecvData::Conn => todo!(),
                                        WsRecvData::Query(query) => {
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(ok.map(|_|query)).unwrap();
                                            }
                                        }
                                        WsRecvData::Fetch(fetch) => {
                                            let id = fetch.id;
                                            if fetch.completed {
                                                ws2.send(
                                                    WsSend::Close(WsResArgs {
                                                        req_id,
                                                        id,
                                                    })
                                                    .to_msg(),
                                                )
                                                .await
                                                .unwrap();
                                            }
                                            let data = ok.map(|_|WsFetchData::Fetch(fetch));
                                            if let Some(v) = fetches_sender.read(&id, |_, v| v.clone()) {
                                                log::info!("send data to fetches with id {}", id);
                                                v.send(data).unwrap();
                                            }
                                        }
                                        // Block type is for binary.
                                        WsRecvData::Block(_) => unreachable!(),
                                    }
                                }
                                Message::Binary(block) => {
                                    dbg!(block.len(), &block);
                                    let mut slice = block.as_slice();
                                    use taos_query::util::InlinableRead;
                                    let res_id = slice.read_u64().unwrap();
                                    let len = (&block[8..12]).read_u32().unwrap();
                                    if block.len() == len as usize + 8 {
                                        // v3
                                        if let Some(_) = fetches_sender.read(&res_id, |_, v| {
                                            log::info!("send data to fetches with id {}", res_id);
                                            // let raw = slice.read_inlinable::<RawBlock>().unwrap();
                                            v.send(Ok(WsFetchData::Block(block[8..].to_vec()).clone())).unwrap();
                                        }) {}
                                    } else {
                                        // v2
                                        log::warn!("the block is in format v2");
                                        if let Some(_) = fetches_sender.read(&res_id, |_, v| {
                                            log::info!("send data to fetches with id {}", res_id);
                                            v.send(Ok(WsFetchData::BlockV2(block[8..].to_vec()))).unwrap();
                                        }) {}
                                    }



                                }
                                Message::Close(_) => {
                                    log::warn!("websocket connection is closed (unexpected?)");
                                    break;
                                }
                                Message::Ping(bytes) => {
                                    ws2.send(Message::Pong(bytes)).await.unwrap();
                                }
                                Message::Pong(_) => {
                                    // do nothing
                                    log::warn!("received (unexpected) pong message, do nothing");
                                }
                                Message::Frame(frame) => {
                                    // do nothing
                                    log::warn!("received (unexpected) frame message, do nothing");
                                    log::debug!("* frame data: {frame:?}");
                                }
                            },
                            Err(err) => {
                                dbg!(err);
                            }
                        }
                    }
                    _ = close_listener.changed() => {
                        log::info!("close reader task");
                        break
                    }
                }
            }
        });

        Ok(Self {
            req_id: Arc::new(AtomicU64::new(req_id + 1)),
            queries,
            fetches,
            ws,
            close_signal: tx,
        })
    }

    fn req_id(&self) -> u64 {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn s_query(&self, sql: &str) -> Result<ResultSet> {
        let req_id = self.req_id();
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        let (tx, rx) = oneshot::channel();
        {
            self.queries.insert(req_id, tx).unwrap();
            self.ws.send(action.to_msg()).await?;
        }
        let resp = rx.await??;

        if resp.fields_count > 0 {
            let names = resp.fields_names.unwrap();
            let types = resp.fields_types.unwrap();
            let bytes = resp.fields_lengths.unwrap();
            let fields: Vec<_> = names
                .into_iter()
                .zip(types)
                .zip(bytes)
                .map(|((name, ty), bytes)| Field::new(name, ty, bytes))
                .collect();

            let (sender, receiver) = std::sync::mpsc::sync_channel(2);
            self.fetches.insert(resp.id, sender).unwrap();
            Ok(ResultSet {
                ws: self.ws.clone(),
                fetches: self.fetches.clone(),
                receiver: Some(receiver),
                fields: Some(fields),
                fields_count: resp.fields_count,
                precision: resp.precision,
                affected_rows: resp.affected_rows,
                args: WsResArgs {
                    req_id,
                    id: resp.id,
                },
            })
        } else {
            Ok(ResultSet {
                affected_rows: resp.affected_rows,
                ws: self.ws.clone(),
                fetches: self.fetches.clone(),
                receiver: None,
                args: WsResArgs {
                    req_id,
                    id: resp.id,
                },
                fields: None,
                fields_count: 0,
                precision: resp.precision,
            })
        }
    }

    pub async fn s_exec(&self, sql: &str) -> Result<usize> {
        let req_id = self.req_id();
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        let (tx, rx) = oneshot::channel();
        {
            self.queries.insert(req_id, tx).unwrap();
            self.ws.send(action.to_msg()).await?;
        }
        let resp = rx.await??;
        Ok(resp.affected_rows)
    }
}

impl ResultSetRef {
    async fn fetch(&mut self) -> Result<Option<Raw>> {
        let fetch = WsSend::Fetch(self.args);
        {
            log::info!("send fetch message: {fetch:?}");
            self.ws.send(fetch.to_msg()).await?;
            log::info!("send done");
            // unlock mutex when out of scope.
        }
        println!("wait for fetch message");
        let fetch_resp = match self.receiver.as_mut().unwrap().recv()?? {
            WsFetchData::Fetch(fetch) => fetch,
            data => panic!("unexpected result {data:?}"),
        };

        if fetch_resp.completed {
            return Ok(None);
        }

        log::info!("fetch with: {fetch_resp:?}");

        let fetch_block = WsSend::FetchBlock(self.args);
        {
            // prepare for receiving.
            log::info!("send fetch message: {fetch_block:?}");
            self.ws.send(fetch_block.to_msg()).await?;
            log::info!("send done");
            // unlock mutex when out of scope.
        }

        log::info!("receiving block...");
        match self.receiver.as_mut().unwrap().recv()?? {
            WsFetchData::Block(mut raw) => {
                let mut raw = Raw::parse_from_raw_block(
                    raw,
                    fetch_resp.rows,
                    self.fields_count,
                    self.precision,
                );

                for row in 0..raw.nrows() {
                    for col in 0..raw.ncols() {
                        log::debug!("at ({}, {})", row, col);
                        let v = unsafe { raw.get_ref_unchecked(row, col) };
                        println!("({}, {}): {:?}", row, col, v);
                    }
                }
                raw.with_fields(self.fields.as_ref().unwrap().to_vec());
                Ok(Some(raw))
            }
            WsFetchData::BlockV2(raw) => {
                let mut raw = Raw::parse_from_raw_block_v2(
                    raw,
                    self.fields.as_ref().unwrap(),
                    dbg!(fetch_resp.lengths.as_ref().unwrap()),
                    fetch_resp.rows,
                    self.precision,
                );

                for row in 0..raw.nrows() {
                    for col in 0..raw.ncols() {
                        log::debug!("at ({}, {})", row, col);
                        let v = unsafe { raw.get_ref_unchecked(row, col) };
                        println!("({}, {}): {:?}", row, col, v);
                    }
                }
                raw.with_fields(self.fields.as_ref().unwrap().to_vec());
                Ok(Some(raw))
            }
            _ => Ok(None),
        }
    }
}

impl futures::Stream for ResultSetRef {
    type Item = Raw;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.fetch()
            .map(|v| v.ok().unwrap_or(None))
            .boxed()
            .poll_unpin(cx)
    }
}

impl AsyncFetchable for ResultSet {
    fn affected_rows(&self) -> i32 {
        self.affected_rows as i32
    }

    fn precision(&self) -> taos_query::common::Precision {
        self.precision
    }

    fn fields(&self) -> &[Field] {
        self.fields.as_ref().unwrap()
    }

    fn summary(&self) -> (usize, usize) {
        todo!()
    }

    type BlockStream = ResultSetRef;

    fn block_stream(&mut self) -> Self::BlockStream {
        ResultSetRef {
            ws: self.ws.clone(),
            fetches: self.fetches.clone(),
            receiver: self.receiver.take(),
            args: self.args,
            fields: self.fields.clone(),
            fields_count: self.fields_count,
            affected_rows: self.affected_rows,
            precision: self.precision,
        }
    }
}

#[async_trait::async_trait]
impl<'q> AsyncQueryable<'q> for WsAsyncClient {
    type Error = Error;

    type AsyncResultSet = ResultSet;

    async fn query<T: AsRef<str> + Send + Sync>(
        &'q self,
        sql: T,
    ) -> StdResult<Self::AsyncResultSet, Self::Error> {
        self.s_query(sql.as_ref()).await
    }
}

// Websocket tests should always use `multi_thread`

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_client() -> anyhow::Result<()> {
    use futures::TryStreamExt;
    std::env::set_var("RUST_LOG", "debug");
    let dsn = std::env::var("TDENGINE_ClOUD_DSN").unwrap_or("http://localhost:6041".to_string());
    // pretty_env_logger::init();

    let client = WsAsyncClient::from_dsn(dsn).await?;
    assert_eq!(client.exec("drop database if exists abc_a").await?, 0);
    assert_eq!(client.exec("create database abc_a").await?, 0);
    assert_eq!(
        client
            .exec("create table abc_a.tb1(ts timestamp, v int)")
            .await?,
        0
    );
    assert_eq!(
        client
            .exec("insert into abc_a.tb1 values(1655793421375, 1)")
            .await?,
        1
    );

    // let mut rs = client.s_query("select * from abc_a.tb1").unwrap().unwrap();
    let mut rs = client.query("select * from abc_a.tb1").await?;

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct A {
        ts: String,
        v: i32,
    }

    let values: Vec<A> = rs.deserialize_stream().try_collect().await?;

    dbg!(values);

    assert_eq!(client.exec("drop database abc_a").await?, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_client_cloud() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    // pretty_env_logger::init();
    let dsn = std::env::var("TDENGINE_ClOUD_DSN");
    if dsn.is_err() {
        println!("Skip test when not in cloud");
        return Ok(());
    }
    let dsn = dsn.unwrap();
    let client = WsAsyncClient::from_dsn(dsn).await?;
    let mut rs = client.query("select * from test.meters limit 10").await?;

    let values = rs.to_records();
    for row in values {
        use itertools::Itertools;
        println!(
            "{}",
            row.into_iter().map(|value| format!("{value}")).join(" | ")
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn ws_show_databases() -> anyhow::Result<()> {
    use taos_query::{Fetchable, Queryable};
    let client = WsAsyncClient::from_dsn(
        "https://gw-aws.cloud.tdengine.com?token=8c7a628b568b7d32cc50f36b0f2d6273ffd060fc",
    )
    .await?;
    let mut rs = client.query("show databases").await?;
    let values = rs.to_records();

    dbg!(values);
    Ok(())
}
