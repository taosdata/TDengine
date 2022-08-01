use once_cell::sync::OnceCell;
use taos_error::Code;
use taos_query::common::{Field, Precision, RawBlock, RawMeta};
use taos_query::{DeError, DsnError, Fetchable, IntoDsn, Queryable};
use thiserror::Error;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::stmt::sync::{WsSyncStmt, WsSyncStmtClient};
use crate::{infra::*, stmt, TaosBuilder};

use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ops::AddAssign;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use scc::HashMap;

type WsFetchResult = std::result::Result<WsFetchData, taos_error::Error>;
type WsQueryResult = std::result::Result<WsQueryResp, taos_error::Error>;

type QuerySender = std::sync::mpsc::SyncSender<WsQueryResult>;

type FetchSender = std::sync::mpsc::SyncSender<WsFetchResult>;
type FetchReceiver = std::sync::mpsc::Receiver<WsFetchResult>;

// type MsgSender = std::sync::mpsc::SyncSender<WsSend>;

type MsgSender = tokio::sync::mpsc::Sender<WsSend>;

pub struct MsgReceiver(std::sync::mpsc::Receiver<WsSend>);
unsafe impl Send for MsgReceiver {}
unsafe impl Sync for MsgReceiver {}

pub struct WsAuth {
    user: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

pub struct WsClient {
    info: TaosBuilder,
    timeout: Duration,
    version: String,
    req_id: Arc<AtomicU64>,
    sender: MsgSender,
    queries: Arc<HashMap<ReqId, QuerySender>>,
    fetches: Arc<HashMap<ResId, FetchSender>>,
    stmt: OnceCell<WsSyncStmtClient>,
    rt: Arc<tokio::runtime::Runtime>,
    close_signal: watch::Sender<bool>,
    alive: Arc<AtomicBool>,
}

impl Drop for WsClient {
    fn drop(&mut self) {
        log::debug!("dropping client");
        // send close signal to reader/writer spawned tasks.
    }
}

#[derive(Debug)]
pub struct ResultSet {
    id: ResId,
    rt: Arc<tokio::runtime::Runtime>,
    timeout: Duration,
    sender: MsgSender,
    fetches: Arc<HashMap<ResId, FetchSender>>,
    receiver: Option<FetchReceiver>,
    args: WsResArgs,
    fields: Option<Vec<Field>>,
    fields_count: usize,
    affected_rows: usize,
    precision: Precision,
    alive: Arc<AtomicBool>,
    timing: UnsafeCell<Duration>,
    summary: UnsafeCell<(usize, usize)>,
}

impl Debug for WsClient {
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
    DeError(#[from] DeError),
    #[error(transparent)]
    TungsteniteError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("{0}")]
    TaosError(#[from] taos_error::Error),
    #[error(transparent)]
    RecvTimeout(#[from] std::sync::mpsc::RecvTimeoutError),
    #[error(transparent)]
    SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<WsSend>),
    #[error("Connection reset or closed by server")]
    ConnClosed,
    #[error(transparent)]
    StmtError(#[from] stmt::Error),
}

#[repr(C)]
pub enum WS_ERROR_NO {
    DSN_ERROR = 0xE000,
    WEBSOCKET_ERROR = 0xE001,
    CONN_CLOSED = 0xE002,
    SEND_MESSAGE_TIMEOUT = 0xE003,
    RECV_MESSAGE_TIMEOUT = 0xE004,
}

impl Error {
    pub const fn errno(&self) -> taos_error::Code {
        match self {
            Error::TaosError(error) => error.code(),
            Error::Dsn(_) => Code::new(WS_ERROR_NO::DSN_ERROR as _),
            Error::TungsteniteError(_) => Code::new(WS_ERROR_NO::WEBSOCKET_ERROR as _),
            Error::SendTimeoutError(_) => Code::new(WS_ERROR_NO::SEND_MESSAGE_TIMEOUT as _),
            Error::RecvTimeout(_) => Code::new(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT as _),
            // Error::RecvFetchError(_) => Code::new(WS_ERROR_NO::RECV_TIMEOUT_FETCH as _),
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

impl WsClient {
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///
    pub fn from_dsn(dsn: impl IntoDsn) -> Result<Self> {
        let dsn = dsn.into_dsn()?;
        let info = TaosBuilder::from_dsn(dsn)?;
        Self::from_wsinfo(&info)
    }

    pub(crate) fn from_wsinfo(info: &TaosBuilder) -> Result<Self> {
        use futures::SinkExt;
        use futures::StreamExt;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let (ws, _) = rt.block_on(connect_async(info.to_query_url()))?;
        let (mut async_sender, mut async_reader) = ws.split();

        rt.block_on(async_sender.send(WsSend::Version.to_msg()))?;

        let _duration = Duration::from_secs(2);

        let get_version = async {
            let sleep = tokio::time::sleep(Duration::from_secs(1));
            tokio::pin!(sleep);
            tokio::select! {
                _ = &mut sleep, if !sleep.is_elapsed() => {
                   log::debug!("get server version timed out");
                   return None;
                }
                message = async_reader.next() => {
                    return message
                }
            };
        };
        let version = match rt.block_on(get_version) {
            Some(Ok(message)) => match message {
                Message::Text(text) => {
                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                    let (_, data, ok) = v.ok();
                    match data {
                        WsRecvData::Version { version } => {
                            ok?;
                            version
                        }
                        _ => "2.x".to_string(),
                    }
                }
                _ => "2.x".to_string(),
            },
            _ => "2.x".to_string(),
        };

        let req_id = 0;
        let login = WsSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };

        // let (mut receiver, mut sender) = client.split().unwrap();

        rt.block_on(async_sender.send(login.to_msg()))?;
        if let Some(Ok(message)) = rt.block_on(async_reader.next()) {
            match message {
                Message::Text(text) => {
                    let v: WsRecv = serde_json::from_str(&text).unwrap();
                    let (_, data, ok) = v.ok();
                    match data {
                        WsRecvData::Conn => ok?,
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }

        use std::collections::hash_map::RandomState;

        let queries = Arc::new(HashMap::<ReqId, QuerySender>::new(100, RandomState::new()));

        let fetches = Arc::new(HashMap::<ResId, FetchSender>::new(100, RandomState::new()));

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();

        // let msg_receiver = Arc::new(tokio::sync::Mutex::new(msg_receiver));
        let (msg_sender, mut msg_receiver) = tokio::sync::mpsc::channel(100);
        let tx2recv = msg_sender.clone();
        // let msg_receiver = MsgReceiver(msg_receiver);

        let (close_signal, mut close_recv) = watch::channel(false);

        let alive = Arc::new(AtomicBool::new(true));
        let alive1 = alive.clone();
        let alive2 = alive.clone();
        rt.spawn(async move {
            loop {
                tokio::select! {
                    message = msg_receiver.recv() => match message {
                        Some(WsSend::Pong(bytes)) => {
                            if let Err(err) = async_sender.send(Message::Pong(bytes)).await {
                                log::error!("send websocket message packet error: {}", err);
                                break;
                            } else {
                            }
                        }
                        Some(msg) => {
                            if let Err(err) = async_sender.send(msg.to_msg()).await {
                                log::error!("send websocket message packet error: {}", err);
                                break;
                            } else {
                            }
                        }
                        None => {
                            break;
                        }
                    },

                    _ = close_recv.changed() => {
                        log::error!("close sender task");
                        break;
                    }
                }
                // match msg_receiver.recv().await {}
            }
            alive1.fetch_and(false, std::sync::atomic::Ordering::SeqCst);
        });

        let is_v3 = version.starts_with("3");

        // message handler for query/fetch/fetch_block
        rt.spawn(async move {
            loop {
                if let Some(message) = async_reader.next().await {
                    if let Ok(message) = message {
                        match message {
                            Message::Text(text) => {
                                // dbg!(&text);
                                let v: WsRecv = serde_json::from_str(&text).unwrap();
                                let (req_id, data, ok) = v.ok();
                                match data {
                                    WsRecvData::Conn => todo!(),
                                    WsRecvData::Query(query) => {
                                        log::info!("query result: {:?}", query);
                                        if let Some(sender) = queries_sender.remove(&req_id) {
                                            if let Err(err) = sender.1.send(ok.map(|_| query)) {
                                                log::error!(
                                                    "Receiver lost for query {}: {}",
                                                    req_id,
                                                    err
                                                );
                                            }
                                        }
                                    }
                                    WsRecvData::Fetch(fetch) => {
                                        let res_id = fetch.id;
                                        log::info!("fetch result: {:?}", fetch);
                                        if let Some(sender) = fetches_sender
                                            .read(&fetch.id, |_, sender| sender.clone())
                                        {
                                            if let Err(err) =
                                                sender.send(ok.map(|_| WsFetchData::Fetch(fetch)))
                                            {
                                                log::error!(
                                                    "Receiver lost for result set ({}, {}): {}",
                                                    req_id,
                                                    res_id,
                                                    err
                                                );
                                            }
                                        }
                                    }
                                    // Block type is for binary.
                                    _ => unreachable!(),
                                }
                            }
                            Message::Binary(block) => {
                                log::debug!("fetch block with {} bytes.", block.len());
                                let mut slice = block.as_slice();
                                use taos_query::util::InlinableRead;
                                let timing = slice.read_u64().unwrap();
                                let timing = Duration::from_nanos(timing as _);
                                let res_id = slice.read_u64().unwrap();
                                if is_v3 {
                                    // v3
                                    log::debug!("parse v3 raw block");
                                    if let Some(v) = fetches_sender.read(&res_id, |_, v| v.clone())
                                    {
                                        log::debug!("send data to fetches with id {}", res_id);
                                        v.send(Ok(WsFetchData::Block(
                                            timing,
                                            block[16..].to_vec(),
                                        )
                                        .clone()))
                                            .unwrap();
                                    }
                                } else {
                                    // v2
                                    log::warn!("the block is in format v2");
                                    if let Some(v) = fetches_sender.read(&res_id, |_, v| v.clone())
                                    {
                                        log::debug!("send data to fetches with id {}", res_id);
                                        v.send(Ok(WsFetchData::BlockV2(
                                            timing,
                                            block[16..].to_vec(),
                                        )))
                                        .unwrap();
                                    }
                                }
                            }
                            Message::Close(_) => {
                                log::error!("received close message, stop");
                                break;
                            }
                            Message::Ping(bytes) => {
                                // let mut writer = tx2recv.lock().unwrap();
                                tx2recv.send(WsSend::Pong(bytes)).await.unwrap()
                            }
                            _ => {
                                // do nothing
                                log::error!("unexpected message, stop");
                                break;
                            }
                        }
                    } else {
                        let err = message.unwrap_err();
                        log::error!("connection seems closed: {}", err);
                        queries_sender
                            .retain_async(|_, v| {
                                let _ = v.send(Err(taos_error::Error::new(
                                    Code::new(WS_ERROR_NO::CONN_CLOSED as _),
                                    err.to_string(),
                                )));
                                false
                            })
                            .await;
                        fetches_sender
                            .retain_async(|_, v| {
                                let _ = v.send(Err(taos_error::Error::new(
                                    Code::new(WS_ERROR_NO::CONN_CLOSED as _),
                                    err.to_string(),
                                )));
                                false
                            })
                            .await;
                        break;
                    }
                }
            }
            alive2.fetch_and(false, std::sync::atomic::Ordering::SeqCst);
        });

        Ok(Self {
            timeout: Duration::from_secs(5),
            req_id: Arc::new(AtomicU64::new(req_id + 1)),
            queries,
            fetches,
            version,
            sender: msg_sender,
            rt: Arc::new(rt),
            stmt: OnceCell::<WsSyncStmtClient>::new(),
            info: info.clone(),
            close_signal,
            alive,
        })
    }

    pub fn close(&self) {
        let _ = self.close_signal.send(true);
    }

    fn req_id(&self) -> u64 {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    // todo: add server version getter.
    // pub fn server_version(&self) -> String {
    // }

    pub fn s_query(&self, sql: &str) -> Result<ResultSet> {
        self.s_query_timeout(sql, self.timeout)
    }
    pub fn s_query_timeout(&self, sql: &str, timeout: Duration) -> Result<ResultSet> {
        log::info!("query with sql: {sql}");
        let req_id = self.req_id();
        if !self.alive.load(std::sync::atomic::Ordering::SeqCst) {
            Err(taos_error::Error::new(
                Code::new(WS_ERROR_NO::CONN_CLOSED as _),
                "connection closed",
            ))?;
        }
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        {
            self.queries.insert(req_id, tx).unwrap();
            self.rt
                .block_on(self.sender.send_timeout(action, timeout))?;
        }
        let resp = rx.recv_timeout(timeout)??;

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

            let (tx, rx) = std::sync::mpsc::sync_channel(100);
            {
                self.fetches.insert(resp.id, tx).unwrap();
            }
            Ok(ResultSet {
                id: resp.id,
                rt: self.rt.clone(),
                timeout: self.timeout.clone(),
                sender: self.sender.clone(),
                fetches: self.fetches.clone(),
                receiver: Some(rx),
                fields: Some(fields),
                fields_count: resp.fields_count,
                precision: resp.precision,
                affected_rows: resp.affected_rows,
                args: WsResArgs {
                    req_id,
                    id: resp.id,
                },
                alive: self.alive.clone(),
                summary: UnsafeCell::default(),
                timing: UnsafeCell::new(resp.timing),
            })
        } else {
            Ok(ResultSet {
                id: resp.id,
                rt: self.rt.clone(),
                timeout: self.timeout.clone(),
                affected_rows: resp.affected_rows,
                sender: self.sender.clone(),
                fetches: self.fetches.clone(),
                receiver: None,
                args: WsResArgs {
                    req_id,
                    id: resp.id,
                },
                fields: None,
                fields_count: 0,
                precision: resp.precision,
                alive: self.alive.clone(),
                summary: UnsafeCell::default(),
                timing: UnsafeCell::new(resp.timing),
            })
        }
    }

    pub fn s_exec(&self, sql: &str) -> Result<usize> {
        self.s_exec_timeout(sql, self.timeout)
    }

    pub fn s_write_meta(&self, _: RawMeta) -> Result<()> {
        // todo: unimplemented
        todo!()
    }
    pub fn s_exec_timeout(&self, sql: &str, timeout: Duration) -> Result<usize> {
        if !self.alive.load(std::sync::atomic::Ordering::SeqCst) {
            Err(taos_error::Error::new(
                Code::new(WS_ERROR_NO::CONN_CLOSED as _),
                "connection closed",
            ))?;
        }
        let req_id = self.req_id();
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        {
            self.queries.insert(req_id, tx).unwrap();
            self.rt
                .block_on(self.sender.send_timeout(action, timeout))?;
        }
        let resp = rx.recv_timeout(timeout)??;
        Ok(resp.affected_rows)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn stmt_init(&self) -> Result<WsSyncStmt> {
        if !self.alive.load(std::sync::atomic::Ordering::SeqCst) {
            Err(taos_error::Error::new(
                Code::new(WS_ERROR_NO::CONN_CLOSED as _),
                "connection closed",
            ))?;
        }
        // let rt = rt.clone();
        let client = self
            .stmt
            .get_or_try_init(|| WsSyncStmtClient::new(&self.info, self.rt.clone()))?;
        Ok(client.stmt_init()?)
    }

    // pub fn s_stmt(&self) -> Result<>
}

impl ResultSet {
    fn summary(&self) -> (usize, usize) {
        unsafe { *self.summary.get() }
    }
    fn update_summary(&mut self, nrows: usize) {
        let summary = self.summary.get_mut();
        summary.0 += 1;
        summary.1 += nrows;
    }
    async fn fetch_block_a(&self) -> Result<Option<RawBlock>> {
        if self.receiver.is_none() {
            return Ok(None);
        }
        if !self.alive.load(std::sync::atomic::Ordering::SeqCst) {
            Err(taos_error::Error::new(
                Code::new(WS_ERROR_NO::CONN_CLOSED as _),
                "connection closed",
            ))?;
        }
        let rx = self.receiver.as_ref().unwrap();
        let fetch = WsSend::Fetch(self.args);

        self.sender.send_timeout(fetch, self.timeout).await?;

        let fetch_resp = if let WsFetchData::Fetch(fetch) = rx.recv_timeout(self.timeout)?? {
            fetch
        } else {
            unreachable!()
        };

        unsafe {
            let t = &mut *self.timing.get();
            t.add_assign(fetch_resp.timing);
        }

        if fetch_resp.completed {
            return Ok(None);
        }

        let fetch_block = WsSend::FetchBlock(self.args);

        self.sender.send_timeout(fetch_block, self.timeout).await?;

        match rx.recv_timeout(self.timeout)?? {
            WsFetchData::Block(timing, raw) => {
                let mut raw = RawBlock::parse_from_raw_block(
                    raw,
                    fetch_resp.rows,
                    self.fields_count,
                    self.precision,
                );

                for row in 0..raw.nrows() {
                    for col in 0..raw.ncols() {
                        let v = unsafe { raw.get_ref_unchecked(row, col) };
                        log::debug!("({}, {}): {:?}", row, col, v);
                    }
                }
                raw.with_field_names(self.fields.as_ref().unwrap().iter().map(Field::name));

                unsafe {
                    let t = &mut *self.timing.get();
                    t.add_assign(timing);
                }
                Ok(Some(raw))
            }
            WsFetchData::BlockV2(timing, raw) => {
                let mut raw = RawBlock::parse_from_raw_block_v2(
                    raw,
                    self.fields.as_ref().unwrap(),
                    fetch_resp.lengths.as_ref().unwrap(),
                    fetch_resp.rows,
                    self.precision,
                );

                for row in 0..raw.nrows() {
                    for col in 0..raw.ncols() {
                        let v = unsafe { raw.get_ref_unchecked(row, col) };
                        log::debug!("({}, {}): {:?}", row, col, v);
                    }
                }

                raw.with_field_names(self.fields.as_ref().unwrap().iter().map(Field::name));

                unsafe {
                    let t = &mut *self.timing.get();
                    t.add_assign(timing);
                }
                Ok(Some(raw))
            }
            _ => Ok(None),
        }
    }
    pub fn fetch_block(&mut self) -> Result<Option<RawBlock>> {
        let rt = &self.rt;
        let future = self.fetch_block_a();
        rt.block_on(future)
    }

    pub fn take_timing(&mut self) -> Duration {
        let timing = *self.timing.get_mut();
        self.timing = UnsafeCell::new(Duration::ZERO);
        timing
    }

    pub fn stop_query(&mut self) {
        if let Some((_, sender)) = self.fetches.remove(&self.id) {
            sender
                .send(Err(taos_error::Error::from_string("").into()))
                .unwrap();
        }
    }
}
impl Iterator for ResultSet {
    type Item = Result<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_block().transpose()
    }
}

impl Fetchable for ResultSet {
    type Error = Error;
    fn affected_rows(&self) -> i32 {
        self.affected_rows as i32
    }

    fn precision(&self) -> taos_query::common::Precision {
        self.precision
    }

    fn fields(&self) -> &[Field] {
        static EMPTY: Vec<Field> = Vec::new();
        self.fields
            .as_ref()
            .map(|v| v.as_slice())
            .unwrap_or(EMPTY.as_slice())
    }

    fn summary(&self) -> (usize, usize) {
        self.summary()
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows)
    }

    fn fetch_raw_block(&mut self) -> Result<Option<RawBlock>> {
        self.fetch_block()
    }
}

impl Queryable for WsClient {
    type Error = Error;

    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> std::result::Result<Self::ResultSet, Self::Error> {
        self.s_query(sql.as_ref())
    }

    fn exec<T: AsRef<str>>(&self, sql: T) -> std::result::Result<usize, Self::Error> {
        self.s_exec(sql.as_ref())
    }

    fn write_meta(&self, raw: taos_query::common::RawMeta) -> std::result::Result<(), Self::Error> {
        self.s_write_meta(raw)
    }
}

#[test]
fn test_client() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "taos-query=trace,main=trace");
    pretty_env_logger::init();
    let client = WsClient::from_dsn("ws://localhost:6041/")?;
    let version = client.version();
    dbg!(version);

    assert_eq!(client.exec("create database if not exists abc")?, 0);
    assert_eq!(
        client.exec("create table if not exists abc.tb1(ts timestamp, v int)")?,
        0
    );
    assert_eq!(client.exec("insert into abc.tb1 values(now, 1)")?, 1);

    // let mut rs = client.s_query("select * from abc.tb1").unwrap().unwrap();
    let mut rs = client.query("select * from abc.tb1")?;

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct A {
        ts: String,
        v: i32,
    }

    use itertools::Itertools;
    let values: Vec<A> = rs.deserialize::<A>().try_collect()?;

    dbg!(values);

    assert_eq!(rs.summary(), (1, 1), "should got 1 block with 1 row");

    assert_eq!(client.exec("drop database abc")?, 0);
    Ok(())
}
