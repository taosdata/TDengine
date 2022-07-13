use once_cell::sync::{Lazy, OnceCell};
use taos_query::common::{Field, Precision, Raw};
use taos_query::{DeError, Dsn, DsnError, Fetchable, IntoDsn, Queryable};
use thiserror::Error;
use tokio::sync::watch;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::stmt::sync::{WsSyncStmt, WsSyncStmtClient};
use crate::{infra::*, stmt, WsInfo};

use std::any;
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc::Sender, Arc, Mutex};
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
    info: WsInfo,
    timeout: Duration,
    version: String,
    req_id: Arc<AtomicU64>,
    sender: MsgSender,
    queries: Arc<HashMap<ReqId, QuerySender>>,
    fetches: Arc<HashMap<ResId, FetchSender>>,
    stmt: OnceCell<WsSyncStmtClient>,
    rt: Arc<tokio::runtime::Runtime>,
    close_signal: watch::Sender<bool>,
}

impl Drop for WsClient {
    fn drop(&mut self) {
        print!("dropping client");
        // send close signal to reader/writer spawned tasks.
    }
}

pub struct ResultSet {
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
    #[error(transparent)]
    SendMessageError(#[from] tokio::sync::mpsc::error::SendError<WsSend>),
    #[error("{0}")]
    TaosError(#[from] taos_error::Error),
    #[error("{0}")]
    RecvFetchError(#[from] std::sync::mpsc::RecvError),
    #[error(transparent)]
    RecvTimeout(#[from] std::sync::mpsc::RecvTimeoutError),
    #[error(transparent)]
    SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<WsSend>),
    #[error(transparent)]
    StmtError(#[from] stmt::Error),
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

impl WsClient {
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///
    pub fn from_dsn(dsn: impl IntoDsn) -> Result<Self> {
        let dsn = dsn.into_dsn()?;
        let info = WsInfo::from_dsn(dsn)?;
        Self::from_wsinfo(&info)
    }

    pub(crate) fn from_wsinfo(info: &WsInfo) -> Result<Self> {
        use futures::SinkExt;
        use futures::StreamExt;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let (ws, _) = rt.block_on(connect_async(info.to_query_url()))?;
        let (mut async_sender, mut async_reader) = ws.split();

        rt.block_on(async_sender.send(WsSend::Version.to_msg()))?;

        let duration = Duration::from_secs(2);

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
                        _ => "version undetermined".to_string(),
                    }
                }
                _ => "version undetermined".to_string(),
            },
            _ => "version undetermined".to_string(),
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
                        println!("close all");
                        log::info!("close sender task");
                        break;
                    }
                }
                // match msg_receiver.recv().await {}
            }
        });

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
                                        if let Some(sender) = queries_sender.remove(&req_id) {
                                            sender.1.send(ok.map(|_| query)).unwrap();
                                        }
                                    }
                                    WsRecvData::Fetch(fetch) => {
                                        log::info!("fetch result: {:?}", fetch);
                                        if let Some(sender) = fetches_sender
                                            .read(&fetch.id, |_, sender| sender.clone())
                                        {
                                            sender
                                                .send(ok.map(|_| WsFetchData::Fetch(fetch)))
                                                .unwrap();
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
                                let res_id = slice.read_u64().unwrap();
                                let len = (&block[8..12]).read_u32().unwrap();
                                if block.len() == len as usize + 8 {
                                    // v3
                                    if let Some(v) = fetches_sender.read(&res_id, |_, v| v.clone())
                                    {
                                        log::info!("send data to fetches with id {}", res_id);
                                        // let raw = slice.read_inlinable::<RawBlock>().unwrap();
                                        v.send(Ok(WsFetchData::Block(block[8..].to_vec()).clone()))
                                            .unwrap();
                                    }
                                } else {
                                    // v2
                                    log::warn!("the block is in format v2");
                                    if let Some(v) = fetches_sender.read(&res_id, |_, v| v.clone())
                                    {
                                        log::info!("send data to fetches with id {}", res_id);
                                        v.send(Ok(WsFetchData::BlockV2(block[8..].to_vec())))
                                            .unwrap();
                                    }
                                }
                            }
                            Message::Close(_) => break,
                            Message::Ping(bytes) => {
                                // let mut writer = tx2recv.lock().unwrap();
                                tx2recv.send(WsSend::Pong(bytes)).await.unwrap()
                            }
                            _ => {
                                // do nothing
                            }
                        }
                    } else {
                        let err = message.unwrap_err();
                        log::error!("connection seems closed: {}", err);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            timeout: Duration::from_secs(10),
            req_id: Arc::new(AtomicU64::new(req_id + 1)),
            queries,
            fetches,
            version,
            sender: msg_sender,
            rt: Arc::new(rt),
            stmt: OnceCell::<WsSyncStmtClient>::new(),
            info: info.clone(),
            close_signal,
        })
    }

    pub fn close(&self) {
        let _ = self.close_signal.send(true).unwrap();
    }

    fn req_id(&self) -> u64 {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    // todo: add server version getter.
    // pub fn server_version(&self) -> String {
    // }

    pub fn s_query(&self, sql: &str) -> Result<ResultSet> {
        let req_id = self.req_id();
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        {
            self.queries.insert(req_id, tx).unwrap();
            self.sender.blocking_send(action).unwrap();
        }
        let resp = rx.recv_timeout(self.timeout)??;

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
            })
        } else {
            Ok(ResultSet {
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
            })
        }
    }

    pub fn s_exec(&self, sql: &str) -> Result<usize> {
        let req_id = self.req_id();
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        {
            self.queries.insert(req_id, tx).unwrap();
            self.sender.blocking_send(action).unwrap();
        }
        let resp = rx.recv()??;
        Ok(resp.affected_rows)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn stmt_init(&self) -> Result<WsSyncStmt> {
        // let rt = rt.clone();
        let client = self
            .stmt
            .get_or_try_init(|| WsSyncStmtClient::new(&self.info, self.rt.clone()))?;
        Ok(client.stmt_init()?)
    }

    // pub fn s_stmt(&self) -> Result<>
}

impl ResultSet {
    async fn fetch_block_a(&mut self) -> Result<Option<Raw>> {
        if self.receiver.is_none() {
            return Ok(None);
        }
        let rx = self.receiver.as_mut().unwrap();
        let fetch = WsSend::Fetch(self.args);

        self.sender.send_timeout(fetch, self.timeout).await?;

        let fetch_resp = if let WsFetchData::Fetch(fetch) = rx.recv_timeout(self.timeout)?? {
            fetch
        } else {
            unreachable!()
        };

        if fetch_resp.completed {
            return Ok(None);
        }

        let fetch_block = WsSend::FetchBlock(self.args);

        self.sender.send_timeout(fetch_block, self.timeout).await?;

        match rx.recv_timeout(self.timeout)?? {
            WsFetchData::Block(raw) => {
                let mut raw = Raw::parse_from_raw_block(
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
                raw.with_fields(self.fields.as_ref().unwrap().to_vec());
                Ok(Some(raw))
            }
            WsFetchData::BlockV2(raw) => {
                let mut raw = Raw::parse_from_raw_block_v2(
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
                raw.with_fields(self.fields.as_ref().unwrap().to_vec());
                Ok(Some(raw))
            }
            _ => Ok(None),
        }
    }
    pub fn fetch_block(&mut self) -> Result<Option<Raw>> {
        self.rt.clone().block_on(self.fetch_block_a())
    }
}
impl Iterator for ResultSet {
    type Item = Raw;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_block().unwrap_or_default()
    }
}

impl Fetchable for ResultSet {
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
        todo!()
    }
}

impl<'q> Queryable<'q> for WsClient {
    type Error = Error;

    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&'q self, sql: T) -> std::result::Result<Self::ResultSet, Self::Error> {
        self.s_query(sql.as_ref())
    }

    fn exec<T: AsRef<str>>(&'q self, sql: T) -> std::result::Result<usize, Self::Error> {
        self.s_exec(sql.as_ref())
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

    assert_eq!(client.exec("drop database abc")?, 0);
    Ok(())
}
