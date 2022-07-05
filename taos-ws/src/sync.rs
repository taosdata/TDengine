use taos_query::common::{Block, Field, Precision, RawBlock};
use taos_query::{DeError, Dsn, DsnError, Fetchable, IntoDsn, Queryable};
use thiserror::Error;
use websocket::sync::Writer;
use websocket::{stream::sync::TcpStream, sync::Client};
use websocket::{ClientBuilder, Message};

use crate::{infra::*, WsInfo};

use std::any;
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc::Sender, Arc, Mutex};

use scc::HashMap;

type WsFetchResult = std::result::Result<WsFetchData, taos_error::Error>;
type WsQueryResult = std::result::Result<WsQueryResp, taos_error::Error>;

type QuerySender = std::sync::mpsc::SyncSender<WsQueryResult>;

type FetchSender = std::sync::mpsc::SyncSender<WsFetchResult>;
type FetchReceiver = std::sync::mpsc::Receiver<WsFetchResult>;

type MsgSender = std::sync::mpsc::Sender<WsSend>;

pub struct WsAuth {
    user: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

pub struct WsClient {
    req_id: Arc<AtomicU64>,
    sender: MsgSender,
    queries: Arc<HashMap<ReqId, QuerySender>>,
    fetches: Arc<HashMap<ResId, FetchSender>>,
}

pub struct ResultSet {
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
    WsParse(#[from] websocket::client::ParseError),
    #[error("{0}")]
    WsConn(#[from] websocket::WebSocketError),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("{0}")]
    TaosError(#[from] taos_error::Error),
    #[error("{0}")]
    RecvFetchError(#[from] std::sync::mpsc::RecvError),
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
        let mut ws = ClientBuilder::new(&info.to_query_url())?;

        let client = ws.connect_insecure()?;

        let req_id = 0;
        let login = WsSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };

        let (mut receiver, mut sender) = client.split().unwrap();

        sender.send_message(&login.to_message()).unwrap();

        let recv = receiver.recv_message()?;

        // connect
        let _ = match recv {
            websocket::OwnedMessage::Text(text) => {
                let v: WsRecv = serde_json::from_str(&text).unwrap();
                let (_, data, ok) = v.ok();
                match data {
                    WsRecvData::Conn => ok?,
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        };

        // let sender = Arc::new(Mutex::new(sender));

        // let tx2recv = sender.clone();

        use std::collections::hash_map::RandomState;

        let queries = Arc::new(HashMap::<ReqId, QuerySender>::new(100, RandomState::new()));

        let (msg_sender, msg_receiver) = std::sync::mpsc::channel();
        let tx2recv = msg_sender.clone();

        let fetches = Arc::new(HashMap::<ResId, FetchSender>::new(100, RandomState::new()));

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();

        let handler = std::thread::spawn(move || 'recv: loop {
            let ws_send = msg_receiver.recv().unwrap();
            match ws_send {
                WsSend::Pong(bytes) => {
                    if let Err(err) = sender.send_message(&Message::pong(bytes)) {
                        log::error!("send websocket message packet error: {}", err);
                        break 'recv;
                    } else {
                    }
                }
                msg => {
                    if let Err(err) = sender.send_message(&msg.to_message()) {
                        log::error!("send websocket message packet error: {}", err);
                        break;
                    } else {
                    }
                }
            }
        });

        // message handler for query/fetch/fetch_block
        std::thread::spawn(move || {
            for message in receiver.incoming_messages() {
                if let Ok(message) = message {
                    match message {
                        websocket::OwnedMessage::Text(text) => {
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
                                    if let Some(sender) =
                                        fetches_sender.read(&fetch.id, |_, sender| sender.clone())
                                    {
                                        sender.send(ok.map(|_| WsFetchData::Fetch(fetch))).unwrap();
                                    }
                                }
                                // Block type is for binary.
                                _ => unreachable!(),
                            }
                        }
                        websocket::OwnedMessage::Binary(block) => {
                            log::debug!("fetch block with {} bytes.", block.len());
                            let mut slice = block.as_slice();
                            use taos_query::util::InlinableRead;
                            let res_id = slice.read_u64().unwrap();
                            let len = (&block[8..12]).read_u32().unwrap();
                            if block.len() == len as usize + 8 {
                                // v3
                                if let Some(v) = fetches_sender.read(&res_id, |_, v| v.clone()) {
                                    log::info!("send data to fetches with id {}", res_id);
                                    let raw = slice.read_inlinable::<RawBlock>().unwrap();
                                    v.send(Ok(WsFetchData::Block(raw).clone())).unwrap();
                                }
                            } else {
                                // v2
                                log::warn!("the block is in format v2");
                                if let Some(v) = fetches_sender.read(&res_id, |_, v| v.clone()) {
                                    log::info!("send data to fetches with id {}", res_id);
                                    v.send(Ok(WsFetchData::BlockV2(block[8..].to_vec())))
                                        .unwrap();
                                }
                            }
                        }
                        websocket::OwnedMessage::Close(_) => break,
                        websocket::OwnedMessage::Ping(bytes) => {
                            // let mut writer = tx2recv.lock().unwrap();
                            tx2recv.send(WsSend::Pong(bytes)).unwrap()
                        }
                        websocket::OwnedMessage::Pong(_) => {
                            // do nothing
                        }
                    }
                } else {
                    let err = message.unwrap_err();
                    dbg!(err);
                }
            }
        });

        Ok(Self {
            req_id: Arc::new(AtomicU64::new(req_id + 1)),
            queries,
            fetches,
            sender: msg_sender,
        })
    }

    fn req_id(&self) -> u64 {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn s_query(&self, sql: &str) -> Result<ResultSet> {
        let req_id = self.req_id();
        let action = WsSend::Query {
            req_id,
            sql: sql.to_string(),
        };
        let message = action.to_message();
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        {
            self.queries.insert(req_id, tx).unwrap();
            self.sender.send(action).unwrap();
        }
        let resp = rx.recv()??;

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
                self.fetches.insert(resp.id, tx);
            }
            Ok(ResultSet {
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
            self.sender.send(action).unwrap();
        }
        let resp = rx.recv()??;
        Ok(resp.affected_rows)
    }
}

impl ResultSet {
    pub fn fetch_block(&mut self) -> Result<Option<Block>> {
        if self.receiver.is_none() {
            return Ok(None);
        }
        let rx = self.receiver.as_mut().unwrap();
        let fetch = WsSend::Fetch(self.args);
        {
            // prepare for receiving.
            self.sender.send(fetch).unwrap();
            // unlock mutex when out of scope.
        }

        let fetch_resp = if let WsFetchData::Fetch(fetch) = rx.recv()?? {
            fetch
        } else {
            unreachable!()
        };

        if fetch_resp.completed {
            return Ok(None);
        }

        let fetch_block = WsSend::FetchBlock(self.args);

        {
            // prepare for receiving.
            self.sender.send(fetch_block).unwrap();
            // unlock mutex when out of scope.
        }

        match rx.recv()?? {
            WsFetchData::Block(mut raw) => {
                raw.with_rows(fetch_resp.rows)
                    .with_cols(self.fields_count)
                    .with_precision(self.precision);

                let mut block = Block::from_raw_block(raw);
                block.with_fields(self.fields.as_ref().unwrap().to_vec());
                Ok(Some(block))
            }
            WsFetchData::BlockV2(raw) => {
                let raw = RawBlock::from_v2(
                    &raw,
                    self.fields.as_ref().unwrap(),
                    fetch_resp.lengths.as_ref().unwrap(),
                    fetch_resp.rows,
                    self.precision,
                );
                let mut block = Block::from_raw_block(raw);
                block.with_fields(self.fields.as_ref().unwrap().to_vec());
                Ok(Some(block))
            }
            _ => Ok(None),
        }
    }
}
impl Iterator for ResultSet {
    type Item = Block;

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
        self.fields.as_ref().unwrap()
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
    std::env::set_var("RUST_LOG", "debug");
    pretty_env_logger::init();
    let client = WsClient::from_dsn("ws://localhost:6041/")?;
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
