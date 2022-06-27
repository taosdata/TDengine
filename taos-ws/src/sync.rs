use taos_query::common::{Block, Field, Precision, RawBlock};
use taos_query::{DeError, Dsn, DsnError, Fetchable, IntoDsn, Queryable};
use thiserror::Error;
use websocket::sync::Writer;
use websocket::{stream::sync::TcpStream, sync::Client};
use websocket::{ClientBuilder, Message};

use crate::infra::*;

use std::any;
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::{
    collections::HashMap,
    sync::{mpsc::Sender, Arc, Mutex},
};

pub struct WsAuth {
    user: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

pub struct WsClient {
    req_id: Arc<AtomicU64>,
    sender: Arc<Mutex<Writer<TcpStream>>>,
    queries: Arc<Mutex<HashMap<ReqId, oneshot::Sender<WsQueryResp>>>>,
    fetches: Arc<Mutex<HashMap<ResId, Sender<WsFetchData>>>>,
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
    RecvError(#[from] oneshot::RecvError),
    #[error("{0}")]
    DeError(#[from] DeError),
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

        let mut ws = ClientBuilder::new("ws://localhost:6041/rest/ws")?;

        let conn = if dsn.params.contains_key("token") {
            WsConnReq::default()
        } else {
            WsConnReq::new(
                dsn.username.unwrap_or_else(|| "root".to_string()),
                dsn.password.unwrap_or_else(|| "taosdata".to_string()),
            )
        };

        let client = ws.connect_insecure()?;

        let req_id = 0;

        let (mut receiver, mut sender) = client.split().unwrap();
        let login = WsSend::Conn { req_id, req: conn };
        sender.send_message(&login.to_message()).unwrap();

        let recv = receiver.recv_message()?;

        // connect
        let _ = match recv {
            websocket::OwnedMessage::Text(text) => {
                let v: WsRecv = serde_json::from_str(&text).unwrap();
                match v.data {
                    WsRecvData::Conn => (),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        };

        let sender = Arc::new(Mutex::new(sender));

        let tx2recv = sender.clone();

        let queries = Arc::new(Mutex::new(
            HashMap::<ReqId, oneshot::Sender<WsQueryResp>>::new(),
        ));

        let fetches = Arc::new(Mutex::new(HashMap::<
            ResId,
            std::sync::mpsc::Sender<WsFetchData>,
        >::new()));

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();
        // message handler for query/fetch/fetch_block
        std::thread::spawn(move || {
            for message in receiver.incoming_messages() {
                if let Ok(message) = message {
                    match message {
                        websocket::OwnedMessage::Text(text) => {
                            dbg!(&text);
                            let v: WsRecv = serde_json::from_str(&text).unwrap();
                            match v.data {
                                WsRecvData::Conn => todo!(),
                                WsRecvData::Query(query) => {
                                    if let Some(sender) =
                                        queries_sender.lock().unwrap().remove(&v.req_id)
                                    {
                                        sender.send(query).unwrap();
                                    }
                                }
                                WsRecvData::Fetch(fetch) => {
                                    if let Some(sender) =
                                        fetches_sender.lock().unwrap().get(&fetch.id)
                                    {
                                        sender.send(WsFetchData::Fetch(fetch)).unwrap();
                                    }
                                }
                                // Block type is for binary.
                                WsRecvData::Block(_) => unreachable!(),
                            }
                        }
                        websocket::OwnedMessage::Binary(block) => {
                            let mut slice = block.as_slice();
                            use taos_query::util::InlinableRead;
                            let res_id = slice.read_u64().unwrap();
                            if let Some(sender) = fetches_sender.lock().unwrap().remove(&res_id) {
                                let raw = slice.read_inlinable::<RawBlock>().unwrap();
                                sender.send(WsFetchData::Block(raw)).unwrap();
                            }
                        }
                        websocket::OwnedMessage::Close(_) => todo!(),
                        websocket::OwnedMessage::Ping(bytes) => {
                            let mut writer = tx2recv.lock().unwrap();
                            writer.send_message(&Message::pong(bytes)).unwrap()
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
            sender,
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
        let (tx, rx) = oneshot::channel();
        {
            self.queries.lock().unwrap().insert(req_id, tx);
            self.sender.lock().unwrap().send_message(&message)?;
        }
        let resp = rx.recv()?;

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
            Ok(ResultSet {
                sender: self.sender.clone(),
                fetches: self.fetches.clone(),
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
        let message = action.to_message();
        let (tx, rx) = oneshot::channel();
        {
            self.queries.lock().unwrap().insert(req_id, tx);
            self.sender.lock().unwrap().send_message(&message)?;
        }
        let resp = rx.recv()?;
        Ok(resp.affected_rows)
    }
}

pub struct ResultSet {
    sender: Arc<Mutex<Writer<TcpStream>>>,
    fetches: Arc<Mutex<HashMap<ResId, Sender<WsFetchData>>>>,
    args: WsResArgs,
    fields: Option<Vec<Field>>,
    fields_count: usize,
    affected_rows: usize,
    precision: Precision,
}
impl Iterator for ResultSet {
    type Item = Block;

    fn next(&mut self) -> Option<Self::Item> {
        let fetch = WsSend::Fetch(self.args);
        let (tx, rx) = std::sync::mpsc::channel();
        {
            // prepare for receiving.
            self.fetches.lock().unwrap().insert(self.args.id, tx);
            self.sender
                .lock()
                .unwrap()
                .send_message(&fetch.to_message())
                .unwrap();
            // unlock mutex when out of scope.
        }
        let fetch_resp = if let WsFetchData::Fetch(fetch) = rx.recv().unwrap() {
            fetch
        } else {
            unreachable!()
        };

        if fetch_resp.completed {
            return None;
        }

        let fetch_block = WsSend::FetchBlock(self.args);

        {
            // prepare for receiving.
            self.sender
                .lock()
                .unwrap()
                .send_message(&fetch_block.to_message())
                .unwrap();
            // unlock mutex when out of scope.
        }

        if let Ok(WsFetchData::Block(mut raw)) = rx.recv() {
            raw.with_rows(fetch_resp.rows)
                .with_cols(self.fields_count)
                .with_precision(self.precision);

            for row in 0..raw.nrows() {
                for col in 0..raw.ncols() {
                    let v = unsafe { raw.get_unchecked(row, col) };
                    println!("({}, {}): {}", row, col, v);
                }
            }
            let mut block = Block::from_raw_block(raw);
            block.with_fields(self.fields.as_ref().unwrap().to_vec());
            Some(block)
        } else {
            None
        }
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
