use futures::{SinkExt, StreamExt};
use itertools::Itertools;
use scc::HashMap;

use taos_query::common::views::views_to_raw_block;
use taos_query::common::ColumnView;
use taos_query::prelude::InlinableWrite;
use taos_query::stmt::Bindable;
use taos_query::{
    block_in_place_or_global, AsyncFetchable, AsyncQueryable, DeError, DsnError, IntoDsn, RawBlock,
    TBuilder,
};
use thiserror::Error;
use tokio::sync::{oneshot, watch};

use tokio::time;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::infra::ToMessage;
use crate::{Taos, TaosBuilder};
use messages::*;

use std::fmt::Debug;
use std::result::Result as StdResult;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

mod messages;

type StmtResult = StdResult<Option<usize>, taos_error::Error>;
type StmtSender = std::sync::mpsc::SyncSender<StmtResult>;
type StmtReceiver = std::sync::mpsc::Receiver<StmtResult>;

type WsSender = tokio::sync::mpsc::Sender<Message>;

trait ToJsonValue {
    fn to_json_value(&self) -> serde_json::Value;
}

impl ToJsonValue for ColumnView {
    fn to_json_value(&self) -> serde_json::Value {
        match self {
            ColumnView::Bool(view) => serde_json::json!(view.to_vec()),
            ColumnView::TinyInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::SmallInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::Int(view) => serde_json::json!(view.to_vec()),
            ColumnView::BigInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::Float(view) => serde_json::json!(view.to_vec()),
            ColumnView::Double(view) => serde_json::json!(view.to_vec()),
            ColumnView::VarChar(view) => serde_json::json!(view.to_vec()),
            ColumnView::Timestamp(view) => serde_json::json!(view
                .iter()
                .map(|ts| ts.map(|ts| ts.to_datetime_with_tz()))
                .collect_vec()),
            ColumnView::NChar(view) => serde_json::json!(view.to_vec()),
            ColumnView::UTinyInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::USmallInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::UInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::UBigInt(view) => serde_json::json!(view.to_vec()),
            ColumnView::Json(view) => serde_json::json!(view.to_vec()),
        }
    }
}

impl Bindable<super::Taos> for Stmt {
    type Error = Error;

    fn init(taos: &super::Taos) -> StdResult<Self, Self::Error> {
        let mut dsn = taos.dsn.clone();
        let database: Option<String> =
            <Taos as taos_query::Queryable>::query_one(taos, "select database()")?;
        dsn.database = database;
        let mut stmt = block_in_place_or_global(Self::from_wsinfo(&dsn))?;
        block_in_place_or_global(stmt.stmt_init())?;
        Ok(stmt)
    }

    fn prepare<S: AsRef<str>>(&mut self, sql: S) -> StdResult<&mut Self, Self::Error> {
        block_in_place_or_global(self.stmt_prepare(sql.as_ref()))?;
        Ok(self)
    }

    fn set_tbname<S: AsRef<str>>(&mut self, sql: S) -> StdResult<&mut Self, Self::Error> {
        block_in_place_or_global(self.stmt_set_tbname(sql.as_ref()))?;
        Ok(self)
    }

    fn set_tags(
        &mut self,
        tags: &[taos_query::common::Value],
    ) -> StdResult<&mut Self, Self::Error> {
        let tags = tags
            .into_iter()
            .map(|tag| tag.to_json_value())
            .collect_vec();
        block_in_place_or_global(self.stmt_set_tags(tags))?;
        Ok(self)
    }

    fn bind(
        &mut self,
        params: &[taos_query::common::ColumnView],
    ) -> StdResult<&mut Self, Self::Error> {
        let columns = params
            .into_iter()
            .map(|view| view.to_json_value())
            .collect_vec();
        block_in_place_or_global(self.stmt_bind_block(params))?;
        Ok(self)
    }

    fn add_batch(&mut self) -> StdResult<&mut Self, Self::Error> {
        block_in_place_or_global(self.stmt_add_batch())?;
        Ok(self)
    }

    fn execute(&mut self) -> StdResult<usize, Self::Error> {
        Ok(block_in_place_or_global(self.stmt_exec())?)
    }

    fn affected_rows(&self) -> usize {
        self.affected_rows
    }
}

pub struct Stmt {
    req_id: Arc<AtomicU64>,
    timeout: Duration,
    ws: WsSender,
    close_signal: watch::Sender<bool>,
    queries: Arc<HashMap<ReqId, oneshot::Sender<StdResult<StmtId, taos_error::Error>>>>,
    fetches: Arc<HashMap<StmtId, StmtSender>>,
    receiver: Option<StmtReceiver>,
    args: Option<StmtArgs>,
    affected_rows: usize,
}

// pub struct WsAsyncStmt {
//     /// Message sending timeout, default is 5min.
//     timeout: Duration,
//     ws: WsSender,
//     fetches: Arc<HashMap<StmtId, StmtSender>>,
//     receiver: StmtReceiver,
//     args: StmtArgs,
// }
// impl Debug for WsAsyncStmt {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("WsAsyncStmt")
//             .field("ws", &"...")
//             .field("fetches", &"...")
//             .field("receiver", &self.receiver)
//             .field("args", &self.args)
//             .finish()
//     }
// }

// impl Drop for WsAsyncStmt {
//     fn drop(&mut self) {
//         self.fetches.remove(&self.args.stmt_id);
//         let args = self.args;
//         let ws = self.ws.clone();
//         let _ = taos_query::block_in_place_or_global(ws.send(StmtSend::Close(args).to_msg()));
//     }
// }

impl Debug for Stmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsClient")
            .field("req_id", &self.req_id)
            .field("...", &"...")
            .finish()
    }
}

use super::asyn::Error;
// #[derive(Debug, Error)]
// pub enum Error {
//     #[error("{0}")]
//     Dsn(#[from] DsnError),
//     #[error("{0}")]
//     FetchError(#[from] oneshot::error::RecvError),
//     #[error("{0}")]
//     SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),
//     #[error(transparent)]
//     SendTimeoutError(#[from] tokio::sync::mpsc::error::SendTimeoutError<Message>),
//     #[error("{0}")]
//     StdSendError(#[from] std::sync::mpsc::SendError<tokio_tungstenite::tungstenite::Message>),
//     #[error("{0}")]
//     RecvError(#[from] std::sync::mpsc::RecvError),
//     #[error("{0}")]
//     RecvTimeout(#[from] std::sync::mpsc::RecvTimeoutError),
//     #[error("{0}")]
//     DeError(#[from] DeError),
//     #[error("{0}")]
//     WsError(#[from] WsError),
//     #[error("{0}")]
//     TaosError(#[from] taos_error::Error),
// }

// impl Error {
//     pub const fn errno(&self) -> taos_error::Code {
//         match self {
//             Error::TaosError(error) => error.code(),
//             _ => taos_error::Code::Failed,
//         }
//     }
//     pub fn errstr(&self) -> String {
//         match self {
//             Error::TaosError(error) => error.message().to_string(),
//             _ => format!("{}", self),
//         }
//     }
// }

type Result<T> = std::result::Result<T, Error>;

impl Drop for Stmt {
    fn drop(&mut self) {
        // send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

impl Stmt {
    pub(crate) async fn from_wsinfo(info: &TaosBuilder) -> Result<Self> {
        let (ws, _) = connect_async(info.to_stmt_url()).await?;
        let req_id = 0;
        let (mut sender, mut reader) = ws.split();

        let login = StmtSend::Conn {
            req_id,
            req: info.to_conn_request(),
        };
        sender.send(login.to_msg()).await?;
        if let Some(Ok(message)) = reader.next().await {
            match message {
                Message::Text(text) => {
                    let v: StmtRecv = serde_json::from_str(&text).unwrap();
                    match v.data {
                        StmtRecvData::Conn => (),
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }

        let queries = Arc::new(HashMap::<ReqId, tokio::sync::oneshot::Sender<_>>::new(
            100,
            RandomState::new(),
        ));

        use std::collections::hash_map::RandomState;
        let fetches = Arc::new(HashMap::<StmtId, StmtSender>::new(100, RandomState::new()));

        let queries_sender = queries.clone();
        let fetches_sender = fetches.clone();

        let (ws, mut msg_recv) = tokio::sync::mpsc::channel(100);
        let ws2 = ws.clone();

        // Connection watcher
        let (tx, mut rx) = watch::channel(false);
        let mut close_listener = rx.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    Some(msg) = msg_recv.recv() => {
                        sender.send(msg).await.unwrap();
                    }
                    _ = rx.changed() => {
                        log::debug!("close sender task");
                        break;
                    }
                }
            }
        });

        // message handler for query/fetch/fetch_block
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = reader.next() => {
                        match message {
                            Ok(message) => match message {
                                Message::Text(text) => {
                                    log::debug!("json response: {}", text);
                                    let v: StmtRecv = serde_json::from_str(&text).unwrap();
                                    match v.ok() {
                                        StmtOk::Conn(_) => {
                                            log::warn!("[{req_id}] received connected response in message loop");
                                        },
                                        StmtOk::Init(req_id, stmt_id) => {
                                            log::debug!("stmt init done: {{ req_id: {}, stmt_id: {:?}}}", req_id, stmt_id);
                                            if let Some((_, sender)) = queries_sender.remove(&req_id)
                                            {
                                                sender.send(stmt_id).unwrap();
                                            }  else {
                                                log::error!("Stmt init failed because req id {req_id} not exist");
                                            }
                                        }
                                        StmtOk::Stmt(stmt_id, res) => {
                                            if let Some(sender) = fetches_sender.read(&stmt_id, |_, sender| sender.clone()) {
                                                log::debug!("send data to fetches with id {}", stmt_id);
                                                // let res = res.clone();
                                                sender.send(res).unwrap();
                                            // }) {

                                            } else {
                                                log::error!("Got unknown stmt id: {stmt_id} with result: {res:?}");
                                            }
                                        }
                                        _ => unreachable!("unknown stmt response"),
                                    }
                                }
                                Message::Binary(_) => {
                                    log::warn!("received (unexpected) binary message, do nothing");
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
                                log::error!("receiving cause error: {err:?}");
                                break;
                            }
                        }
                    }
                    _ = close_listener.changed() => {
                        log::debug!("close reader task");
                        break
                    }
                }
            }
        });

        Ok(Self {
            req_id: Arc::new(AtomicU64::new(req_id + 1)),
            queries,
            timeout: Duration::from_secs(5),
            fetches,
            ws,
            close_signal: tx,
            receiver: None,
            args: None,
            affected_rows: 0,
        })
    }
    /// Build TDengine websocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041/
    /// ```
    ///
    pub async fn from_dsn(dsn: impl IntoDsn) -> Result<Self> {
        let info = TaosBuilder::from_dsn(dsn)?;
        Self::from_wsinfo(&info).await
    }

    fn req_id(&self) -> u64 {
        self.req_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn stmt_init(&mut self) -> Result<&mut Self> {
        let req_id = self.req_id();
        let action = StmtSend::Init { req_id };
        let (tx, rx) = oneshot::channel();
        {
            self.queries.insert(req_id, tx).unwrap();
            self.ws.send(action.to_msg()).await?;
        }
        let stmt_id = rx.await??; // 1. RecvError, 2. TaosError
        let args = StmtArgs { req_id, stmt_id };

        let (sender, receiver) = std::sync::mpsc::sync_channel(2);

        let _ = self.fetches.insert(stmt_id, sender);

        self.args = Some(args);
        self.receiver = Some(receiver);
        Ok(self)
    }

    pub async fn s_stmt(&mut self, sql: &str) -> Result<&mut Self> {
        let stmt = self.stmt_init().await?;
        stmt.stmt_prepare(sql).await?;
        Ok(stmt)
    }

    pub fn set_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = timeout;
        self
    }
    pub async fn stmt_prepare(&mut self, sql: &str) -> Result<()> {
        let prepare = StmtSend::Prepare {
            args: self.args.unwrap(),
            sql: sql.to_string(),
        };
        self.ws.send(prepare.to_msg()).await?;
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)??;
        Ok(())
    }
    pub async fn stmt_add_batch(&mut self) -> Result<()> {
        log::debug!("add batch");
        let message = StmtSend::AddBatch(self.args.unwrap());
        self.ws.send(message.to_msg()).await?;
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)??;
        Ok(())
    }
    pub async fn stmt_bind(&mut self, columns: Vec<serde_json::Value>) -> Result<()> {
        let message = StmtSend::Bind {
            args: self.args.unwrap(),
            columns: columns,
        };
        {
            log::debug!("bind with: {message:?}");
            log::debug!("bind string: {}", message.to_msg());
            self.ws.send(message.to_msg()).await?;
        }
        log::debug!("begin receive");
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)??;
        Ok(())
    }

    pub async fn stmt_bind_block(&mut self, columns: &[ColumnView]) -> Result<()> {
        let args = self.args.unwrap();

        let mut bytes = Vec::new();
        // p0 uin64  req_id
        // p0+8 uint64 stmt_id
        // p0+16 uint64 (1 (set tag) 2 (bind))
        // p0+24 uint64 rows
        // p0+32 uint64 cols
        // p0+40 raw block
        bytes.write_u64_le(args.req_id)?;
        bytes.write_u64_le(args.stmt_id)?;
        bytes.write_u64_le(2)?; // bind: 2
        bytes.write_u64_le(columns.len() as u64)?;
        let rows = columns.first().map(|c| c.len()).unwrap_or_default() as u64;
        bytes.write_u64_le(rows)?;

        let block = views_to_raw_block(columns);

        bytes.extend(&block);

        self.ws.send(Message::Binary(bytes)).await?;
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)??;

        Ok(())
    }

    /// Call bind and add batch.
    pub async fn bind_all(&mut self, columns: Vec<serde_json::Value>) -> Result<()> {
        self.stmt_bind(columns).await?;
        self.stmt_add_batch().await
    }

    pub async fn stmt_set_tbname(&mut self, name: &str) -> Result<()> {
        let message = StmtSend::SetTableName {
            args: self.args.unwrap(),
            name: name.to_string(),
        };
        self.ws.send_timeout(message.to_msg(), self.timeout).await?;
        let _ = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)??;
        Ok(())
    }

    pub async fn stmt_set_tags(&mut self, tags: Vec<serde_json::Value>) -> Result<()> {
        let message = StmtSend::SetTags {
            args: self.args.unwrap(),
            tags: tags,
        };
        self.ws.send_timeout(message.to_msg(), self.timeout).await?;
        let _ = self.receiver.as_ref().unwrap().recv_timeout(self.timeout)?;
        Ok(())
    }

    pub async fn stmt_exec(&mut self) -> Result<usize> {
        log::debug!("exec");
        let message = StmtSend::Exec(self.args.unwrap());
        self.ws.send_timeout(message.to_msg(), self.timeout).await?;
        if let Some(affected) = self
            .receiver
            .as_ref()
            .unwrap()
            .recv_timeout(self.timeout)??
        {
            self.affected_rows += affected;
            Ok(affected)
        } else {
            panic!("")
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use taos_query::{Dsn, TBuilder};

    use crate::{stmt::Stmt, TaosBuilder};

    // !Websocket tests should always use `multi_thread`
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_client() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?.build()?;
        taos.exec("drop database if exists stmt").await?;
        taos.exec("create database stmt").await?;
        taos.exec("create table stmt.ctb (ts timestamp, v int)")
            .await?;

        std::env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();
        let mut client = Stmt::from_dsn("taos+ws://localhost:6041/stmt").await?;
        let stmt = client.s_stmt("insert into stmt.ctb values(?, ?)").await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;
        let res = stmt.stmt_exec().await?;

        assert_eq!(res, 2);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_stmt_stable() -> anyhow::Result<()> {
        use taos_query::AsyncQueryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;
        dbg!(&dsn);

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?.build()?;
        taos.exec("drop database if exists stmt_s").await?;
        taos.exec("create database stmt_s").await?;
        taos.exec("create table stmt_s.stb (ts timestamp, v int) tags(t1 int)")
            .await?;

        std::env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();
        let mut client = Stmt::from_dsn("taos+ws://localhost:6041/stmt_s").await?;
        let stmt = client
            .s_stmt("insert into ? using stb tags(?) values(?, ?)")
            .await?;

        stmt.stmt_set_tbname("tb1").await?;

        // stmt.set_tags(vec![json!({"name": "value"})]).await?;

        stmt.stmt_set_tags(vec![json!(1)]).await?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])
        .await?;
        let res = stmt.stmt_exec().await?;

        assert_eq!(res, 2);
        taos.exec("drop database stmt_s").await?;
        Ok(())
    }
}
