use std::fmt::Debug;
use std::future::Future;
use std::io::Write;
use std::mem::transmute;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::{Duration, Instant};

use byteorder::{ByteOrder, LittleEndian};
use chrono_tz::Tz;
use dashmap::DashMap;
use faststr::FastStr;
use futures::{future, FutureExt, SinkExt, StreamExt};
use itertools::Itertools;
use taos_query::common::{Field, Precision, RawBlock, RawMeta, SmlData};
use taos_query::prelude::{Code, RawError, RawResult};
use taos_query::util::{generate_req_id, CleanUp, InlinableWrite};
use taos_query::{
    block_in_place_or_global, AsyncFetchable, AsyncQueryable, DeError, DsnError, IntoDsn,
};
use thiserror::Error;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{self, timeout};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, Instrument};

use crate::{EndpointType, Stmt2Inner};

use super::messages::*;
use super::TaosBuilder;

type QueryChannelSender = oneshot::Sender<RawResult<WsRecvData>>;
type QueryInner = scc::HashMap<ReqId, QueryChannelSender>;
type QueryAgent = Arc<QueryInner>;
type QueryResMapper = scc::HashMap<ResId, ReqId>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    Connected,
    Reconnecting,
    Disconnected,
}

#[derive(Debug)]
pub struct WsTaos {
    conn_id: u64,
    sender: WsQuerySender,
    close_signal: watch::Sender<bool>,
    tz: Option<Tz>,
    builder: Arc<TaosBuilder>,
    stmt2s: Arc<DashMap<u64, Weak<Stmt2Inner>>>,
    state: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    recover_token: Arc<Mutex<Option<CancellationToken>>>,
    recover_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl WsTaos {
    /// Build TDengine WebSocket client from dsn.
    ///
    /// ```text
    /// ws://localhost:6041
    /// ```
    pub async fn from_dsn<T: IntoDsn>(dsn: T) -> RawResult<Arc<Self>> {
        let dsn = dsn.into_dsn()?;
        let builder = TaosBuilder::from_dsn(dsn)?;
        Self::from_builder(&builder).await
    }

    pub(super) async fn from_builder(builder: &TaosBuilder) -> RawResult<Arc<Self>> {
        let conn_id = generate_req_id();
        let span = tracing::info_span!("ws_conn", conn_id = conn_id);

        let (ws_stream, version) = builder.connect().instrument(span.clone()).await?;

        let (message_tx, message_rx) = flume::bounded(64);
        let (close_tx, close_rx) = watch::channel(false);

        let query_sender = WsQuerySender {
            version_info: VersionInfo::new(version),
            req_id: Arc::default(),
            sender: message_tx,
            queries: Arc::default(),
            results: Arc::default(),
            read_timeout: builder.read_timeout,
        };

        let builder = Arc::new(builder.clone());
        let ws_taos = Arc::new(WsTaos {
            conn_id,
            close_signal: close_tx,
            sender: query_sender.clone(),
            tz: builder.tz,
            builder: builder.clone(),
            stmt2s: Arc::default(),
            state: Arc::new(AtomicUsize::new(ConnState::Connected as usize)),
            notify: Arc::default(),
            recover_token: Arc::default(),
            recover_handle: Arc::default(),
        });

        tokio::spawn(
            super::conn::run(
                Arc::downgrade(&ws_taos),
                builder,
                ws_stream,
                query_sender,
                message_rx,
                close_rx,
            )
            .instrument(span),
        );

        Ok(ws_taos)
    }

    pub async fn write_meta(&self, raw: &RawMeta) -> RawResult<()> {
        let req_id = self.sender.req_id();
        let message_id = req_id;

        let mut meta = Vec::new();
        meta.write_u64_le(req_id).map_err(Error::from)?;
        meta.write_u64_le(message_id).map_err(Error::from)?;
        meta.write_u64_le(3).map_err(Error::from)?;

        meta.write_u32_le(raw.raw_len()).map_err(Error::from)?;
        meta.write_u16_le(raw.raw_type()).map_err(Error::from)?;
        meta.write_all(raw.raw_slice()).map_err(Error::from)?;
        let len = meta.len();

        tracing::trace!("write meta with req_id: 0x{req_id:x}, raw data length: {len}",);

        let h = self
            .sender
            .send_recv(WsSend::Binary(meta))
            .in_current_span();
        tokio::pin!(h);
        let mut interval = time::interval(Duration::from_secs(60));
        const MAX_WAIT_TICKS: usize = 5; // means 5 minutes
        const TIMEOUT_ERROR: &str = "Write raw meta timeout, maybe the connection has been lost";
        let mut ticks = 0;
        loop {
            select! {
                _ = interval.tick() => {
                    ticks += 1;
                    if ticks >= MAX_WAIT_TICKS {
                        tracing::warn!("{}", TIMEOUT_ERROR);
                        return Err(RawError::new(
                            0xE002, // Connection closed
                            TIMEOUT_ERROR,
                        ));
                    }
                    if let Err(err) = time::timeout(Duration::from_secs(30), self.exec("select server_version()").in_current_span()).await {
                        tracing::warn!(error = format!("{err:#}"), TIMEOUT_ERROR);
                        return Err(RawError::new(
                            0xE002, // Connection closed
                            TIMEOUT_ERROR,
                        ));
                    }
                }
                res = &mut h => {
                    res?;
                    return Ok(())
                }
            }
        }
    }

    pub async fn s_query(&self, sql: &str) -> RawResult<ResultSet> {
        let req_id = self.sender.req_id();
        self.s_query_with_req_id(sql, req_id)
            .in_current_span()
            .await
    }

    #[instrument(skip(self))]
    pub async fn s_query_with_req_id(&self, sql: &str, req_id: u64) -> RawResult<ResultSet> {
        let data = if self.is_support_binary_sql() {
            let mut bytes = Vec::with_capacity(sql.len() + 30);
            bytes.write_u64_le(req_id).map_err(Error::from)?;
            bytes.write_u64_le(0).map_err(Error::from)?; // result ID, uesless here
            bytes.write_u64_le(6).map_err(Error::from)?; // action, 6 for query
            bytes.write_u16_le(1).map_err(Error::from)?; // version
            bytes.write_u32_le(sql.len() as _).map_err(Error::from)?;
            bytes.write_all(sql.as_bytes()).map_err(Error::from)?;
            self.sender.send_recv(WsSend::Binary(bytes)).await?
        } else {
            let action = WsSend::Query {
                req_id,
                sql: sql.to_string(),
            };
            self.sender.send_recv(action).await?
        };

        let resp = match data {
            WsRecvData::Query(resp) => resp,
            _ => unreachable!(),
        };

        let res_id = resp.id;
        let args = WsResArgs { id: res_id, req_id };

        let (close_tx, close_rx) = oneshot::channel();

        tokio::task::spawn(
            async move {
                let now = Instant::now();
                let _ = close_rx.await;
                tracing::trace!("result {} lived for {:?}", res_id, now.elapsed());
            }
            .in_current_span(),
        );

        if resp.fields_count > 0 {
            let names = resp.fields_names.unwrap();
            let types = resp.fields_types.unwrap();
            let lens = resp.fields_lengths.unwrap();
            let fields: Vec<Field> = names
                .iter()
                .zip(types)
                .zip(lens)
                .map(|((name, ty), len)| Field::new(name, ty, len))
                .collect();

            let precision = resp.precision;
            let sender = self.sender.clone();

            let (raw_block_tx, raw_block_rx) = mpsc::channel(64);
            let (fetch_done_tx, fetch_done_rx) = mpsc::channel(1);

            if sender.version_info.support_binary_sql() {
                fetch_binary(
                    sender,
                    res_id,
                    raw_block_tx,
                    precision,
                    names,
                    fetch_done_tx,
                )
                .await;
            } else {
                fetch(
                    sender,
                    res_id,
                    raw_block_tx,
                    precision,
                    fields.clone(),
                    names,
                    fetch_done_tx,
                )
                .await;
            }

            Ok(ResultSet {
                sender: self.sender.clone(),
                args,
                fields: Some(fields),
                fields_count: resp.fields_count,
                affected_rows: resp.affected_rows,
                precision,
                summary: (0, 0),
                timing: resp.timing,
                block_future: None,
                closer: Some(close_tx),
                metrics: QueryMetrics::default(),
                blocks_buffer: Some(raw_block_rx),
                fields_precisions: resp.fields_precisions,
                fields_scales: resp.fields_scales,
                fetch_done_reader: Some(fetch_done_rx),
                tz: self.tz,
            })
        } else {
            Ok(ResultSet {
                sender: self.sender.clone(),
                args,
                affected_rows: resp.affected_rows,
                precision: resp.precision,
                summary: (0, 0),
                timing: resp.timing,
                closer: Some(close_tx),
                metrics: QueryMetrics::default(),
                fields: None,
                fields_count: 0,
                block_future: None,
                blocks_buffer: None,
                fields_precisions: None,
                fields_scales: None,
                fetch_done_reader: None,
                tz: self.tz,
            })
        }
    }

    pub async fn s_exec(&self, sql: &str) -> RawResult<usize> {
        let req = WsSend::Query {
            req_id: self.sender.req_id(),
            sql: sql.to_string(),
        };
        match self.sender.send_recv(req).await? {
            WsRecvData::Query(resp) => Ok(resp.affected_rows),
            _ => unreachable!(),
        }
    }

    pub async fn s_put(&self, sml: &SmlData) -> RawResult<(Option<usize>, Option<usize>)> {
        let req = WsSend::Insert {
            protocol: sml.protocol() as u8,
            precision: sml.precision().into(),
            data: sml.data().join("\n"),
            ttl: sml.ttl(),
            req_id: sml.req_id(),
            table_name_key: sml.table_name_key().cloned(),
        };
        tracing::trace!("sml req: {req:?}");
        match self.sender.send_recv(req).await? {
            WsRecvData::Insert(resp) => {
                tracing::trace!("sml resp: {resp:?}");
                Ok((resp.affected_rows, resp.total_rows))
            }
            _ => unreachable!(),
        }
    }

    pub async fn validate_sql(&self, sql: &str) -> RawResult<()> {
        let mut req = Vec::with_capacity(30 + sql.len());
        let req_id = generate_req_id();
        req.write_u64_le(req_id).map_err(Error::from)?;
        req.write_u64_le(0).map_err(Error::from)?;
        req.write_u64_le(10).map_err(Error::from)?;
        req.write_u16_le(1).map_err(Error::from)?;
        req.write_u32_le(sql.len() as _).map_err(Error::from)?;
        req.write_all(sql.as_bytes()).map_err(Error::from)?;

        match self.sender.send_recv(WsSend::Binary(req)).await? {
            WsRecvData::ValidateSql { result_code, .. } => {
                if result_code != 0 {
                    Err(RawError::new(result_code, "validate sql error"))
                } else {
                    Ok(())
                }
            }
            _ => unreachable!(),
        }
    }

    pub async fn options_connection(&self, options: &[ConnOption]) -> RawResult<()> {
        for opt in options {
            if opt.option == -1 {
                self.builder.conn_options.clear();
            } else {
                self.builder
                    .conn_options
                    .insert(opt.option, opt.value.clone());
            }
        }

        let req = WsSend::OptionsConnection {
            req_id: self.sender.req_id(),
            options: options.to_vec(),
        };
        tracing::trace!("options_connection req: {req:?}");
        match self.sender.send_recv(req).await? {
            WsRecvData::OptionsConnection { .. } => Ok(()),
            _ => unreachable!("Unexpected response type for options_connection"),
        }
    }

    pub fn version(&self) -> FastStr {
        block_in_place_or_global(self.sender.version_info.version())
    }

    pub fn is_support_binary_sql(&self) -> bool {
        self.sender.version_info.support_binary_sql()
    }

    pub fn get_req_id(&self) -> ReqId {
        self.sender.req_id()
    }

    pub(crate) async fn send_request(&self, req: WsSend) -> RawResult<WsRecvData> {
        self.sender.send_recv(req).await
    }

    pub(crate) async fn send_only(&self, req: WsSend) -> RawResult<()> {
        self.sender.send_only(req).await
    }

    pub(crate) fn insert_stmt2(&self, stmt2: Arc<Stmt2Inner>) {
        tracing::trace!("insert stmt2: {stmt2:?}");
        self.stmt2s.insert(stmt2.id(), Arc::downgrade(&stmt2));
    }

    pub(crate) fn remove_stmt2(&self, id: u64) {
        tracing::trace!("remove stmt2 with id: {id}");
        self.stmt2s.remove(&id);
    }

    pub(crate) async fn stmt2_req_ids(&self) -> Vec<ReqId> {
        let futs = self.stmt2s.iter().filter_map(|entry| {
            entry
                .value()
                .upgrade()
                .map(|stmt2| async move { stmt2.req_id().await })
        });
        future::join_all(futs).await
    }

    pub(crate) async fn wait_for_previous_recover_stmt2(&self) {
        let mut token_guard = self.recover_token.lock().await;
        if let Some(token) = token_guard.take() {
            token.cancel();
            tracing::trace!("cancelled stmt2 recover task");
        }

        let mut handle_guard = self.recover_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            tracing::trace!("waiting for stmt2 recover task to finish");
            let _ = handle.await;
        }
    }

    pub(crate) async fn recover_stmt2(self: Arc<Self>) {
        if self.stmt2s.is_empty() {
            tracing::trace!("no stmt2 instances to recover");
            return;
        }

        let ws_taos = self.clone();
        let recover_token = CancellationToken::new();
        let token = recover_token.clone();

        let recover_handle = tokio::spawn(
            async move {
                tokio::select! {
                    _ = token.cancelled() => {
                        tracing::trace!("stmt2 recover cancelled");
                    }
                    _ = ws_taos._recover_stmt2() => {
                        ws_taos.set_state(ConnState::Connected);
                        *ws_taos.recover_token.lock().await = None;
                        *ws_taos.recover_handle.lock().await = None;
                        tracing::trace!("stmt2 recover finished");
                    }
                }
            }
            .in_current_span(),
        );

        *self.recover_token.lock().await = Some(recover_token);
        *self.recover_handle.lock().await = Some(recover_handle);
    }

    async fn _recover_stmt2(&self) {
        let len = self.stmt2s.len();
        tracing::trace!("recovering {len} stmt2 instances");

        let futs = self.stmt2s.iter().filter_map(|entry| {
            entry.value().upgrade().map(|stmt2| async move {
                match stmt2.recover().await {
                    Err(err) => Some((stmt2.id(), err)),
                    Ok(_) => None,
                }
            })
        });

        let mut errors = Vec::new();
        for err in (future::join_all(futs).await).into_iter().flatten() {
            errors.push(err);
        }

        if errors.is_empty() {
            tracing::info!("successfully recovered {len} stmt2 instances");
        } else {
            tracing::warn!(
                "recovered {}/{len} stmt2 instances, {} failed",
                len - errors.len(),
                errors.len()
            );
            for (id, err) in &errors {
                tracing::error!("failed to recover stmt2, id: {id}, err: {err:?}");
            }
        }
    }

    pub(crate) async fn wait_for_reconnect(&self) -> RawResult<()> {
        loop {
            match self.state() {
                ConnState::Connected => return Ok(()),
                ConnState::Reconnecting => self.notify.notified().await,
                ConnState::Disconnected => {
                    return Err(RawError::from_code(WS_ERROR_NO::CONN_CLOSED.as_code())
                        .context("WebSocket connection is closed (wait)"));
                }
            }
        }
    }

    pub(crate) fn set_state(&self, state: ConnState) {
        tracing::trace!("set connection state to: {state:?}");
        self.state.store(state as usize, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) fn state(&self) -> ConnState {
        match self.state.load(Ordering::Acquire) {
            0 => ConnState::Connected,
            1 => ConnState::Reconnecting,
            _ => ConnState::Disconnected,
        }
    }

    pub(crate) fn sender(&self) -> WsQuerySender {
        self.sender.clone()
    }

    pub(crate) fn timezone(&self) -> Option<Tz> {
        self.tz
    }

    async fn s_write_raw_block(&self, raw: &RawBlock) -> RawResult<()> {
        let req_id = self.sender.req_id();
        self.s_write_raw_block_with_req_id(raw, req_id)
            .in_current_span()
            .await
    }

    async fn s_write_raw_block_with_req_id(&self, raw: &RawBlock, req_id: u64) -> RawResult<()> {
        let message_id = req_id;

        if self.version().starts_with("3.0.1.") {
            const ACTION: u64 = 4;

            let mut meta = Vec::new();
            meta.write_u64_le(req_id).map_err(Error::from)?;
            meta.write_u64_le(message_id).map_err(Error::from)?;
            meta.write_u64_le(ACTION).map_err(Error::from)?;
            meta.write_u32_le(raw.nrows() as u32).map_err(Error::from)?;
            meta.write_inlined_str::<2>(raw.table_name().unwrap())
                .map_err(Error::from)?;
            meta.write_all(raw.as_raw_bytes()).map_err(Error::from)?;

            let len = meta.len();
            tracing::trace!("write block with req_id: 0x{req_id:x}, raw data len: {len}",);

            match self.sender.send_recv(WsSend::Binary(meta)).await? {
                WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => Ok(()),
                _ => Err(RawError::from_string("write raw block error"))?,
            }
        } else {
            const ACTION: u64 = 5;

            let mut meta = Vec::new();
            meta.write_u64_le(req_id).map_err(Error::from)?;
            meta.write_u64_le(message_id).map_err(Error::from)?;
            meta.write_u64_le(ACTION).map_err(Error::from)?;
            meta.write_u32_le(raw.nrows() as u32).map_err(Error::from)?;
            meta.write_inlined_str::<2>(raw.table_name().unwrap())
                .map_err(Error::from)?;
            meta.write_all(raw.as_raw_bytes()).map_err(Error::from)?;
            let fields = raw
                .fields()
                .into_iter()
                .map(|f| f.to_c_field())
                .collect_vec();

            let fields =
                unsafe { std::slice::from_raw_parts(fields.as_ptr() as _, fields.len() * 72) };
            meta.write_all(fields).map_err(Error::from)?;
            let len = meta.len();
            tracing::trace!("write block with req_id: 0x{req_id:x}, raw data len: {len}",);

            let recv = time::timeout(
                Duration::from_secs(60),
                self.sender.send_recv(WsSend::Binary(meta)),
            )
            .in_current_span()
            .await
            .map_err(|_| {
                tracing::warn!("Write raw data timeout, maybe the connection has been lost");
                RawError::new(
                    0xE002, // Connection closed
                    "Write raw data timeout, maybe the connection has been lost",
                )
            })??;

            match recv {
                WsRecvData::WriteRawBlock | WsRecvData::WriteRawBlockWithFields => Ok(()),
                _ => Err(RawError::from_string("write raw block error"))?,
            }
        }
    }
}

#[async_trait::async_trait]
impl AsyncQueryable for WsTaos {
    type AsyncResultSet = ResultSet;

    #[instrument(skip_all)]
    async fn query<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<Self::AsyncResultSet> {
        self.s_query(sql.as_ref()).in_current_span().await
    }

    #[instrument(skip_all)]
    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        sql: T,
        req_id: u64,
    ) -> RawResult<Self::AsyncResultSet> {
        self.s_query_with_req_id(sql.as_ref(), req_id).await
    }

    #[instrument(skip_all)]
    async fn write_raw_meta(&self, raw: &RawMeta) -> RawResult<()> {
        self.write_meta(raw).in_current_span().await
    }

    #[instrument(skip_all)]
    async fn write_raw_block(&self, block: &RawBlock) -> RawResult<()> {
        self.s_write_raw_block(block).await
    }

    #[instrument(skip_all)]
    async fn write_raw_block_with_req_id(&self, block: &RawBlock, req_id: u64) -> RawResult<()> {
        self.s_write_raw_block_with_req_id(block, req_id)
            .in_current_span()
            .await
    }

    async fn put(&self, data: &SmlData) -> RawResult<()> {
        let _ = self.s_put(data).in_current_span().await?;
        Ok(())
    }
}

impl Drop for WsTaos {
    fn drop(&mut self) {
        tracing::trace!("dropping ws connection, conn_id: {}", self.conn_id);
        self.set_state(ConnState::Disconnected);
        // Send close signal to reader/writer spawned tasks.
        let _ = self.close_signal.send(true);
    }
}

pub(crate) async fn fetch_binary(
    query_sender: WsQuerySender,
    res_id: ResId,
    raw_block_sender: mpsc::Sender<Result<(RawBlock, Duration), RawError>>,
    precision: Precision,
    field_names: Vec<String>,
    fetch_done_sender: mpsc::Sender<()>,
) {
    tokio::spawn(
        async move {
            tracing::trace!("fetch binary, result id: {res_id}");

            let mut metrics = QueryMetrics::default();

            let mut bytes = vec![0u8; 26];
            LittleEndian::write_u64(&mut bytes[8..], res_id);
            LittleEndian::write_u64(&mut bytes[16..], 7); // action, 7 for fetch
            LittleEndian::write_u16(&mut bytes[24..], 1); // version

            loop {
                LittleEndian::write_u64(&mut bytes, generate_req_id());

                let fetch_start = Instant::now();
                match query_sender.send_recv(WsSend::Binary(bytes.clone())).await {
                    Ok(WsRecvData::BlockNew {
                        block_code,
                        block_message,
                        timing,
                        finished,
                        raw,
                        ..
                    }) => {
                        tracing::trace!("fetch binary, result id: {res_id}, finished: {finished}");

                        metrics.num_of_fetches += 1;
                        metrics.time_cost_in_fetch += fetch_start.elapsed();

                        if block_code != 0 {
                            let err = RawError::new(block_code, block_message);
                            tracing::debug!("fetch binary failed, result id: {res_id}, err: {err:?}");
                            let _ = raw_block_sender.send(Err(err)).await;
                            break;
                        }

                        if finished {
                            drop(raw_block_sender);
                            break;
                        }

                        let parse_start = Instant::now();
                        let mut raw_block = RawBlock::parse_from_raw_block(raw, precision);
                        raw_block.with_field_names(&field_names);
                        metrics.time_cost_in_block_parse += parse_start.elapsed();

                        if raw_block_sender
                            .send(Ok((raw_block, timing)))
                            .await
                            .is_err()
                        {
                            tracing::debug!("fetch binary, failed to send raw block to receiver, result id: {res_id}");
                            break;
                        }
                    }
                    Ok(_) => unreachable!("unexpected response for result: {res_id}"),
                    Err(err) => {
                        if raw_block_sender.send(Err(err)).await.is_err() {
                            tracing::warn!("fetch binary, failed to send error to receiver, result id: {res_id}");
                            break;
                        }
                    }
                }
            }

            let _ = fetch_done_sender.send(()).await;

            tracing::trace!("fetch binary completed, result id: {res_id}, metrics: {metrics:?}");
        }
        .in_current_span(),
    );
}

async fn fetch(
    query_sender: WsQuerySender,
    res_id: ResId,
    raw_block_sender: mpsc::Sender<Result<(RawBlock, Duration), RawError>>,
    precision: Precision,
    fields: Vec<Field>,
    field_names: Vec<String>,
    fetch_done_sender: mpsc::Sender<()>,
) {
    tokio::spawn(
        async move {
            tracing::trace!("fetch, result id: {res_id}");

            let mut metrics = QueryMetrics::default();

            loop {
                let args = WsResArgs {
                    id: res_id,
                    req_id: generate_req_id(),
                };
                let fetch = WsSend::Fetch(args);

                let fetch_start = Instant::now();
                let fetch_resp = match query_sender.send_recv(fetch).await {
                    Ok(WsRecvData::Fetch(fetch)) => fetch,
                    Ok(_) => unreachable!("unexpected response for result: {res_id}"),
                    Err(err) => {
                        let _ = raw_block_sender.send(Err(err)).await;
                        break;
                    }
                };

                tracing::trace!("fetch, result id: {res_id}, resp: {fetch_resp:?}");

                if fetch_resp.completed {
                    drop(raw_block_sender);
                    break;
                }

                let fetch_block = WsSend::FetchBlock(args);
                match query_sender.send_recv(fetch_block).await {
                    Ok(WsRecvData::Block { timing, raw }) => {
                        metrics.num_of_fetches += 1;
                        metrics.time_cost_in_fetch += fetch_start.elapsed();

                        let parse_start = Instant::now();
                        let mut raw = RawBlock::parse_from_raw_block(raw, precision);
                        raw.with_field_names(&field_names);
                        metrics.time_cost_in_block_parse += parse_start.elapsed();

                        if raw_block_sender
                            .send(Ok((raw, timing)))
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                "fetch block, failed to send raw block to receiver, result id: {res_id}"
                            );
                            break;
                        }
                    }
                    Ok(WsRecvData::BlockV2 { timing, raw }) => {
                        metrics.num_of_fetches += 1;
                        metrics.time_cost_in_fetch += fetch_start.elapsed();

                        let parse_start = Instant::now();

                        let lengths = fetch_resp.lengths.as_ref().unwrap();
                        let rows = fetch_resp.rows;
                        let mut raw = RawBlock::parse_from_raw_block_v2(
                            raw, &fields, lengths, rows, precision,
                        );
                        raw.with_field_names(&field_names);

                        metrics.time_cost_in_block_parse += parse_start.elapsed();

                        if raw_block_sender
                            .send(Ok((raw, timing)))
                            .await
                            .is_err()
                        {
                            tracing::warn!(
                                "fetch block v2, failed to send raw block to receiver, result id: {res_id}"
                            );
                            break;
                        }
                    }
                    Ok(_) => unreachable!("unexpected response for result: {res_id}"),
                    Err(err) => {
                        if raw_block_sender.send(Err(err)).await.is_err() {
                            tracing::warn!("fetch, failed to send error to receiver, result id: {res_id}");
                            break;
                        }
                    }
                }
            }

            let _ = fetch_done_sender.send(()).await;

            tracing::trace!("fetch completed, result id: {res_id}, metrics: {metrics:?}");
        }
        .in_current_span(),
    );
}

#[derive(Debug, Clone)]
pub(super) struct VersionInfo {
    version: Arc<RwLock<FastStr>>,
    is_v3: Arc<AtomicBool>,
    support_binary_sql: Arc<AtomicBool>,
}

impl VersionInfo {
    fn new<T: Into<FastStr>>(version: T) -> Self {
        let version = version.into();
        let is_v3 = !version.starts_with('2');
        let support_binary_sql = is_v3 && is_support_binary_sql(&version);
        Self {
            version: Arc::new(RwLock::new(version)),
            is_v3: Arc::new(AtomicBool::new(is_v3)),
            support_binary_sql: Arc::new(AtomicBool::new(support_binary_sql)),
        }
    }

    pub(super) async fn update<T: Into<FastStr>>(&self, version: T) {
        let version = version.into();
        if version == self.version().await {
            return;
        }

        let is_v3 = !version.starts_with('2');
        let support_binary_sql = is_v3 && is_support_binary_sql(&version);

        *self.version.write().await = version;
        self.is_v3.store(is_v3, Ordering::Relaxed);
        self.support_binary_sql
            .store(support_binary_sql, Ordering::Relaxed);
    }

    async fn version(&self) -> FastStr {
        self.version.read().await.clone()
    }

    pub(super) fn is_v3(&self) -> bool {
        self.is_v3.load(Ordering::Relaxed)
    }

    fn support_binary_sql(&self) -> bool {
        self.support_binary_sql.load(Ordering::Relaxed)
    }
}

pub(crate) fn is_support_binary_sql(v1: &str) -> bool {
    if v1 == "3.x" {
        return true;
    }
    is_greater_than_or_equal_to(v1, "3.3.2.0")
}

#[inline]
fn is_greater_than_or_equal_to(v1: &str, v2: &str) -> bool {
    !matches!(compare_versions(v1, v2), std::cmp::Ordering::Less)
}

fn compare_versions(v1: &str, v2: &str) -> std::cmp::Ordering {
    fn normalize(v: &str) -> Vec<u32> {
        v.split(['.', '-'])
            .take(4)
            .map(|s| {
                s.chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .parse()
                    .unwrap_or(0)
            })
            .collect()
    }

    let nums1 = normalize(v1);
    let nums2 = normalize(v2);
    nums1.cmp(&nums2)
}

#[derive(Debug, Clone)]
pub(crate) struct WsQuerySender {
    pub(super) version_info: VersionInfo,
    pub(super) req_id: Arc<AtomicU64>,
    pub(super) results: Arc<QueryResMapper>,
    pub(super) sender: flume::Sender<WsMessage>,
    pub(super) queries: QueryAgent,
    read_timeout: Duration,
}

const WRITE_TIMEOUT: Duration = Duration::from_secs(1);

impl WsQuerySender {
    fn req_id(&self) -> ReqId {
        self.req_id.fetch_add(1, Ordering::Relaxed)
    }

    #[instrument(skip_all)]
    async fn send_recv(&self, message: WsSend) -> RawResult<WsRecvData> {
        let req_id = message.req_id();
        let (data_tx, data_rx) = oneshot::channel();
        let _ = self.queries.insert_async(req_id, data_tx).await;

        let cleanup = || {
            let res = self.queries.remove(&req_id);
            tracing::trace!("send_recv, clean up queries, req_id: 0x{req_id:x}, res: {res:?}");
        };
        let _cleanup_queries = CleanUp { f: Some(cleanup) };

        let mut cleanup_results = None;

        if let WsSend::FetchBlock(args) = message {
            let id = args.id;
            if self.results.contains_async(&id).await {
                Err(RawError::from_string(format!(
                    "FetchBlock: result id: {id} already exists"
                )))?;
            }
            let _ = self.results.insert_async(id, args.req_id).await;

            let cleanup = move || {
                let res = self.results.remove(&id);
                tracing::trace!("send_recv, clean up results, res_id: {id}, res: {res:?}");
            };
            cleanup_results = Some(CleanUp { f: Some(cleanup) });
        }

        let _ = cleanup_results;

        tracing::trace!("sending message with req_id 0x{req_id:x}: {message:?}");

        timeout(
            WRITE_TIMEOUT,
            self.sender.send_async(WsMessage::Command(message)),
        )
        .await
        .map_err(|e| {
            tracing::error!("send request timeout, req_id: 0x{req_id:x}, err: {e}");
            Error::from(e)
        })?
        .map_err(Error::from)?;

        tracing::trace!("message sent, waiting for response, req_id: 0x{req_id:x}");

        let data = timeout(self.read_timeout, data_rx)
            .await
            .map_err(|e| {
                tracing::error!(
                    "receive response timeout, req_id: 0x{req_id:x}, timeout: {:?}, error: {e:#}",
                    self.read_timeout
                );
                RawError::new(
                    WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code(),
                    format!(
                        "Receive data via websocket timeout after {:?}",
                        self.read_timeout
                    ),
                )
            })?
            .map_err(|_| RawError::from_string(format!("0x{req_id:x} request cancelled")))?
            .map_err(Error::from)?;

        tracing::trace!("send_recv, req_id: 0x{req_id:x}, received data: {data:?}");

        Ok(data)
    }

    async fn send_only(&self, message: WsSend) -> RawResult<()> {
        let req_id = message.req_id();
        tracing::trace!("send_only, req_id: 0x{req_id:x}, message: {message:?}");
        timeout(
            WRITE_TIMEOUT,
            self.sender.send_async(WsMessage::Command(message)),
        )
        .await
        .map_err(|e| {
            tracing::error!("send_only, send request timeout, req_id: 0x{req_id:x}, err: {e}");
            Error::from(e)
        })?
        .map_err(Error::from)?;
        Ok(())
    }

    fn send_blocking(&self, message: WsSend) -> RawResult<()> {
        self.sender
            .send(WsMessage::Command(message))
            .map_err(Error::from)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct QueryMetrics {
    pub(crate) num_of_fetches: usize,
    pub(crate) time_cost_in_fetch: Duration,
    pub(crate) time_cost_in_block_parse: Duration,
    pub(crate) time_cost_in_flume: Duration,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Dsn(#[from] DsnError),
    #[error("Authentication failure: \"{0}\"")]
    Unauthorized(String),
    #[error("{0}")]
    FetchError(#[from] tokio::sync::oneshot::error::RecvError),
    #[error(transparent)]
    FlumeSendError(#[from] flume::SendError<WsMessage>),
    #[error("Send data via websocket timeout")]
    SendTimeoutError(#[from] tokio::time::error::Elapsed),
    #[error("Query timed out with sql: {0}")]
    QueryTimeout(String),
    #[error("{0}")]
    TaosError(#[from] RawError),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("WebSocket internal error: {0}")]
    TungsteniteError(#[from] WsError),
    #[error(transparent)]
    TungsteniteSendTimeoutError(#[from] mpsc::error::SendTimeoutError<Message>),
    #[error(transparent)]
    TungsteniteSendError(#[from] mpsc::error::SendError<Message>),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Websocket has been closed: {0}")]
    WsClosed(String),
    #[error("Common error: {0}")]
    CommonError(String),
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum WS_ERROR_NO {
    DSN_ERROR = 0xE000,
    WEBSOCKET_ERROR = 0xE001,
    CONN_CLOSED = 0xE002,
    SEND_MESSAGE_TIMEOUT = 0xE003,
    RECV_MESSAGE_TIMEOUT = 0xE004,
    IO_ERROR = 0xE005,
    UNAUTHORIZED = 0xE006,
    DE_ERROR = 0xE007,
    WEBSOCKET_DISCONNECTED = 0xE008,
    TLS_ERROR = 0xE009,
}

impl WS_ERROR_NO {
    pub const fn as_code(&self) -> Code {
        Code::new(*self as _)
    }
}

impl Error {
    pub const fn errno(&self) -> Code {
        match self {
            Error::TaosError(error) => error.code(),
            Error::Unauthorized(_) => Code::new(WS_ERROR_NO::UNAUTHORIZED as _),
            Error::Dsn(_) => Code::new(WS_ERROR_NO::DSN_ERROR as _),
            Error::IoError(_) => Code::new(WS_ERROR_NO::IO_ERROR as _),
            Error::TungsteniteError(_) => Code::new(WS_ERROR_NO::WEBSOCKET_ERROR as _),
            Error::SendTimeoutError(_) => Code::new(WS_ERROR_NO::SEND_MESSAGE_TIMEOUT as _),
            Error::FlumeSendError(_) => Code::new(WS_ERROR_NO::CONN_CLOSED as _),
            Error::DeError(_) => Code::new(WS_ERROR_NO::DE_ERROR as _),
            _ => Code::FAILED,
        }
    }

    pub fn errstr(&self) -> String {
        match self {
            Error::TaosError(error) => error.message(),
            _ => format!("{self}"),
        }
    }
}

impl From<Error> for RawError {
    fn from(value: Error) -> Self {
        match value {
            Error::TaosError(error) => error,
            error => {
                let code = error.errno();
                if code == Code::FAILED {
                    RawError::from_any(error)
                } else {
                    RawError::new(code, error.to_string())
                }
            }
        }
    }
}

type BlockFuture = Pin<Box<dyn Future<Output = RawResult<Option<RawBlock>>> + Send>>;

pub struct ResultSet {
    pub(crate) sender: WsQuerySender,
    pub(crate) args: WsResArgs,
    pub(crate) fields: Option<Vec<Field>>,
    pub(crate) fields_count: usize,
    pub(crate) affected_rows: usize,
    pub(crate) precision: Precision,
    pub(crate) summary: (usize, usize),
    pub(crate) timing: Duration,
    pub(crate) block_future: Option<BlockFuture>,
    pub(crate) closer: Option<oneshot::Sender<()>>,
    pub(crate) metrics: QueryMetrics,
    pub(crate) blocks_buffer: Option<mpsc::Receiver<RawResult<(RawBlock, Duration)>>>,
    pub(crate) fields_precisions: Option<Vec<i64>>,
    pub(crate) fields_scales: Option<Vec<i64>>,
    pub(crate) fetch_done_reader: Option<mpsc::Receiver<()>>,
    pub(crate) tz: Option<Tz>,
}

unsafe impl Sync for ResultSet {}
unsafe impl Send for ResultSet {}

impl ResultSet {
    async fn fetch(&mut self) -> RawResult<Option<RawBlock>> {
        if self.blocks_buffer.is_none() {
            return Ok(None);
        }
        let now = Instant::now();
        self.blocks_buffer
            .as_mut()
            .unwrap()
            .recv()
            .await
            .map(|res| {
                res.map(|(raw, timing)| {
                    self.metrics.time_cost_in_flume += now.elapsed();
                    self.timing = timing;
                    raw
                })
            })
            .transpose()
    }

    pub fn take_timing(&self) -> Duration {
        self.timing
    }

    pub async fn stop(&self) {
        println!("Stop result set: {}", self.args.id);
        if let Some((_, req_id)) = self.sender.results.remove_async(&self.args.id).await {
            self.sender.queries.remove_async(&req_id).await;
        }

        let _ = self.sender.send_only(WsSend::FreeResult(self.args)).await;
    }

    pub fn affected_rows64(&self) -> i64 {
        self.affected_rows as _
    }

    pub fn fields_precisions(&self) -> Option<&[i64]> {
        self.fields_precisions.as_deref()
    }

    pub fn fields_scales(&self) -> Option<&[i64]> {
        self.fields_scales.as_deref()
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
        static EMPTY_FIELDS: Vec<Field> = Vec::new();
        self.fields.as_ref().unwrap_or(&EMPTY_FIELDS)
    }

    fn summary(&self) -> (usize, usize) {
        self.summary
    }

    fn update_summary(&mut self, nrows: usize) {
        self.summary.0 += 1;
        self.summary.1 += nrows;
    }

    fn fetch_raw_block(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<RawResult<Option<RawBlock>>> {
        if let Some(mut f) = self.block_future.take() {
            let res = f.poll_unpin(cx);
            match res {
                std::task::Poll::Ready(v) => Poll::Ready(v),
                std::task::Poll::Pending => {
                    self.block_future = Some(f);
                    Poll::Pending
                }
            }
        } else {
            let mut f = self.fetch().boxed();
            let res = f.poll_unpin(cx);
            match res {
                std::task::Poll::Ready(v) => Poll::Ready(v),
                std::task::Poll::Pending => {
                    self.block_future = Some(unsafe { transmute(f) });
                    Poll::Pending
                }
            }
        }
    }

    fn timezone(&self) -> Option<Tz> {
        self.tz
    }
}

impl taos_query::Fetchable for ResultSet {
    fn affected_rows(&self) -> i32 {
        self.affected_rows as i32
    }

    fn precision(&self) -> taos_query::common::Precision {
        self.precision
    }

    fn fields(&self) -> &[Field] {
        static EMPTY: Vec<Field> = Vec::new();
        self.fields.as_deref().unwrap_or(EMPTY.as_slice())
    }

    fn summary(&self) -> (usize, usize) {
        self.summary
    }

    fn update_summary(&mut self, nrows: usize) {
        self.summary.0 += 1;
        self.summary.1 += nrows;
    }

    fn fetch_raw_block(&mut self) -> RawResult<Option<RawBlock>> {
        taos_query::block_in_place_or_global(self.fetch())
    }
}

impl Debug for ResultSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultSet")
            .field("args", &self.args)
            .field("fields", &self.fields)
            .field("fields_count", &self.fields_count)
            .field("affected_rows", &self.affected_rows)
            .field("precision", &self.precision)
            .finish_non_exhaustive()
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        tracing::trace!("dropping result set, metrics: {:?}", self.metrics);

        let args = self.args;
        let query_sender = self.sender.clone();
        let closer = self.closer.take();
        let blocks_rx = self.blocks_buffer.take();
        let fetch_done_rx = self.fetch_done_reader.take();

        let clean = move || {
            if let Some((_, req_id)) = query_sender.results.remove(&args.id) {
                query_sender.queries.remove(&req_id);
            }
            if let Some(blocks_rx) = blocks_rx {
                drop(blocks_rx);
            }
            if let Some(closer) = closer {
                let _ = closer.send(());
            }
            if let Some(mut fetch_done_rx) = fetch_done_rx {
                tracing::trace!("waiting for fetch done, args: {args:?}");
                let _ = fetch_done_rx.try_recv();
                tracing::trace!(
                    "sending free result message after fetch done or timeout, args: {args:?}"
                );
                let _ = query_sender.send_blocking(WsSend::FreeResult(args));
            }
        };

        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(clean);
        } else {
            std::thread::spawn(clean);
        }
    }
}

pub async fn check_server_status<T>(dsn: &str, fqdn: T, port: i32) -> RawResult<(i32, String)>
where
    T: Into<Option<FastStr>>,
{
    let builder = TaosBuilder::from_dsn(dsn)?;
    let (mut ws_stream, _) = builder.connect_with_ty(EndpointType::Ws).await?;

    let req = WsSend::CheckServerStatus {
        req_id: generate_req_id(),
        fqdn: fqdn.into(),
        port,
    };

    ws_stream.send(req.to_msg()).await.map_err(Error::from)?;

    if let Some(Ok(message)) = ws_stream.next().await {
        if let Message::Text(text) = message {
            let resp: WsRecv = serde_json::from_str(&text)
                .map_err(|e| RawError::from_string(format!("failed to parse JSON: {e:?}")))?;

            let (_, data, ok) = resp.ok();
            if let WsRecvData::CheckServerStatus {
                status, details, ..
            } = data
            {
                ok?;
                return Ok((status, details));
            }
            return Err(RawError::from_string(
                "unexpected response data type".to_string(),
            ));
        }
        return Err(RawError::from_string(format!(
            "unexpected message type: {message:?}"
        )));
    }

    Ok((0, String::new()))
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;

    use super::*;

    #[test]
    fn test_errno() {
        let (tx, rx) = flume::unbounded();
        drop(rx);

        let send_err = tx.send(WsMessage::Command(WsSend::Version)).unwrap_err();

        let err = Error::FlumeSendError(send_err);
        let errno: i32 = err.errno().into();
        assert_eq!(WS_ERROR_NO::CONN_CLOSED as i32, errno);
    }

    #[test]
    fn test_is_support_binary_sql() -> anyhow::Result<()> {
        unsafe { std::env::set_var("RUST_LOG", "debug") };

        let version_a: &str = "3.3.0.0";
        let version_b: &str = "3.3.3.0";
        let version_c: &str = "2.6.0";

        assert!(!is_support_binary_sql(version_a));
        assert!(is_support_binary_sql(version_b));
        assert!(!is_support_binary_sql(version_c));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_client() -> anyhow::Result<()> {
        use futures::TryStreamExt;
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        let dsn = "http://localhost:6041";
        let client = WsTaos::from_dsn(dsn).await?;

        let _version = client.version();
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

        let mut rs = client.query("select * from abc_a.tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: i32,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        dbg!(values);

        assert_eq!(client.exec("drop database abc_a").await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn ws_show_databases() -> anyhow::Result<()> {
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        use futures::TryStreamExt;
        let dsn = "http://localhost:6041";
        let client = WsTaos::from_dsn(dsn).await?;
        let mut rs = client.query("show databases").await?;

        let mut blocks = rs.blocks();
        while let Some(block) = blocks.try_next().await? {
            let values = block.to_values();
            dbg!(values);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ws_write_raw_block() -> anyhow::Result<()> {
        let mut raw = RawBlock::parse_from_raw_block_v2(
            &[0, 0, 0, 0, 0, 0, 0, 0, 2][..],
            &[
                Field::new("ts", taos_query::common::Ty::Timestamp, 8),
                Field::new("v", taos_query::common::Ty::Bool, 1),
            ],
            &[8, 1],
            1,
            Precision::Millisecond,
        );
        raw.with_table_name("tb1");
        dbg!(&raw);

        use futures::TryStreamExt;
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        let dsn = "http://localhost:6041";

        let client = WsTaos::from_dsn(dsn).await?;

        let _version = client.version();

        client
            .exec_many([
                "drop database if exists write_raw_block_test",
                "create database write_raw_block_test keep 36500",
                "use write_raw_block_test",
                "create table if not exists tb1(ts timestamp, v bool)",
            ])
            .await?;

        client.write_raw_block(&raw).await?;

        let mut rs = client.query("select * from tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: Option<bool>,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        dbg!(values);

        assert_eq!(client.exec("drop database write_raw_block_test").await?, 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ws_write_raw_block_with_req_id() -> anyhow::Result<()> {
        let mut raw = RawBlock::parse_from_raw_block_v2(
            &[0, 0, 0, 0, 0, 0, 0, 0, 2][..],
            &[
                Field::new("ts", taos_query::common::Ty::Timestamp, 8),
                Field::new("v", taos_query::common::Ty::Bool, 1),
            ],
            &[8, 1],
            1,
            Precision::Millisecond,
        );
        raw.with_table_name("tb1");
        dbg!(&raw);

        use futures::TryStreamExt;
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        let dsn = "http://localhost:6041";
        let client = WsTaos::from_dsn(dsn).await?;

        let _version = client.version();

        client
            .exec_many([
                "drop database if exists test_ws_write_raw_block_with_req_id",
                "create database test_ws_write_raw_block_with_req_id keep 36500",
                "use test_ws_write_raw_block_with_req_id",
                "create table if not exists tb1(ts timestamp, v bool)",
            ])
            .await?;

        let req_id = 10003;
        client.write_raw_block_with_req_id(&raw, req_id).await?;

        let mut rs = client.query("select * from tb1").await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            v: Option<bool>,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        dbg!(values);

        assert_eq!(
            client
                .exec("drop database test_ws_write_raw_block_with_req_id")
                .await?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn ws_persistent_connection() -> anyhow::Result<()> {
        unsafe { std::env::set_var("RUST_LOG", "trace") };
        pretty_env_logger::init();
        let client = WsTaos::from_dsn("taosws://localhost:6041/").await?;
        let db = "ws_persistent_connection";
        assert_eq!(
            client.exec(format!("drop database if exists {db}")).await?,
            0
        );
        assert_eq!(
            client
                .exec(format!("create database {db} keep 36500"))
                .await?,
            0
        );
        assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            ).await?,
            0
        );

        // loop n times to test persistent connection
        // do not run in ci env
        let n = 100;
        let interval = Duration::from_secs(3);
        for _ in 0..n {
            assert_eq!(
                client
                    .exec(format!(
                        r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                    ))
                    .await?,
                2
            );
            assert_eq!(
                client
                    .exec(format!(
                        r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                    ))
                    .await?,
                2
            );
            // wait to test persistent connection
            tokio::time::sleep(interval).await;
        }

        client.exec(format!("drop database {db}")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn ws_async_data_flow() -> anyhow::Result<()> {
        unsafe { std::env::set_var("RUST_LOG", "debug") };
        let client = WsTaos::from_dsn("taosws://localhost:6041/").await?;
        let db = "ws_async_data_flow";
        assert_eq!(
            client.exec(format!("drop database if exists {db}")).await?,
            0
        );
        assert_eq!(
            client
                .exec(format!("create database {db} keep 36500"))
                .await?,
            0
        );
        assert_eq!(
            client.exec(
                format!("create table {db}.stb1(ts timestamp,\
                    b1 bool, c8i1 tinyint, c16i1 smallint, c32i1 int, c64i1 bigint,\
                    c8u1 tinyint unsigned, c16u1 smallint unsigned, c32u1 int unsigned, c64u1 bigint unsigned,\
                    cb1 binary(100), cn1 nchar(10),

                    b2 bool, c8i2 tinyint, c16i2 smallint, c32i2 int, c64i2 bigint,\
                    c8u2 tinyint unsigned, c16u2 smallint unsigned, c32u2 int unsigned, c64u2 bigint unsigned,\
                    cb2 binary(10), cn2 nchar(16)) tags (jt json)")
            ).await?,
            0
        );
        assert_eq!(
            client
                .exec(format!(
                    r#"insert into {db}.tb1 using {db}.stb1 tags('{{"key":"数据"}}')
                   values(0,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65535,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                ))
                .await?,
            2
        );
        assert_eq!(
            client
                .exec(format!(
                    r#"insert into {db}.tb2 using {db}.stb1 tags(NULL)
                   values(1,    true, -1,  -2,  -3,  -4,   1,   2,   3,   4,   'abc', '涛思',
                                false,-5,  -6,  -7,  -8,   5,   6,   7,   8,   'def', '数据')
                         (65536,NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL,
                                NULL, NULL,NULL,NULL,NULL, NULL,NULL,NULL,NULL, NULL,  NULL)"#
                ))
                .await?,
            2
        );

        let mut rs = client
            .query(format!("select * from {db}.tb1 order by ts limit 1"))
            .await?;

        #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
        #[allow(dead_code)]
        struct A {
            ts: String,
            b1: bool,
            c8i1: i8,
            c16i1: i16,
            c32i1: i32,
            c64i1: i64,
            c8u1: u8,
            c16u1: u16,
            c32u1: u32,
            c64u1: u64,

            c8i2: i8,
            c16i2: i16,
            c32i2: i32,
            c64i2: i64,
            c8u2: u8,
            c16u2: u16,
            c32u2: u32,
            c64u2: u64,

            cb1: String,
            cb2: String,
            cn1: String,
            cn2: String,
        }

        let values: Vec<A> = rs.deserialize().try_collect().await?;

        assert_eq!(
            values[0],
            A {
                ts: "1970-01-01T08:00:00+08:00".to_string(),
                b1: true,
                c8i1: -1,
                c16i1: -2,
                c32i1: -3,
                c64i1: -4,
                c8u1: 1,
                c16u1: 2,
                c32u1: 3,
                c64u1: 4,
                c8i2: -5,
                c16i2: -6,
                c32i2: -7,
                c64i2: -8,
                c8u2: 5,
                c16u2: 6,
                c32u2: 7,
                c64u2: 8,
                cb1: "abc".to_string(),
                cb2: "def".to_string(),
                cn1: "涛思".to_string(),
                cn2: "数据".to_string(),
            }
        );

        client.exec(format!("drop database {db}")).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_validate_sql() -> anyhow::Result<()> {
        let taos = WsTaos::from_dsn("ws://localhost:6041").await?;
        taos.validate_sql("create database if not exists test_1741338182")
            .await?;
        let _ = taos.validate_sql("select * from t0").await.unwrap_err();
        Ok(())
    }

    #[tokio::test]
    async fn test_check_server_status() -> anyhow::Result<()> {
        let dsn = "ws://localhost:6041";

        let (status, details) = check_server_status(dsn, Some("127.0.0.1".into()), 6030).await?;
        assert_eq!(status, 2);
        println!("status: {status}, details: {details}");

        let (status, details) = check_server_status(dsn, None, 0).await?;
        assert_eq!(status, 2);
        println!("status: {status}, details: {details}");

        Ok(())
    }

    #[tokio::test]
    async fn test_s_exec() -> anyhow::Result<()> {
        let taos = WsTaos::from_dsn("ws://localhost:6041").await?;
        taos.exec_many([
            "drop database if exists test_1748241233",
            "create database test_1748241233",
            "use test_1748241233",
            "create table t0(ts timestamp, c1 int)",
        ])
        .await?;

        let affected_rows = taos.s_exec("insert into t0 values(now, 1)").await?;
        assert_eq!(affected_rows, 1);

        taos.exec("drop database test_1748241233").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_read_timeout() {
        let (tx, _rx) = flume::bounded(1);

        let qs = WsQuerySender {
            version_info: VersionInfo::new("3.3.8.0"),
            req_id: Arc::new(AtomicU64::new(1)),
            results: Arc::new(QueryResMapper::new()),
            sender: tx,
            queries: Arc::new(QueryInner::new()),
            read_timeout: Duration::from_millis(50),
        };

        let req = WsSend::Query {
            req_id: 1001,
            sql: "show databases".to_string(),
        };
        let err = qs.send_recv(req).await.unwrap_err();
        assert_eq!(err.code(), WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code());
    }

    #[test]
    fn test_is_greater_than_or_equal_to() {
        assert!(is_greater_than_or_equal_to("3.3.2.0", "3.3.2.0"));
        assert!(is_greater_than_or_equal_to("3.3.3.0", "3.3.2.0"));
        assert!(!is_greater_than_or_equal_to("3.3.1.0", "3.3.2.0"));
        assert!(is_greater_than_or_equal_to("3.3.2.0.alpha", "3.3.2.0"));
        assert!(is_greater_than_or_equal_to("3.3.2.0-community", "3.3.2.0"));
        assert!(is_greater_than_or_equal_to("3.3.2-1-alpha", "3.3.2.0"));
        assert!(!is_greater_than_or_equal_to(
            "3.3.2-0.alpha",
            "3.3.2.1-community"
        ));
        assert!(is_greater_than_or_equal_to("3.3.2.x", "3.3.2.0"));
        assert!(is_greater_than_or_equal_to("3.-abc.x", "3.0.0.0"));
        assert!(!is_greater_than_or_equal_to("3.-abc1.x", "3.1.0.0"));
    }
}

#[cfg(feature = "rustls-ring-crypto-provider")]
#[cfg(test)]
mod cloud_tests {
    use futures::TryStreamExt;
    use taos_query::common::{Field, Precision, Timestamp, Ty, Value};
    use taos_query::{AsyncFetchable, AsyncQueryable, RawBlock};

    use crate::query::WsTaos;

    #[tokio::test]
    async fn test_sql() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_sql");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_sql");
            return Ok(());
        }

        let dsn = format!("{}/rust_test?token={}", url.unwrap(), token.unwrap());
        let taos = WsTaos::from_dsn(dsn).await?;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let tbname = format!("t_sql_{ts}");

        taos.exec_many([
            format!("create table {tbname} (ts timestamp, c1 int)"),
            format!("insert into {tbname} values(1655793421375, 1)"),
        ])
        .await?;

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct Record {
            ts: i64,
            c1: i32,
        }

        let mut rs = taos.query(format!("select * from {tbname}")).await?;
        let records: Vec<Record> = rs.deserialize().try_collect().await?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].ts, 1655793421375);
        assert_eq!(records[0].c1, 1);

        let mut rs = taos.query(format!("select * from {tbname}")).await?;
        let values = rs.to_records().await?;

        assert_eq!(values.len(), 1);
        assert_eq!(values[0].len(), 2);
        assert_eq!(
            values[0][0],
            Value::Timestamp(Timestamp::new(1655793421375, Precision::Millisecond))
        );
        assert_eq!(values[0][1], Value::Int(1));

        taos.exec(format!("drop table {tbname}")).await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_raw_block() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_write_raw_block");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_write_raw_block");
            return Ok(());
        }

        let dsn = format!("{}/rust_test?token={}", url.unwrap(), token.unwrap());
        let taos = WsTaos::from_dsn(dsn).await?;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let tbname = format!("t_raw_block_{ts}");

        taos.exec_many([
            format!("drop table if exists {tbname}"),
            format!("create table {tbname} (ts timestamp, c1 bool)"),
        ])
        .await?;

        let mut raw = RawBlock::parse_from_raw_block_v2(
            &[0, 0, 0, 0, 0, 2, 0, 0, 2][..],
            &[
                Field::new("ts", Ty::Timestamp, 8),
                Field::new("c1", Ty::Bool, 1),
            ],
            &[8, 1],
            1,
            Precision::Millisecond,
        );

        raw.with_table_name(&tbname);

        dbg!(&raw);

        taos.write_raw_block(&raw).await?;

        let mut rs = taos.query(format!("select * from {tbname}")).await?;

        #[derive(Debug, serde::Deserialize)]
        struct Record {
            ts: i64,
            c1: Option<bool>,
        }

        let records: Vec<Record> = rs.deserialize().try_collect().await?;

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].ts, 2199023255552);
        assert_eq!(records[0].c1, None);

        taos.exec(format!("drop table {tbname}")).await?;

        Ok(())
    }
}
