use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono_tz::Tz;
use dashmap::DashMap as HashMap;
use itertools::Itertools;
use messages::*;
use once_cell::sync::Lazy;
use taos_query::common::{Field, JsonMeta, RawMeta};
use taos_query::prelude::{Code, RawError};
use taos_query::tmq::{
    AsAsyncConsumer, AsConsumer, Assignment, IsAsyncData, IsAsyncMeta, IsData, IsOffset,
    MessageSet, SyncOnAsync, Timeout, VGroupId,
};
use taos_query::util::{generate_req_id, AsyncInlinable, CleanUp, Edition, InlinableRead};
use taos_query::{DeError, DsnError, IntoDsn, RawBlock, RawResult, TBuilder};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::Error as WsError;
use tracing::Instrument;

use crate::query::asyn::WS_ERROR_NO;
use crate::query::messages::WsConnReq;
use crate::{EndpointType, TaosBuilder};

mod conn;
mod messages;

type WsSender = flume::Sender<WsMessage>;
type WsTmqAgent = Arc<HashMap<ReqId, mpsc::Sender<RawResult<TmqRecvData>>>>;

#[derive(Debug, Clone)]
struct WsTmqSender {
    req_id: Arc<AtomicU64>,
    sender: WsSender,
    queries: WsTmqAgent,
}

impl WsTmqSender {
    fn req_id(&self) -> ReqId {
        self.req_id.fetch_add(1, Ordering::SeqCst)
    }

    async fn send_recv(&self, message: TmqSend) -> RawResult<TmqRecvData> {
        let req_id = message.req_id();
        let (data_tx, mut data_rx) = mpsc::channel(1);
        self.queries.insert(req_id, data_tx);

        let cleanup = || {
            let res = self.queries.remove(&req_id);
            tracing::trace!("tmq send_recv, clean up queries, req_id: 0x{req_id:x}, res: {res:?}");
        };
        let _cleanup = CleanUp { f: Some(cleanup) };

        tracing::trace!("tmq send_recv, req_id: 0x{req_id:x}, sending message: {message:?}");

        tokio::time::timeout(
            Duration::from_secs(5),
            self.sender.send_async(WsMessage::Command(message)),
        )
        .await
        .map_err(WsTmqError::from)?
        .map_err(WsTmqError::from)?;

        tracing::trace!("tmq send_recv, message sent, waiting for response, req_id: 0x{req_id:x}");

        let data = data_rx.recv().await.ok_or(WsTmqError::ChannelClosedError)?;

        tracing::trace!("tmq send_recv, req_id: 0x{req_id:x}, received data: {data:?}");

        data
    }
}

#[derive(Debug)]
pub struct TmqBuilder {
    info: TaosBuilder,
    conf: TmqInit,
    timeout: Timeout,
    server_version: std::sync::OnceLock<String>,
}

impl TBuilder for TmqBuilder {
    type Target = Consumer;

    fn available_params() -> &'static [&'static str] {
        &[
            "token",
            "timeout",
            "group.id",
            "client.id",
            "auto.offset.reset",
            "enable.auto.commit",
            "auto.commit.interval.ms",
            "experimental.snapshot.enable",
            "msg.with.table.name",
            "msg.enable.batchmeta",
            "msg.consume.excluded",
            "msg.consume.rawdata",
            "bearer_token",
            "td.connect.token",
        ]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::new(dsn)
    }

    fn client_version() -> &'static str {
        "0"
    }

    fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        taos_query::block_in_place_or_global(self.build_consumer())
    }

    fn server_version(&self) -> RawResult<&str> {
        Ok("3.0.0.0")
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        todo!()
    }

    fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        let addr = self.info.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
        }

        let taos = TBuilder::build(&self.info)?;

        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(&taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TmqBuilder {
    type Target = Consumer;

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::new(dsn)
    }

    fn client_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }

    async fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        Ok(())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        self.build_consumer().await
    }

    async fn server_version(&self) -> RawResult<&str> {
        match self.server_version.get().map(String::as_str) {
            Some(v) => Ok(v),
            None => {
                use taos_query::prelude::AsyncQueryable;

                let taos = taos_query::AsyncTBuilder::build(&self.info).await?;
                // Ensure server is ready.
                let version: Option<String> = taos.query_one("select server_version()").await?;
                if let Some(version) = version {
                    let _ = self.server_version.set(version);
                }
                self.server_version
                    .get()
                    .map(String::as_str)
                    .ok_or_else(|| RawError::from_string("Server version is unknown"))
            }
        }
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        taos_query::AsyncTBuilder::get_edition(self)
            .await
            .map(|edition| edition.is_enterprise_edition())
    }

    async fn get_edition(&self) -> RawResult<taos_query::util::Edition> {
        use taos_query::prelude::AsyncQueryable;

        let taos = taos_query::AsyncTBuilder::build(&self.info).await?;
        // Ensure server is ready
        taos.exec("select server_version()").await?;

        let addr = self.info.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
        }

        let grant: RawResult<Option<(String, bool)>> = AsyncQueryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .await;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                AsyncQueryable::query_one(&taos, "show grants").await;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[derive(Debug)]
struct WsMessageBase {
    is_support_fetch_raw: bool,
    sender: WsTmqSender,
    message_id: MessageId,
    raw_blocks: Arc<Mutex<Option<VecDeque<RawBlock>>>>,
    tz: Option<Tz>,
}

impl WsMessageBase {
    fn new(
        is_support_fetch_raw: bool,
        sender: WsTmqSender,
        message_id: MessageId,
        tz: Option<Tz>,
    ) -> Self {
        Self {
            is_support_fetch_raw,
            sender,
            message_id,
            raw_blocks: Arc::new(Mutex::new(None)),
            tz,
        }
    }

    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        let block = if self.is_support_fetch_raw {
            self.fetch_raw_block_new().await?
        } else {
            self.fetch_raw_block_old().await?
        };

        Ok(block.map(|b| b.with_timezone(self.tz)))
    }

    async fn fetch_raw_block_new(&self) -> RawResult<Option<RawBlock>> {
        let raw_blocks_option = &mut *self.raw_blocks.lock().await;
        if let Some(raw_blocks) = raw_blocks_option {
            if !raw_blocks.is_empty() {
                return Ok(raw_blocks.pop_front());
            }
            return Ok(None);
        }

        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchRawData(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;

        if let TmqRecvData::Bytes(bytes) = data {
            let raw = RawBlock::parse_from_multi_raw_block(bytes)
                .map_err(|_| RawError::from_string("parse multi raw blocks error!"))?;
            if !raw.is_empty() {
                raw_blocks_option.replace(raw);
                return Ok(raw_blocks_option.as_mut().unwrap().pop_front());
            }
        }
        Ok(None)
    }

    async fn fetch_raw_block_old(&self) -> RawResult<Option<RawBlock>> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::Fetch(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        let fetch = if let TmqRecvData::Fetch(fetch) = data {
            fetch
        } else {
            unreachable!()
        };

        if fetch.completed {
            return Ok(None);
        }

        let msg = TmqSend::FetchBlock(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::Bytes(bytes) = data {
            let mut raw = RawBlock::parse_from_raw_block(bytes, fetch.precision);
            raw.with_field_names(fetch.fields().iter().map(Field::name));
            if let Some(name) = fetch.table_name {
                raw.with_table_name(name);
            }
            return Ok(Some(raw));
        }
        todo!()
    }

    async fn fetch_json_meta(&self) -> RawResult<JsonMeta> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchJsonMeta(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::FetchJsonMeta { data } = data {
            let json: JsonMeta = serde_json::from_value(data).map_err(WsTmqError::from)?;
            return Ok(json);
        }
        unreachable!()
    }

    async fn fetch_raw_meta(&self) -> RawResult<RawMeta> {
        let req_id = self.sender.req_id();
        let msg = TmqSend::FetchRaw(MessageArgs {
            req_id,
            message_id: self.message_id,
        });
        let data = self.sender.send_recv(msg).await?;
        if let TmqRecvData::Bytes(bytes) = data {
            let message_type = bytes.as_ref().read_u64().unwrap();
            debug_assert_eq!(message_type, 3, "should be meta message type");
            let mut slice = &bytes.iter().as_slice()[8..];
            let raw = RawMeta::read_inlined(&mut slice)
                .await
                .map_err(|err| RawError::from_string(format!("read raw meta error: {err:?}")))?;
            return Ok(raw);
        }
        unreachable!()
    }
}

#[derive(Debug)]
pub struct Meta(WsMessageBase);

#[async_trait::async_trait]
impl IsAsyncMeta for Meta {
    async fn as_raw_meta(&self) -> RawResult<RawMeta> {
        self.0.fetch_raw_meta().await
    }

    async fn as_json_meta(&self) -> RawResult<JsonMeta> {
        self.0.fetch_json_meta().await
    }
}

impl SyncOnAsync for Meta {}

#[derive(Debug)]
pub struct Data(WsMessageBase);

impl Data {
    pub async fn fetch_block(&self) -> RawResult<Option<RawBlock>> {
        self.0.fetch_raw_block().await
    }
}

impl Iterator for Data {
    type Item = RawResult<RawBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        taos_query::block_in_place_or_global(self.fetch_block()).transpose()
    }
}

#[async_trait::async_trait]
impl IsAsyncData for Data {
    async fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        self.0.fetch_raw_meta().await
    }

    async fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        self.fetch_block().await
    }
}

impl IsData for Data {
    fn as_raw_data(&self) -> RawResult<taos_query::common::RawData> {
        taos_query::block_in_place_or_global(self.0.fetch_raw_meta())
    }

    fn fetch_raw_block(&self) -> RawResult<Option<RawBlock>> {
        taos_query::block_in_place_or_global(self.fetch_block())
    }
}

pub enum WsMessageSet {
    Meta(Meta),
    Data(Data),
}

impl WsMessageSet {
    pub const fn message_type(&self) -> MessageType {
        match self {
            WsMessageSet::Meta(_) => MessageType::Meta,
            WsMessageSet::Data(_) => MessageType::Data,
        }
    }
}

impl Consumer {
    pub(crate) async fn poll_timeout(
        &self,
        timeout: Duration,
    ) -> RawResult<Option<(Offset, MessageSet<Meta, Data>)>> {
        tracing::trace!("poll_timeout: {timeout:?}");
        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        tokio::select! {
            biased;
            message = self.poll(timeout) => {
                tracing::trace!("poll_timeout message: {message:?}");
                message
            }
            _ = &mut sleep, if !sleep.is_elapsed() => {
                tracing::trace!("poll_timeout timeout");
                Ok(None)
            }
        }
    }

    async fn poll(&self, timeout: Duration) -> RawResult<Option<(Offset, MessageSet<Meta, Data>)>> {
        #[inline(always)]
        fn now() -> u64 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        }

        self.auto_commit().await;

        let permit = self
            .cache_sender
            .reserve()
            .await
            .expect("poll cache channel closed");

        let mut guard = self.cache_reader.lock().await;
        if !guard.is_empty() {
            if let Some(Some(data)) = guard.recv().await {
                tracing::trace!("poll data from cache, data: {data:?}");
                permit.send(None);
                return Ok(self.parse_data(data));
            }
        } else {
            let now = now();
            let last_poll = self.last_poll_time.load(Ordering::Relaxed);
            if now - last_poll < 60 * 1000 {
                tracing::trace!("poll data wait cache, now: {now:?}, last poll: {last_poll}");
                if let Some(Some(data)) = guard.recv().await {
                    tracing::trace!("poll data wait cache, data: {data:?}");
                    permit.send(None);
                    return Ok(self.parse_data(data));
                }
            }

            tracing::warn!("poll data lost: no data received from cache for over 60 seconds, possible data loss");
        }

        drop(guard);

        self.last_poll_time.store(now(), Ordering::Relaxed);

        let req = TmqSend::Poll {
            req_id: self.sender.req_id(),
            blocking_time: (timeout.as_millis() / 2) as _,
            message_id: self.message_id.load(Ordering::Relaxed),
        };

        let now = tokio::time::Instant::now();
        let data = self.sender.send_recv(req).await?;
        tracing::trace!(
            "poll received data: {data:?}, elapsed: {}ms",
            now.elapsed().as_millis()
        );
        Ok(self.parse_data(data))
    }

    async fn auto_commit(&self) {
        if !self.auto_commit {
            return;
        }

        if let Some(offset) = &self.auto_commit_offset.0 {
            let now = Instant::now();
            let last_commit = self.auto_commit_offset.1;
            let interval = Duration::from_millis(self.auto_commit_interval_ms.unwrap());
            if now - last_commit > interval {
                tracing::trace!("poll auto commit, commit offset: {offset:?}");
                if let Err(err) = AsAsyncConsumer::commit(self, offset.clone()).await {
                    tracing::error!("poll auto commit failed, err: {err:?}");
                } else {
                    // Safety: This code guarantees that there is no concurrent access to the `auto_commit_offset` field.
                    unsafe {
                        let ptr = self as *const Self as *mut Self;
                        let auto_commit_offset = &mut (*ptr).auto_commit_offset;
                        auto_commit_offset.0 = None;
                        auto_commit_offset.1 = now;
                    }
                }
            }
        }
    }

    fn parse_data(&self, data: TmqRecvData) -> Option<(Offset, MessageSet<Meta, Data>)> {
        if let TmqRecvData::Poll(TmqPoll {
            message_id,
            database,
            have_message,
            topic,
            vgroup_id,
            message_type,
            offset,
            timing,
        }) = data
        {
            if !have_message {
                return None;
            }

            let offset = Offset {
                message_id,
                database,
                topic,
                vgroup_id,
                offset,
                timing,
            };

            if self.auto_commit {
                tracing::trace!("poll auto commit, set offset: {offset:?}");

                // Safety: This code guarantees that there is no concurrent access to the `auto_commit_offset` field.
                unsafe {
                    let ptr = self as *const Self as *mut Self;
                    let auto_commit_offset = &mut (*ptr).auto_commit_offset;
                    auto_commit_offset.0 = Some(offset.clone());
                }
            }

            self.message_id.store(message_id, Ordering::Relaxed);

            let message = WsMessageBase::new(
                self.support_fetch_raw(),
                self.sender.clone(),
                message_id,
                self.tz,
            );

            return match message_type {
                MessageType::Meta => Some((offset, MessageSet::Meta(Meta(message)))),
                MessageType::Data => Some((offset, MessageSet::Data(Data(message)))),
                MessageType::MetaData => Some((
                    offset,
                    MessageSet::MetaData(
                        Meta(message),
                        Data(WsMessageBase::new(
                            self.support_fetch_raw(),
                            self.sender.clone(),
                            message_id,
                            self.tz,
                        )),
                    ),
                )),
                MessageType::Invalid => unreachable!(),
            };
        }

        unreachable!()
    }

    fn support_fetch_raw(&self) -> bool {
        self.support_fetch_raw.load(Ordering::Relaxed)
    }

    async fn topics(&self) -> Vec<String> {
        self.topics.read().await.clone()
    }

    async fn with_topics(&self, topics: Vec<String>) {
        *self.topics.write().await = topics;
    }
}

#[async_trait::async_trait]
impl AsAsyncConsumer for Consumer {
    type Offset = Offset;
    type Meta = Meta;
    type Data = Data;

    async fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        let topics = topics.into_iter().map(Into::into).collect_vec();
        self.with_topics(topics.clone()).await;

        let action = TmqSend::Subscribe {
            req_id: self.sender.req_id(),
            req: self.tmq_conf.clone().disable_auto_commit(),
            topics: topics.clone(),
            conn: self.conn.clone(),
        };
        if let Err(err) = self.sender.send_recv(action).await {
            tracing::error!("subscribe error: {err:?}");
            if self.tmq_conf.enable_batch_meta.is_none() {
                return Err(err);
            }

            let code: i32 = err.code().into();
            if code & 0xFFFF == 0xFFFE {
                // subscribe conf error -2.
                let action = TmqSend::Subscribe {
                    req_id: self.sender.req_id(),
                    req: self
                        .tmq_conf
                        .clone()
                        .disable_batch_meta()
                        .disable_auto_commit(),
                    topics: topics.clone(),
                    conn: self.conn.clone(),
                };
                self.sender.send_recv(action).await?;
            } else {
                return Err(err);
            }
        }

        if let Some(offset) = self.tmq_conf.offset_seek.as_deref() {
            let offsets = offset
                .split(',')
                .map(|s| {
                    s.split(':')
                        .map(|i| i.parse::<i64>().unwrap())
                        .collect_vec()
                })
                .collect_vec();

            let topic_name = &topics[0];

            for offset in offsets {
                let vgroup_id = offset[0] as i32;
                let offset = offset[1];
                tracing::trace!(
                    "topic {topic_name} seeking to offset {offset} for vgroup {vgroup_id}"
                );

                let action = TmqSend::Seek(OffsetSeekArgs {
                    req_id: self.sender.req_id(),
                    topic: topic_name.to_string(),
                    vgroup_id,
                    offset,
                });

                let _ = self
                    .sender
                    .send_recv(action)
                    .await
                    .unwrap_or(crate::consumer::messages::TmqRecvData::Seek { timing: 0 });
            }
        }

        Ok(())
    }

    async fn unsubscribe(self) {
        let req_id = self.sender.req_id();
        tracing::trace!("unsubscribe req_id:{} start", req_id);
        let action = TmqSend::Unsubscribe { req_id };
        if let Err(err) = self.sender.send_recv(action).await {
            tracing::warn!("unsubscribe error: {err:?}");
        }
        drop(self);
    }

    async fn recv_timeout(
        &self,
        timeout: taos_query::tmq::Timeout,
    ) -> RawResult<
        Option<(
            Self::Offset,
            taos_query::tmq::MessageSet<Self::Meta, Self::Data>,
        )>,
    > {
        match timeout {
            Timeout::Never | Timeout::None => self.poll_timeout(Duration::MAX).await,
            Timeout::Duration(timeout) => self.poll_timeout(timeout).await,
        }
    }

    async fn commit_all(&self) -> RawResult<()> {
        let has_data = { !self.cache_reader.lock().await.is_empty() };
        if has_data {
            return Err(RawError::from_string(
                "polling data is in queue, can't commit all",
            ));
        }

        let action = TmqSend::Commit(MessageArgs {
            req_id: self.sender.req_id(),
            message_id: 0,
        });
        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        let action = TmqSend::Commit(MessageArgs {
            req_id: self.sender.req_id(),
            message_id: offset.message_id,
        });
        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn commit_offset(
        &self,
        topic_name: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        let action = TmqSend::CommitOffset(OffsetSeekArgs {
            req_id: self.sender.req_id(),
            topic: topic_name.to_string(),
            vgroup_id,
            offset,
        });
        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn list_topics(&self) -> RawResult<Vec<String>> {
        Ok(self.topics().await)
    }

    async fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        let topics = self.topics().await;
        tracing::trace!("topics: {topics:?}");
        let mut res = Vec::new();
        for topic in topics {
            let assignments = self.topic_assignment(&topic).await;
            res.push((topic, assignments));
        }
        Some(res)
    }

    async fn topic_assignment(&self, topic: &str) -> Vec<Assignment> {
        let action = TmqSend::Assignment(TopicAssignmentArgs {
            req_id: self.sender.req_id(),
            topic: topic.to_string(),
        });
        match self.sender.send_recv(action).await.ok() {
            Some(TmqRecvData::Assignment(TopicAssignment { assignment, timing })) => {
                tracing::trace!("assignment: {assignment:?}, timing: {timing:?}");
                assignment
            }
            _ => vec![],
        }
    }

    async fn offset_seek(
        &mut self,
        topic: &str,
        vgroup_id: VGroupId,
        offset: i64,
    ) -> RawResult<()> {
        let action = TmqSend::Seek(OffsetSeekArgs {
            req_id: self.sender.req_id(),
            topic: topic.to_string(),
            vgroup_id,
            offset,
        });
        let _ = self.sender.send_recv(action).await?;
        Ok(())
    }

    async fn committed(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        let action = TmqSend::Committed(OffsetArgs {
            req_id: self.sender.req_id(),
            topic_vgroup_ids: vec![OffsetInnerArgs {
                topic: topic.to_string(),
                vgroup_id,
            }],
        });
        let data = self.sender.send_recv(action).await?;
        if let TmqRecvData::Committed { committed } = data {
            return Ok(committed[0]);
        }
        unreachable!()
    }

    async fn position(&self, topic: &str, vgroup_id: VGroupId) -> RawResult<i64> {
        let action = TmqSend::Position(OffsetArgs {
            req_id: self.sender.req_id(),
            topic_vgroup_ids: vec![OffsetInnerArgs {
                topic: topic.to_string(),
                vgroup_id,
            }],
        });
        let data = self.sender.send_recv(action).await?;
        if let TmqRecvData::Position { position } = data {
            return Ok(position[0]);
        }
        unreachable!()
    }

    fn default_timeout(&self) -> Timeout {
        self.timeout
    }
}

impl AsConsumer for Consumer {
    type Offset = Offset;
    type Meta = Meta;
    type Data = Data;

    fn subscribe<T: Into<String>, I: IntoIterator<Item = T> + Send>(
        &mut self,
        topics: I,
    ) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::subscribe(self, topics))
    }

    fn recv_timeout(
        &self,
        timeout: Timeout,
    ) -> RawResult<Option<(Self::Offset, MessageSet<Self::Meta, Self::Data>)>> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::recv_timeout(
            self, timeout,
        ))
    }

    fn commit_all(&self) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::commit_all(self))
    }

    fn commit(&self, offset: Self::Offset) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::commit(self, offset))
    }

    fn commit_offset(&self, topic_name: &str, vgroup_id: VGroupId, offset: i64) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::commit_offset(
            self, topic_name, vgroup_id, offset,
        ))
    }

    fn list_topics(&self) -> RawResult<Vec<String>> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::list_topics(self))
    }

    fn assignments(&self) -> Option<Vec<(String, Vec<Assignment>)>> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::assignments(self))
    }

    fn offset_seek(&mut self, topic: &str, vg_id: VGroupId, offset: i64) -> RawResult<()> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::offset_seek(
            self, topic, vg_id, offset,
        ))
    }

    fn committed(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::committed(
            self, topic, vg_id,
        ))
    }

    fn position(&self, topic: &str, vg_id: VGroupId) -> RawResult<i64> {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::position(
            self, topic, vg_id,
        ))
    }

    fn unsubscribe(self) {
        taos_query::block_in_place_or_global(<Consumer as AsAsyncConsumer>::unsubscribe(self));
    }
}

static AVAILABLE_PARAMS: Lazy<HashSet<&str>> = Lazy::new(|| {
    let mut params = HashSet::new();
    params.insert("group.id");
    params.insert("client.id");
    params.insert("auto.offset.reset");
    params.insert("experimental.snapshot.enable");
    params.insert("snapshot");
    params.insert("msg.with.table.name");
    params.insert("enable.auto.commit");
    params.insert("auto.commit.interval.ms");
    params.insert("offset");
    params.insert("msg.enable.batchmeta");
    params.insert("enable.batch.meta");
    params.insert("batchmeta");
    params.insert("msg.consume.excluded");
    params.insert("replica");
    params.insert("msg.consume.rawdata");
    params.insert("timeout");
    params.insert("max_queue_length");
    params.insert("max_errors_in_window");
    params.insert("busy_threshold");
    params.insert("compression");
    params.insert("health_check_window_in_second");
    params.insert("td.connect.websocket.scheme");
    params
});

impl TmqBuilder {
    pub fn new<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        let mut dsn = dsn.into_dsn()?;

        let token = dsn
            .remove("td.connect.token")
            .or_else(|| dsn.remove("bearer_token"));
        if let Some(token) = token {
            dsn.set("td.connect.token".to_string(), token.clone());
            dsn.set("bearer_token".to_string(), token);
        }

        let info = TaosBuilder::from_dsn(&dsn)?;

        let group_id = dsn
            .params
            .get("group.id")
            .map(ToString::to_string)
            .ok_or_else(|| DsnError::RequireParam("group.id".to_string()))?;

        let client_id = dsn.params.get("client.id").map(ToString::to_string);
        let offset_reset = dsn.params.get("auto.offset.reset").map(ToString::to_string);

        let mut auto_commit = "false".to_owned();
        if let Some(s) = dsn.params.get("enable.auto.commit") {
            if !s.is_empty() {
                let s1 = s.to_lowercase();
                let _ = s1.parse::<bool>().map_err(|_| {
                    DsnError::InvalidParam("enable.auto.commit".to_owned(), s.to_owned())
                })?;
                auto_commit = s1;
            }
        }

        let mut auto_commit_interval_ms = None;
        if auto_commit == "true" {
            let mut ms = "5000";
            if let Some(s) = dsn.params.get("auto.commit.interval.ms") {
                if !s.is_empty() {
                    let _ = s.to_lowercase().parse::<u64>().map_err(|_| {
                        DsnError::InvalidParam("auto.commit.interval.ms".to_owned(), s.to_owned())
                    })?;
                    ms = s;
                }
            }
            auto_commit_interval_ms = Some(ms.to_owned());
        }

        let snapshot_enable = dsn
            .params
            .get("experimental.snapshot.enable")
            .or_else(|| dsn.params.get("snapshot"))
            .and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            })
            .unwrap_or("false".to_string());

        let with_table_name = dsn
            .params
            .get("msg.with.table.name")
            .and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            })
            .unwrap_or("true".to_string());

        let timeout = if let Some(timeout) = dsn.get("timeout") {
            Timeout::from_str(timeout).map_err(RawError::from_any)?
        } else {
            Timeout::Duration(Duration::from_secs(5))
        };

        let offset_seek = dsn.params.get("offset").and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });

        let enable_batch_meta = dsn
            .get("msg.enable.batchmeta")
            .or_else(|| dsn.params.get("batchmeta"))
            .or_else(|| dsn.params.get("enable.batch.meta"))
            .map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    "1".to_string()
                } else {
                    s.to_string()
                }
            })
            .or_else(|| Some("1".to_string()))
            .and_then(|s| {
                if matches!(
                    s.as_str(),
                    "0" | "F" | "false" | "FALSE" | "no" | "N" | "NO"
                ) {
                    None
                } else {
                    Some(s)
                }
            });

        let msg_consume_excluded = dsn
            .get("msg.consume.excluded")
            .or_else(|| dsn.params.get("replica"))
            .map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    "1".to_string()
                } else {
                    s.to_string()
                }
            });

        let msg_consume_rawdata = dsn.get("msg.consume.rawdata").map(|s| {
            let s = s.trim();
            if s.is_empty() {
                "1".to_string()
            } else {
                s.to_string()
            }
        });

        let config = {
            let filtered: std::collections::HashMap<_, _> = dsn
                .params
                .iter()
                .filter(|(key, _)| !AVAILABLE_PARAMS.contains(key.as_str()) && key.contains('.'))
                .map(|(key, val)| (key.clone(), val.clone()))
                .collect();

            if filtered.is_empty() {
                None
            } else {
                Some(filtered)
            }
        };

        let conf = TmqInit {
            group_id,
            client_id,
            offset_reset,
            auto_commit,
            auto_commit_interval_ms,
            snapshot_enable,
            with_table_name,
            offset_seek,
            enable_batch_meta,
            msg_consume_excluded,
            msg_consume_rawdata,
            config,
        };

        Ok(Self {
            info,
            conf,
            timeout,
            server_version: std::sync::OnceLock::new(),
        })
    }

    async fn build_consumer(&self) -> RawResult<Consumer> {
        let conn_id = generate_req_id();
        let span = tracing::info_span!("tmq_conn", conn_id = conn_id);

        let (ws_stream, version) = self
            .info
            .connect_with_ty(EndpointType::Tmq)
            .instrument(span.clone())
            .await?;

        let (message_tx, message_rx) = flume::bounded(100);
        let (close_tx, close_rx) = watch::channel(false);

        let (poll_cache_tx, poll_cache_rx) = mpsc::channel(8);
        let _ = poll_cache_tx.send(None).await;

        let tmq_sender = WsTmqSender {
            req_id: Arc::new(AtomicU64::new(1)),
            queries: WsTmqAgent::default(),
            sender: message_tx,
        };

        let topics: Arc<RwLock<Vec<String>>> = Arc::default();

        let support_fetch_raw = conn::is_fetch_raw_supported(&version);
        let support_fetch_raw = Arc::new(AtomicBool::new(support_fetch_raw));

        tokio::spawn(
            conn::run(
                self.info.clone(),
                ws_stream,
                tmq_sender.clone(),
                poll_cache_tx.clone(),
                message_rx,
                close_rx,
                self.conf.clone(),
                topics.clone(),
                support_fetch_raw.clone(),
            )
            .instrument(span),
        );

        let auto_commit_interval_ms = self
            .conf
            .auto_commit_interval_ms
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok());

        Ok(Consumer {
            conn: self.info.build_conn_request(),
            conn_id,
            tmq_conf: self.conf.clone(),
            sender: tmq_sender,
            close_signal: close_tx,
            timeout: self.timeout,
            topics,
            support_fetch_raw,
            auto_commit: self.conf.auto_commit == "true",
            auto_commit_interval_ms,
            auto_commit_offset: (None, Instant::now()),
            message_id: AtomicU64::new(0),
            cache_sender: poll_cache_tx,
            cache_reader: Mutex::new(poll_cache_rx),
            last_poll_time: AtomicU64::new(0),
            tz: self.info.tz,
        })
    }
}

#[derive(Debug)]
pub struct Consumer {
    conn: WsConnReq,
    conn_id: u64,
    tmq_conf: TmqInit,
    sender: WsTmqSender,
    close_signal: watch::Sender<bool>,
    timeout: Timeout,
    topics: Arc<RwLock<Vec<String>>>,
    support_fetch_raw: Arc<AtomicBool>,
    auto_commit: bool,
    auto_commit_interval_ms: Option<u64>,
    auto_commit_offset: (Option<Offset>, Instant),
    message_id: AtomicU64,
    cache_sender: mpsc::Sender<Option<TmqRecvData>>,
    cache_reader: Mutex<mpsc::Receiver<Option<TmqRecvData>>>,
    last_poll_time: AtomicU64,
    tz: Option<Tz>,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        tracing::trace!("dropping tmq connection, conn_id: {}", self.conn_id);
        let _ = self.close_signal.send(true);
    }
}

#[derive(Debug, Clone)]
pub struct Offset {
    message_id: MessageId,
    database: String,
    topic: String,
    vgroup_id: i32,
    offset: i64,
    timing: i64,
}

impl IsOffset for Offset {
    fn database(&self) -> &str {
        &self.database
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn vgroup_id(&self) -> i32 {
        self.vgroup_id
    }

    fn offset(&self) -> i64 {
        self.offset
    }

    fn timing(&self) -> i64 {
        self.timing
    }
}

#[derive(Debug, Error)]
pub enum WsTmqError {
    #[error("{0}")]
    Dsn(#[from] DsnError),
    #[error("{0}")]
    FetchError(#[from] oneshot::error::RecvError),
    #[error("{0}")]
    Send2Error(#[from] mpsc::error::SendError<Message>),
    #[error(transparent)]
    SendTimeoutError(#[from] mpsc::error::SendTimeoutError<Message>),
    #[error("{0}")]
    DeError(#[from] DeError),
    #[error("Deserialize json error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("{0}")]
    WsError(#[from] WsError),
    #[error("{0}")]
    TaosError(#[from] RawError),
    #[error("Receive timeout in {0}")]
    QueryTimeout(String),
    #[error("channel closed")]
    ChannelClosedError,
    #[error("{0}")]
    WsSendElapsed(#[from] tokio::time::error::Elapsed),
    #[error("{0}")]
    FlumeSendError(#[from] flume::SendError<WsMessage>),
}

unsafe impl Send for WsTmqError {}
unsafe impl Sync for WsTmqError {}

impl WsTmqError {
    pub const fn errno(&self) -> Code {
        match self {
            WsTmqError::TaosError(err) => err.code(),
            WsTmqError::WsError(_) => WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code(),
            _ => Code::FAILED,
        }
    }

    pub fn errstr(&self) -> String {
        match self {
            WsTmqError::TaosError(error) => error.message(),
            _ => format!("{self}"),
        }
    }
}

impl From<WsTmqError> for RawError {
    fn from(value: WsTmqError) -> Self {
        match value {
            WsTmqError::TaosError(error) => error,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::{mpsc, oneshot, watch};

    use super::{TaosBuilder, TmqBuilder};
    use crate::consumer::{Data, Meta};

    #[tokio::test]
    async fn test_ws_tmq_meta_batch() -> anyhow::Result<()> {
        use taos_query::prelude::{AsyncTBuilder, *};

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::DEBUG)
            .compact()
            .try_init();

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists ws_tmq_meta_batch",
            "drop database if exists ws_tmq_meta_batch",
            "create database ws_tmq_meta_batch wal_retention_period 3600",
            "create topic ws_tmq_meta_batch with meta as database ws_tmq_meta_batch",
            "use ws_tmq_meta_batch",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta_batch2",
            "create database if not exists ws_tmq_meta_batch2 wal_retention_period 3600",
            "use ws_tmq_meta_batch2",
        ])
        .await?;

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=5s&auto.offset.reset=earliest&msg.enable.batchmeta=true&experimental.snapshot.enable=true",
        )?;

        let _ = dbg!(builder.get_edition().await);
        let _ = dbg!(builder.server_version().await);
        assert!(builder.ready().await);
        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["ws_tmq_meta_batch"]).await?;

        {
            let mut stream = consumer.stream();

            let mut count = 0;
            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();
                count += 1;

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                // 3. meta + data
                match message {
                    MessageSet::Meta(meta) => {
                        let _raw = meta.as_raw_meta().await?;
                        // taos.write_meta(raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        tracing::info!(count, batch.size = json.iter().len(), "{json:?}");
                        for meta in &json {
                            let sql = meta.to_string();
                            tracing::debug!(count, "sql: {}", sql);
                            if let Err(err) = taos.exec(sql).await {
                                match err.code() {
                                    Code::TAG_ALREADY_EXIST => {
                                        tracing::trace!("tag already exists")
                                    }
                                    Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                    Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                    Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                    Code::INVALID_COLUMN_NAME => {
                                        tracing::trace!("invalid column name")
                                    }
                                    Code::MODIFIED_ALREADY => {
                                        tracing::trace!("modified already done")
                                    }
                                    Code::TABLE_NOT_EXIST => {
                                        tracing::trace!("table does not exists")
                                    }
                                    Code::STABLE_NOT_EXIST => {
                                        tracing::trace!("stable does not exists")
                                    }
                                    _ => tracing::error!(count, "{}", err),
                                }
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(_data) = data.fetch_block().await? {}
                    }
                    _ => unreachable!(),
                }
                consumer.commit(offset).await?;
            }
        }

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec_many([
            "drop database ws_tmq_meta_batch2",
            "drop topic ws_tmq_meta_batch",
            "drop database ws_tmq_meta_batch",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ws_tmq_meta() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::DEBUG)
            .compact()
            .try_init();

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?
            .build()
            .await?;
        taos.exec_many([
            "drop topic if exists ws_tmq_meta",
            "drop database if exists ws_tmq_meta",
            "create database ws_tmq_meta wal_retention_period 3600",
            "create topic ws_tmq_meta with meta as database ws_tmq_meta",
            "use ws_tmq_meta",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // // kind 9: drop normal table
            // "drop table `table`",
            // // kind 10: drop child table
            // "drop table `tb2` `tb1`",
            // // kind 11: drop super table
            // "drop table `stb2`",
            // "drop table `stb1`",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta2",
            "create database if not exists ws_tmq_meta2 wal_retention_period 3600",
            "use ws_tmq_meta2",
        ])
        .await?;

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=5s&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["ws_tmq_meta"]).await?;

        {
            let mut stream = consumer.stream();

            let mut count = 0;
            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                //  similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();
                count += 1;

                // Different to kafka message, TDengine consumer would consume two kind of messages.
                //
                // 1. meta
                // 2. data
                // 3. meta + data
                match message {
                    MessageSet::Meta(meta) => {
                        let _raw = meta.as_raw_meta().await?;
                        // taos.write_meta(raw).await?;

                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in &json {
                            let sql = meta.to_string();
                            tracing::debug!("sql: {}", sql);
                            if let Err(err) = taos.exec(sql).await {
                                match err.code() {
                                    Code::TAG_ALREADY_EXIST => {
                                        tracing::trace!("tag already exists")
                                    }
                                    Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                    Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                    Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                    Code::INVALID_COLUMN_NAME => {
                                        tracing::trace!("invalid column name")
                                    }
                                    Code::MODIFIED_ALREADY => {
                                        tracing::trace!("modified already done")
                                    }
                                    Code::TABLE_NOT_EXIST => {
                                        tracing::trace!("table does not exists")
                                    }
                                    Code::STABLE_NOT_EXIST => {
                                        tracing::trace!("stable does not exists")
                                    }
                                    _ => tracing::error!(count, "{}", err),
                                }
                            }
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(_data) = data.fetch_block().await? {}
                    }
                    _ => unreachable!(),
                }
                consumer.commit(offset).await?;
            }
        }
        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec_many([
            "drop database ws_tmq_meta2",
            "drop topic ws_tmq_meta",
            "drop database ws_tmq_meta",
        ])
        .await?;
        Ok(())
    }

    #[test]
    fn test_ws_tmq_meta_sync() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?.build()?;
        taos.exec_many([
            "drop topic if exists ws_tmq_meta_sync",
            "drop database if exists ws_tmq_meta_sync",
            "create database ws_tmq_meta_sync wal_retention_period 3600",
            "create topic ws_tmq_meta_sync with meta as database ws_tmq_meta_sync",
            "use ws_tmq_meta_sync",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // kind 9: drop normal table
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta_sync2",
            "create database if not exists ws_tmq_meta_sync2 wal_retention_period 3600",
            "use ws_tmq_meta_sync2",
        ])?;

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_tmq_meta_sync"])?;

        let topics = consumer.list_topics();
        tracing::debug!("topics: {:?}", topics);

        let iter = consumer.iter_with_timeout(Timeout::from_secs(1));

        for msg in iter {
            let (offset, message) = msg?;
            // Offset contains information for topic name, database name and vgroup id,
            //  similar to kafka topic/partition/offset.
            let _ = offset.topic();
            let _ = offset.database();
            let _ = offset.vgroup_id();

            // Different to kafka message, TDengine consumer would consume two kind of messages.
            //
            // 1. meta
            // 2. data
            match message {
                MessageSet::Meta(meta) => {
                    let _raw = meta.as_raw_meta()?;
                    // taos.write_meta(raw)?;

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let json = meta.as_json_meta()?;
                    for meta in &json {
                        let sql = meta.to_string();
                        tracing::debug!("sql: {}", sql);
                        if let Err(err) = taos.exec(sql) {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => tracing::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => tracing::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => tracing::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => tracing::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => tracing::trace!("stable does not exists"),
                                _ => {
                                    tracing::error!("{}", err);
                                }
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let _block = block?;
                        // dbg!(block.table_name());
                        // dbg!(block);
                    }
                }
                _ => unreachable!(),
            }
            consumer.commit(offset)?;
        }

        // get assignments
        let assignments = consumer.assignments();
        tracing::debug!("assignments all: {:?}", assignments);

        if let Some(assignments) = assignments {
            for (topic, assignment_vec) in assignments {
                for assignment in assignment_vec {
                    tracing::debug!("assignment: {:?} {:?}", topic, assignment);
                    let vgroup_id = assignment.vgroup_id();
                    let end = assignment.end();

                    let position = consumer.position(&topic, vgroup_id);
                    tracing::debug!("position: {:?}", position);
                    let committed = consumer.committed(&topic, vgroup_id);
                    tracing::debug!("committed: {:?}", committed);

                    let res = consumer.offset_seek(&topic, vgroup_id, end);
                    tracing::debug!("seek: {:?}", res);

                    let position = consumer.position(&topic, vgroup_id);
                    tracing::debug!("after seek position: {:?}", position);
                    let committed = consumer.committed(&topic, vgroup_id);
                    tracing::debug!("after seek committed: {:?}", committed);

                    let res = consumer.commit_offset(&topic, vgroup_id, end);
                    tracing::debug!("commit offset: {:?}", res);

                    let position = consumer.position(&topic, vgroup_id);
                    tracing::debug!("after commit offset position: {:?}", position);
                    let committed = consumer.committed(&topic, vgroup_id);
                    tracing::debug!("after commit offset committed: {:?}", committed);
                }
            }
        }

        consumer.unsubscribe();

        std::thread::sleep(Duration::from_secs(5));

        taos.exec_many([
            "drop database ws_tmq_meta_sync2",
            "drop topic ws_tmq_meta_sync",
            "drop database ws_tmq_meta_sync",
        ])?;
        Ok(())
    }

    #[test]
    fn test_ws_tmq_metadata() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?.build()?;
        taos.exec_many([
            "drop topic if exists ws_tmq_meta_sync3",
            "drop database if exists ws_tmq_meta_sync3",
            "create database ws_tmq_meta_sync3 wal_retention_period 3600",
            "create topic ws_tmq_meta_sync3 with meta as database ws_tmq_meta_sync3",
            "use ws_tmq_meta_sync3",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, \
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16), \
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned) \
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思', \
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思', \
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL, \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
            // kind 9: drop normal table
            "drop table `table`",
            // kind 10: drop child table
            "drop table `tb2`, `tb1`",
            // kind 11: drop super table
            "drop table `stb2`",
            "drop table `stb1`",
        ])?;

        taos.exec_many([
            "drop database if exists ws_tmq_meta_sync32",
            "create database if not exists ws_tmq_meta_sync32 wal_retention_period 3600",
            "use ws_tmq_meta_sync32",
        ])?;

        let builder = TmqBuilder::new(
            "ws://localhost:6041?group.id=10&timeout=1000ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build()?;
        consumer.subscribe(["ws_tmq_meta_sync3"])?;

        let iter = consumer.iter_with_timeout(Timeout::from_secs(1));

        for msg in iter {
            let (offset, message) = msg?;
            // Offset contains information for topic name, database name and vgroup id,
            //  similar to kafka topic/partition/offset.
            let _ = offset.topic();
            let _ = offset.database();
            let _ = offset.vgroup_id();

            // Different to kafka message, TDengine consumer would consume two kind of messages.
            //
            // 1. meta
            // 2. data
            match message {
                MessageSet::Meta(meta) => {
                    let _raw = meta.as_raw_meta()?;

                    // meta data can be write to an database seamlessly by raw or json (to sql).
                    let json = meta.as_json_meta()?;
                    for json in &json {
                        let sql = json.to_string();
                        tracing::debug!("sql: {}", sql);
                        if let Err(err) = taos.exec(sql) {
                            match err.code() {
                                Code::TAG_ALREADY_EXIST => tracing::trace!("tag already exists"),
                                Code::TAG_NOT_EXIST => tracing::trace!("tag not exist"),
                                Code::COLUMN_EXISTS => tracing::trace!("column already exists"),
                                Code::COLUMN_NOT_EXIST => tracing::trace!("column not exists"),
                                Code::INVALID_COLUMN_NAME => tracing::trace!("invalid column name"),
                                Code::MODIFIED_ALREADY => tracing::trace!("modified already done"),
                                Code::TABLE_NOT_EXIST => tracing::trace!("table does not exists"),
                                Code::STABLE_NOT_EXIST => tracing::trace!("stable does not exists"),
                                _ => {
                                    tracing::error!("{}", err);
                                }
                            }
                        }
                    }
                }
                MessageSet::Data(data) => {
                    // data message may have more than one data block for various tables.
                    for block in data {
                        let _block = block?;
                    }
                }
                _ => unreachable!(),
            }
            consumer.commit(offset)?;
        }
        consumer.unsubscribe();

        std::thread::sleep(Duration::from_secs(5));

        taos.exec_many([
            "drop database ws_tmq_meta_sync32",
            "drop topic ws_tmq_meta_sync3",
            "drop database ws_tmq_meta_sync3",
        ])?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ws_tmq_poll_lost() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::TRACE)
            .compact()
            .try_init();

        let taos = TaosBuilder::from_dsn("taos://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists test_ws_tmq_poll_lost",
            "drop database if exists test_ws_tmq_poll_lost",
            "create database test_ws_tmq_poll_lost wal_retention_period 3600",
            "create topic test_ws_tmq_poll_lost with meta as database test_ws_tmq_poll_lost",
            "use test_ws_tmq_poll_lost",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "insert into tb1 using stb1 tags(NULL) values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists test_ws_tmq_poll_lost2",
            "create database if not exists test_ws_tmq_poll_lost2 wal_retention_period 3600",
            "use test_ws_tmq_poll_lost2",
        ])
        .await?;

        let builder = TmqBuilder::new(
            "taos://localhost:6041?group.id=10&timeout=200ms&auto.offset.reset=earliest",
        )?;
        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["test_ws_tmq_poll_lost"]).await?;
        unsafe { std::env::set_var("TEST_POLLING_LOST", "1") };

        {
            let sleep = tokio::time::sleep(Duration::from_millis(400));
            tokio::pin!(sleep);
            let _ = tokio::select! {
                res = consumer.recv_timeout(Timeout::Duration(Duration::from_millis(400))) => {
                    res
                }
                _ = &mut sleep => {
                    println!("sleep");
                    Ok(None)
                }
            }
            .inspect_err(|err| {
                println!("err: {:?}", err);
            })?;

            let _ = tokio::select! {
                res = consumer.recv_timeout(Timeout::Duration(Duration::from_millis(400))) => {
                    res
                }
                _ = &mut sleep => {
                    println!("sleep");
                    Ok(None)
                }
            }
            .inspect_err(|err| {
                println!("err: {:?}", err);
            })?;

            let _ = tokio::select! {
                res = consumer.recv_timeout(Timeout::Duration(Duration::from_millis(400))) => {
                    res
                }
                _ = &mut sleep => {
                    println!("sleep");
                    Ok(None)
                }
            }
            .inspect_err(|err| {
                println!("err: {:?}", err);
            })?;

            let mut stream = consumer.stream();

            while let Some((offset, message)) = stream.try_next().await? {
                // Offset contains information for topic name, database name and vgroup id,
                // similar to kafka topic/partition/offset.
                let _ = offset.topic();
                let _ = offset.database();
                let _ = offset.vgroup_id();

                match message {
                    MessageSet::Meta(meta) => {
                        let _raw = meta.as_raw_meta().await?;
                        // meta data can be write to an database seamlessly by raw or json (to sql).
                        let json = meta.as_json_meta().await?;
                        for meta in &json {
                            let sql = meta.to_string();
                            tracing::debug!("sql: {}", sql);
                        }
                    }
                    MessageSet::Data(data) => {
                        // data message may have more than one data block for various tables.
                        while let Some(block) = data.fetch_block().await? {
                            println!("{}", block.pretty_format());
                        }
                    }
                    _ => unreachable!(),
                }
                consumer.commit(offset).await?;
            }
        }

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec_many([
            "drop database test_ws_tmq_poll_lost2",
            "drop topic test_ws_tmq_poll_lost",
            "drop database test_ws_tmq_poll_lost",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_auto_commit() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1741091352",
            "drop database if exists test_1741091352",
            "create database test_1741091352 wal_retention_period 3600",
            "create topic topic_1741091352 with meta as database test_1741091352",
            "use test_1741091352",
            // kind 1: create super table using all types
            "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
            // kind 2: create child table with json tag
            "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
            "create table tb1 using stb1 tags(NULL)",
            "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "insert into tb1 using stb1 tags(NULL) values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            // kind 3: create super table with all types except json (especially for tags)
            "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
            // kind 4: create child table with all types except json
            "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
            "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
            // kind 5: create common table
            "create table `table` (ts timestamp, v int)",
            // kind 6: column in super table
            "alter table stb1 add column new1 bool",
            "alter table stb1 add column new2 tinyint",
            "alter table stb1 add column new10 nchar(16)",
            "alter table stb1 modify column new10 nchar(32)",
            "alter table stb1 drop column new10",
            "alter table stb1 drop column new2",
            "alter table stb1 drop column new1",
            // kind 7: add tag in super table
            "alter table `stb2` add tag new1 bool",
            "alter table `stb2` rename tag new1 new1_new",
            "alter table `stb2` modify tag t10 nchar(32)",
            "alter table `stb2` drop tag new1_new",
            // kind 8: column in common table
            "alter table `table` add column new1 bool",
            "alter table `table` add column new2 tinyint",
            "alter table `table` add column new10 nchar(16)",
            "alter table `table` modify column new10 nchar(32)",
            "alter table `table` rename column new10 new10_new",
            "alter table `table` drop column new10_new",
            "alter table `table` drop column new2",
            "alter table `table` drop column new1",
        ])
        .await?;

        taos.exec_many([
            "drop database if exists test_1741138531",
            "create database test_1741138531 wal_retention_period 3600",
            "use test_1741138531",
        ])
        .await?;

        let mut consumer = TmqBuilder::new(
            "ws://localhost:6041?group.id=10&timeout=500ms&auto.offset.reset=earliest&enable.auto.commit=true&auto.commit.interval.ms=1",
        )?.build_consumer().await?;

        consumer.subscribe(["topic_1741091352"]).await?;

        {
            let mut pre_topic = None;
            let mut pre_vgroup_id = 0;
            let mut pre_offset = 0;

            let mut stream = consumer.stream();
            while let Some((offset, _)) = stream.try_next().await? {
                if let Some(pre_topic) = pre_topic.as_deref() {
                    let offset = consumer.committed(pre_topic, pre_vgroup_id).await?;
                    assert!(offset >= pre_offset);
                }

                pre_topic = Some(offset.topic().to_owned());
                pre_vgroup_id = offset.vgroup_id();
                pre_offset = offset.offset();
            }
        }

        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec_many([
            "drop database test_1741138531",
            "drop topic topic_1741091352",
            "drop database test_1741091352",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_tmq_config() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1741674686",
            "drop database if exists test_1741674686",
            "create database test_1741674686",
            "create topic topic_1741674686 as database test_1741674686",
            "use test_1741674686",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values(now, 1)",
        ])
        .await?;

        let builder = TmqBuilder::new(
            "ws://localhost:6041?group.id=10&client.id=1&auto.offset.reset=earliest&\
            experimental.snapshot.enable=false&msg.with.table.name=true&enable.auto.commit=false&\
            auto.commit.interval.ms=5000&msg.enable.batchmeta=1&msg.consume.excluded=1&\
            msg.consume.rawdata=1&timeout=200ms&td.connect.ip=localhost&enable.replay=false&\
            compress&interval=5s&any_other_config_without_dot=value1",
        )?;

        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["topic_1741674686"]).await?;
        consumer.unsubscribe().await;

        taos.exec_many([
            "drop topic topic_1741674686",
            "drop database test_1741674686",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_filter_td_connect_websocket_scheme_config() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1743505369",
            "drop database if exists test_1743505369",
            "create database test_1743505369 wal_retention_period 3600",
            "create topic topic_1743505369 with meta as database test_1743505369",
            "use test_1743505369",
            "create table meters(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, \
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16), c11 tinyint unsigned, \
            c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned) tags(t1 int)",
            "create table d0 using meters tags(1000)",
            "create table d1 using meters tags(null)",
            "insert into d0 values(now, null, null, null, null, null, null, null, null, null, null, \
            null, null, null, null) \
            d1 values(now, true, -2, -3, -4, -5, '2022-02-02 02:02:02.222', -0.1, -0.12345678910, \
            'abc 和我', 'Unicode + 涛思', 254, 65534, 1, 1)",
        ])
        .await?;

        let builder = TmqBuilder::new(
            "ws://localhost:6041?td.connect.websocket.scheme=ws&group.id=0&auto.offset.reset=earliest",
        )?;

        let mut consumer = builder.build_consumer().await?;
        consumer.subscribe(["topic_1743505369"]).await?;
        consumer.unsubscribe().await;

        taos.exec_many([
            "drop topic topic_1743505369",
            "drop database test_1743505369",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_poll() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1748505708",
            "drop database if exists test_1748505708",
            "create database test_1748505708 vgroups 10",
            "create topic topic_1748505708 as database test_1748505708",
            "use test_1748505708",
            "create table t0 (ts timestamp, c1 int, c2 float, c3 float)",
        ])
        .await?;

        let num = 5000;

        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<()>)>(100);

        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_block().await? {
                        cnt += block.nrows();
                    }
                }
                let _ = done_tx.send(());
            }
            assert_eq!(cnt, num);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq =
                TmqBuilder::from_dsn("ws://localhost:6041?group.id=10&auto.offset.reset=earliest")?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1748505708"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(2));

            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        break;
                    }
                    res = consumer.recv_timeout(timeout) => {
                        if let Some((offset, message)) = res? {
                            let (done_tx, done_rx) = oneshot::channel();
                            msg_tx.send((message, done_tx)).await?;
                            let _ = done_rx.await;
                            consumer.commit(offset).await?;
                        }
                    }
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        let mut sqls = Vec::with_capacity(100);

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        for i in 0..num {
            sqls.push(format!(
                "insert into t0 values ({}, {}, {}, {})",
                ts + i as i64,
                i,
                i as f32 * 1.1,
                i as f32 * 2.2
            ));

            if (i + 1) % 100 == 0 {
                taos.exec_many(&sqls).await?;
                sqls.clear();
            }
        }

        tokio::time::sleep(Duration::from_secs(20)).await;

        let _ = cancel_tx.send(true);

        poll_handle.await??;
        cnt_handle.await??;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic topic_1748505708",
            "drop database test_1748505708",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_poll_with_sleep() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1748512568",
            "drop database if exists test_1748512568",
            "create database test_1748512568 vgroups 10",
            "create topic topic_1748512568 as database test_1748512568",
            "use test_1748512568",
            "create table t0 (ts timestamp, c1 int, c2 float, c3 float)",
        ])
        .await?;

        let num = 3000;

        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<()>)>(100);

        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_block().await? {
                        cnt += block.nrows();
                    }
                }
                let _ = done_tx.send(());
            }
            assert_eq!(cnt, num);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq =
                TmqBuilder::from_dsn("ws://localhost:6041?group.id=10&auto.offset.reset=earliest")?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1748512568"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        break;
                    }
                    res = consumer.recv_timeout(timeout) => {
                        if let Some((offset, message)) = res? {
                            let (done_tx, done_rx) = oneshot::channel();
                            msg_tx.send((message, done_tx)).await?;
                            let _ = done_rx.await;
                            consumer.commit(offset).await?;
                        }
                    }
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        let mut sqls = Vec::with_capacity(100);

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        for i in 0..num {
            sqls.push(format!(
                "insert into t0 values ({}, {}, {}, {})",
                ts + i as i64,
                i,
                i as f32 * 1.1,
                i as f32 * 2.2
            ));

            if (i + 1) % 100 == 0 {
                taos.exec_many(&sqls).await?;
                sqls.clear();
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }

        tokio::time::sleep(Duration::from_secs(20)).await;

        let _ = cancel_tx.send(true);

        poll_handle.await??;
        cnt_handle.await??;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic topic_1748512568",
            "drop database test_1748512568",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_poll_data_loss() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1748512722",
            "drop database if exists test_1748512722",
            "create database test_1748512722",
            "create topic topic_1748512722 as database test_1748512722",
            "use test_1748512722",
            "create table t0 (ts timestamp, c1 int, c2 float, c3 float)",
        ])
        .await?;

        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<()>)>(100);

        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_block().await? {
                        cnt += block.nrows();
                    }
                }
                let _ = done_tx.send(());
            }
            assert_eq!(cnt, 3);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq =
                TmqBuilder::from_dsn("ws://localhost:6041?group.id=10&auto.offset.reset=earliest")?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1748512722"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        break;
                    }
                    res = consumer.recv_timeout(timeout) => {
                        if let Some((offset, message)) = res? {
                            let (done_tx, done_rx) = oneshot::channel();
                            msg_tx.send((message, done_tx)).await?;
                            let _ = done_rx.await;
                            consumer.commit(offset).await?;
                        }
                    }
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        tokio::time::sleep(Duration::from_secs(2)).await;

        taos.exec("insert into t0 values(now, 1, 2.2, 3.3)").await?;

        tokio::time::sleep(Duration::from_secs(20)).await;

        taos.exec("insert into t0 values(now, 1, 2.2, 3.3)").await?;

        tokio::time::sleep(Duration::from_secs(20)).await;

        taos.exec("insert into t0 values(now, 1, 2.2, 3.3)").await?;

        tokio::time::sleep(Duration::from_secs(10)).await;

        let _ = cancel_tx.send(true);

        poll_handle.await??;
        cnt_handle.await??;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic topic_1748512722",
            "drop database test_1748512722",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_poll_network_packet_loss() -> anyhow::Result<()> {
        use futures::{SinkExt, StreamExt};
        use serde_json::json;
        use taos_query::AsyncTBuilder;
        use tokio::sync::mpsc;
        use tokio::task::JoinHandle;
        use tracing::debug;
        use warp::Filter;

        let poll_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn("ws://127.0.0.1:8864?group.id=10")?;
            let consumer = tmq.build().await?;
            let timeout = Duration::from_secs(5);

            loop {
                match consumer.poll_timeout(timeout).await? {
                    Some(res) => {
                        debug!("Received message: {res:?}");
                        break;
                    }
                    None => debug!("No message received within timeout"),
                }
            }

            Ok(())
        });

        let (close_tx, mut close_rx) = mpsc::channel(1);

        let routes = warp::path!("rest" / "tmq").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let mut poll_cnt = 0;
                    let (mut tx, mut rx) = ws.split();

                    while let Some(msg) = rx.next().await {
                        let msg = msg.unwrap();
                        debug!("ws recv msg: {msg:?}");
                        if msg.is_text() {
                            let text = msg.to_str().unwrap();
                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "version message",
                                    "action": "version",
                                    "req_id": 1,
                                    "version": "3.0"
                                });
                                let msg = warp::ws::Message::text(data.to_string());
                                let _ = tx.send(msg).await;
                            } else if text.contains("poll") {
                                if poll_cnt == 0 {
                                    debug!("first poll, waiting for 60 seconds");
                                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                                    poll_cnt += 1;
                                } else if poll_cnt == 1 {
                                    debug!("second poll, sending message and closing");
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
                                    let msg = warp::ws::Message::text(data.to_string());
                                    let _ = tx.send(msg).await;
                                    let _ = close.send(()).await;
                                    break;
                                }
                            }
                        }
                    }
                })
            }
        });

        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 8864), async move {
                let _ = close_rx.recv().await;
                debug!("Shutting down...");
            });

        server.await;

        poll_handle.await??;

        Ok(())
    }

    #[tokio::test]
    async fn test_poll_data_timeout_return() -> anyhow::Result<()> {
        use futures::{SinkExt, StreamExt};
        use serde_json::json;
        use taos_query::AsyncTBuilder;
        use tokio::sync::mpsc;
        use tokio::task::JoinHandle;
        use tracing::debug;
        use warp::Filter;

        let poll_handle: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn("ws://127.0.0.1:7749?group.id=10")?;
            let consumer = tmq.build().await?;
            let timeout = Duration::from_secs(5);

            loop {
                match consumer.poll_timeout(timeout).await? {
                    Some(res) => {
                        debug!("Received message: {res:?}");
                        break;
                    }
                    None => debug!("No message received within timeout"),
                }
            }

            Ok(())
        });

        let (close_tx, mut close_rx) = mpsc::channel(1);

        let routes = warp::path!("rest" / "tmq").and(warp::ws()).map({
            move |ws: warp::ws::Ws| {
                let close = close_tx.clone();
                ws.on_upgrade(move |ws| async {
                    let close = close;
                    let (mut tx, mut rx) = ws.split();

                    while let Some(msg) = rx.next().await {
                        let msg = msg.unwrap();
                        debug!("ws recv msg: {msg:?}");
                        if msg.is_text() {
                            let text = msg.to_str().unwrap();
                            if text.contains("version") {
                                let data = json!({
                                    "code": 0,
                                    "message": "version message",
                                    "action": "version",
                                    "req_id": 1001,
                                    "version": "3.0"
                                });
                                let msg = warp::ws::Message::text(data.to_string());
                                let _ = tx.send(msg).await;
                            } else if text.contains("poll") {
                                debug!("poll waiting for 6 seconds");
                                tokio::time::sleep(std::time::Duration::from_secs(6)).await;
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
                                let msg = warp::ws::Message::text(data.to_string());
                                let _ = tx.send(msg).await;
                                let _ = close.send(()).await;
                                break;
                            }
                        }
                    }
                })
            }
        });

        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 7749), async move {
                let _ = close_rx.recv().await;
                debug!("Shutting down...");
            });

        server.await;

        poll_handle.await??;

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test]
    async fn test_poll_blob() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1753089865",
            "drop database if exists test_1753089865",
            "create database test_1753089865",
            "create topic topic_1753089865 as database test_1753089865",
            "use test_1753089865",
            "create table t0 (ts timestamp, c1 int, c2 blob)",
        ])
        .await?;

        let num = 100;

        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<()>)>(32);

        let (cancel_tx, mut cancel_rx) = watch::channel(false);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_block().await? {
                        cnt += block.nrows();
                    }
                }
                let _ = done_tx.send(());
            }
            assert_eq!(cnt, num);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq =
                TmqBuilder::from_dsn("ws://localhost:6041?group.id=10&auto.offset.reset=earliest")?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1753089865"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(5));

            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        break;
                    }
                    res = consumer.recv_timeout(timeout) => {
                        if let Some((offset, message)) = res? {
                            let (done_tx, done_rx) = oneshot::channel();
                            msg_tx.send((message, done_tx)).await?;
                            let _ = done_rx.await;
                            consumer.commit(offset).await?;
                        }
                    }
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut sql = "insert into t0 values ".to_string();
        for i in 0..num {
            sql.push_str(&format!("({}, {}, 'blob_{}'), ", ts + i as i64, i, i));
        }

        taos.exec(sql).await?;

        tokio::time::sleep(Duration::from_secs(20)).await;

        let _ = cancel_tx.send(true);

        poll_handle.await??;
        cnt_handle.await??;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic topic_1753089865",
            "drop database test_1753089865",
        ])
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_recv_timeout_never() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1758855878",
            "drop database if exists test_1758855878",
            "create database test_1758855878",
            "create topic topic_1758855878 as database test_1758855878",
        ])
        .await?;

        let handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn("ws://localhost:6041?group.id=10")?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1758855878"]).await?;
            let _ = consumer.recv_timeout(Timeout::Never).await?;
            Ok(())
        });

        let res = tokio::time::timeout(Duration::from_secs(90), handle).await;
        assert!(res.is_err());

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test]
    async fn test_report_user_ip_and_app() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        #[derive(Debug, serde::Deserialize)]
        struct Record {
            user_ip: String,
            user_app: String,
        }

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1766045982",
            "drop database if exists test_1766045982",
            "create database test_1766045982",
            "create topic topic_1766045982 as database test_1766045982",
        ])
        .await?;

        {
            let tmq = TmqBuilder::from_dsn(
                "ws://localhost:6041?group.id=10&user_ip=192.168.1.1&user_app=rust_tmq_test",
            )?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1766045982"]).await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let found = records.iter().any(|record| {
                record.user_ip == "192.168.1.1" && record.user_app == "rust_tmq_test"
            });
            assert!(found);

            consumer.unsubscribe().await;
        }

        {
            let tmq = TmqBuilder::from_dsn(
                "ws://localhost:6041?group.id=10&user_ip=  192.168.1.2  &user_app=  rust_tmq_test  ",
            )?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1766045982"]).await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let found = records.iter().any(|record| {
                record.user_ip == "192.168.1.2" && record.user_app == "rust_tmq_test"
            });
            assert!(found);

            consumer.unsubscribe().await;
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic if exists topic_1766045982",
            "drop database if exists test_1766045982",
        ])
        .await?;

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test]
    async fn test_report_connector_info() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        #[derive(Debug, serde::Deserialize)]
        struct Record {
            connector_info: String,
        }

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1764581863",
            "drop database if exists test_1764581863",
            "create database test_1764581863",
            "create topic topic_1764581863 as database test_1764581863",
        ])
        .await?;

        {
            let tmq = TmqBuilder::from_dsn("ws://localhost:6041?group.id=10")?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1764581863"]).await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let cnt = records
                .iter()
                .filter(|record| record.connector_info == crate::CONNECTOR_INFO)
                .count();
            assert!(cnt >= 2);

            consumer.unsubscribe().await;
        }

        {
            let tmq = TmqBuilder::from_dsn(
                "ws://localhost:6041?group.id=10&connector_info=rust_tmq-0.0.1",
            )?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1764581863"]).await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let found = records
                .iter()
                .any(|record| record.connector_info == "rust_tmq-0.0.1");
            assert!(found);

            consumer.unsubscribe().await;
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic if exists topic_1764581863",
            "drop database if exists test_1764581863",
        ])
        .await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_td_connect_token() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        use taos_query::util::test_utils::test_username;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop token if exists token_1772264645",
            "drop topic if exists topic_1772264645",
            "drop database if exists test_1772264645",
            "create database test_1772264645",
            "create topic topic_1772264645 as database test_1772264645",
            "use test_1772264645",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut rs = taos
            .query(format!(
                "create token token_1772264645 from user {}",
                test_username()
            ))
            .await?;
        let mut rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = rows.remove(0);

        let num = 100;
        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<bool>)>(32);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_block().await? {
                        cnt += block.nrows();
                    }
                }
                if cnt >= num {
                    let _ = done_tx.send(true);
                    break;
                } else {
                    let _ = done_tx.send(false);
                }
            }
            assert_eq!(cnt, num);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn(format!(
                "ws://invalid_user:invalid_pass@localhost:6041?group.id=3749&auto.offset.reset=earliest&td.connect.token={token}"
            ))?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1772264645"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(5));

            loop {
                if let Some((offset, message)) = consumer.recv_timeout(timeout).await? {
                    let (done_tx, done_rx) = oneshot::channel();
                    msg_tx.send((message, done_tx)).await?;
                    if done_rx.await? {
                        break;
                    }
                    consumer.commit(offset).await?;
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut sql = "insert into t0 values ".to_string();
        for i in 0..num {
            sql.push_str(&format!("({}, {}), ", ts + i as i64, i));
        }

        taos.exec(sql).await?;

        poll_handle.await??;
        cnt_handle.await??;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop token if exists token_1772264645",
            "drop topic if exists topic_1772264645",
            "drop database if exists test_1772264645",
        ])
        .await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_bearer_token() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        use taos_query::util::test_utils::test_username;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop token if exists token_1772505637",
            "drop topic if exists topic_1772505637",
            "drop database if exists test_1772505637",
            "create database test_1772505637",
            "create topic topic_1772505637 as database test_1772505637",
            "use test_1772505637",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut rs = taos
            .query(format!(
                "create token token_1772505637 from user {}",
                test_username()
            ))
            .await?;
        let mut rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = rows.remove(0);

        let num = 100;
        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<bool>)>(32);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_block().await? {
                        cnt += block.nrows();
                    }
                }
                if cnt >= num {
                    let _ = done_tx.send(true);
                    break;
                } else {
                    let _ = done_tx.send(false);
                }
            }
            assert_eq!(cnt, num);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn(format!(
                "ws://invalid_user:invalid_pass@localhost:6041?group.id=8463&auto.offset.reset=earliest&bearer_token={token}"
            ))?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["topic_1772505637"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(5));

            loop {
                if let Some((offset, message)) = consumer.recv_timeout(timeout).await? {
                    let (done_tx, done_rx) = oneshot::channel();
                    msg_tx.send((message, done_tx)).await?;
                    if done_rx.await? {
                        break;
                    }
                    consumer.commit(offset).await?;
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut sql = "insert into t0 values ".to_string();
        for i in 0..num {
            sql.push_str(&format!("({}, {}), ", ts + i as i64, i));
        }

        taos.exec(sql).await?;

        poll_handle.await??;
        cnt_handle.await??;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop token if exists token_1772505637",
            "drop topic if exists topic_1772505637",
            "drop database if exists test_1772505637",
        ])
        .await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_invalid_token() -> anyhow::Result<()> {
        use taos_query::prelude::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop topic if exists topic_1772507246",
            "drop database if exists test_1772507246",
            "create database test_1772507246",
            "create topic topic_1772507246 as database test_1772507246",
            "use test_1772507246",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let tmq = TmqBuilder::from_dsn(
            "ws://invalid_user:invalid_pass@localhost:6041?group.id=8367&td.connect.token=invalid_token"
        )?;
        let mut consumer = tmq.build().await?;
        let err = consumer.subscribe(["topic_1772507246"]).await.unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        let tmq = TmqBuilder::from_dsn(
            "ws://invalid_user:invalid_pass@localhost:6041?group.id=8367&bearer_token=invalid_token"
        )?;
        let mut consumer = tmq.build().await?;
        let err = consumer.subscribe(["topic_1772507246"]).await.unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        let tmq = TmqBuilder::from_dsn(
            "ws://invalid_user:invalid_pass@localhost:6041?group.id=8367&bearer_token=",
        )?;
        let mut consumer = tmq.build().await?;
        let err = consumer.subscribe(["topic_1772507246"]).await.unwrap_err();
        assert!(err.to_string().contains("init tscObj with token failed"));

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop topic if exists topic_1772507246",
            "drop database if exists test_1772507246",
        ])
        .await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_token_priority() -> anyhow::Result<()> {
        use taos_query::prelude::*;
        use taos_query::util::test_utils::test_username;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec_many([
            "drop token if exists token_1772595214",
            "drop topic if exists topic_1772595214",
            "drop database if exists test_1772595214",
            "create database test_1772595214",
            "create topic topic_1772595214 as database test_1772595214",
            "use test_1772595214",
            "create table t0 (ts timestamp, c1 int)",
        ])
        .await?;

        let mut rs = taos
            .query(format!(
                "create token token_1772595214 from user {}",
                test_username()
            ))
            .await?;
        let mut rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = rows.remove(0);

        let tmq = TmqBuilder::from_dsn(format!(
            "ws://invalid_user:invalid_pass@localhost:6041?group.id=8367&td.connect.token={token}&bearer_token=invalid_token"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772595214"]).await?;
        consumer.unsubscribe().await;

        let tmq = TmqBuilder::from_dsn(format!(
            "ws://invalid_user:invalid_pass@localhost:6041?group.id=8367&td.connect.token={token}"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772595214"]).await?;
        consumer.unsubscribe().await;

        let tmq = TmqBuilder::from_dsn(format!(
            "ws://invalid_user:invalid_pass@localhost:6041?group.id=8367&bearer_token={token}"
        ))?;
        let mut consumer = tmq.build().await?;
        consumer.subscribe(["topic_1772595214"]).await?;
        consumer.unsubscribe().await;

        tokio::time::sleep(Duration::from_secs(3)).await;

        taos.exec_many([
            "drop token if exists token_1772595214",
            "drop topic if exists topic_1772595214",
            "drop database if exists test_1772595214",
        ])
        .await?;

        Ok(())
    }

    #[test]
    fn test_tmq_builder_available_params() {
        use taos_query::TBuilder;

        let expected = [
            "token",
            "timeout",
            "group.id",
            "client.id",
            "auto.offset.reset",
            "enable.auto.commit",
            "auto.commit.interval.ms",
            "experimental.snapshot.enable",
            "msg.with.table.name",
            "msg.enable.batchmeta",
            "msg.consume.excluded",
            "msg.consume.rawdata",
            "bearer_token",
            "td.connect.token",
        ];
        assert_eq!(TmqBuilder::available_params(), expected);
    }
}

#[cfg(feature = "rustls-ring-crypto-provider")]
#[cfg(test)]
mod cloud_tests {
    use std::time::Duration;

    use taos_query::prelude::*;
    use tokio::sync::{mpsc, oneshot};

    use crate::consumer::{Data, Meta};
    use crate::{TaosBuilder, TmqBuilder};

    #[tokio::test]
    async fn test_poll() -> anyhow::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_poll");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_poll");
            return Ok(());
        }

        let url = url.unwrap();
        let token = token.unwrap();

        let group_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let dsn = format!("{url}?token={token}&group.id={group_id}&auto.offset.reset=earliest",);

        let (msg_tx, mut msg_rx) =
            mpsc::channel::<(MessageSet<Meta, Data>, oneshot::Sender<()>)>(64);

        let cnt_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let mut cnt = 0;
            while let Some((mut msg, done_tx)) = msg_rx.recv().await {
                if let Some(data) = msg.data() {
                    while let Some(block) = data.fetch_block().await? {
                        cnt += block.nrows();
                    }
                }
                let _ = done_tx.send(());
            }
            assert_eq!(cnt, 100);
            Ok(())
        });

        let poll_handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            let tmq = TmqBuilder::from_dsn(dsn)?;
            let mut consumer = tmq.build().await?;
            consumer.subscribe(["rust_tmq_test_topic"]).await?;

            let timeout = Timeout::Duration(Duration::from_secs(5));

            loop {
                let res = consumer.recv_timeout(timeout).await;
                if let Some((offset, message)) = res? {
                    let (done_tx, done_rx) = oneshot::channel();
                    msg_tx.send((message, done_tx)).await?;
                    let _ = done_rx.await;
                    consumer.commit(offset).await?;
                } else {
                    break;
                }
            }

            consumer.unsubscribe().await;

            Ok(())
        });

        poll_handle.await??;
        cnt_handle.await??;

        let taos = TaosBuilder::from_dsn(format!("{url}/rust_test?token={token}"))?
            .build()
            .await?;

        taos.exec(format!(
            "drop consumer group `{group_id}` on rust_tmq_test_topic"
        ))
        .await?;

        Ok(())
    }
}
