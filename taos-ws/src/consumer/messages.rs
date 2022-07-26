



use bytes::Bytes;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;


use taos_query::common::Field;
use taos_query::common::Precision;
use taos_query::common::Ty;
use taos_query::tmq::VGroupId;

use crate::infra::ToMessage;
use crate::infra::WsConnReq;

pub type ReqId = u64;

/// Type for result ID.
pub type ResId = u64;

pub type ConsumerId = u64;

pub type MessageId = u64;

#[derive(Debug, Serialize, Default, Clone)]
pub struct MessageArgs {
    pub(crate) req_id: ReqId,
    pub(crate) message_id: MessageId,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(i32)]
pub enum MessageType {
    Invalid = 0,
    Data = 1,
    Meta,
}

impl Default for MessageType {
    fn default() -> Self {
        Self::Invalid
    }
}

#[serde_as]
#[derive(Debug, Serialize, Default, Clone)]
pub struct TmqInit {
    pub group_id: String,
    pub client_id: Option<String>,
    pub offset_reset: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TmqArgs {
    pub req_id: ReqId,
    #[serde(default)]
    pub message_id: ConsumerId,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum TmqSend {
    Subscribe {
        req_id: ReqId,
        #[serde(flatten)]
        conn: WsConnReq,
        #[serde(flatten)]
        req: TmqInit,
        topics: Vec<String>,
    },
    Poll {
        req_id: ReqId,
        blocking_time: i64,
    },
    FetchJsonMeta(MessageArgs),
    FetchRawMeta(MessageArgs),
    Fetch(MessageArgs),
    FetchBlock(MessageArgs),
    Commit(MessageArgs),
    Close,
}

unsafe impl Send for TmqSend {}
unsafe impl Sync for TmqSend {}

impl TmqSend {
    pub fn req_id(&self) -> ReqId {
        match self {
            TmqSend::Subscribe {
                req_id,
                conn: _,
                req: _,
                topics: _,
            } => *req_id,
            TmqSend::Poll {
                req_id,
                blocking_time: _,
            } => *req_id,
            TmqSend::FetchJsonMeta(args) => args.req_id,
            TmqSend::FetchRawMeta(args) => args.req_id,
            TmqSend::Fetch(args) => args.req_id,
            TmqSend::FetchBlock(args) => args.req_id,
            TmqSend::Commit(args) => args.req_id,
            TmqSend::Close => unreachable!(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WsFetchArgs {
    req_id: ReqId,
    id: ResId,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct TmqPoll {
    pub message_id: MessageId,
    pub database: String,
    pub have_message: bool,
    pub topic: String,
    pub vgroup_id: VGroupId,
    pub message_type: MessageType,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TmqFetch {
    pub completed: bool,
    pub table_name: Option<String>,
    pub fields_count: usize,
    pub fields_names: Option<Vec<String>>,
    pub fields_types: Option<Vec<Ty>>,
    pub fields_lengths: Option<Vec<u32>>,
    pub precision: Precision,
    pub rows: usize,
}

impl TmqFetch {
    pub fn fields(&self) -> Vec<Field> {
        (0..self.fields_count)
            .map(|i| {
                Field::new(
                    self.fields_names.as_ref().unwrap()[i].to_string(),
                    self.fields_types.as_ref().unwrap()[i],
                    self.fields_lengths.as_ref().unwrap()[i],
                )
            })
            .collect_vec()
    }
}

#[derive(Debug, Clone)]
pub enum TmqMsgData {
    Fetch(TmqFetch),
    Block(Vec<u8>),
    JsonMeta(Value),
    RawMeta(Vec<u8>),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum TmqRecvData {
    Subscribe,
    Poll(TmqPoll),
    Fetch(TmqFetch),
    FetchJsonMeta {
        data: Value,
    },
    #[serde(skip)]
    Bytes(Bytes),
    FetchRawMeta {
        #[serde(skip)]
        meta: Bytes,
    },
    FetchBlock {
        #[serde(skip)]
        data: Bytes,
    },
    Block(Vec<u32>),
    Commit,
    Close,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct TmqRecv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    pub req_id: ReqId,
    // #[serde(flatten)]
    // pub args: TmqArgs,
    #[serde(flatten)]
    pub data: TmqRecvData,
}

impl TmqRecv {
    pub(crate) fn ok(self) -> (ReqId, TmqRecvData, Result<(), taos_error::Error>) {
        (
            self.req_id,
            self.data,
            if self.code == 0 {
                Ok(())
            } else {
                Err(taos_error::Error::new(
                    self.code,
                    self.message.unwrap_or_default(),
                ))
            },
        )
    }
}

#[test]
fn test_serde_recv_data() {
    let json = r#"{
        "code": 1,
        "message": "",
        "action": "poll",
        "req_id": 1
    }"#;
    let d: TmqRecv = serde_json::from_str(&json).unwrap();
    dbg!(d.ok());
}

impl ToMessage for TmqSend {}
