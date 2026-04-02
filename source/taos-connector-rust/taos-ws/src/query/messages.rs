use std::time::Duration;

use faststr::FastStr;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, NoneAsEmptyString};
use taos_query::common::{Precision, Ty};
use taos_query::prelude::RawError;
use taos_query::util::generate_req_id;
use tokio_tungstenite::tungstenite::Message;

pub type ReqId = u64;
pub type ResId = u64;
pub type StmtId = u64;
pub type MessageId = u64;

#[serde_as]
#[derive(Debug, Serialize, Default, Clone)]
pub struct WsConnReq {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    pub(crate) db: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) mode: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tz: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) app: Option<String>,
    pub(crate) connector: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) totp_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bearer_token: Option<String>,
}

impl WsConnReq {
    #[cfg(test)]
    fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: Some(user.into()),
            password: Some(password.into()),
            db: None,
            mode: None,
            tz: None,
            ip: None,
            app: None,
            connector: crate::CONNECTOR_INFO.to_string(),
            totp_code: None,
            bearer_token: None,
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct WsResArgs {
    pub id: ResId,
    pub req_id: ReqId,
}

#[derive(Debug, Serialize, Clone)]
pub struct ConnOption {
    pub option: i32,
    pub value: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum WsSend {
    Version,
    Conn {
        req_id: ReqId,
        #[serde(flatten)]
        req: WsConnReq,
    },
    OptionsConnection {
        req_id: ReqId,
        options: Vec<ConnOption>,
    },
    Insert {
        protocol: u8,
        precision: String,
        data: String,
        ttl: Option<i32>,
        req_id: Option<ReqId>,
        table_name_key: Option<String>,
    },
    Query {
        req_id: ReqId,
        sql: String,
    },
    Fetch(WsResArgs),
    FetchBlock(WsResArgs),
    Binary(Vec<u8>),
    FreeResult(WsResArgs),
    Stmt2Init {
        req_id: ReqId,
        single_stb_insert: bool,
        single_table_bind_once: bool,
    },
    Stmt2Prepare {
        req_id: ReqId,
        stmt_id: StmtId,
        sql: String,
        get_fields: bool,
    },
    Stmt2Exec {
        req_id: ReqId,
        stmt_id: StmtId,
    },
    Stmt2Result {
        req_id: ReqId,
        stmt_id: StmtId,
    },
    Stmt2Close {
        req_id: ReqId,
        stmt_id: StmtId,
    },
    CheckServerStatus {
        req_id: ReqId,
        fqdn: Option<FastStr>,
        port: i32,
    },
}

impl WsSend {
    pub(crate) fn req_id(&self) -> ReqId {
        match self {
            WsSend::Conn { req_id, .. }
            | WsSend::OptionsConnection { req_id, .. }
            | WsSend::Query { req_id, .. }
            | WsSend::Stmt2Init { req_id, .. }
            | WsSend::Stmt2Prepare { req_id, .. }
            | WsSend::Stmt2Exec { req_id, .. }
            | WsSend::Stmt2Result { req_id, .. }
            | WsSend::Stmt2Close { req_id, .. }
            | WsSend::CheckServerStatus { req_id, .. } => *req_id,
            WsSend::Insert { req_id, .. } => req_id.unwrap_or(0),
            WsSend::Binary(bytes) => unsafe { *(bytes.as_ptr() as *const u64) as _ },
            WsSend::Fetch(args) | WsSend::FetchBlock(args) | WsSend::FreeResult(args) => {
                args.req_id
            }
            WsSend::Version => unreachable!(),
        }
    }
}

unsafe impl Send for WsSend {}
unsafe impl Sync for WsSend {}

#[serde_as]
#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct WsQueryResp {
    pub id: ResId,
    pub is_update: bool,
    pub affected_rows: usize,
    pub fields_count: usize,
    pub fields_names: Option<Vec<String>>,
    pub fields_types: Option<Vec<Ty>>,
    pub fields_lengths: Option<Vec<u32>>,
    pub precision: Precision,
    #[serde_as(as = "serde_with::DurationNanoSeconds")]
    pub timing: Duration,
    pub fields_precisions: Option<Vec<i64>>,
    pub fields_scales: Option<Vec<i64>>,
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub struct InsertResp {
    #[serde_as(as = "serde_with::DurationNanoSeconds")]
    pub timing: Duration,
    pub affected_rows: Option<usize>,
    pub total_rows: Option<usize>,
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub struct WsFetchResp {
    pub id: ResId,
    pub completed: bool,
    pub lengths: Option<Vec<u32>>,
    pub rows: usize,
    #[serde_as(as = "serde_with::DurationNanoSeconds")]
    pub timing: Duration,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct WsRecv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    #[serde(default)]
    pub req_id: ReqId,
    #[serde(flatten)]
    pub data: WsRecvData,
}

impl WsRecv {
    pub(crate) fn ok(self) -> (ReqId, WsRecvData, Result<(), RawError>) {
        (
            self.req_id,
            self.data,
            if self.code == 0 {
                Ok(())
            } else if self.message.as_deref() == Some("success") {
                Err(RawError::from_code(self.code))
            } else {
                Err(RawError::new(self.code, self.message.unwrap_or_default()))
            },
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Clone)]
#[serde_as]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum WsRecvData {
    Conn,
    OptionsConnection {
        timing: u64,
    },
    Version {
        version: String,
    },
    Insert(InsertResp),
    #[serde(alias = "binary_query")]
    Query(WsQueryResp),
    Fetch(WsFetchResp),
    /// Will only produced by error
    FetchBlock,
    Block {
        #[serde(default)]
        #[serde_as(as = "serde_with::DurationNanoSeconds")]
        timing: Duration,
        raw: Vec<u8>,
    },
    BlockNew {
        #[allow(dead_code)]
        block_version: u16,
        #[serde(default)]
        #[serde_as(as = "serde_with::DurationNanoSeconds")]
        timing: Duration,
        #[allow(dead_code)]
        block_req_id: ReqId,
        block_code: u32,
        block_message: String,
        finished: bool,
        raw: Vec<u8>,
    },
    BlockV2 {
        #[serde(default)]
        #[serde_as(as = "serde_with::DurationNanoSeconds")]
        timing: Duration,
        raw: Vec<u8>,
    },
    WriteMeta,
    WriteRaw,
    WriteRawBlock,
    WriteRawBlockWithFields,
    Stmt2Init {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Prepare {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        is_insert: bool,
        #[serde(default)]
        fields: Option<Vec<Stmt2Field>>,
        #[serde(default)]
        fields_count: usize,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Bind {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Exec {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        affected: usize,
        #[serde(default)]
        timing: u64,
    },
    Stmt2Result {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        id: u64,
        #[serde(default)]
        fields_count: u64,
        #[serde(default)]
        fields_names: Vec<String>,
        #[serde(default)]
        fields_types: Vec<Ty>,
        #[serde(default)]
        fields_lengths: Vec<u64>,
        #[serde(default)]
        precision: Precision,
        #[serde(default)]
        timing: u64,
        #[serde(default)]
        fields_precisions: Option<Vec<i64>>,
        #[serde(default)]
        fields_scales: Option<Vec<i64>>,
    },
    Stmt2Close {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        timing: u64,
    },
    ValidateSql {
        #[serde(default)]
        timing: u64,
        #[serde(default)]
        result_code: i64,
    },
    CheckServerStatus {
        #[serde(default)]
        timing: u64,
        #[serde(default)]
        status: i32,
        #[serde(default)]
        details: String,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
pub struct Stmt2Field {
    pub name: String,
    pub field_type: i8,
    pub precision: u8,
    pub scale: u8,
    pub bytes: i32,
    pub bind_type: BindType,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BindType {
    Column = 1,
    Tag = 2,
    TableName = 4,
}

impl<'de> Deserialize<'de> for BindType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BindTypeVisitor;

        impl serde::de::Visitor<'_> for BindTypeVisitor {
            type Value = BindType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a valid number for BindType")
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(match v {
                    1 => BindType::Column,
                    2 => BindType::Tag,
                    4 => BindType::TableName,
                    _ => return Err(E::custom(format!("Invalid bind type: {v}"))),
                })
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u8(v as _)
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u8(v as _)
            }
        }

        deserializer.deserialize_any(BindTypeVisitor)
    }
}

pub trait ToMessage: Serialize {
    fn to_msg(&self) -> Message {
        Message::Text(serde_json::to_string(self).unwrap())
    }
}

impl ToMessage for WsSend {}

#[derive(Debug)]
pub enum WsMessage {
    Command(WsSend),
    Raw(Message),
}

impl WsMessage {
    pub(crate) fn req_id(&self) -> ReqId {
        match self {
            WsMessage::Raw(_) => generate_req_id(),
            WsMessage::Command(ws_send) => ws_send.req_id(),
        }
    }

    pub(crate) fn into_message(self) -> Message {
        match self {
            WsMessage::Raw(message) => message,
            WsMessage::Command(ws_send) => match ws_send {
                WsSend::Binary(bytes) => Message::Binary(bytes),
                _ => ws_send.to_msg(),
            },
        }
    }

    pub(crate) fn should_cache(&self) -> bool {
        match self {
            WsMessage::Raw(_) => false,
            WsMessage::Command(ws_send) => match ws_send {
                WsSend::Insert { .. }
                | WsSend::Query { .. }
                | WsSend::CheckServerStatus { .. }
                | WsSend::OptionsConnection { .. } => true,
                WsSend::Binary(bytes) => {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    matches!(action, 4 | 5 | 6 | 10)
                }
                _ => false,
            },
        }
    }

    pub(crate) fn is_stmt2(&self) -> bool {
        match self {
            WsMessage::Raw(_) => false,
            WsMessage::Command(ws_send) => match ws_send {
                WsSend::Stmt2Init { .. }
                | WsSend::Stmt2Prepare { .. }
                | WsSend::Stmt2Exec { .. }
                | WsSend::Stmt2Result { .. }
                | WsSend::Stmt2Close { .. } => true,
                WsSend::Binary(bytes) => {
                    let action = unsafe { *(bytes.as_ptr().offset(16) as *const u64) };
                    action == 9
                }
                _ => false,
            },
        }
    }

    pub(crate) fn is_stmt2_close(&self) -> bool {
        match self {
            WsMessage::Raw(_) => false,
            WsMessage::Command(ws_send) => matches!(ws_send, WsSend::Stmt2Close { .. }),
        }
    }
}

#[cfg(test)]
mod tests {
    use taos_query::util::test_utils::{test_password, test_username};

    use crate::query::messages::{WsRecv, WsSend};
    use crate::query::WsConnReq;
    use crate::TaosBuilder;

    use super::*;

    #[test]
    fn test_serde_send() {
        let conn = WsSend::Conn {
            req_id: 1,
            req: WsConnReq::new(test_username(), test_password()),
        };
        let actual = serde_json::to_value(conn).unwrap();
        let expected = serde_json::json!({
            "action": "conn",
            "args": {
                "req_id": 1,
                "user": test_username(),
                "password": test_password(),
                "db": "",
                "connector": crate::CONNECTOR_INFO,
            }
        });
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serde_recv_data() {
        let json = r#"{
        "code": 0,
        "message": "",
        "action": "conn",
        "req_id": 1
        }"#;
        let d: WsRecv = serde_json::from_str(json).unwrap();
        dbg!(d);
    }

    #[test]
    fn dsn_error() {
        let _ = TaosBuilder::from_dsn("").unwrap_err();
    }

    #[test]
    fn test_bind_type_deserialize() {
        let valid_cases = vec![
            (1, BindType::Column),
            (2, BindType::Tag),
            (4, BindType::TableName),
        ];
        for (val, expected) in valid_cases {
            let res: BindType = serde_json::from_value(serde_json::json!(val as i64)).unwrap();
            assert_eq!(res, expected);

            let res: BindType = serde_json::from_value(serde_json::json!(val as u64)).unwrap();
            assert_eq!(res, expected);
        }

        let invalid_cases = vec![0, 3, 255];
        for val in invalid_cases {
            let res: Result<BindType, _> = serde_json::from_value(serde_json::json!(val as i64));
            assert!(res.is_err());

            let res: Result<BindType, _> = serde_json::from_value(serde_json::json!(val as u64));
            assert!(res.is_err());
        }

        let res: Result<BindType, _> = serde_json::from_value(serde_json::json!("invalid"));
        assert!(res.is_err());
    }

    #[test]
    fn test_insert_resp_deserialize() {
        let json = r#"{
            "timing": 123456789,
            "affected_rows": 10,
            "total_rows": 20
        }"#;
        let resp: InsertResp = serde_json::from_str(json).unwrap();
        assert_eq!(resp.timing, Duration::from_nanos(123456789));
        assert_eq!(resp.affected_rows, Some(10));
        assert_eq!(resp.total_rows, Some(20));

        let json = r#"{
            "timing": 123456789
        }"#;
        let resp: InsertResp = serde_json::from_str(json).unwrap();
        assert_eq!(resp.timing, Duration::from_nanos(123456789));
        assert_eq!(resp.affected_rows, None);
        assert_eq!(resp.total_rows, None);
    }
}
