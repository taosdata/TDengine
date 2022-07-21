use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;
use taos_query::common::{Precision, Ty};

pub type ReqId = u64;

/// Type for result ID.
pub type ResId = u64;

#[serde_as]
#[derive(Debug, Serialize, Default, Clone)]
pub struct WsConnReq {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
    #[serde_as(as = "NoneAsEmptyString")]
    pub(crate) db: Option<String>,
}

impl WsConnReq {
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            user: Some(user.into()),
            password: Some(password.into()),
            db: None,
        }
    }
    pub fn with_database(mut self, db: impl Into<String>) -> Self {
        self.db = Some(db.into());
        self
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct WsResArgs {
    pub req_id: ReqId,
    pub id: ResId,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum WsSend {
    Version,
    Pong(Vec<u8>),
    Conn {
        req_id: ReqId,
        #[serde(flatten)]
        req: WsConnReq,
    },
    Query {
        req_id: ReqId,
        sql: String,
    },
    Fetch(WsResArgs),
    FetchBlock(WsResArgs),
    Close(WsResArgs),
}

unsafe impl Send for WsSend {}
unsafe impl Sync for WsSend {}

#[test]
fn test_serde_send() {
    let s = WsSend::Conn {
        req_id: 1,
        req: WsConnReq::new("root", "taosdata"),
    };
    let v = serde_json::to_value(&s).unwrap();
    let j = serde_json::json!({
        "action": "conn",
        "args": {
            "req_id": 1,
            "user": "root",
            "password": "taosdata",
            "db": ""
        }
    });
    assert_eq!(v, j);
}

#[derive(Debug, Serialize)]
pub struct WsFetchArgs {
    req_id: ReqId,
    id: ResId,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct WsQueryResp {
    #[serde(default)]
    pub id: ResId,
    pub is_update: bool,
    pub affected_rows: usize,
    pub fields_count: usize,
    pub fields_names: Option<Vec<String>>,
    pub fields_types: Option<Vec<Ty>>,
    pub fields_lengths: Option<Vec<u32>>,
    pub precision: Precision,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WsFetchResp {
    pub id: ResId,
    pub completed: bool,
    pub lengths: Option<Vec<u32>>,
    pub rows: usize,
}

#[derive(Debug, Clone)]
pub enum WsFetchData {
    Fetch(WsFetchResp),
    Block(Vec<u8>),
    BlockV2(Vec<u8>),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum WsRecvData {
    Conn,
    Version { version: String },
    Query(WsQueryResp),
    Fetch(WsFetchResp),
    Block(Vec<u32>),
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
    pub(crate) fn ok(self) -> (ReqId, WsRecvData, Result<(), taos_error::Error>) {
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
        "code": 0,
        "message": "",
        "action": "conn",
        "req_id": 1
    }"#;
    let d: WsRecv = serde_json::from_str(&json).unwrap();
    dbg!(d);
}

pub(crate) trait ToMessage: Serialize {
    // #[cfg(feature = "async")]
    fn to_msg(&self) -> tokio_tungstenite::tungstenite::Message {
        tokio_tungstenite::tungstenite::Message::Text(serde_json::to_string(self).unwrap())
    }
}

impl ToMessage for WsSend {}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    // use websocket::ClientBuilder;

    use crate::*;

    use super::*;

    #[test]
    fn dsn_error() {
        WsInfo::from_dsn("").unwrap_err();
    }

    // #[test]
    // fn test_connect_sequential() -> anyhow::Result<()> {
    //     let mut ws = ClientBuilder::new("ws://localhost:6041/rest/ws")?;

    //     let mut client = ws.connect_insecure()?;

    //     let login = WsSend::Conn {
    //         req_id: 1,
    //         req: WsConnReq::new("root", "taosdata"),
    //     };

    //     dbg!(&login);
    //     client.send_message(&login.to_message()).unwrap();

    //     println!("receiving");

    //     let recv = client.recv_message()?;
    //     println!("received");
    //     match recv {
    //         websocket::OwnedMessage::Text(text) => {
    //             let v: WsRecv = serde_json::from_str(&text).unwrap();
    //             dbg!(v);
    //         }
    //         websocket::OwnedMessage::Binary(_) => todo!(),
    //         websocket::OwnedMessage::Close(_) => todo!(),
    //         websocket::OwnedMessage::Ping(_) => todo!(),
    //         websocket::OwnedMessage::Pong(_) => todo!(),
    //     }

    //     let query = WsSend::Query {
    //         req_id: 2,
    //         sql: "show databases".to_string(),
    //     };

    //     client.send_message(&query.to_message()).unwrap();

    //     println!("receiving");

    //     let query_resp = loop {
    //         let recv = client.recv_message()?;
    //         println!("received");
    //         match recv {
    //             websocket::OwnedMessage::Text(text) => {
    //                 let j: serde_json::Value = serde_json::from_str(&text).unwrap();
    //                 dbg!(j);
    //                 let v: WsRecv = serde_json::from_str(&text).unwrap();
    //                 dbg!(&v);
    //                 match v.data {
    //                     WsRecvData::Conn => todo!(),
    //                     WsRecvData::Query(resp) => break resp,
    //                     WsRecvData::Fetch(_) => todo!(),
    //                     WsRecvData::Block(_) => todo!(),
    //                 }
    //             }
    //             websocket::OwnedMessage::Binary(_) => todo!(),
    //             websocket::OwnedMessage::Close(_) => todo!(),
    //             websocket::OwnedMessage::Ping(_) => todo!(),
    //             websocket::OwnedMessage::Pong(_) => todo!(),
    //         }
    //     };
    //     dbg!(&query_resp);

    //     let id = query_resp.id;
    //     let req_id = 2;
    //     let res_args = WsResArgs { req_id, id };

    //     assert!(!query_resp.is_update);

    //     loop {
    //         // Now we can fetch blocks.

    //         // 1. call fetch
    //         let fetch = WsSend::Fetch(res_args);
    //         client.send_message(&fetch.to_message()).unwrap();
    //         let fetch_resp = match client.recv_message()? {
    //             websocket::OwnedMessage::Text(text) => {
    //                 let j: serde_json::Value = serde_json::from_str(&text).unwrap();
    //                 dbg!(j);
    //                 let v: WsRecv = serde_json::from_str(&text).unwrap();
    //                 dbg!(&v);
    //                 match v.data {
    //                     WsRecvData::Conn => unreachable!(),
    //                     WsRecvData::Query(_) => unreachable!(),
    //                     WsRecvData::Fetch(resp) => resp,
    //                     WsRecvData::Block(_) => todo!(),
    //                 }
    //             }
    //             websocket::OwnedMessage::Binary(_) => todo!(),
    //             websocket::OwnedMessage::Close(_) => todo!(),
    //             websocket::OwnedMessage::Ping(_) => todo!(),
    //             websocket::OwnedMessage::Pong(_) => todo!(),
    //         };
    //         dbg!(&fetch_resp);

    //         if fetch_resp.completed {
    //             break;
    //         }

    //         let fetch_block = WsSend::FetchBlock(res_args);
    //         client.send_message(&fetch_block.to_message()).unwrap();

    //         let mut raw = match client.recv_message()? {
    //             websocket::OwnedMessage::Binary(bytes) => {
    //                 use taos_query::util::InlinableRead;

    //                 dbg!(&bytes[0..16]);

    //                 dbg!(bytes.len());

    //                 let mut slice = bytes.as_slice();
    //                 let _ = slice.read_u64().unwrap();
    //                 slice.read_inlinable::<RawBlock>().unwrap()
    //             }
    //             _ => unreachable!(),
    //         };

    //         raw.with_rows(fetch_resp.rows)
    //             .with_cols(query_resp.fields_count)
    //             .with_precision(query_resp.precision);
    //         dbg!(&raw);

    //         for row in 0..raw.nrows() {
    //             for col in 0..raw.ncols() {
    //                 let v = unsafe { raw.get_unchecked(row, col) };
    //                 println!("({}, {}): {}", row, col, v);
    //             }
    //         }
    //     }

    //     let close = WsSend::Close(res_args);
    //     client.send_message(&close.to_message()).unwrap();

    //     Ok(())
    // }
}
