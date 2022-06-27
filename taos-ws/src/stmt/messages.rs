use std::any::Any;
use std::ops::Deref;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::serde_as;
use serde_with::NoneAsEmptyString;

use taos_error::Error;

use crate::infra::ToMessage;
use crate::infra::WsConnReq;

pub type ReqId = u64;

/// Type for result ID.
pub type StmtId = u64;

#[derive(Debug, Serialize)]
pub struct StmtInit {
    req_id: ReqId,
}
#[derive(Debug, Serialize)]
pub struct StmtPrepare {
    req_id: ReqId,
    stmt_id: StmtId,
    sql: String,
}
#[derive(Debug, Serialize)]
pub struct StmtSetTableName {
    req_id: ReqId,
    stmt_id: StmtId,
    name: String,
}

#[derive(Debug, Serialize)]
pub struct StmtSetTags {
    req_id: ReqId,
    stmt_id: StmtId,
    tags: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct StmtBind {
    req_id: ReqId,
    stmt_id: StmtId,
    columns: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct StmtAddBatch {
    req_id: ReqId,
    stmt_id: StmtId,
}
#[derive(Debug, Serialize)]
pub struct StmtExec {
    req_id: ReqId,
    stmt_id: StmtId,
}

#[derive(Debug, Serialize)]
pub struct StmtClose {
    req_id: ReqId,
    stmt_id: StmtId,
}
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum WsSendData {
    Conn(WsConnReq),
    Init(StmtInit),
    Prepare(StmtPrepare),
    SetTableName(StmtSetTableName),
    SetTags(StmtSetTags),
    Bind(StmtBind),
    AddBatch(StmtAddBatch),
    Exec(StmtExec),
    Close(),
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct StmtArgs {
    pub req_id: ReqId,
    pub stmt_id: StmtId,
}
#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "args")]
#[serde(rename_all = "snake_case")]
pub enum StmtSend {
    Conn {
        req_id: ReqId,
        #[serde(flatten)]
        req: WsConnReq,
    },
    Init {
        req_id: ReqId,
    },
    Prepare {
        #[serde(flatten)]
        args: StmtArgs,
        sql: String,
    },
    SetTableName {
        #[serde(flatten)]
        args: StmtArgs,
        name: String,
    },
    SetTags {
        #[serde(flatten)]
        args: StmtArgs,
        tags: Vec<Value>,
    },
    Bind {
        #[serde(flatten)]
        args: StmtArgs,
        columns: Vec<Value>,
    },
    AddBatch(StmtArgs),
    Exec(StmtArgs),
    Close(StmtArgs),
}

impl ToMessage for StmtSend {}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "action")]
#[serde(rename_all = "snake_case")]
pub enum StmtRecvData {
    Conn,
    Init {
        #[serde(default)]
        stmt_id: StmtId,
    },
    Prepare {
        #[serde(default)]
        stmt_id: StmtId,
    },
    SetTableName {
        #[serde(default)]
        stmt_id: StmtId,
    },
    SetTags {
        #[serde(default)]
        stmt_id: StmtId,
    },
    Bind {
        #[serde(default)]
        stmt_id: StmtId,
    },
    AddBatch {
        #[serde(default)]
        stmt_id: StmtId,
    },
    Exec {
        #[serde(default)]
        stmt_id: StmtId,
        #[serde(default)]
        affected: usize,
    },
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct StmtRecv {
    pub code: i32,
    #[serde_as(as = "NoneAsEmptyString")]
    pub message: Option<String>,
    pub req_id: ReqId,
    #[serde(flatten)]
    pub data: StmtRecvData,
}

#[derive(Debug)]
pub enum StmtOk {
    Conn(Result<(), Error>),
    Init(ReqId, Result<StmtId, Error>),
    Stmt(StmtId, Result<Option<usize>, Error>),
}

impl StmtRecv {
    pub(crate) fn ok(self) -> StmtOk {
        macro_rules! _e {
            () => {
                Err(taos_error::Error::new(
                    if self.code == 65536 { -1 } else { self.code },
                    self.message.unwrap_or_default(),
                ))
            };
        }
        match self.data {
            StmtRecvData::Conn => StmtOk::Conn({
                if self.code == 0 {
                    Ok(())
                } else {
                    _e!()
                }
            }),
            StmtRecvData::Init { stmt_id } => StmtOk::Init(self.req_id, {
                if self.code == 0 {
                    Ok(stmt_id)
                } else {
                    _e!()
                }
            }),
            StmtRecvData::Prepare { stmt_id }
            | StmtRecvData::SetTableName { stmt_id }
            | StmtRecvData::SetTags { stmt_id }
            | StmtRecvData::Bind { stmt_id }
            | StmtRecvData::AddBatch { stmt_id } => StmtOk::Stmt(stmt_id, {
                if self.code == 0 {
                    Ok(None)
                } else {
                    _e!()
                }
            }),
            StmtRecvData::Exec { stmt_id, affected } => StmtOk::Stmt(stmt_id, {
                if self.code == 0 {
                    Ok(Some(affected))
                } else {
                    _e!()
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Ok;

    use super::*;

    #[test]
    fn stmt() -> anyhow::Result<()> {
        Ok(())
    }
}
