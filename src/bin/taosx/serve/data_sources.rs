use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    time::{Duration, Instant},
};

use actix_web::{
    delete, get,
    http::header::ContentType,
    patch, post,
    web::{Data, Json, Path, Query, ServiceConfig},
    HttpResponse, Responder,
};
use anyhow::Context;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use sqlx::{migrate::Migrator, sqlite::SqliteJournalMode};
use std::str::FromStr;
use taos::{AsyncQueryable, Code, Dsn, TBuilder, TaosBuilder};
use taosx::TaskOpts;
use tokio::{runtime::Runtime, sync::RwLock};
use tokio_util::sync::CancellationToken;
use utoipa::*;


mod definition;
pub use definition::*;

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub(super) struct DataSourceInput {
    id: String,
    protocol: Option<String>,
    hostname: Option<String>,
    port: Option<u16>,
    subject: Option<String>,
    params: Option<BTreeMap<String, Option<String>>>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub(super) struct CloudTarget {
    cluster_id: Option<String>,
    url: String,
    token: Option<String>,
    database: Option<String>,
    params: Option<BTreeMap<String, Option<String>>>,
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[non_exhaustive]
#[serde(rename_all = "kebab-case")]
pub(super) enum Transformer {
    Reheader(Vec<String>),
    Schema {
        tbname: String,
        using: Option<String>,
        tags: Vec<String>,
    },
}

#[test]
fn transformer_test() {
    let t = Transformer::Reheader(vec!["A".to_string(); 2]);
    let s = serde_json::to_string(&t).unwrap();
    dbg!(s);
    let v: Transformer = serde_json::from_str(r#"{ "reheader": ["A", "A"]}"#).unwrap();

    dbg!(v);
    panic!()
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub(super) struct DataIn {
    name: String,
    source: DataSourceInput,
    cloud: CloudTarget,
    transform: Vec<Transformer>,
}

// const a: &str = r#"
// {
//   "from": "tmq+ws://customuser:password@external-domain.com:6041/topic1,topic2",
//   "to": "taos+wss://cloud.tdengine.com/db2?token=xxxx",
//   "labels": [
//     "to_cluster::dfajklddfadfadfad",
//     "data::in"
//   ]
// }
// "#;

/// List available data source definitions.
#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "Available data sources", body = Vec<DataSource>),
    ),
)]
#[get("/ds/in")]
pub(super) async fn data_sources_in() -> impl Responder {
    HttpResponse::Ok().content_type(ContentType::json()).body(r#"
    [{
  "id": "tmq",
  "name": "TDengine Subscription",
  "description": "TMQ data source is a reader-only data source for TDengine.\n\n## Protocols\n\n- **ws**: websocket protocol with plain HTTP connection.\n- **wss**: websocket protocol with TLS http connection.\n\nWithout protocol settings, TMQ will use native connection.\n\n## Subject\n\nTMQ data source could subscribe data from a database or\na specified table with fully \"database.name\" format.\n",
  "type": "uri",
  "protocol": [
    {
      "name": "__",
      "display": "None",
      "description": "Use taosc native connection",
      "default": true,
    },
    {
        "name": "ws",
        "display": "WS",
        "description": "WebSocket with HTTP."
      },
      {
        "name": "wss",
        "display": "WSS",
        "description": "WebSocket with HTTPS."
      }
  ],
  "options": {
    "hostname": {
        "display": "Host",
        "description": "TDengine fqdn. Leave it empty if use server localhost(relative to taosX server).",
        "placeholder": "localhost"
    },
    "port": {
        "display": "Port",
        "description": "TDengine connection port, leave it empty if use default port.",
        "placeholder": "auto"
    },
    "username": {
      "display": "Username",
      "description": "TDengine username. The default is root.",
      "placeholder": "root"
    },
    "password": {
      "display": "Password",
      "description": "TDengine password. The default is taosdata.",
      "placeholder": "taosdata"
    },
    "subject": {
      "required": true,
      "display": "Topics",
      "description": "Database name, database.table name or topic name is all available.",
      "placeholder": "Example: db1,db1.stb1,topic1"
    }
  },
  "strict": false,
  "groups": [
    {
      "name": "Cloud Authentication",
      "display_order": 1,
      "description": "Token for TDcloud.",
      "params": [
        {
          "name": "token",
          "hint": "str",
          "description": "Copy the token from TDcloud admin panel."
        }
      ]
    },
    {
      "name": "Subscribe Options",
      "display_order": 2,
      "description": "Options for TMQ subscription.",
      "params": [
        {
          "name": "group.id",
          "hint": "str",
          "required": true,
          "description": "A consumer group id. One group id shares consume offsets globally.\n"
        },
        {
          "name": "client.id",
          "hint": "str",
          "description": "Consumer client id to distinguish from individual clients.\n"
        },
        {
          "name": "auto.offset.reset",
          "hint": {
            "type": "str",
            "choices": [
              "earliest",
              "latest",
              "none"
            ]
          }
        },
        {
          "name": "use.topic.name",
          "hint": "str",
          "description": "Use specific topic name for selected database or database.name,\nwhich does not work if the subject is direct topic name.\n"
        },
        {
          "name": "timeout",
          "hint": "timeout",
          "description": "A timeout for polling data from the topic.\n\nThe input value should be one of:\n\n- `never`: means waiting for valid message without timeout.\n- A duration string like `5s`, `1m` etc.\n",
          "placeholder": "5s"
        }
      ]
    }
  ],
  "definitions": {
    "hints": [
      {
        "name": "timeout",
        "type": "str",
        "choices": [
          "Never",
          {
            "type": "duration"
          }
        ]
      }
    ]
  }
}]
    "#)
}
