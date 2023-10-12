use std::collections::BTreeMap;

use actix_files::NamedFile;
use actix_web::{
    get,
    http::header::ContentType,
    post,
    web::{self, Data, Json, Query},
    HttpResponse, Responder, HttpRequest,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use taos::Code;
use taosx_core::{list_datasets_from, DataSetsReq, validate_dsn};
use utoipa::*;

mod definition;

pub use definition::*;

use crate::serve::{
    controller::TaskControllerRef,
    task::{Failed, ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT},
};

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

// {
//   "id": "tmq",
//   "protocol": "ws",
//   "options": {
//     "host": "192.168.0.201",
//     "port": ""
//     "username": "root",
//     "password": "password",
//     "subject": "topic1"
//   },
//   "params": {
//     "group.id": "gid1"
//   }
// }
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

#[derive(Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Lang {
    En,
    Zh,
}

#[derive(Deserialize, Debug, ToSchema, IntoParams)]
pub struct LangQuery {
    lang: Option<Lang>,
}

impl LangQuery {
    pub fn is_cn(&self) -> bool {
        self.lang
            .as_ref()
            .map(|lang| *lang == Lang::Zh)
            .unwrap_or(false)
    }
}

/// List available data source definitions.
#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "Available data sources", body = Vec<DataSourceDefinition>),
    ),
    params(
        LangQuery,
    ),
)]
#[get("/ds/in")]
pub(super) async fn data_sources_in(lang: Query<LangQuery>) -> impl Responder {
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .json(if lang.is_cn() {
            super::controller::DATA_SOURCE_DEFINITIONS_CN
                .values()
                .collect_vec()
        } else {
            super::controller::DATA_SOURCE_DEFINITIONS
                .values()
                .collect_vec()
        })
}

/// Get data source definition by name(id).
#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "Data source definition of some", body = DataSourceDefinition),
    ),
    params(
        LangQuery,
    ),
)]
#[get("/ds/in/{name}")]
pub(super) async fn data_sources_in_one(
    name: web::Path<String>,
    lang: Query<LangQuery>,
) -> impl Responder {
    let name = name.as_str();
    match if lang.is_cn() {
        super::controller::DATA_SOURCE_DEFINITIONS_CN.get(name)
    } else {
        super::controller::DATA_SOURCE_DEFINITIONS.get(name)
    } {
        Some(ds) => HttpResponse::Ok()
            .content_type(ContentType::json())
            .json(ds),
        None => HttpResponse::NotFound()
            .content_type(ContentType::json())
            .json(Failed {
                code: Code::new(-1),
                message: "Data source not found".into(),
            }),
    }
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub(super) struct DataSets {
    id: String,
    name: Option<String>,
    r#type: Option<String>,
}

// impl DataSetsReq {
//     pub fn datasets(&self) -> anyhow::Result<Vec<DataSets>> {
//         let dsn: Dsn = data.from.parse()?;
//     }
// }
#[utoipa::path(
    tag = "data sources",
    request_body = DataSetsReq,
    responses(
        (status = 200, description = "Available data sources", body = Vec<DataSets>),
        (status = 500, description = "List data sets error", body = Failed),
    ),
)]
#[post("/ds/in/sets")]
pub(super) async fn data_source_collection(
    controller: Data<TaskControllerRef>,
    data: Json<DataSetsReq>,
) -> impl Responder {
    let path = ENV_TAOSX_UPLOAD_FILE_HOME_DEFAULT.replace("files", "");
    let root = std::path::Path::new(path.as_str());
    let _ = std::env::set_current_dir(&root);
    let data = data.into_inner();
    match if let Some(agent) = data.via {
        controller.list_datasets_via_agent(agent, data).await
    } else {
        list_datasets_from(&data).await
    } {
        Ok(data) => HttpResponse::Ok()
            .content_type(ContentType::json())
            .json(&data),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: 0xFFFF.into(),
            message: format!("{:#}", err),
        }),
    }
}

/// check data source validation by dsn
#[utoipa::path(
    get,
    path = "/ds/in/validate",
    responses(
        (status = 200, description = "data source is valid or not", body = DataSourceValidation),
        (status = 500, description = "check data source failed", body = Failed),
    ),
    params(
        ("dsn" = DsnQuery, description = "dsn string")
    ),
)]
#[get("/ds/in/validate")]
pub(super) async fn data_source_is_valid(query: Query<DsnQuery>) -> impl Responder {
    let dsn = query.into_inner().dsn;
    let dsv = validate_dsn(dsn);
    HttpResponse::Ok().content_type(ContentType::json()).json(dsv)
}

#[derive(Deserialize, Debug, ToSchema, IntoParams)]
pub struct DsnQuery {
    dsn: String,
}

#[utoipa::path(
    tag = "data sources",
    // request_body = DataSetsReq,
    responses(
        (status = 200, description = "Available data sources", body = Vec<DataSets>),
        (status = 500, description = "List data sets error", body = Failed),
    ),
)]
#[get("/ds/in/download/all_data_sets")]
pub(super) async fn download_all_data_set_file(
    controller: Data<TaskControllerRef>,
    params: Query<DownloadAllPointsParams>,
    // data: Query<DataSetsReq>,
    req: HttpRequest,
) -> impl Responder {
     // match download_all_point_csv_file(controller, data).await {
     match download_all_point_csv_file(controller, params).await {
        Ok(named_file) => named_file.into_response(&req),
        Err(err) => HttpResponse::InternalServerError().json(Failed {
            code: 0xFFFF.into(),
            message: format!("{:#}", err),
        }),
    }
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DownloadAllPointsParams {
    from: String,
    via: Option<i64>,
    categories: String,
}

async fn download_all_point_csv_file(
    controller: Data<TaskControllerRef>,
    // data: Query<DataSetsReq>,
    params: Query<DownloadAllPointsParams>,
) -> anyhow::Result<NamedFile> {
    let params = params.into_inner();
    let data = get_all_points(params.from, params.via, params.categories, controller.into_inner().as_ref()).await?;
    
    let mut config_file = tempfile::NamedTempFile::new()?;
    tracing::debug!("temp file path: {}", &config_file.path().to_str().unwrap_or(""));
    use std::io::Write;
    write!(config_file, "{}", &data)?;
    Ok(NamedFile::open(config_file.path().to_path_buf())?)
}

use crate::serve::TaskController;
pub(crate) async fn get_all_points(from: String, via: Option<i64>, categories: String, controller: &TaskController) -> anyhow::Result<String> {
    use taos::IntoDsn;
    let from = from.into_dsn()?;
    let pattern;
    match from.driver.as_str() {
        "pi" | "pibackfill" => {
            pattern = Some(String::from("*"));
        },
        _ => {
            pattern = Some(String::from(".*"));
        }
    }
    let limit = usize::MAX / 2 - 1; // cause usize::MAX out of range i64 type when exec toml::to_string()
    let data  = DataSetsReq {
        from: from.to_string(),
        categories: vec![categories],
        via,
        offset: 0,
        pattern,
        limit,
        lang: None,
    };
    match if let Some(agent) = data.via {
        controller.list_datasets_via_agent(agent, data).await
    } else {
        list_datasets_from(&data).await
    } {
        Ok(data) => {
            let data = match from.driver.as_str() {
                "pi" | "pibackfill" => data.into_iter().map(|set| set.id).join("\n"),
                "opcua" | "opcda" => {
                    // generate opc template csv
                    let mut result = String::new();
                    result.push_str("Point Code(Required and will be point child table name),OPC Point Id (Required)\ntbname,point_id\n");
                    let data = if from.driver.eq("opcua") {
                        data.into_iter().map(|set| format!("Meter_{{ns}}_{{id}},{}", set.id)).join("\n")
                    } else {
                        data.into_iter().map(|set| format!("Meter_{{TagName}},{}", set.id)).join("\n")
                    };
                    result.push_str(data.as_str());
                    result
                },
                _ => unimplemented!(),
            };
            Ok(data)
        },
        Err(err) => Err(err),
    }
}