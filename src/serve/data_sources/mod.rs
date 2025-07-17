use std::collections::BTreeMap;
use std::time::Duration;

use actix_web::{
    HttpRequest, HttpResponse, Responder, get,
    http::header::{ContentDisposition, ContentType},
    post,
    web::{self, Data, Json, Query},
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::{Code, IntoDsn};
use taosx_task::validate::validate_dsn;
use tokio::time::timeout;
use tracing::{Instrument, Span, instrument};
use utoipa::*;

use crate::serve::{controller::TaskControllerRef, task::Failed};
pub use definition::*;
pub use point_loader::*;
use taosx_core::plugins::transform::sample::DsSamples;
use taosx_core::runners::opc::{csv::CsvParser, model::ModelType};
use taosx_core::utils::dsn::json_to_dsn;
use taosx_core::utils::timeout::{Timeout, TimeoutType};
use taosx_core::{DataSetsReq, get_data_dir, list_datasets_from};
use taosx_core::{QueryDataSourceReq, dsv::DataSourceValidation, utils::license};
use taosx_core::{
    plugins::transform::parse::plugin::ParserPlugin,
    runners::pi::{
        parse_query_datasource_params,
        transform::{PIElementModelConfig, PIPointModelConfig},
    },
};

mod definition;
pub(crate) mod kafka;
pub(crate) mod opc;
mod point_loader;

mod query;

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
#[ignore]
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
            .json(Failed::new(
                Code::new(-1),
                "Data source not found".to_string(),
                (),
            )),
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct DsField {
    name: String,
    scope: String,
    r#type: String,
}
#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
pub struct DsSampleOut {
    fields: Vec<DsField>,
    columns: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Debug, ToSchema, IntoParams)]
pub struct TzQuery {
    /// Timezone name, e.g. "Asia/Shanghai"
    tz: Option<String>,
}
/// Flat stream transform sample data simulation.
#[utoipa::path(
    tag = "transform",
    request_body = DsSampleIn,
    responses(
        (status = 200, description = "Sample data output", body = Vec<DsSampleOut>),
    ),
    params(
        TzQuery,
    )
)]
#[post("/transform/sample/flat")]
pub(super) async fn data_source_sample(
    data: Json<DsSamples>,
    tz: Query<TzQuery>,
) -> impl Responder {
    match data.into_inner() {
        DsSamples::Simple(sample_in) => match sample_in.transform(tz.tz.as_deref()) {
            Ok(output) => Ok(HttpResponse::Ok()
                .content_type(ContentType::json())
                .json(output)),
            Err(err) => Err(Failed::from_error(err)),
        },
        DsSamples::MultiSchema(samples) => match samples.transform(tz.tz.as_deref()) {
            Ok(output) => Ok(HttpResponse::Ok()
                .content_type(ContentType::json())
                .json(output)),
            Err(e) => Err(Failed::from_error(e)),
        },
    }
}

#[post("/transform/sample/flat/s_model/preview")]
pub(super) async fn stable_preview(data: Json<DsSamples>) -> impl Responder {
    match data.into_inner() {
        DsSamples::Simple(sample_in) => match sample_in.stable_preview() {
            Ok(output) => Ok(HttpResponse::Ok()
                .content_type(ContentType::json())
                .json(output)),
            Err(e) => Err(Failed::from_error(e)),
        },
        DsSamples::MultiSchema(sample_in) => match sample_in.stable_preview() {
            Ok(output) => Ok(HttpResponse::Ok()
                .content_type(ContentType::json())
                .json(output)),
            Err(e) => Err(Failed::from_error(e)),
        },
    }
}

#[utoipa::path(
    tag = "transform parser plugins",
    responses(
        (status = 200, description = "list all the parser plugins", body = Vec<String>),
    )
)]
#[get("/transform/parser/plugins")]
pub(super) async fn list_all_parser_plugins() -> impl Responder {
    let plugins = ParserPlugin::list_all_plugins();
    // let plugins = vec!["hebeipower".to_string(), "taos".to_string()];
    HttpResponse::Ok()
        .content_type(ContentType::json())
        .json(plugins)
}

#[test]
fn test_sample_flat() {
    let json = r#"
{"parser":{"parse":{"current":{"regex":"(?P<current>\\d+\\.\\d+)"}}},"input":[{"current":"10.3","groupid":"2","id":"1001","location":"California.SanFrancisco","phase":"0.31","timestamp":"1538548685000","voltage":"219"},{"current":"10.2","groupid":"3","id":"1002","location":"California.SanFrancisco","phase":"0.23","timestamp":"1538548684000","voltage":"220"},{"current":"11.5","groupid":"3","id":"1003","location":"California.LosAngeles","phase":"0.35","timestamp":"1538548686500","voltage":"221"}]}
    "#;
    let sample_in: taosx_core::task_set::prelude::DsSampleIn = serde_json::from_str(json).unwrap();
    let output = sample_in.transform(Some("Asia/Shanghai")).unwrap();
    dbg!(serde_json::to_string(&output).unwrap());
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
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let data = data.into_inner();
    match if let Some(agent) = data.via {
        controller.list_datasets_via_agent(agent, data).await
    } else {
        list_datasets_from(&data).await
    } {
        Ok(data) => Ok(HttpResponse::Ok()
            .content_type(ContentType::json())
            .json(&data)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[derive(Deserialize, Debug, ToSchema, IntoParams)]
pub struct DsnAgentQuery {
    /// source dsn
    #[param(allow_reserved)]
    dsn: serde_json::Value,
    /// sink dsn
    to: Option<String>,
    /// agent id
    via: Option<i64>,
    /// request timeout
    timeout: Option<u64>,
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
        ("dsn" = String, description = "dsn string"),
        ("via" = String, description = "agent id"),
        ("timeout" = Option<String>, description = "timeout seconds, use default 30s when not set")
    ),
)]
#[get("/ds/in/validate")]
pub(super) async fn data_source_is_valid(
    controller: Data<TaskControllerRef>,
    query: Query<DsnAgentQuery>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let query = query.into_inner();

    let timeout_sec = query.timeout.unwrap_or_else(|| {
        let dsn = json_to_dsn(&query.dsn);
        if let Ok(dsn) = dsn {
            Timeout::get(TimeoutType::ValidateDataSource(dsn))
        } else {
            Timeout::get(TimeoutType::Default)
        }
    });
    let span = Span::current();
    let result = timeout(
        Duration::from_secs(timeout_sec),
        is_datasource_valid_impl(controller, query).instrument(span),
    )
    .await;
    match result {
        Ok(dsv) => Ok(HttpResponse::Ok().json(dsv)),
        Err(_) => Err(Failed::new(
            Code::FAILED,
            "Failed to connect to dsn: timed out".to_string(),
            (),
        )),
    }
}

async fn is_datasource_valid_impl(
    controller: Data<TaskControllerRef>,
    query: DsnAgentQuery,
) -> DataSourceValidation {
    let dsn = json_to_dsn(&query.dsn);
    match dsn {
        Err(err) => {
            DataSourceValidation::invalid("unknown".to_string(), format!("DSN error: {err:#}"))
        }
        Ok(d) => {
            let via = query.via;
            match via {
                None => validate_dsn(d).await,
                Some(agent) => controller.validate_dsn_via_agent(agent, &d).await,
            }
        }
    }
}

#[derive(Deserialize, Debug, ToSchema, IntoParams)]
pub struct DsnAgentQueryV2 {
    #[param(allow_reserved)]
    from: Option<String>,
    from_json: Option<serde_json::Value>,
    #[param(allow_reserved)]
    to: Option<String>,
    via: Option<i64>,
    timeout: Option<u64>,
}

/// check data source validation by dsn
#[utoipa::path(
    post,
    path = "/ds/in/validate",
    responses(
        (status = 200, description = "data source is valid or not", body = DataSourceValidation),
        (status = 500, description = "check data source failed", body = Failed),
    )
)]
#[post("/ds/in/validate")]
pub(super) async fn data_source_sink_is_valid(
    controller: Data<TaskControllerRef>,
    query: Json<DsnAgentQueryV2>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let query = query.into_inner();
    let query_timeout =
        query
            .timeout
            .unwrap_or_else(|| match (&query.from, &query.from_json, &query.to) {
                (_, Some(from_json), Some(to)) => {
                    let a = json_to_dsn(from_json)
                        .map(|dsn| Timeout::get(TimeoutType::ValidateDataSource(dsn)))
                        .unwrap_or(Timeout::get(TimeoutType::Default));
                    let b = to
                        .into_dsn()
                        .map(|dsn| Timeout::get(TimeoutType::ValidateDataSource(dsn)))
                        .unwrap_or(Timeout::get(TimeoutType::Default));
                    a.max(b)
                }
                (Some(from), None, Some(to)) => {
                    let a = json_to_dsn(&serde_json::Value::String(from.clone()))
                        .map(|dsn| Timeout::get(TimeoutType::ValidateDataSource(dsn)))
                        .unwrap_or(Timeout::get(TimeoutType::Default));
                    let b = to
                        .into_dsn()
                        .map(|dsn| Timeout::get(TimeoutType::ValidateDataSource(dsn)))
                        .unwrap_or(Timeout::get(TimeoutType::Default));
                    a.max(b)
                }
                (_, Some(from_json), None) => json_to_dsn(from_json)
                    .map(|dsn| Timeout::get(TimeoutType::ValidateDataSource(dsn)))
                    .unwrap_or(Timeout::get(TimeoutType::Default)),
                (Some(from), None, None) => json_to_dsn(&serde_json::Value::String(from.clone()))
                    .map(|dsn| Timeout::get(TimeoutType::ValidateDataSource(dsn)))
                    .unwrap_or(Timeout::get(TimeoutType::Default)),
                (None, None, Some(to)) => to
                    .into_dsn()
                    .map(|dsn| Timeout::get(TimeoutType::ValidateDataSource(dsn)))
                    .unwrap_or(Timeout::get(TimeoutType::Default)),
                (None, None, None) => Timeout::get(TimeoutType::Default),
            });
    let span = Span::current();
    let result = timeout(
        Duration::from_secs(query_timeout),
        dsn_and_license_validate(controller, query).instrument(span),
    )
    .await;
    match result {
        Ok(dsv) => Ok(HttpResponse::Ok().json(dsv)),
        Err(_) => Err(Failed::new(
            Code::FAILED,
            "Failed to connect to dsn: timed out".to_string(),
            (),
        )),
    }
}

async fn dsn_and_license_validate(
    controller: Data<TaskControllerRef>,
    query: DsnAgentQueryV2,
) -> DataSourceValidation {
    match (query.from, query.from_json, query.to) {
        // 保留之前检查 DataSource 的逻辑
        (_, Some(from_json), Some(to)) => match json_to_dsn(&from_json) {
            Err(err) => {
                DataSourceValidation::invalid("unknown", format!("invalid DSN, cause: {}", err))
            }
            Ok(dsn) => validate_2dsn_and_license(dsn.to_string(), to, query.via, controller).await,
        },
        (Some(from), None, Some(to)) => match json_to_dsn(&serde_json::Value::String(from.clone()))
        {
            Err(err) => {
                DataSourceValidation::invalid("unknown", format!("invalid DSN, cause: {}", err))
            }
            Ok(dsn) => validate_2dsn_and_license(dsn.to_string(), to, query.via, controller).await,
        },
        // 如果 from 和 to 中只有一个 DSN，那么只检查一个 DSN。
        // 例如：备份任务检查 S3 的连通性，from 为 None，to 为 Some("local:$BACKUP_DIR?$S3_PARAMS...")
        // TODO：只检查了 dsn 的连通性，没有检查 license
        (_, Some(from_json), None) => match json_to_dsn(&from_json) {
            Err(err) => {
                DataSourceValidation::invalid("unknown", format!("invalid DSN, cause: {}", err))
            }
            Ok(dsn) => validate_dsn(dsn.to_string()).await,
        },
        (Some(from), None, None) => match json_to_dsn(&serde_json::Value::String(from.clone())) {
            Err(err) => {
                DataSourceValidation::invalid("unknown", format!("invalid DSN, cause: {}", err))
            }
            Ok(dsn) => validate_dsn(dsn.to_string()).await,
        },
        (None, None, Some(to)) => validate_dsn(to).await,
        // 两个 DSN 都为空，返回错误
        (None, None, None) => DataSourceValidation::invalid(
            "unknown".to_string(),
            "invalid DSN not found".to_string(),
        ),
    }
}

async fn validate_2dsn_and_license(
    from: String,
    to: String,
    via: Option<i64>,
    controller: Data<TaskControllerRef>,
) -> DataSourceValidation {
    let from = match from.into_dsn() {
        Ok(dsn) => dsn,
        Err(err) => {
            return DataSourceValidation::invalid(
                "unknown".to_string(),
                format!("DSN error: {err:#}"),
            );
        }
    };

    let res = match via {
        None => validate_dsn(from.clone()).await,
        Some(agent) => controller.validate_dsn_via_agent(agent, &from).await,
    };

    let to = match to.into_dsn() {
        Ok(dsn) => dsn,
        Err(err) => {
            return DataSourceValidation::invalid(
                "unknown".to_string(),
                format!("Target DSN error: {err:#}"),
            );
        }
    };

    match license::validate_enterprise_license(&from, &to).await {
        Ok(license::LicenseKind::Good { .. }) => res,
        Ok(license::LicenseKind::Feature(err))
        | Ok(license::LicenseKind::Edition(err))
        | Ok(license::LicenseKind::Connector(err))
        | Err(err) => DataSourceValidation::invalid(
            "unknown".to_string(),
            format!("DSN license validate error: {err:#}"),
        ),
    }
}

/// get sample data from data source
#[utoipa::path(
    get,
    path = "/ds/in/sample",
    responses(
        (status = 200, description = "sample data from data source", body = DsSampleIn),
        (status = 500, description = "get sample data failed", body = Failed),
    ),
    params(
        ("dsn" = String, description = "dsn string"),
        ("via" = String, description = "agent id"),
        ("timeout" = Option<String>, description = "timeout seconds")
    ),
)]
#[post("/ds/in/sample")]
pub(super) async fn get_sample(
    controller: Data<TaskControllerRef>,
    query: Json<DsnAgentQuery>,
) -> impl Responder {
    let query = query.into_inner();

    let query_timeout = query.timeout.unwrap_or_else(|| {
        let dsn = json_to_dsn(&query.dsn);
        if let Ok(dsn) = dsn {
            Timeout::get(TimeoutType::GetSample(dsn))
        } else {
            Timeout::get(TimeoutType::Default)
        }
    });
    let query_timeout = Duration::from_secs(query_timeout);
    tracing::debug!(
        "get sample from: {:?}, timeout: {:?}",
        query.dsn,
        query_timeout
    );

    match timeout(query_timeout, get_sample_impl(controller, query)).await {
        Ok(Ok(sample)) => Ok(HttpResponse::Ok().json(sample)),
        Ok(Err(err)) => {
            tracing::error!("failed to get sample from data source, cause: {err:#}");
            Err(Failed::new(
                Code::FAILED,
                format!("failed to get sample from data source, cause: {err:#}"),
                (),
            ))
        }
        Err(_) => {
            tracing::error!("get sample from data source timeout");
            Err(Failed::new(
                Code::FAILED,
                "get sample from data source timeout".to_string(),
                (),
            ))
        }
    }
}

async fn get_sample_impl(
    controller: Data<TaskControllerRef>,
    query: DsnAgentQuery,
) -> anyhow::Result<DsSamples> {
    let via = query.via;
    let dsn = json_to_dsn(&query.dsn)?;

    match via {
        None => taosx_task::sample::get_sample(&dsn).await,
        Some(agent) => {
            controller
                .get_sample_via_agent(agent, dsn.to_string())
                .await
        }
    }
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
    req: HttpRequest,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    // match download_all_point_csv_file(controller, data).await {
    match download_all_point_csv_file(controller, params).await {
        Ok(named_file) => Ok(named_file.into_response(&req)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "init download opc file task successfully", body = String),
        (status = 500, description = "task start error", body = Failed),
    ),
)]
#[get("/ds/in/point/file/download/task")]
pub(super) async fn init_download_file_task_get(
    controller: Data<TaskControllerRef>,
    params: Query<DownloadAllPointsParams>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    // transform Query to Json
    let params = Json(params.into_inner());

    match arrange_point_file_download_task(controller, params).await {
        Ok(task_id) => Ok(HttpResponse::Ok().json(TaskTicket::new_task(task_id))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "init download opc file task successfully", body = String),
        (status = 500, description = "task start error", body = Failed),
    ),
)]
#[post("/ds/in/point/file/download/task")]
pub(super) async fn init_download_file_task_post(
    controller: Data<TaskControllerRef>,
    params: Json<DownloadAllPointsParams>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    match arrange_point_file_download_task(controller, params).await {
        Ok(task_id) => Ok(HttpResponse::Ok().json(TaskTicket::new_task(task_id))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "check opc file ready", body = String),
        (status = 500, description = "check opc file error", body = Failed),
    ),
)]
#[get("/ds/in/point/file/are/you/ready")]
pub(super) async fn check_point_file_ready(params: Query<TaskTicket>) -> impl Responder {
    match check_task_complete(params.ticket.clone()).await {
        Ok(complete) => {
            Ok(HttpResponse::Ok().json(TaskTicket::complete(params.ticket.clone(), complete)))
        }
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "download opc file successfully", body = String),
        (status = 500, description = "download opc file error", body = Failed),
    ),
)]
#[get("/ds/in/point/file/async")]
pub(super) async fn download_point_file(
    params: Query<TaskTicket>,
    req: HttpRequest,
) -> impl Responder {
    match load_point_file(&params.ticket, false).await {
        Ok(named_file) => {
            let content_disposition = ContentDisposition::attachment("point.csv");
            Ok(named_file
                .set_content_disposition(content_disposition)
                .into_response(&req))
        }
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "", body = String),
        (status = 500, description = "check opc file error", body = Failed),
    ),
)]
#[get("/ds/in/point/data/page")]
pub(super) async fn page_point_data(params: Query<TaskTicket>) -> impl Responder {
    match load_point_data_page(&params).await {
        Ok(page) => Ok(HttpResponse::Ok().json(R::success(page))),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[derive(Serialize, Deserialize)]
struct DatasourceTemplateQuery {
    driver: String,
    lang: Option<String>,
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "download template file", body = String),
        (status = 500, description = "download template file error", body = Failed),
    ),
)]
#[get("/ds/in/point/file/template")]
pub(super) async fn download_point_template_file(
    params: Query<DatasourceTemplateQuery>,
    req: HttpRequest,
) -> impl Responder {
    let lang: String = params.lang.clone().unwrap_or("zh".to_string());

    match get_point_file_template(&params.driver, &lang).await {
        Ok(named_file) => {
            let content_disposition = ContentDisposition::attachment("point_template.csv");
            Ok(named_file
                .set_content_disposition(content_disposition)
                .into_response(&req))
        }
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "check opc csv file ready", body = String),
        (status = 500, description = "check opc csv file error", body = Failed),
    ),
)]
#[post("/ds/in/point/file/is_valid")]
pub async fn is_opc_csv_valid(req: Json<DsnAgentQuery>) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let query = req.into_inner();
    let timeout_sec = query.timeout.unwrap_or(Timeout::get(TimeoutType::Default));

    let result = timeout(
        Duration::from_secs(timeout_sec),
        is_opc_csv_valid_impl(query),
    )
    .await;

    match result {
        Ok(Ok(())) => Ok(HttpResponse::Ok().json(serde_json::json!({
            "valid": true,
            "message": "csv file is valid"
        }))),
        Ok(Err(err)) => {
            tracing::error!("check csv file failed, cause: {:?}", err);
            Err(Failed::new(
                Code::FAILED,
                format!("check csv file failed, cause: {}", err),
                (),
            ))
        }
        Err(err) => {
            tracing::error!("check csv file timeout, cause: {:?}", err);
            Err(Failed::new(
                Code::FAILED,
                format!("check csv file timeout, cause: {}", err),
                (),
            ))
        }
    }
}

async fn is_opc_csv_valid_impl(req: DsnAgentQuery) -> anyhow::Result<()> {
    let from = json_to_dsn(&req.dsn)?;

    let parser = CsvParser::from_dsn(&from)
        .map_err(|err| anyhow::anyhow!("failed to parse dsn: {}, cause: {:?}", from, err))?;

    let model_config = parser
        .parse()
        .await
        .map_err(|err| anyhow::anyhow!("failed to parse dsn: {}, cause: {:?}", from, err))?;

    // 检查 csv 文件是否满足合法性
    model_config
        .validate()
        .map_err(|err| anyhow::anyhow!("failed to validate csv file, cause: {:?}", err))?;

    // 如果 req.dsn.model_type 和 req.to 不为空，则检查 database 中的 schema 和 csv 中的 schema 是否冲突
    if let (Some(model_type), Some(to)) = (ModelType::from_dsn(&from), req.to.as_ref()) {
        let to = to.into_dsn()?;
        model_config
            .validate_with_sink(model_type, &to)
            .await
            .map_err(|err| anyhow::anyhow!("conflict between csv and database, cause: {:?}", err))?
    }

    Ok(())
}

#[post("/ds/in/download/pi_default_config")]
pub(super) async fn download_pi_default_config(
    controller: Data<TaskControllerRef>,
    params: Json<GetPIDefaultConfigParams>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    match get_pi_default_config(controller, params).await {
        Ok(file_name) => Ok(file_name),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPIDefaultConfigParams {
    from: serde_json::Value,
    via: Option<i64>,
    task_id: Option<i64>,
    update: Option<bool>,
}

#[instrument(skip_all)]
pub async fn get_pi_default_config(
    controller: Data<TaskControllerRef>,
    params: Json<GetPIDefaultConfigParams>,
) -> anyhow::Result<String> {
    let params = params.into_inner();
    tracing::debug!("params: {:?}", params);
    let update = params.update.unwrap_or(false);
    let file_name = match params.task_id {
        Some(task_id) => format!("./files/pi2td_task_{}.csv", task_id),
        None => format!(
            "./files/pi2td_{}.csv",
            chrono::Local::now().format("%y%m%d%H%M")
        ),
    };
    let exists = std::path::Path::new(file_name.as_str()).exists();
    if params.task_id.is_none() || !exists || update {
        let dsn = json_to_dsn(&params.from)?;
        let (mode, pattern, pattern_type) = parse_query_datasource_params(&dsn);
        tracing::debug!(?mode, ?pattern, ?pattern_type);
        let mut args = vec![mode.to_string(), pattern.to_string()];
        if !pattern_type.is_empty() {
            args.push(pattern_type.to_string());
        }

        let req = QueryDataSourceReq {
            from: params.from.clone(),
            args,
        };
        let pi_data =
            query::query_data_source(req, params.via, Some(controller.into_inner().as_ref()))
                .await?;
        let config_data: String = match mode {
            "-pp" => {
                let config = PIPointModelConfig::from_json(pi_data.as_str(), false).unwrap();
                config.to_string()
            }
            "-px" => {
                let config = PIPointModelConfig::from_json(pi_data.as_str(), true).unwrap();
                config.to_string()
            }
            "-pt" => {
                let config = PIElementModelConfig::from_json(pi_data.as_str()).unwrap();
                config.to_string()
            }
            _ => unimplemented!(),
        };
        // 保存原始 json 数据
        std::fs::write(file_name.as_str().replace(".csv", ".json"), pi_data).unwrap();
        // 保存配置文件
        std::fs::write(file_name.as_str(), config_data).unwrap();
    }

    return Ok(file_name);
}
