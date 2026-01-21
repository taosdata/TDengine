use std::collections::BTreeMap;
use std::time::Duration;

use actix_web::{
    HttpRequest, HttpResponse, Responder, get,
    http::header::{ContentDisposition, ContentType},
    post,
    web::{Data, Json, Query},
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use taos::{Code, Dsn, IntoDsn};
use tokio::time::timeout;
use tracing::instrument;
use utoipa::*;

use crate::serve::{controller::TaskControllerRef, task::Failed};
pub use point_loader::*;
use taosx_core::QueryDataSourceReq;
use taosx_core::plugins::sink::point::{csv::CsvParser, model::ModelType};
use taosx_core::utils::timeout::{Timeout, TimeoutType};
use taosx_core::{DataSetsReq, get_data_dir, list_datasets_from};
use taosx_core::{
    plugins::transform::parse::plugin::ParserPlugin,
    runners::pi::{
        parse_query_datasource_params,
        transform::{PIElementModelConfig, PIPointModelConfig},
    },
};
use taosx_utils::dsn::json_to_dsn;

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
#[allow(dead_code)] // keep this for swagger-ui
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

    tracing::info!("try to list datasets, req: {:?}", data);

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

/// Get datasource point options (temporary string response)
#[utoipa::path(
    tag = "data sources",
    request_body = DataSetsReq,
    responses(
        (status = 200, description = "Point options (temporary string)", body = String),
        (status = 500, description = "List point options error", body = Failed),
    ),
)]
#[post("/ds/in/point/options")]
pub(super) async fn get_point_options(
    controller: Data<TaskControllerRef>,
    data: Json<DataSetsReq>,
) -> impl Responder {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());
    let data = data.into_inner();
    match get_point_options_impl(controller, data).await {
        Ok(options) => Ok(HttpResponse::Ok()
            .content_type(ContentType::json())
            .json(&options)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

async fn get_point_options_impl(
    controller: Data<TaskControllerRef>,
    data: DataSetsReq,
) -> anyhow::Result<serde_json::Value> {
    tracing::info!("try to get kinghistorian point options, req: {:?}", data);

    // 解析 from DSN
    let mut from = match (&data.from_json, &data.from) {
        (Some(s), _) => json_to_dsn(s)?,
        (_, Some(s)) => s.into_dsn()?,
        _ => {
            anyhow::bail!("from dsn is required");
        }
    };
    // 为 from dsn 设置参数 only_groups=true
    from.params
        .insert("only_groups".to_string(), "true".to_string());
    // 构造新的请求，携带修改过的 from
    let req = DataSetsReq {
        from: Some(from.to_string()),
        from_json: None,
        categories: data.categories,
        via: data.via,
        offset: data.offset,
        pattern: data.pattern,
        limit: data.limit,
        lang: data.lang,
    };

    // 获取点位列表
    let result = if let Some(agent) = req.via {
        controller.list_datasets_via_agent(agent, req).await
    } else {
        list_datasets_from(&req).await
    };
    let datasets = result.context("Failed to list datasets when getting point options")?;

    // 将点位列表转换为 serde_json::Value
    let options = match from.driver.as_str() {
        "kinghist" => source_kinghistorian::to_point_options(datasets)?,
        _ => anyhow::bail!(
            "failed to get point options since: Unsupported driver: {}",
            from.driver
        ),
    };

    Ok(options)
}

#[derive(Deserialize, Debug, ToSchema, IntoParams)]
pub struct DsnAgentQuery {
    /// source dsn
    #[param(allow_reserved)]
    dsn: serde_json::Value,
    /// sink dsn
    to: Option<String>,
    /// request timeout
    timeout: Option<u64>,
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
pub async fn is_csv_valid(req: Json<DsnAgentQuery>) -> impl Responder {
    let query = req.into_inner();
    let timeout_sec = query.timeout.unwrap_or(Timeout::get(TimeoutType::Default));

    let result = timeout(Duration::from_secs(timeout_sec), is_csv_valid_impl(query)).await;

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

pub async fn is_csv_valid_impl(req: DsnAgentQuery) -> anyhow::Result<()> {
    // set current dir to DATA_DIR
    let _ = std::env::set_current_dir(get_data_dir());

    let from = json_to_dsn(&req.dsn)?;

    let driver = from.driver.to_lowercase();
    match driver.as_str() {
        "opcua" | "opcda" | "opc+ua" | "opc+da" => is_opc_csv_valid(from, req.to).await,
        source_kinghistorian::KING_HIST_ID => source_kinghistorian::is_csv_valid(&from).await,
        _ => Err(anyhow::anyhow!(
            "unsupported driver: {} for csv validation",
            driver
        ))?,
    }
}

async fn is_opc_csv_valid(from: Dsn, to: Option<String>) -> anyhow::Result<()> {
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
    if let (Some(model_type), Some(to)) = (ModelType::from_dsn(&from), to.as_ref()) {
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

        // Ensure parent directory exists before writing files (fixes "path not found" errors)
        if let Some(parent) = std::path::Path::new(&file_name).parent() {
            std::fs::create_dir_all(parent)
                .inspect_err(|err| {
                    tracing::error!("failed to create directory {:?}: {err:?}", parent)
                })
                .context("failed to create parent directory for PI default config file")?;
        }

        // 保存原始 json 数据
        let json_file = file_name.as_str().replace(".csv", ".json");
        std::fs::write(&json_file, pi_data)
            .inspect_err(|err| tracing::error!("{err:?}"))
            .context("failed to write PI query json file")?;
        // 保存配置文件
        std::fs::write(file_name.as_str(), config_data)
            .inspect_err(|err| tracing::error!("{err:?}"))
            .context("failed to write PI default config csv file")?;
    }

    return Ok(file_name);
}
