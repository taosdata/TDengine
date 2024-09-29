use actix_files::NamedFile;
use actix_web::web::{Data, Query};
use anyhow::anyhow;
use csv::Reader;
use itertools::Itertools;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use taos::IntoDsn;
use tempfile::TempPath;
use tokio::sync::RwLock;
use utoipa::*;

use taosx_core::{list_datasets_from, DataSetsReq};
use taosx_ipc::types::DataSet;

use crate::serve::controller::TaskControllerRef;
use crate::serve::TaskController;

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DownloadAllPointsParams {
    from: String,
    via: Option<i64>,
    categories: String,
    lang: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct TaskTicket {
    pub code: Option<i32>,
    pub ticket: String,
    pub complete: Option<bool>,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

impl TaskTicket {
    pub fn new_task(ticket: String) -> Self {
        Self {
            code: Some(0),
            ticket,
            complete: None,
            page: None,
            page_size: None,
        }
    }

    pub fn complete(ticket: String, ready: bool) -> Self {
        Self {
            code: Some(0),
            ticket,
            complete: Some(ready),
            page: None,
            page_size: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct R<T> {
    pub code: i32,
    pub data: Option<T>,
    pub msg: Option<String>,
}

impl<T> R<T> {
    pub fn success(data: T) -> Self {
        Self {
            code: 0,
            data: Some(data),
            msg: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Pagination<T> {
    pub page: usize,
    pub page_size: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_page: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub list: Option<Vec<T>>,
}

impl<T> Pagination<T> {
    pub fn new(page: usize, page_size: usize) -> Self {
        Self {
            page,
            page_size,
            total: None,
            total_page: None,
            list: None,
        }
    }

    pub fn with_total(mut self, total: usize) -> Self {
        self.total = Some(total);
        self.total_page = Some((total as f64 / self.page_size as f64).ceil() as usize);
        self
    }

    pub fn with_list(mut self, list: Vec<T>) -> Self {
        self.list = Some(list);
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OpcPoint {
    pub id: String,
    pub name: Option<String>,
    pub enabled: Option<i8>,
}

/// 同步下载所有数据点位
pub async fn download_all_point_csv_file(
    controller: Data<TaskControllerRef>,
    // data: Query<DataSetsReq>,
    params: Query<DownloadAllPointsParams>,
) -> anyhow::Result<NamedFile> {
    let params = params.into_inner();
    let (data, _) = get_all_points(
        params.from,
        params.via,
        params.categories,
        controller.into_inner().as_ref(),
        params.lang,
    )
    .await?;

    let mut config_file = tempfile::NamedTempFile::new()?;
    tracing::debug!(
        "temp file path: {}",
        &config_file.path().to_str().unwrap_or("")
    );
    write!(config_file, "{}", &data)?;
    Ok(NamedFile::open(config_file.path())?)
}

enum TaskStatus {
    Running,
    Complete((TempPath, usize)),
    Error(String),
}

// Define a static shared hashmap， task_id -> task_status
lazy_static! {
    static ref SHARED_MAP: Arc<RwLock<HashMap<String, TaskStatus>>> = {
        let map = HashMap::new();
        Arc::new(RwLock::new(map))
    };
}

// 异步下载数据点位，会将当前任务id返回给前端
pub async fn arrange_point_file_download_task(
    controller: Data<TaskControllerRef>,
    // data: Query<DataSetsReq>,
    params: Query<DownloadAllPointsParams>,
) -> anyhow::Result<String> {
    let params = params.into_inner();
    let task_id = uuid::Uuid::new_v4().to_string();
    let task_id_to_return = task_id.clone();
    {
        let mut map = SHARED_MAP.write().await;
        map.insert(task_id.clone(), TaskStatus::Running);
    }

    tokio::spawn(async move {
        tracing::debug!("start async download task: {}", &task_id);

        match get_all_points(
            params.from,
            params.via,
            params.categories,
            controller.into_inner().as_ref(),
            params.lang,
        )
        .await
        {
            Ok((data, point_count)) => {
                let mut config_file = tempfile::NamedTempFile::new().unwrap();
                tracing::debug!(
                    "temp file path: {}",
                    &config_file.path().to_str().unwrap_or("")
                );
                write!(config_file, "{}", &data).unwrap();
                {
                    let mut map = SHARED_MAP.write().await;
                    map.insert(
                        task_id,
                        TaskStatus::Complete((config_file.into_temp_path(), point_count)),
                    );
                }
            }
            Err(err) => {
                let mut map = SHARED_MAP.write().await;
                let err_msg = err.to_string();
                let err_msg = if let Some(idx) = err_msg.find("\n") {
                    err_msg[..idx].to_string()
                } else {
                    err_msg
                };
                map.insert(task_id, TaskStatus::Error(err_msg));
            }
        }
    });

    Ok(task_id_to_return)
}

// 异步下载数据点位，根据 task_id 检查任务是否执行完毕
pub async fn check_task_complete(ticket: String) -> anyhow::Result<bool> {
    let map = SHARED_MAP.read().await;
    map.get(&ticket)
        .map(|status| match status {
            TaskStatus::Running => Ok(false),
            TaskStatus::Complete(_) => Ok(true),
            TaskStatus::Error(_) => Ok(true),
        })
        .unwrap_or(Err(anyhow!("task not found")))
}

// 下载生成好的数据点位文件
pub async fn load_point_file(ticket: &String, remain: bool) -> anyhow::Result<NamedFile> {
    if remain {
        let map = SHARED_MAP.read().await;
        map.get(ticket)
            .map(|status| match status {
                TaskStatus::Running => Err(anyhow!("task is running")),
                TaskStatus::Complete((file_path, _)) => Ok(NamedFile::open(file_path)?),
                TaskStatus::Error(err) => Err(anyhow!("task error: {}", err)),
            })
            .unwrap_or(Err(anyhow!("task not found")))
    } else {
        let mut map = SHARED_MAP.write().await;
        map.remove(ticket)
            .map(|status| match status {
                TaskStatus::Running => Err(anyhow!("task is running")),
                TaskStatus::Complete((file_path, _)) => Ok(NamedFile::open(file_path)?),
                TaskStatus::Error(err) => Err(anyhow!("task error: {}", err)),
            })
            .unwrap_or(Err(anyhow!("task not found")))
    }
}

pub async fn load_point_data_page(params: &TaskTicket) -> anyhow::Result<Pagination<OpcPoint>> {
    let map = SHARED_MAP.write().await;
    // adjust page to 0-based
    let page = params.page.unwrap_or(1) - 1;
    let page_size = params.page_size.unwrap_or(1000);

    map.get(params.ticket.as_str())
        .map(|status| match status {
            TaskStatus::Running => Err(anyhow!("task is running")),
            TaskStatus::Complete((file_path, point_count)) => {
                let f = NamedFile::open(file_path)?;
                let mut reader = Reader::from_reader(f.file());

                // skip +1， is the header
                let data: Vec<OpcPoint> = reader
                    .records()
                    .skip(page * page_size)
                    .take(page_size)
                    .map(|record| {
                        let record = record.unwrap();
                        let id = record.get(1).unwrap_or("").to_string();
                        let enabled = record.get(2).unwrap_or("1").parse::<i8>().unwrap_or(1);
                        let name = record.get(13).unwrap_or("").to_string();

                        OpcPoint {
                            id,
                            name: Some(name),
                            enabled: Some(enabled),
                        }
                    })
                    .collect();

                // return the page data, 1-based
                Ok(Pagination::new(page + 1, page_size)
                    .with_total(*point_count)
                    .with_list(data))
            }
            TaskStatus::Error(err) => Err(anyhow!("task error: {}", err)),
        })
        .unwrap_or(Err(anyhow!("task not found")))
}

pub async fn get_point_file_template(driver: &str, lang: &str) -> anyhow::Result<NamedFile> {
    let template_file_data = match driver {
        "opcua" => get_opcua_csv_header(lang, true),
        "opcda" => get_opcda_csv_header(lang, true),
        _ => String::new(),
    };

    let mut config_file = tempfile::NamedTempFile::new()?;
    tracing::debug!(
        "template file path: {}",
        &config_file.path().to_str().unwrap_or("")
    );
    write!(config_file, "{}", &template_file_data)?;
    Ok(NamedFile::open(config_file.path())?)
}

fn get_safe_string_for_csv(s: &String) -> String {
    let mut safe_str = s.clone();
    if safe_str.contains(",") {
        safe_str = format!("\"{}\"", safe_str.replace("\"", "\"\""));
    }
    safe_str
}

async fn get_all_points(
    from: String,
    via: Option<i64>,
    categories: String,
    controller: &TaskController,
    lang: Option<String>,
) -> anyhow::Result<(String, usize)> {
    let mut from = from.into_dsn()?;

    let pattern;
    match from.driver.as_str() {
        "pi" | "pibackfill" => {
            pattern = None;
        }
        _ => {
            pattern = Some(String::from(".*"));
        }
    }
    let limit = usize::MAX / 2 - 1; // cause usize::MAX out of range i64 type when exec toml::to_string()
    let lang = lang.unwrap_or("zh".to_string());

    match if let Some(agent) = via {
        controller
            .list_datasets_via_agent_v1(agent, &mut from, categories, via)
            .await
    } else {
        let data = DataSetsReq {
            from: from.to_string(),
            categories: vec![categories],
            via,
            offset: 0,
            pattern,
            limit,
            lang: None,
        };
        list_datasets_from(&data).await
    } {
        Ok(data) => {
            let point_count = data.len();
            let data = match from.driver.as_str() {
                "opcda" => {
                    let mut result = get_opcda_csv_header(&lang, false);

                    let mut i = 1;
                    data.iter().for_each(|item| {
                        let enabled = get_enabled(item.clone());
                        let point_item = format!(
                            "{},{},{},opc_{{type}},t_{{tag_name}},val,,,quality,ts,rts,,,{}\n",
                            i,
                            item.id,
                            enabled,
                            item.name.clone().unwrap_or("".to_string())
                        );
                        result.push_str(point_item.as_str());
                        i += 1;
                    });

                    result
                }
                "opcua" => {
                    let mut result = get_opcua_csv_header(&lang, false);
                    let mut i = 1;

                    data.iter().for_each(|item| {
                        // 转义id 和 name, 使其可以安全的包含在csv文件中
                        let safe_id = get_safe_string_for_csv(&item.id);
                        let safe_name =
                            get_safe_string_for_csv(&(item.name.clone().unwrap_or("".to_string())));
                        let enabled = get_enabled(item.clone());
                        let point_item = format!(
                            "{},{},{},opc_{{type}},t_{{ns}}_{{id}},val,,,quality,ts,rts,,,{}\n",
                            i, safe_id, enabled, safe_name
                        );
                        result.push_str(point_item.as_str());
                        i += 1;
                    });

                    result
                }
                _ => unimplemented!(),
            };
            Ok((data, point_count))
        }
        Err(err) => Err(err),
    }
}

fn get_enabled(item: DataSet) -> i8 {
    item.options
        .map(|o| {
            if o.is_empty() {
                return 1;
            }
            o.iter()
                .find(|o| o.name == "enabled")
                .map(|o| {
                    if o.display == "0" {
                        return 0;
                    }
                    1
                })
                .unwrap_or(1)
        })
        .unwrap_or(1)
}

fn get_opcua_csv_header(_lang: &str, demo: bool) -> String {
    // let header = match lang {
    //     "zh" => "\u{FEFF}序号,数据点位id(必填),\"是否启用(1 - 启动, 0 - 停用。配置为0, 将删除数据点位对应的子表)\",超级表名,子表名(必填),采集值列名,采集值转换规则(可留空),\"采集值类型(可留空,默认根据实际类型自动填充,可选值有int, double, float, varchar)\",数据质量列名,OPC原始时间列名(默认作为时间戳主键),\"TD 服务端接收时间列名(使用本列作为时间戳主键,请剪切到 ts_col 之前)\", ts_col 的时间戳转换规则(可留空), received_ts_col 的时间戳转换规则(可留空),\" 标签列(不需要可删除,需要多个,可以在右侧添加新列,可指定列名和类型）\"\n",
    //     _ => "\u{FEFF}S/N,OPC Point Id (Required),\"Enable point?(1-Enable,0-Disable.if set to 0, will delete the sub table)\",Stable Name,sub table name(Required),value column name,value transform rule(Can be empty),\"value data type(Can be empty, candidate values:int, float, double, varchar)\",Quality Column Name,OPC original time column name(default to be the primary key),\"TDengine received time column name (if you want to use this column as the primary key, move it to the left of ts_col.)\",ts_col transform rule(Can be empty),receive_ts_col transform rule(Can be empty),\"Tag column(if need more, add new column to the right)\"\n",
    // };

    let columns = vec![
        "No.",
        "point_id",
        "enabled",
        "stable",
        "tbname",
        "value_col",
        "value_transform",
        "type",
        "quality_col",
        "ts_col",
        "received_ts_col",
        "ts_transform",
        "received_ts_transform",
        "tag::VARCHAR(200)::name",
    ];
    let mut header = columns.iter().join(",");
    header.push('\n');

    if demo {
        header.push_str("1,ns=3;i=1010,1,opc_{type},t_{ns}_{id},val,val * 1.8 + 32,double,quality,ts,rts,,,temperature\n");
        header.push_str("2,ns=3;i=1011,1,opc_{type},t_{ns}_{id},val,val + 10,int,quality,ts,rts,ts + 8 * 3600 * 1000,rts + 8 * 3600 * 1000,pressure\n");
        header.push_str("3,ns=5;s=hw202401250013,1,opc_{type},t_{ns}_{id},val,,,quality,ts,rts,ts - 6 * 1000,rts - 6 * 1000,current\n");
    }

    header
}

fn get_opcda_csv_header(_lang: &str, demo: bool) -> String {
    // let header = match lang {
    //     "zh" => "\u{FEFF}序号,数据点位 TagName(必填),\"是否启用(1 - 启动, 0 - 停用。配置为0, 将删除数据点位对应的子表)\",超级表名,子表名(必填),采集值列名,采集值转换规则(可留空),\"采集值类型(可留空,默认根据实际类型自动填充,可选值有int, double, float, varchar)\",数据质量列名,OPC原始时间列名(默认作为时间戳主键),\"TD 服务端接收时间列名(使用本列作为时间戳主键,请剪切到 ts_col 之前)\", ts_col 的时间戳转换规则(可留空), received_ts_col 的时间戳转换规则(可留空),\" 标签列(不需要可删除,需要多个,可以在右侧添加新列,可指定列名和类型）\"\n",
    //     _ => "\u{FEFF}S/N,OPC Point TagName (Required),\"Enable point?(1-Enable,0-Disable.if set to 0, will delete the sub table)\",Stable Name,sub table name(Required),value column name,value transform rule(Can be empty),\"value data type(Can be empty, candidate values:int, float, double, varchar)\",Quality Column Name,OPC original time column name(default to be the primary key),\"TDengine received time column name (if you want to use this column as the primary key, move it to the left of ts_col.)\",ts_col transform rule(Can be empty),receive_ts_col transform rule(Can be empty),\"Tag column(if need more, add new column to the right)\"\n",
    // };

    let columns = vec![
        "No.",
        "tag_name",
        "enabled",
        "stable",
        "tbname",
        "value_col",
        "value_transform",
        "type",
        "quality_col",
        "ts_col",
        "received_ts_col",
        "ts_transform",
        "received_ts_transform",
        "tag::VARCHAR(200)::name",
    ];
    let mut header = columns.iter().join(",");
    header.push('\n');

    if demo {
        header.push_str("1,root.parent.tempeture,1,opc_{type},t_{tag_name},val,val * 1.8 + 32,float,quality,ts,rts,,,temperature\n");
        header.push_str("2,root.parent.pressure,1,opc_{type},t_{tag_name},val,val + 10,,quality,ts,rts,ts + 8*3600*1000,rts + 8*3600*1000,pressure\n");
        header.push_str("3,root.parent.current,1,opc_{type},t_{tag_name},val,,,quality,ts,rts,ts - 6*1000,rts - 6*1000,current\n");
    }

    header
}
