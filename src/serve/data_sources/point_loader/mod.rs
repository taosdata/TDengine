use actix_files::NamedFile;
use actix_web::web::Json;
use actix_web::web::{Data, Query};
use anyhow::anyhow;
use csv::Reader;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use taos::IntoDsn;
use taosx_core::utils::dsn::json_to_dsn;
use tempfile::TempPath;
use tokio::sync::RwLock;
use utoipa::*;

use taosx_core::runners::opc::{
    OpcType,
    csv::header::{DA_ROW, UA_ROW, get_template},
};
use taosx_core::{DataSetsReq, list_datasets_from};
use taosx_ipc::types::DataSet;

use crate::serve::TaskController;
use crate::serve::controller::TaskControllerRef;

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DownloadAllPointsParams {
    from: Option<String>,
    from_json: Option<serde_json::Value>,
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
    let from = if let Some(from_json) = params.from_json {
        let from = json_to_dsn(&from_json)?;
        from.to_string()
    } else if let Some(from) = params.from {
        from
    } else {
        return Err(anyhow!("from is required"));
    };

    let (data, _) = get_all_points(
        from,
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
    params: Json<DownloadAllPointsParams>,
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
        let from = if let Some(from_json) = params.from_json {
            match json_to_dsn(&from_json) {
                Ok(dsn) => dsn.to_string(),
                Err(_) => String::new(),
            }
        } else {
            params.from.unwrap_or_default()
        };

        match get_all_points(
            from,
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

pub async fn get_point_file_template(opc_type: &str, _lang: &str) -> anyhow::Result<NamedFile> {
    let opc_type = OpcType::try_from(opc_type)?;
    let template = get_template(opc_type, true);

    let mut config_file = tempfile::NamedTempFile::new()?;
    tracing::debug!(
        "template file path: {}",
        &config_file.path().to_str().unwrap_or("")
    );
    write!(config_file, "{}", &template)?;
    Ok(NamedFile::open(config_file.path())?)
}

fn get_safe_string_for_csv(s: &str) -> String {
    let mut safe_str = s.to_string();
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
    _lang: Option<String>,
) -> anyhow::Result<(String, usize)> {
    let mut from = from.into_dsn()?;

    let pattern = match from.driver.as_str() {
        "pi" | "pibackfill" => None,
        _ => Some(String::from(".*")),
    };
    let limit = usize::MAX / 2 - 1; // cause usize::MAX out of range i64 type when exec toml::to_string()

    match if let Some(agent) = via {
        controller
            .list_datasets_via_agent_v1(agent, &mut from, categories, via)
            .await
    } else {
        let data = DataSetsReq {
            from: Some(from.to_string()),
            from_json: None,
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
                    // header
                    let mut result = get_template(OpcType::OPCDA, false);
                    // rows
                    data.iter().enumerate().for_each(|(i, item)| {
                        let row = da_template_row(i + 1, item);
                        result.push_str(row.as_str());
                    });

                    result
                }
                "opcua" => {
                    // header
                    let mut result = get_template(OpcType::OPCUA, false);
                    // rows
                    data.iter().enumerate().for_each(|(i, item)| {
                        let row = ua_template_row(i + 1, item);
                        result.push_str(row.as_str());
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

// 替换 UA_ROW 的前三个字段和最后一个字段
fn ua_template_row(row_idx: usize, item: &DataSet) -> String {
    let mut cols = vec![];
    for (idx, col) in UA_ROW.iter().enumerate() {
        if idx == 0 {
            // No.
            cols.push(row_idx.to_string());
        } else if idx == 1 {
            // point_id
            let point_id = get_safe_string_for_csv(&item.id);
            cols.push(point_id.clone());
        } else if idx == 2 {
            // enabled
            let enabled = get_enabled(item.clone());
            cols.push(enabled.to_string());
        } else if idx == (UA_ROW.len() - 1) {
            // tag::VARCHAR(255)::name
            let safe_name = get_safe_string_for_csv(&(item.name.clone().unwrap_or("".to_string())));
            cols.push(safe_name.clone());
        } else {
            cols.push(col.to_string());
        }
    }
    format!("{}\n", cols.join(","))
}

fn da_template_row(row_idx: usize, item: &DataSet) -> String {
    // 替换 DA_ROW 的前三个字段和最后一个字段
    let mut cols = vec![];
    for (idx, col) in DA_ROW.iter().enumerate() {
        if idx == 0 {
            // No.
            cols.push(row_idx.to_string());
        } else if idx == 1 {
            // tag_name
            cols.push(item.id.clone());
        } else if idx == 2 {
            // enabled
            let enabled = get_enabled(item.clone());
            cols.push(enabled.to_string());
        } else if idx == (DA_ROW.len() - 1) {
            // tag::VARCHAR(255)::name
            cols.push(item.name.clone().unwrap_or("".to_string()));
        } else {
            cols.push(col.to_string());
        }
    }
    format!("{}\n", cols.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use taosx_ipc::types::DataSet;

    #[test]
    fn test_ua_template_row() {
        let item = DataSet {
            id: "ns=3;i=1001".to_string(),
            name: Some("tag1".to_string()),
            category: None,
            r#type: None,
            options: None,
            format: None,
        };
        let row = ua_template_row(1, &item);
        assert_eq!(
            row,
            "1,ns=3;i=1001,1,opc_{type},t_{ns}_{id},val,,,quality,ts,,qts,,rts,,tag1\n".to_string()
        );
    }

    #[test]
    fn test_da_template_row() {
        let item = DataSet {
            id: "/ASSETS/AB/EDCGQ".to_string(),
            name: Some("tag1".to_string()),
            category: None,
            r#type: None,
            options: None,
            format: None,
        };
        let row = da_template_row(1, &item);
        assert_eq!(
            row,
            "1,/ASSETS/AB/EDCGQ,1,opc_{type},t_{tag_name},val,,,quality,ts,,qts,,rts,,tag1\n"
                .to_string()
        );
    }
}
