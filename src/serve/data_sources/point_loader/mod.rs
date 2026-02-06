use actix_files::NamedFile;
use actix_web::web::Json;
use actix_web::web::{Data, Query};
use anyhow::{anyhow, bail};
use csv::Reader;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, LazyLock};
use taosx_core::runners::opc::OpcType;
use taosx_utils::dsn::json_to_dsn;
use tempfile::TempPath;
use tokio::sync::RwLock;
use utoipa::*;

use crate::serve::controller::TaskControllerRef;

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DownloadAllPointsParams {
    from: Option<String>,
    from_json: Option<serde_json::Value>,
    via: Option<i64>,
    categories: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>, // preserve header order for dynamic rendering
}

impl<T> Pagination<T> {
    pub fn new(page: usize, page_size: usize) -> Self {
        Self {
            page,
            page_size,
            total: None,
            total_page: None,
            list: None,
            columns: None,
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

    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = Some(columns);
        self
    }
}

/// 同步下载所有数据点位
pub async fn download_all_point_csv_file(
    controller: Data<TaskControllerRef>,
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

    let (data, _) = controller
        .get_all_points(from, params.via, params.categories)
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
static SHARED_MAP: LazyLock<Arc<RwLock<HashMap<String, TaskStatus>>>> = LazyLock::new(|| {
    let map = HashMap::new();
    Arc::new(RwLock::new(map))
});

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

        match controller
            .get_all_points(from, params.via, params.categories)
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
                // Preserve the full error chain for better diagnostics in logs and HTTP responses
                let err_msg = format!("{:#}", err);
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

pub async fn load_point_data_page(
    params: &TaskTicket,
) -> anyhow::Result<Pagination<std::collections::HashMap<String, String>>> {
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

                // read header to build dynamic columns and map rows
                let headers = reader
                    .headers()
                    .map(|h| h.iter().map(|s| s.to_string()).collect::<Vec<String>>())
                    .unwrap_or_default();

                let data: Vec<std::collections::HashMap<String, String>> = reader
                    .records()
                    .skip(page * page_size)
                    .take(page_size)
                    .map(|record| {
                        let record = record.unwrap();
                        let mut row = std::collections::HashMap::new();
                        for (idx, key) in headers.iter().enumerate() {
                            let val = record.get(idx).unwrap_or("").to_string();
                            row.insert(key.clone(), val);
                        }
                        row
                    })
                    .collect();

                // return the page data, 1-based
                Ok(Pagination::new(page + 1, page_size)
                    .with_total(*point_count)
                    .with_columns(headers)
                    .with_list(data))
            }
            TaskStatus::Error(err) => Err(anyhow!("task error: {}", err)),
        })
        .unwrap_or(Err(anyhow!("task not found")))
}

pub async fn get_point_file_template(driver: &str, _lang: &str) -> anyhow::Result<NamedFile> {
    let template = match driver.to_lowercase().as_str() {
        "opcua" => source_opc::get_template(OpcType::OPCUA, true),
        "opcda" => source_opc::get_template(OpcType::OPCDA, true),
        source_kinghistorian::KING_HIST_ID => source_kinghistorian::get_template(),
        _ => bail!("unsupported driver: {}", driver),
    };

    let mut config_file = tempfile::NamedTempFile::new()?;
    tracing::debug!(
        "template file path: {}",
        &config_file.path().to_str().unwrap_or("")
    );
    write!(config_file, "{}", &template)?;
    Ok(NamedFile::open(config_file.path())?)
}
