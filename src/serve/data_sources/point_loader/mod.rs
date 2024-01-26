use actix_files::NamedFile;
use actix_web::web::{Data, Query};
use anyhow::anyhow;
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

use crate::serve::controller::TaskControllerRef;
use crate::serve::TaskController;

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DownloadAllPointsParams {
    from: String,
    via: Option<i64>,
    categories: String,
}

#[derive(Serialize, Deserialize)]
pub struct TaskTicket {
    pub code: Option<i32>,
    pub ticket: String,
    pub complete: Option<bool>,
}

impl TaskTicket {
    pub fn new_task(ticket: String) -> Self {
        Self {
            code: Some(0),
            ticket: ticket,
            complete: None,
        }
    }

    pub fn complete(ticket: String, ready: bool) -> Self {
        Self {
            code: Some(0),
            ticket: ticket,
            complete: Some(ready),
        }
    }
}

// 同步下载所有数据点位
pub async fn download_all_point_csv_file(
    controller: Data<TaskControllerRef>,
    // data: Query<DataSetsReq>,
    params: Query<DownloadAllPointsParams>,
) -> anyhow::Result<NamedFile> {
    let params = params.into_inner();
    let data = get_all_points(
        params.from,
        params.via,
        params.categories,
        controller.into_inner().as_ref(),
    )
    .await?;

    let mut config_file = tempfile::NamedTempFile::new()?;
    tracing::debug!(
        "temp file path: {}",
        &config_file.path().to_str().unwrap_or("")
    );
    write!(config_file, "{}", &data)?;
    Ok(NamedFile::open(config_file.path().to_path_buf())?)
}

enum TaskStatus {
    Running,
    Complete(TempPath),
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

    tokio::spawn(async move {
        let data = get_all_points(
            params.from,
            params.via,
            params.categories,
            controller.into_inner().as_ref(),
        )
        .await
        .unwrap_or_default();

        let mut config_file = tempfile::NamedTempFile::new().unwrap();
        tracing::debug!(
            "temp file path: {}",
            &config_file.path().to_str().unwrap_or("")
        );
        write!(config_file, "{}", &data).unwrap();
        {
            let mut map = SHARED_MAP.write().await;
            map.insert(task_id, TaskStatus::Complete(config_file.into_temp_path()));
        }
    });

    {
        let mut map = SHARED_MAP.write().await;
        map.insert(task_id_to_return.clone(), TaskStatus::Running);
    }

    Ok(task_id_to_return)
}

// 异步下载数据点位，根据 task_id 检查任务是否执行完毕
pub async fn check_task_complete(ticket: String) -> anyhow::Result<bool> {
    let map = SHARED_MAP.read().await;
    map.get(&ticket)
        .map(|status| match status {
            TaskStatus::Running => Ok(false),
            TaskStatus::Complete(_) => Ok(true),
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
                TaskStatus::Complete(file_path) => Ok(NamedFile::open(file_path)?),
            })
            .unwrap_or(Err(anyhow!("task not found")))
    } else {
        let mut map = SHARED_MAP.write().await;
        map.remove(ticket)
            .map(|status| match status {
                TaskStatus::Running => Err(anyhow!("task is running")),
                TaskStatus::Complete(file_path) => Ok(NamedFile::open(file_path)?),
            })
            .unwrap_or(Err(anyhow!("task not found")))
    }
}

async fn get_all_points(
    from: String,
    via: Option<i64>,
    categories: String,
    controller: &TaskController,
) -> anyhow::Result<String> {
    let from = from.into_dsn()?;
    let pattern;
    match from.driver.as_str() {
        "pi" | "pibackfill" => {
            pattern = Some(String::from("*"));
        }
        _ => {
            pattern = Some(String::from(".*"));
        }
    }
    let limit = usize::MAX / 2 - 1; // cause usize::MAX out of range i64 type when exec toml::to_string()
    let data = DataSetsReq {
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
                        data.into_iter()
                            .map(|set| format!("Meter_{{ns}}_{{id}},{}", set.id))
                            .join("\n")
                    } else {
                        data.into_iter()
                            .map(|set| format!("Meter_{{TagName}},{}", set.id))
                            .join("\n")
                    };
                    result.push_str(data.as_str());
                    result
                }
                _ => unimplemented!(),
            };
            Ok(data)
        }
        Err(err) => Err(err),
    }
}
