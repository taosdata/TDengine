use crate::serve::controller::TaskControllerRef;
use crate::serve::task::Failed;
use actix_web::web::{Data, Path};
use actix_web::{get, HttpResponse, Responder};
use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde::Serialize;
use taos::IntoDsn;
use taosx_core::taoz::ZFile;
use taosx_core::tmq_to_local::conf::BackupConfig;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub struct BackupPoint {
    /// 备份点
    point: DateTime<Utc>,
    /// 文件大小
    #[serde(serialize_with = "serialize_bytes")]
    file_size: u64,
    /// 文件数量
    file_count: u64,
}

fn serialize_bytes<S>(size: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let human_readable = if *size >= 1 << 30 {
        format!("{:.2} GB", *size as f64 / (1 << 30) as f64)
    } else if *size >= 1 << 20 {
        format!("{:.2} MB", *size as f64 / (1 << 20) as f64)
    } else if *size >= 1 << 10 {
        format!("{:.2} KB", *size as f64 / (1 << 10) as f64)
    } else {
        format!("{} B", size)
    };
    serializer.serialize_str(&human_readable)
}

/// 历史备份，列出所有备份点
#[utoipa::path(tag = "backup")]
#[get("/backup/{id}/points")]
pub async fn get_backup_points(
    id: Path<i64>,
    task_store: Data<TaskControllerRef>,
) -> impl Responder {
    let id = id.into_inner();

    match get_backup_points_impl(id, task_store).await {
        Ok(Some(v)) => Ok(HttpResponse::Ok().json(v)),
        Ok(None) => Ok(HttpResponse::NotFound().finish()),
        Err(err) => Err(Failed::from_error(err)),
    }
}

async fn get_backup_points_impl(
    id: i64,
    task_store: Data<TaskControllerRef>,
) -> anyhow::Result<Option<Vec<BackupPoint>>> {
    let task = task_store.get(id).await?;
    if task.is_none() {
        return Ok(None);
    }

    let task = task.unwrap();
    let to = task.to.as_str().into_dsn().map_err(|err| {
        anyhow::Error::from(err).context(format!("failed to convert dsn: {}", &task.to))
    })?;

    let topic = task
        .oneshot_topic
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("oneshot topic not found, task_id: {}", id))?;

    let task_id = Some(id.to_string());
    let backup_dir = BackupConfig::parse_backup_dir(&to, &task_id)?;

    // 列出目录下的所有文件名
    let mut backup_files = vec![];
    let mut entries = tokio::fs::read_dir(backup_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() {
            let metadata = tokio::fs::metadata(&path).await?;
            let file_size = metadata.len();
            let file_name = path.file_name().unwrap().to_string_lossy();

            // 解析文件名: $TOPIC-$TIMESTAMP-$VG_ID-$INDEX.z
            if !file_name.starts_with(topic) {
                continue;
            }
            if let Ok((_, ts, _, _)) = ZFile::parse_file_name(file_name.as_ref()) {
                backup_files.push(BackupPoint {
                    point: ts,
                    file_size,
                    file_count: 1,
                });
            }
        }
    }

    // 按照 point 合并
    let points = backup_files
        .into_iter()
        .chunk_by(|p| p.point)
        .into_iter()
        .map(|(point, group)| {
            let (file_size, file_count) = group.fold((0, 0), |(size, count), p| {
                (size + p.file_size, count + p.file_count)
            });
            BackupPoint {
                point,
                file_size,
                file_count,
            }
        })
        .collect();

    Ok(Some(points))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_backup_point() {
        let p = BackupPoint {
            point: Utc::now(),
            file_size: 1024 * 1024 * 30,
            file_count: 2,
        };

        let json = serde_json::to_string(&p).unwrap();
        dbg!(json);
    }
}
