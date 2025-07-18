use crate::serve::controller::TaskControllerRef;
use crate::serve::task::Failed;
use actix_web::web::{Data, Path};
use actix_web::{HttpResponse, Responder, get};
use anyhow::Context;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use opendal::EntryMode;
use serde::Serialize;
use taos::IntoDsn;
use taosx_core::s3::{S3_ENABLE, S3Config, S3Loader};
use taosx_core::taoz::ZFile;
use taosx_core::tmq::BackupObject;
use taosx_core::utils;
use tmq_to_local::conf::BackupConfig;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub struct BackupPoint {
    #[serde(flatten)]
    /// 备份对象
    backup_obj: BackupObject,
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
        Ok(v) => Ok(HttpResponse::Ok().json(v)),
        Err(err) => {
            tracing::error!("failed to get backup points: {:?}", err);
            Err(Failed::from_error(err))
        }
    }
}

/// 列出备份目录下的所有备份点
async fn get_backup_points_impl(
    id: i64,
    task_store: Data<TaskControllerRef>,
) -> anyhow::Result<Vec<BackupPoint>> {
    let task = task_store
        .get(id)
        .await?
        .ok_or(anyhow::anyhow!("task not found, id: {}", id))?;
    let from = task.from.as_str().into_dsn().map_err(|err| {
        anyhow::Error::from(err).context(format!("failed to convert dsn: {}", &task.from))
    })?;
    let to = task
        .to
        .as_str()
        .into_dsn()
        .context(format!("failed to convert dsn: {}", &task.to))?;
    let task_id = id.to_string();
    let topic = task
        .oneshot_topic
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("oneshot topic not found, task_id: {}", id))?;

    let backup_dir = BackupConfig::parse_backup_dir(&to, Some(task_id.as_str()))?;
    // 如果目录不存在，返回空列表，不报错，因为可能是备份计划还没有执行
    if tokio::fs::metadata(&backup_dir).await.is_err() {
        tracing::warn!("backup dir not found: {:?}", backup_dir);
        return Ok(vec![]);
    }

    let mut backup_files = vec![];
    // 如果是 S3 备份，列出 S3 上的所有文件
    if let Ok(Some(true)) = utils::parse_key_in_dsn::<bool>(&to, S3_ENABLE) {
        let s3_config = S3Config::from_dsn(&to)?;
        let loader = S3Loader::try_from(&s3_config).await?;
        let files = loader.list().await?;
        for f in files {
            let meta = f.metadata();
            match meta.mode() {
                EntryMode::FILE => {
                    let file_size = meta.content_length();
                    let file_name = f.name();
                    // 解析文件名: $TOPIC-$TIMESTAMP-$VG_ID-$INDEX.z
                    if !file_name.starts_with(topic) {
                        continue;
                    }
                    if let Ok((_, ts, _, _)) = ZFile::parse_file_name(file_name) {
                        backup_files.push((ts, file_size, 1));
                    }
                }
                EntryMode::DIR | EntryMode::Unknown => {
                    continue;
                }
            }
        }
    }

    // 列出目录下的所有文件名
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
                backup_files.push((ts, file_size, 1));
            }
        }
    }

    // backup_files 中去重
    backup_files = backup_files.into_iter().unique().collect();

    let mut backup_obj = BackupObject::try_from_taos(&from)
        .await
        .context(format!(
            "failed to get backup object from taos, from: {}",
            &from
        ))?
        .ok_or(anyhow::anyhow!("backup obj not found in dsn: {}", &from))?;
    backup_obj.task_id = Some(task_id.clone());
    backup_obj.topic = Some(topic.clone());

    Ok(group_by_point(backup_files, &backup_obj))
}

/// 备份文件按照备份点的时间戳分组，计算分组的文件大小和文件数量
fn group_by_point(
    files: Vec<(DateTime<Utc>, u64, u64)>,
    backup_obj: &BackupObject,
) -> Vec<BackupPoint> {
    files
        .into_iter()
        .sorted_by(|(a, _, _), (b, _, _)| b.cmp(a))
        .chunk_by(|p| p.0)
        .into_iter()
        .map(|(ts, group)| {
            let (file_size, file_count) =
                group.fold((0, 0), |(size, count), p| (size + p.1, count + p.2));
            BackupPoint {
                backup_obj: backup_obj.clone(),
                point: ts,
                file_size,
                file_count,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_group_by_point() {
        let files = vec![
            (DateTime::<Utc>::from_timestamp(1738995480, 0), 126532449, 1),
            (DateTime::<Utc>::from_timestamp(1738997280, 0), 7749230, 1),
            (DateTime::<Utc>::from_timestamp(1738995480, 0), 44466371, 1),
            (DateTime::<Utc>::from_timestamp(1738999080, 0), 880408, 1),
            (DateTime::<Utc>::from_timestamp(1738997280, 0), 292196, 1),
            (DateTime::<Utc>::from_timestamp(1738999080, 0), 293548, 1),
        ]
        .into_iter()
        .map(|(s, size, count)| (s.unwrap(), size, count))
        .collect_vec();

        let backup_obj = BackupObject {
            task_id: Some("82".to_string()),
            topic: Some("abc".to_string()),
            db_name: Some("abc".to_string()),
            db_sql: Some("abc".to_string()),
            stable_name: None,
            stable_sql: None,
        };

        let points = group_by_point(files, &backup_obj);
        dbg!(&points);
        assert_eq!(3, points.len());

        assert_eq!("2025-02-08T07:18:00+00:00", points[0].point.to_rfc3339());
        assert_eq!(880408 + 293548, points[0].file_size);
        assert_eq!(2, points[0].file_count);

        assert_eq!("2025-02-08T06:48:00+00:00", points[1].point.to_rfc3339());
        assert_eq!(7749230 + 292196, points[1].file_size);
        assert_eq!(2, points[1].file_count);

        assert_eq!("2025-02-08T06:18:00+00:00", points[2].point.to_rfc3339());
        assert_eq!(126532449 + 44466371, points[2].file_size);
        assert_eq!(2, points[2].file_count);
    }

    #[test]
    fn test_serialize_backup_point() {
        let p = BackupPoint {
            backup_obj: BackupObject {
                task_id: None,
                topic: Some("abc".to_string()),
                db_name: Some("abc".to_string()),
                db_sql: Some("abc".to_string()),
                stable_name: None,
                stable_sql: None,
            },
            point: DateTime::parse_from_rfc3339("2021-08-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            file_size: 1024 * 1024 * 30,
            file_count: 2,
        };

        let json = serde_json::to_value(&p).unwrap();
        let expect = json!({
            "topic": "abc",
            "db_name": "abc",
            "db_sql": "abc",
            "point": "2021-08-01T00:00:00Z",
            "file_size": "30.00 MB",
            "file_count": 2,
        });
        assert_eq!(json, expect);
    }
}
