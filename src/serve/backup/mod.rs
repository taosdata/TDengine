use crate::serve::controller::TaskControllerRef;
use crate::serve::task::Failed;
use actix_web::web::{Data, Path};
use actix_web::{get, HttpResponse, Responder};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use utoipa::ToSchema;

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
    _task_store: Data<TaskControllerRef>,
) -> anyhow::Result<Option<Vec<BackupPoint>>> {
    // TODO
    let mut points = vec![];
    let now = Utc::now();
    for i in 0..10 {
        let p = BackupPoint {
            point: now + Duration::minutes(10 * i),
            vgroup_id: id,
            file_size: 1024 * 1024 * 30,
            file_count: 2,
        };
        points.push(p);
    }

    Ok(Some(points))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BackupPoint {
    /// 备份点
    point: DateTime<Utc>,
    /// vgroup id
    vgroup_id: i64,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_backup_point() {
        let p = BackupPoint {
            point: Utc::now(),
            vgroup_id: 1,
            file_size: 1024 * 1024 * 30,
            file_count: 2,
        };

        let json = serde_json::to_string(&p).unwrap();
        dbg!(json);
    }
}
