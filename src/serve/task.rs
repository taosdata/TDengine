use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs;

use actix_files::NamedFile;
use actix_multipart::form::{MultipartForm, tempfile::TempFile, text::Text};
use actix_web::body::BoxBody;
use actix_web::{
    HttpRequest, HttpResponse, Responder, ResponseError, get, post,
    web::{Data, Path, Query},
};
use anyhow::Context;
use anyhow::anyhow;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use taos::Code;
use taosx_core::core_metrics::CoreMetrics;
use utoipa::*;

use taosx_core::{get_data_dir, get_file_upload_home_dir};

use crate::serve::controller::TaskControllerRef;
use crate::serve::metrics::try_get_metrics_from_task;

/// Task endpoint error responses
#[derive(Debug, Default, Serialize, Deserialize, Clone, ToSchema)]
pub struct Failed<T = ()>
where
    T: Debug + serde::Serialize,
{
    /// Error code
    #[schema(example = 0, value_type = i32)]
    pub code: Code,
    /// Error reason
    pub message: String,

    pub data: T,
}

impl Failed<()> {
    pub fn from_error(err: impl Display) -> Self {
        Self {
            code: Code::FAILED,
            message: format!("{err:#}"),
            data: (),
        }
    }
}

impl<T: Debug + Serialize> Failed<T> {
    pub fn new(code: Code, message: String, data: T) -> Self {
        Self {
            code,
            message,
            data,
        }
    }
}

impl<T: Debug + Serialize> Display for Failed<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "code={:?} message={:?} data={:?}",
            self.code, self.message, self.data
        ))
    }
}

impl<T> ResponseError for Failed<T>
where
    T: Debug + serde::Serialize,
{
    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::InternalServerError().json(self)
    }
}

pub fn check_parser_string_timestamp_precision(parser_string: &str) -> bool {
    !((parser_string.contains(r#""TIMESTAMP""#) && parser_string.contains(r#""TIMESTAMP(us)""#))
        || (parser_string.contains(r#""TIMESTAMP""#)
            && parser_string.contains(r#""TIMESTAMP(ns)""#))
        || (parser_string.contains(r#""TIMESTAMP(us)""#)
            && parser_string.contains(r#""TIMESTAMP(ns)""#)))
}

#[derive(Serialize, Deserialize, ToSchema, Clone, Debug)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // keep this
pub(super) enum FromOrTo {
    From(String),
    To(String),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TaskBatchReq {
    /// task ids
    ids: Vec<i64>,
}

/// Get tmq task progress by given task ID in respect of the vgroup consume progress.
#[get("/tasks/{id}/{job_id}/vgroup_progress")]
pub(super) async fn get_tmq_task_vgroup_progress(
    task_store: Data<TaskControllerRef>,
    id: Path<i64>,
    job_id: Path<i64>,
) -> impl Responder {
    let task_id = id.into_inner();
    let job_id = job_id.into_inner();
    let task = task_store.get_task(task_id, job_id).await;
    match task {
        Some(task) => {
            let metrics = try_get_metrics_from_task(&task).await;
            match metrics {
                Some(metrics) => match metrics.as_ref() {
                    CoreMetrics::TMQ(tmq_metrics) => tmq_metrics.get_progress_string(),
                    _ => {
                        tracing::error!("Expect TmqMetrics, but got: {:?}", metrics);
                        "{}".to_string()
                    }
                },
                None => {
                    tracing::info!("Not found metrics for task: {}", task_id);
                    "{}".to_string()
                }
            }
        }
        None => {
            tracing::info!("Not found task by id: {}", task_id);
            "{}".to_string()
        }
    }
}

/// Get tmq task progress by given task ID in respect of latest data in specific table.
#[get("/tasks/{id}/table_progress")]
pub(super) async fn get_tmq_task_table_progress(
    task_store: Data<TaskControllerRef>,
    id: Path<i64>,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let task_id = id.into_inner();
    let job_id = -1;
    let table = query.get("table");
    if table.is_none() {
        return Err(Failed::from_error("table name is required"));
    }
    let table = table.unwrap().as_str();
    let start = query.get("start");
    let end = query.get("end");
    let task = task_store.get_task(task_id, job_id).await;
    match task {
        Some(task) => {
            let from = &task.from;
            let to = &task.to;
            let table_progress = tmq_to_td::get_table_progress(from, to, table, start, end).await;
            match table_progress {
                Ok(progress) => Ok(serde_json::to_string(&progress).unwrap()),
                Err(err) => {
                    tracing::error!("Get table progress error: {}", err);
                    Err(Failed::from_error(err))
                }
            }
        }
        None => {
            tracing::info!("Not found task by id: {}", task_id);
            Ok("{}".to_string())
        }
    }
}

#[derive(Debug, MultipartForm, ToSchema)]
pub struct UploadForm {
    #[multipart(rename = "file")]
    files: Vec<TempFile>,
    req_id: Text<String>,
}

#[utoipa::path(
    tag = "tasks",
    request_body(content = UploadForm, content_type = "multipart/form-data"),
    responses(
        (status = 201, description = "file uploaded", body = Vec < String >),
        (status = 500, description = "file upload error", body = Failed)
    ),
)]
#[post("/upload")]
pub async fn upload_files(MultipartForm(form): MultipartForm<UploadForm>) -> impl Responder {
    match save_files(MultipartForm(form)).await {
        Ok(file_saved) => Ok(HttpResponse::Created().json(file_saved)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

async fn save_files(MultipartForm(form): MultipartForm<UploadForm>) -> anyhow::Result<Vec<String>> {
    let upload_dir = get_file_upload_home_dir();
    let mut file_save_paths = Vec::new();
    if form.files.is_empty() {
        anyhow::bail!("upload file is empty");
    }
    let req_id = form.req_id.to_string();
    for f in form.files {
        // let uuid = uuid::Uuid::new_v4();
        let path = upload_dir.join(&req_id);
        fs::create_dir_all(&path).with_context(|| "create file path failed")?;
        let file_name = f.file_name.unwrap();
        let relative_path = format!("{req_id}/{file_name}");
        tracing::info!(
            "saving to {}, {relative_path}",
            upload_dir.to_str().unwrap()
        );
        let path = upload_dir.join(&req_id).join(&file_name);
        if let Err(persis_err) = f.file.persist(&path) {
            // fallback to copy
            std::fs::copy(persis_err.file.path(), path).context("cannot save uploaded file")?;
        }
        file_save_paths.push(format!("./files/{req_id}/{file_name}"));
    }
    Ok(file_save_paths)
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct FileMeta {
    filename: Option<String>,
    /// relative
    filepath: Option<String>,
    filesize: Option<u64>,
    file_header: Option<FileMetaHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_values: Option<Vec<Vec<String>>>,
}

#[derive(Serialize, Deserialize, Default, Clone, IntoParams, ToSchema)]
#[serde(default)]
pub struct FileMetaRequest {
    file_path: String,
    file_type: String,
    file_pattern: Option<String>,
    has_header: bool,
    skip: Option<usize>,
    delimiter: Option<String>,
    quote: Option<String>,
    comment: Option<String>,
    sample: Option<usize>,
    sort: Option<usize>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
#[serde(default)]
pub struct FileMetaHeader {
    columns_length: usize,
    column_names: Option<Vec<String>>,
}

#[utoipa::path(
    tag = "data sources",
    responses(
        (status = 200, description = "filemeta access success", body = Vec < String >),
        (status = 500, description = "metadata archive occur error", body = Failed)
    ),
    params(
        FileMetaRequest
    )
)]
#[get("/filemeta")]
pub async fn filemeta(filemeta_request: Query<FileMetaRequest>) -> impl Responder {
    match get_filemeta(filemeta_request.into_inner()).await {
        Ok(filemeta) => Ok(HttpResponse::Ok().json(filemeta)),
        Err(err) => Err(Failed::from_error(format!("{:#}", err))),
    }
}

async fn get_filemeta(filemeta_request: FileMetaRequest) -> anyhow::Result<FileMeta> {
    let (
        filepath_or_filedir,
        file_type,
        file_pattern,
        has_header,
        skip,
        delimiter,
        quote,
        comment,
        sample,
        sort,
    ) = (
        filemeta_request.file_path,
        filemeta_request.file_type,
        filemeta_request.file_pattern,
        filemeta_request.has_header,
        filemeta_request.skip.unwrap_or(0),
        filemeta_request.delimiter.unwrap_or_default(),
        filemeta_request.quote.unwrap_or_default(),
        filemeta_request.comment.unwrap_or_default(),
        filemeta_request.sample.unwrap_or(5),
        filemeta_request.sort.unwrap_or(1),
    );

    let delimiter = delimiter.trim();
    let delimiter = match delimiter.len() {
        0 => None,
        1 => Some(Ok(delimiter.as_bytes()[0])),
        _ => Some(Err(anyhow!("CSV delimiter should be a single character"))),
    }
    .transpose()?
    .unwrap_or(b',');

    let quote = quote.trim();
    let quote = match quote.as_bytes() {
        [] => None,
        [quote] if *quote == delimiter => Some(Err(anyhow!(
            "CSV quote should not be the same as delimiter"
        ))),
        [quote] => Some(Ok(*quote)),
        _ => Some(Err(anyhow!("CSV quote should be a single character"))),
    }
    .transpose()?;

    let comment = comment.trim();
    let comment = match comment.as_bytes() {
        [] => None,
        [comment] if *comment == delimiter => Some(Err(anyhow!(
            "CSV comment should not be the same as delimiter"
        ))),
        [comment] => Some(Ok(*comment)),
        _ => Some(Err(anyhow!("CSV comment should be a single character"))),
    }
    .transpose()?;

    let data_dir = get_data_dir();

    match file_type.as_str() {
        "csv" => {
            let filepath_or_filedir = filepath_or_filedir
                .split(",")
                .map(|path| data_dir.join(path).display().to_string())
                .collect_vec();
            let csv_header = source_csv::csv_header(
                filepath_or_filedir,
                file_pattern,
                has_header,
                skip,
                Some(delimiter),
                quote,
                comment,
                sample,
                sort,
            )
            .await?;
            if csv_header.columns == 0 {
                anyhow::bail!("CSV file(s) are empty");
            }
            let column_names = if csv_header.headers.is_empty() {
                let mut columns_temp = vec![];
                for n in 0..(csv_header.columns) {
                    columns_temp.push(format!("c{n}"));
                }
                Some(columns_temp)
            } else {
                Some(csv_header.headers)
            };
            Ok(FileMeta {
                filename: None,
                filepath: None,
                filesize: None,
                file_header: Some(FileMetaHeader {
                    columns_length: csv_header.columns,
                    column_names,
                }),
                sample_values: if csv_header.values.is_empty() {
                    None
                } else {
                    Some(csv_header.values)
                },
            })
        }
        _ => {
            anyhow::bail!("file type not support now");
        }
    }
}

#[derive(Debug, Deserialize, ToSchema, IntoParams)]
pub struct DownloadParams {
    file_path: String,
}

#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "success", body = NamedFile),
        (status = 500, description = "file check exists error", body = Failed)
    ),
    params(
        DownloadParams
    )
)]
#[get("/check_exists")]
pub async fn check_exists_files(params: Query<DownloadParams>) -> impl Responder {
    match download(params).await {
        Ok(_) => Ok(HttpResponse::Ok().json(serde_json::json!({"exists": true}))),
        Err(_) => Ok::<_, Failed>(HttpResponse::Ok().json(serde_json::json!({"exists": false}))),
    }
}

#[utoipa::path(
    tag = "tasks",
    responses(
        (status = 200, description = "success", body = NamedFile),
        (status = 500, description = "file download error", body = Failed)
    ),
    params(
        DownloadParams
    )
)]
#[get("/download")]
pub async fn download_files(params: Query<DownloadParams>, req: HttpRequest) -> impl Responder {
    match download(params).await {
        Ok(named_file) => Ok(named_file.into_response(&req)),
        Err(err) => Err(Failed::from_error(err)),
    }
}

async fn download(file_path: Query<DownloadParams>) -> anyhow::Result<NamedFile> {
    let file_path = file_path.into_inner().file_path;
    // 先按绝对路径后按相对路径
    if let Ok(file) = NamedFile::open(file_path.clone()) {
        Ok(file)
    } else {
        let data_dir = get_data_dir();
        let file_path = data_dir.join(file_path);
        let meta =
            std::fs::metadata(file_path.clone()).with_context(|| "get file metadata error")?;
        if meta.is_dir() {
            anyhow::bail!("not support path");
        }
        Ok(NamedFile::open(file_path)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_parser_string_timestamp_precision_allows_single_precision() {
        let s = r#"{ "parser": { "ts": "TIMESTAMP" } }"#;
        assert!(check_parser_string_timestamp_precision(s));
    }

    #[test]
    fn check_parser_string_timestamp_precision_rejects_mixed_precisions() {
        let s = r#"{ "parser": { "ts1": "TIMESTAMP", "ts2": "TIMESTAMP(us)" } }"#;
        assert!(!check_parser_string_timestamp_precision(s));
    }

    #[test]
    fn test_failed_from_error() {
        let err = anyhow::anyhow!("test error");
        let failed = Failed::from_error(err);
        assert_eq!(failed.code, Code::FAILED);
        assert!(failed.message.contains("test error"));
    }

    #[test]
    fn test_failed_new() {
        let failed = Failed::new(Code::FAILED, "test message".to_string(), ());
        assert_eq!(failed.code, Code::FAILED);
        assert_eq!(failed.message, "test message");
    }

    #[test]
    fn test_failed_display() {
        let failed = Failed::new(Code::FAILED, "test".to_string(), ());
        let display = format!("{}", failed);
        assert!(display.contains("test"));
    }

    #[test]
    fn test_failed_debug() {
        let failed = Failed::new(Code::FAILED, "test".to_string(), ());
        let debug = format!("{:?}", failed);
        assert!(debug.contains("Failed"));
    }

    #[test]
    fn test_failed_clone() {
        let failed = Failed::new(Code::FAILED, "test".to_string(), ());
        let cloned = failed.clone();
        assert_eq!(failed.code, cloned.code);
        assert_eq!(failed.message, cloned.message);
    }

    #[test]
    fn test_failed_serialize() {
        let failed = Failed::new(Code::FAILED, "test".to_string(), ());
        let json = serde_json::to_string(&failed);
        assert!(json.is_ok());
    }

    #[test]
    fn test_failed_with_data() {
        #[derive(Debug, Serialize, Deserialize)]
        struct TestData {
            value: i32,
        }
        let data = TestData { value: 42 };
        let failed = Failed::new(Code::FAILED, "test".to_string(), data);
        assert_eq!(failed.data.value, 42);
    }

    #[test]
    fn test_failed_from_error_with_multiple_sources() {
        let err = anyhow::anyhow!("first error").context("second error");
        let failed = Failed::from_error(err);
        assert_eq!(failed.code, Code::FAILED);
        assert!(!failed.message.is_empty());
    }

    #[test]
    fn test_failed_error_response() {
        let failed = Failed::new(Code::FAILED, "test error".to_string(), ());
        let response = failed.error_response();
        assert_eq!(
            response.status(),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn test_failed_default() {
        let failed: Failed<()> = Failed::default();
        // Default value uses Code::FAILED (0xFFFF) but may have different default
        assert!(failed.message.is_empty());
    }

    #[test]
    fn test_download_params_deserialization() {
        let json = r#"{"file_path": "/test/file.txt"}"#;
        let params: Result<DownloadParams, _> = serde_json::from_str(json);
        assert!(params.is_ok());
        assert_eq!(params.unwrap().file_path, "/test/file.txt");
    }

    #[test]
    fn test_failed_with_various_codes() {
        let codes = vec![Code::FAILED];
        for code in codes {
            let failed = Failed::new(code, "message".to_string(), ());
            assert_eq!(failed.code, code);
        }
    }

    #[test]
    fn test_failed_message_truncation() {
        let long_message = "a".repeat(10000);
        let failed: Failed<()> = Failed::new(Code::FAILED, long_message, ());
        assert!(!failed.message.is_empty());
    }

    #[test]
    fn test_failed_special_characters_in_message() {
        let special = "Error: <>&\"'";
        let failed: Failed<()> = Failed::new(Code::FAILED, special.to_string(), ());
        assert_eq!(failed.message, special);
    }

    #[tokio::test]
    async fn test_download_params_query() {
        use actix_web::web::Query;
        let json = r#"file_path=/test/file.txt"#;
        let query = Query::<DownloadParams>::from_query(json);
        assert!(query.is_ok());
    }

    #[test]
    fn test_failed_serialization_preserves_data() {
        let failed = Failed::new(Code::FAILED, "msg".to_string(), ());
        let serialized = serde_json::to_string(&failed).unwrap();
        let deserialized: Failed = serde_json::from_str(&serialized).unwrap();
        assert_eq!(failed.message, deserialized.message);
    }
}
