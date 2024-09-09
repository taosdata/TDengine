use actix_files::NamedFile;
use actix_web::{post, web::Json, Responder};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use taos::Dsn;
use taosx_core::migrations::*;
use tracing::instrument;
use utoipa::ToSchema;

use crate::serve::Failed;

#[serde_as]
#[derive(Debug, Deserialize, ToSchema)]
struct MigrateRequestBody {
    #[serde_as(as = "DisplayFromStr")]
    pub from: Dsn,
    #[serde_as(as = "DisplayFromStr")]
    pub to: Dsn,
    #[serde(default)]
    pub options: Option<Options>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ApplyResponseBody(ApplyResults);

#[utoipa::path(
    tag = "privileges",
		request_body = ImportRequestBody,
    responses(
        (status = 200, description = "success", body = ApplyResponseBody),
        (status = 500, description = "file download error", body = Failed)
    )
)]
#[post("/privileges/migrate")]
#[instrument(skip_all)]
pub async fn privileges_migrate(params: Json<MigrateRequestBody>) -> impl Responder {
    let params = params.into_inner();
    let options = params.options.unwrap_or_default();
    match migrate(&params.from, &params.to, &options).await {
        Ok(named_file) => Ok(serde_json::to_string(&ApplyResponseBody(named_file)).unwrap()),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[serde_as]
#[derive(Debug, Deserialize, ToSchema)]
struct ExportRequestBody {
    #[serde_as(as = "DisplayFromStr")]
    pub from: Dsn,
    #[serde(default)]
    pub options: Option<Options>,
}

#[utoipa::path(
    tag = "privileges",
		request_body = ExportRequestBody,
    responses(
        (status = 200, description = "success", body = NamedFile),
        (status = 500, description = "file download error", body = Failed)
    )
)]
#[post("/privileges/export")]
#[instrument(skip_all)]
pub async fn privileges_export(params: Json<ExportRequestBody>) -> impl Responder {
    let params = params.into_inner();
    let options = params.options.unwrap_or_default();
    let f = async move {
        let dir = tempfile::tempdir()?;
        let name = uuid::Uuid::new_v4().to_string();
        let path = dir.path().join(format!("{}.json", name));
        export(&params.from, &path, &options).await?;
        let file = NamedFile::open(&path)?;
        anyhow::Ok(file)
    };
    match f.await {
        Ok(file) => Ok(file),
        Err(err) => Err(Failed::from_error(err)),
    }
}

#[serde_as]
#[derive(Debug, Deserialize, ToSchema)]
struct ImportRequestBody {
    #[serde_as(as = "DisplayFromStr")]
    pub to: Dsn,
    pub data: Export,
    #[serde(default)]
    pub options: Option<Options>,
}

#[utoipa::path(
    tag = "privileges",
		request_body = ImportRequestBody,
    responses(
        (status = 200, description = "success", body = Json<ApplyResponseBody>),
        (status = 500, description = "file download error", body = Failed)
    )
)]
#[post("/privileges/import")]
#[instrument(skip_all)]
pub async fn privileges_import(params: Json<ImportRequestBody>) -> impl Responder {
    let mut params = params.into_inner();
    let options = params.options.unwrap_or_default();
    match params.data.apply_to(&params.to, &options).await {
        Ok(file) => Ok(Json(ApplyResponseBody(file))),
        Err(err) => Err(Failed::from_error(err)),
    }
}
