use std::{str::FromStr, sync::OnceLock, time::Duration};

use actix_web::{web, HttpRequest};
use anyhow::Context;
use faststr::FastStr;
use http_auth_basic::Credentials;
use reqwest::header::AUTHORIZATION;
use sqlx::{
    migrate::Migrator,
    pool::PoolOptions,
    sqlite::SqliteJournalMode,
    types::chrono::{self, Utc},
    ConnectOptions, QueryBuilder, SqlitePool,
};
use tracing::{instrument, warn};

use crate::R;

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

pub(super) static TAOSX_VERIFICATION_SUBJECT: OnceLock<FastStr> = OnceLock::new();

const TABLE_NAME: &str = "sql_favorites";

const DESCRIPTION_MAX_LENGTH: usize = 20;

type Result<T> = std::result::Result<T, R<()>>;

#[derive(Clone)]
pub struct FavoritesSql {
    pool: SqlitePool,
}

impl FavoritesSql {
    pub async fn new(data_dir: &str) -> anyhow::Result<Self> {
        if !tokio::fs::try_exists(data_dir)
            .await
            .context("Cannot find data dir")?
        {
            tokio::fs::create_dir_all(data_dir)
                .await
                .context("Cannot create directory for database")?;
        }

        let connect_options =
            sqlx::sqlite::SqliteConnectOptions::from_str(&format!("sqlite:{data_dir}/explorer.db"))
                .context("parse database url error")?
                .create_if_missing(true)
                .busy_timeout(Duration::from_secs(10))
                .auto_vacuum(sqlx::sqlite::SqliteAutoVacuum::Incremental)
                .optimize_on_close(true, None)
                .log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(2))
                .journal_mode(SqliteJournalMode::Wal);

        let pool = PoolOptions::new()
            .min_connections(4)
            .max_connections(128)
            .acquire_timeout(Duration::from_secs(60))
            .idle_timeout(Some(Duration::from_secs(60 * 60)))
            .max_lifetime(Some(Duration::from_secs(60 * 60 * 24)))
            .connect_with(connect_options)
            .await
            .context("connect to database error")?;

        // run migrate
        if let Err(err) = MIGRATOR.run(&pool).await {
            warn!("Try to run migrations error, check if the schema is up to date: {err:#}");
        }

        Ok(Self { pool })
    }

    pub async fn upsert_registration(
        &self,
        subject: &str,
        cid: &str,
        version: &str,
    ) -> anyhow::Result<()> {
        let masked_subject = FastStr::from_string(mask_string(subject));
        if let Err(err) = TAOSX_VERIFICATION_SUBJECT.set(masked_subject) {
            warn!("Failed to set TAOSX_VERIFICATION_SUBJECT: {:?}", err);
        }
        let _ =
            sqlx::query("insert into registration (`subject`, `cid`, `version`) values (?, ?, ?)")
                .bind(subject)
                .bind(cid)
                .bind(version)
                .execute(&self.pool)
                .await?;
        Ok(())
    }

    /// Check if anyone registered with this explorer.
    pub async fn is_registered(&self) -> bool {
        if TAOSX_VERIFICATION_SUBJECT.get().is_some() {
            return true;
        }

        if let Ok(Some(subject)) =
            sqlx::query_scalar::<_, String>("select `subject` from registration limit 1")
                .fetch_optional(&self.pool)
                .await
                .inspect_err(|err| {
                    tracing::error!("Persist registration error: {err:?}");
                })
        {
            let subject = FastStr::from_string(mask_string(&subject));
            tracing::trace!(%subject, "select subject from registration");
            if let Err(err) = TAOSX_VERIFICATION_SUBJECT.set(subject) {
                tracing::warn!("Setting verification subject error: {err}");
            }
            return true;
        }
        TAOSX_VERIFICATION_SUBJECT.get().is_some()
    }
}

#[derive(serde::Deserialize)]
pub struct AddSql {
    sql: String,
    description: Option<String>,
}

fn err_empty_sql() -> R<()> {
    R::fail(101, "SQL is empty")
}

fn err_sql_already_exists() -> R<()> {
    R::fail(102, "SQL already exists")
}

fn err_description_too_long() -> R<()> {
    R::fail(103, "Description is too long")
}

fn err_page_num_is_zero() -> R<()> {
    R::fail(104, "Page number is zero")
}

/// add new favorites sql
#[instrument(skip_all)]
pub async fn add_favorites_sql(
    favorites: web::Data<FavoritesSql>,
    req: HttpRequest,
    sql: web::Json<AddSql>,
) -> Result<R<()>> {
    if sql.sql.trim().is_empty() {
        return Err(err_empty_sql());
    }

    if sql
        .description
        .as_ref()
        .is_some_and(|d| d.chars().count() > DESCRIPTION_MAX_LENGTH)
    {
        return Err(err_description_too_long());
    }

    let mut query_builder = sqlx::QueryBuilder::new(format!(
        "insert into {TABLE_NAME} (username, sql, description) values ("
    ));
    let mut separated = query_builder.separated(" , ");
    // username
    separated.push_bind(get_username_from_header(&req)?);
    // sql
    separated.push_bind(&sql.sql);
    // description
    match sql.description.as_ref().filter(|s| !s.trim().is_empty()) {
        Some(description) => {
            separated.push_bind(description);
        }
        None => {
            separated.push(" NULL ");
        }
    }
    separated.push_unseparated(" ) ");

    query_builder
        .build()
        .execute(&favorites.pool)
        .await
        .map_err(|e| {
            if e.as_database_error()
                .is_some_and(|e| e.is_unique_violation())
            {
                err_sql_already_exists()
            } else {
                let err = anyhow::Error::new(e).context("add sql error");
                R::internal(err)
            }
        })?;

    Ok(R::default())
}

#[derive(serde::Deserialize)]
pub struct SearchParams {
    page: u32,
    page_size: u32,
    sql_desc_fuzzy: Option<String>,
    is_public: Option<bool>,
}

#[derive(sqlx::FromRow, serde::Serialize)]
pub struct FavoritesSqlData {
    id: u32,
    username: String,
    sql: String,
    description: Option<String>,
    created_at: chrono::DateTime<Utc>,
    is_public: bool,
}

#[derive(serde::Serialize)]
pub struct FavoritesSqlPageData {
    page: u32,
    page_size: u32,
    total: u32,
    total_page: u32,
    list: Vec<FavoritesSqlData>,
}

/// get favorites sql by page
#[instrument(skip_all)]
pub async fn get_favorites_sql_page(
    req: HttpRequest,
    favorites: web::Data<FavoritesSql>,
    search: web::Query<SearchParams>,
) -> Result<R<FavoritesSqlPageData>> {
    if search.page == 0 {
        return Err(err_page_num_is_zero());
    }
    let search = search.into_inner();
    let username = get_username_from_header(&req)?;
    // get total
    let mut total_query_builder =
        sqlx::QueryBuilder::new(format!("select count(id) as total from {TABLE_NAME}"));
    build_page_query(&mut total_query_builder, &search, &username);
    let total = total_query_builder
        .build_query_scalar()
        .fetch_one(&favorites.pool)
        .await
        .context("fetch favorites total count error")
        .map_err(R::internal)?;

    // get page result
    let mut query_builder = sqlx::QueryBuilder::new(format!("select * from {TABLE_NAME}"));
    build_page_query(&mut query_builder, &search, &username);
    let rows: Vec<FavoritesSqlData> = query_builder
        .push(" order by created_at desc")
        .push(" limit ")
        .push_bind(search.page_size)
        .push(" offset ")
        .push_bind((search.page - 1) * search.page_size)
        .build_query_as()
        .fetch_all(&favorites.pool)
        .await
        .context("fetch favorites page data from db err")
        .map_err(R::internal)?;

    Ok(R::success(FavoritesSqlPageData {
        page: search.page,
        page_size: search.page_size,
        total,
        total_page: total.div_ceil(search.page_size),
        list: rows,
    }))
}

fn build_page_query<'a, 'b>(
    query_builder: &'a mut QueryBuilder<'b, sqlx::Sqlite>,
    search: &'a SearchParams,
    username: &'b str,
) {
    match search.is_public {
        Some(true) => {
            query_builder.push(" where is_public = true ");
        }
        Some(false) => {
            query_builder.push(" where is_public = false ");
            query_builder.push(" and username = ").push_bind(username);
        }
        None => {
            query_builder.push(" where username = ").push_bind(username);
        }
    }

    if let Some(fuzzy) = search
        .sql_desc_fuzzy
        .as_ref()
        .filter(|s| !s.trim().is_empty())
    {
        query_builder
            .push(" and ( ")
            .push(" sql like")
            .push_bind(format!("%{fuzzy}%"))
            .push(" or description like ")
            .push_bind(format!("%{fuzzy}%"))
            .push(" ) ");
    }
}

/// delete favorites sql by id
#[instrument(skip_all)]
pub async fn delete_favorites_sql(
    favorites: web::Data<FavoritesSql>,
    req: HttpRequest,
    id: web::Path<u32>,
) -> Result<R<()>> {
    sqlx::query(&format!(
        "delete from {TABLE_NAME} where id = ? and username = ?"
    ))
    .bind(*id)
    .bind(get_username_from_header(&req)?)
    .execute(&favorites.pool)
    .await
    .context("delete favorites sql error")
    .map_err(R::internal)?;
    Ok(R::default())
}

#[derive(serde::Deserialize)]
pub struct UpdateParam {
    public: Option<bool>,
    description: Option<String>,
}

/// set favorites sql to public/private
#[instrument(skip_all)]
pub async fn update_favorites_sql(
    favorites: web::Data<FavoritesSql>,
    req: HttpRequest,
    id: web::Path<u32>,
    param: web::Json<UpdateParam>,
) -> Result<R<()>> {
    if param
        .description
        .as_ref()
        .is_some_and(|d| d.chars().count() > DESCRIPTION_MAX_LENGTH)
    {
        return Err(err_description_too_long());
    }

    if param.public.is_none()
        && param
            .description
            .as_ref()
            .filter(|s| !s.trim().is_empty())
            .is_none()
    {
        return Ok(R::default());
    }

    let mut query_builder = sqlx::QueryBuilder::new(format!("update {TABLE_NAME} set "));
    let mut update_public = false;
    if let Some(public) = param.public.as_ref() {
        query_builder.push(" is_public = ").push_bind(public);
        update_public = true;
    }
    if let Some(description) = param.description.as_ref().map(|s| s.trim()) {
        if update_public {
            query_builder.push(", ");
        }
        if description.is_empty() {
            query_builder.push(" description = NULL");
        } else {
            query_builder.push(" description = ").push_bind(description);
        }
    }
    query_builder
        .push(" where id = ")
        .push_bind(*id)
        .push(" and username = ")
        .push_bind(get_username_from_header(&req)?)
        .build()
        .execute(&favorites.pool)
        .await
        .map_err(|e| {
            if e.as_database_error()
                .is_some_and(|e| e.is_unique_violation())
            {
                err_sql_already_exists()
            } else {
                let err = anyhow::Error::new(e).context("set sql public state error");
                R::internal(err)
            }
        })?;
    Ok(R::default())
}

/// convenience function to get username from http header
pub fn get_username_from_header(req: &HttpRequest) -> Result<String> {
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default();
    let credentials = Credentials::from_header(header.to_string()).map_err(R::internal)?;
    Ok(credentials.user_id)
}
pub fn mask_string(s: &str) -> String {
    if s.len() < 3 {
        // If string is too short, return all stars
        return "*".repeat(s.len());
    }
    let mask_len = (s.len() - 1) / 3 + 1;

    let lr = s.len() - mask_len;
    let r = (lr / 2).min(4);
    let l = (lr - r).min(4);
    format!(
        "{}{}{}",
        &s[..l],
        "*".repeat(mask_len.min(4)),
        &s[s.len() - r..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_string() {
        assert_eq!(mask_string(""), "");
        assert_eq!(mask_string("a"), "*");
        assert_eq!(mask_string("ab"), "**");
        assert_eq!(mask_string("abc"), "a*c");
        assert_eq!(mask_string("abcd"), "a**d");
        assert_eq!(mask_string("abcde"), "ab**e");
        assert_eq!(mask_string("abcdef"), "ab**ef");
        assert_eq!(mask_string("abcdefg"), "ab***fg");
        assert_eq!(mask_string("abcdefgh"), "abc***gh");
        assert_eq!(mask_string("abcdefghi"), "abc***ghi");
        assert_eq!(mask_string("abcdefghij"), "abc****hij");
        assert_eq!(mask_string("abcdefghijk"), "abcd****ijk");
        assert_eq!(mask_string("abcdefghijkl"), "abcd****ijkl");
        assert_eq!(mask_string("abcdefghijklm"), "abcd****jklm");
        assert_eq!(mask_string("abcdefghijklmn"), "abcd****klmn");
    }
}
