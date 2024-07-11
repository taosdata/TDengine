use std::{str::FromStr, time::Duration};

use actix_web::{web, HttpRequest};
use anyhow::Context;
use http_auth_basic::Credentials;
use reqwest::header::AUTHORIZATION;
use sqlx::{
    migrate::Migrator,
    pool::PoolOptions,
    sqlite::SqliteJournalMode,
    types::chrono::{self, Utc},
    ConnectOptions, QueryBuilder, SqlitePool,
};
use taos::tokio;

use crate::R;

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

const TABLE_NAME: &str = "sql_favorites";

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
        MIGRATOR
            .run(&pool)
            .await
            .context("migrate favorites_sql error")?;

        Ok(Self { pool })
    }
}

#[derive(serde::Deserialize)]
pub struct AddSql {
    sql: String,
}

fn err_empty_sql() -> R<()> {
    R::fail(101, "SQL is empty")
}

fn err_sql_already_exists() -> R<()> {
    R::fail(102, "SQL already exists")
}

/// add new favorites sql
pub async fn add_favorites_sql(
    favorites: web::Data<FavoritesSql>,
    req: HttpRequest,
    sql: web::Json<AddSql>,
) -> Result<R<()>> {
    if sql.sql.trim().is_empty() {
        return Err(err_empty_sql());
    }

    let username = get_username_from_header(&req)?;

    sqlx::query(&format!(
        "insert into {TABLE_NAME} (username, sql) values (?, ?)"
    ))
    .bind(username)
    .bind(&sql.sql)
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
pub async fn get_favorites_sql_page(
    favorites: web::Data<FavoritesSql>,
    search: web::Query<SearchParams>,
) -> Result<R<FavoritesSqlPageData>> {
    let search = search.into_inner();
    // get total
    let mut total_query_builder =
        sqlx::QueryBuilder::new(format!("select count(id) as total from {TABLE_NAME}"));
    build_page_query(&mut total_query_builder, &search);
    let total = total_query_builder
        .build_query_scalar()
        .fetch_one(&favorites.pool)
        .await
        .context("fetch favorites total count error")
        .map_err(R::internal)?;

    // get page result
    let mut query_builder = sqlx::QueryBuilder::new(format!("select * from {TABLE_NAME}"));
    build_page_query(&mut query_builder, &search);
    let rows: Vec<FavoritesSqlData> = query_builder
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

fn build_page_query(query_builder: &mut QueryBuilder<sqlx::Sqlite>, search: &SearchParams) {
    query_builder
        .push(" where is_public = ")
        .push_bind(search.is_public.unwrap_or_default());

    if let Some(fuzzy) = search
        .sql_desc_fuzzy
        .as_ref()
        .filter(|s| !s.trim().is_empty())
    {
        query_builder
            .push(" and sql like ")
            .push_bind(format!("%{fuzzy}%"))
            .push(" or description like ")
            .push_bind(format!("%{fuzzy}%"));
    }
}

/// delete favorites sql by id
pub async fn delete_favorites_sql(
    favorites: web::Data<FavoritesSql>,
    id: web::Path<u32>,
) -> Result<R<()>> {
    sqlx::query(&format!("delete from {TABLE_NAME} where id = ?"))
        .bind(*id)
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
pub async fn update_favorites_sql(
    favorites: web::Data<FavoritesSql>,
    id: web::Path<u32>,
    mut param: web::Json<UpdateParam>,
) -> Result<R<()>> {
    let mut query_builder = sqlx::QueryBuilder::new(format!("update {TABLE_NAME} set "));
    let mut update_public = false;
    if let Some(public) = param.public.take() {
        query_builder.push(" is_public = ").push_bind(public);
        update_public = true;
    }
    if let Some(description) = param.description.take().filter(|s| !s.trim().is_empty()) {
        if update_public {
            query_builder.push(", ");
        }
        query_builder.push(" description = ").push_bind(description);
    }
    query_builder
        .push(" where id = ")
        .push_bind(*id)
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
