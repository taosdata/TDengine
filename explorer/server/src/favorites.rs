use std::{str::FromStr, sync::OnceLock, time::Duration};

use actix_web::{web, HttpRequest};
use anyhow::Context;
use faststr::FastStr;
use sqlx::{
    migrate::Migrator,
    pool::PoolOptions,
    sqlite::SqliteJournalMode,
    types::chrono::{self, Utc},
    ConnectOptions, QueryBuilder, SqlitePool,
};
use tracing::{instrument, warn, Instrument};

use crate::{oauth::middleware::extract_auth_from_request, R};

static MIGRATOR: Migrator = sqlx::migrate!(); // defaults to "./migrations"

pub(super) static TAOSX_VERIFICATION_SUBJECT: OnceLock<FastStr> = OnceLock::new();

const TABLE_NAME: &str = "sql_favorites";

const DESCRIPTION_MAX_LENGTH: usize = 20;

type Result<T> = std::result::Result<T, R<()>>;

#[derive(Clone)]
pub struct Storage {
    pub pool: SqlitePool,
}

impl Storage {
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
        let owned_subject = FastStr::new(subject);
        if let Err(err) = TAOSX_VERIFICATION_SUBJECT.set(owned_subject) {
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
            tracing::trace!(register = %mask_string(&subject), "select subject from registration");
            let subject = FastStr::from_string(subject);
            if let Err(err) = TAOSX_VERIFICATION_SUBJECT.set(subject) {
                tracing::warn!("Setting verification subject error: {err}");
            }
            return true;
        }
        TAOSX_VERIFICATION_SUBJECT.get().is_some()
    }

    /// add new favorites sql
    pub async fn add_favorites_sql<'b>(
        &self,
        username: &'b str,
        sql: &'b str,
        description: Option<&'b str>,
    ) -> Result<()> {
        if sql.trim().is_empty() {
            return Err(err_empty_sql());
        }

        if description
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
        separated.push_bind(username);
        // sql
        separated.push_bind(sql);
        // description
        match description.as_ref().filter(|s| !s.trim().is_empty()) {
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
            .execute(&self.pool)
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

        Ok(())
    }

    /// set favorites sql to public/private
    #[instrument(skip_all)]
    pub async fn update_favorites_sql<'b>(
        &self,
        id: u32,
        username: &'b str,
        param: &'b UpdateParam,
    ) -> Result<()> {
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
            return Ok(());
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
            .push_bind(id)
            .push(" and username = ")
            .push_bind(username)
            .build()
            .execute(&self.pool)
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
        Ok(())
    }

    /// get favorites sql by page
    pub async fn get_favorites_sql_page<'b>(
        &self,
        username: &'b str,
        search: &'b SearchParams,
    ) -> Result<FavoritesSqlPageData> {
        if search.page == 0 {
            return Err(err_page_num_is_zero());
        }
        // get total
        let mut total_query_builder: QueryBuilder<'_, sqlx::Sqlite> =
            sqlx::QueryBuilder::new(format!("select count(id) as total from {TABLE_NAME}"));
        build_page_query(&mut total_query_builder, search, username);
        let total = total_query_builder
            .build_query_scalar()
            .fetch_one(&self.pool)
            .await
            .context("fetch favorites total count error")
            .map_err(R::internal)?;

        // get page result
        let mut query_builder = sqlx::QueryBuilder::new(format!("select * from {TABLE_NAME}"));
        build_page_query(&mut query_builder, search, username);
        let rows: Vec<FavoritesSqlData> = query_builder
            .push(" order by created_at desc")
            .push(" limit ")
            .push_bind(search.page_size)
            .push(" offset ")
            .push_bind((search.page - 1) * search.page_size)
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .context("fetch favorites page data from db err")
            .map_err(R::internal)?;

        Ok(FavoritesSqlPageData {
            page: search.page,
            page_size: search.page_size,
            total,
            total_page: total.div_ceil(search.page_size),
            list: rows,
        })
    }

    /// delete favorites sql by id
    ///
    /// This function deletes a favorite SQL entry from the database by its ID and username.
    ///
    /// Returns the number of rows deleted.
    pub async fn delete_favorites_sql(&self, id: u32, username: &str) -> Result<usize> {
        sqlx::query(&format!(
            "delete from {TABLE_NAME} where id = ? and username = ?"
        ))
        .bind(id)
        .bind(username)
        .execute(&self.pool)
        .await
        .map(|res| res.rows_affected() as usize)
        .context("delete favorites sql error")
        .map_err(R::internal)
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
    favorites: web::Data<Storage>,
    req: HttpRequest,
    sql: web::Json<AddSql>,
) -> Result<R<()>> {
    let auth = extract_auth_from_request(&req)
        .await
        .map_err(R::internal)?
        .ok_or_else(|| R::fail(401, "Unauthorized"))?;
    favorites
        .add_favorites_sql(&auth.username, &sql.sql, sql.description.as_deref())
        .in_current_span()
        .await?;
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
#[cfg_attr(test, derive(Debug))]
pub struct FavoritesSqlData {
    id: u32,
    username: String,
    sql: String,
    description: Option<String>,
    created_at: chrono::DateTime<Utc>,
    is_public: bool,
}

#[derive(serde::Serialize)]
#[cfg_attr(test, derive(Debug))]
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
    favorites: web::Data<Storage>,
    search: web::Query<SearchParams>,
) -> Result<R<FavoritesSqlPageData>> {
    let auth = extract_auth_from_request(&req)
        .await
        .map_err(R::internal)?
        .ok_or_else(|| R::fail(401, "Unauthorized"))?;
    favorites
        .get_favorites_sql_page(&auth.username, &search)
        .in_current_span()
        .await
        .map(R::success)
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
    favorites: web::Data<Storage>,
    req: HttpRequest,
    id: web::Path<u32>,
) -> Result<R<usize>> {
    let auth = extract_auth_from_request(&req)
        .await
        .map_err(R::internal)?
        .ok_or_else(|| R::fail(401, "Unauthorized"))?;
    favorites
        .delete_favorites_sql(id.into_inner(), &auth.username)
        .in_current_span()
        .await
        .map(R::success)
}

#[derive(serde::Deserialize)]
pub struct UpdateParam {
    public: Option<bool>,
    description: Option<String>,
}

/// set favorites sql to public/private
#[instrument(skip_all)]
pub async fn update_favorites_sql(
    favorites: web::Data<Storage>,
    req: HttpRequest,
    id: web::Path<u32>,
    param: web::Json<UpdateParam>,
) -> Result<R<()>> {
    let auth = extract_auth_from_request(&req)
        .await
        .map_err(R::internal)?
        .ok_or_else(|| R::fail(401, "Unauthorized"))?;
    favorites
        .update_favorites_sql(id.into_inner(), &auth.username, &param)
        .await
        .map(|_| R::default())
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
    use http::header::AUTHORIZATION;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_favorites_sql_new() {
        let temp_dir = assert_fs::TempDir::with_prefix("favorites_sql_test").unwrap();
        let fav_sql = Storage::new(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();
        assert!(temp_dir.exists(), "temp dir should exist");
        assert!(fav_sql.pool.acquire().await.is_ok());
        assert!(!fav_sql.is_registered().await, "not registered yet");
        assert!(TAOSX_VERIFICATION_SUBJECT.get().is_none());
        fav_sql
            .upsert_registration("18900000000", "cid", "test")
            .await
            .unwrap();
        assert!(fav_sql.is_registered().await);
        assert!(TAOSX_VERIFICATION_SUBJECT.get().is_some());
        assert_eq!(
            TAOSX_VERIFICATION_SUBJECT.get().unwrap().as_str(),
            "18900000000"
        );
        temp_dir.close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_favorites_sql_operations() {
        let temp_dir = assert_fs::TempDir::with_prefix("favorites_sql_operations").unwrap();
        let fav_sql = Storage::new(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        let expects = [
            (("root", "", None), R::fail(101, "SQL is empty")),
            (("root", "select 1", None), R::success(())),
            (("root", "select 2", Some("This is a test")), R::success(())),
            (
                ("root", "select 1", None),
                R::fail(102, "SQL already exists"),
            ),
            (
                (
                    "root",
                    "select 3",
                    Some("This is a long long long long long long long invalid description"),
                ),
                R::fail(103, "Description is too long"),
            ),
        ];
        for (input, expect) in expects {
            let res = fav_sql
                .add_favorites_sql(input.0, input.1, input.2)
                .await
                .map(R::success)
                .unwrap_or_else(|e| e);
            assert_eq!(
                res.to_string(),
                expect.to_string(),
                "input: {input:?}, output: {res:?}, expect: {expect:?}",
            );
        }

        {
            // 1.list with page 0
            let page_data = fav_sql
                .get_favorites_sql_page(
                    "root",
                    &SearchParams {
                        page: 0,
                        page_size: 10,
                        sql_desc_fuzzy: None,
                        is_public: None,
                    },
                )
                .await;
            assert_eq!(
                page_data.unwrap_err().to_string(),
                err_page_num_is_zero().to_string()
            );

            // list all
            let page_data = fav_sql
                .get_favorites_sql_page(
                    "root",
                    &SearchParams {
                        page: 1,
                        page_size: 10,
                        sql_desc_fuzzy: None,
                        is_public: None,
                    },
                )
                .await
                .unwrap();
            println!("{:?}", &page_data);
            assert_eq!(page_data.total, 2);
            assert_eq!(page_data.list.len(), 2);
            assert_eq!(page_data.list[0].sql, "select 1");
            assert_eq!(page_data.list[1].sql, "select 2");
            assert!(!page_data.list[0].is_public);
            assert!(!page_data.list[1].is_public);

            let page_data = fav_sql
                .get_favorites_sql_page(
                    "root1",
                    &SearchParams {
                        page: 1,
                        page_size: 10,
                        sql_desc_fuzzy: None,
                        is_public: None,
                    },
                )
                .await
                .unwrap();
            assert!(page_data.list.is_empty());
        }

        let expect = [
            (
                1,
                "root",
                UpdateParam {
                    public: Some(true),
                    description: Some("updated desc".to_string()),
                },
                R::success(()),
            ),
            (
                2,
                "root",
                UpdateParam {
                    public: None,
                    description: Some("".to_string()),
                },
                R::success(()),
            ),
            (
                3,
                "root",
                UpdateParam {
                    public: None,
                    description: None,
                },
                R::success(()),
            ),
            (
                1,
                "root",
                UpdateParam {
                    public: Some(false),
                    description: Some("a".repeat(30)),
                },
                R::fail(103, "Description is too long"),
            ),
        ];

        for (id, username, param, expect) in expect {
            let res = fav_sql
                .update_favorites_sql(id, username, &param)
                .await
                .map(R::success)
                .unwrap_or_else(|e| e);
            assert_eq!(
                res.to_string(),
                expect.to_string(),
                "id: {id}, username: {username}, output: {res:?}, expect: {expect:?}",
            );
        }

        let rows = fav_sql.delete_favorites_sql(1, "root").await.unwrap();
        assert_eq!(rows, 1);
        let rows = fav_sql.delete_favorites_sql(1, "root1").await.unwrap();
        assert_eq!(rows, 0, "no rows deleted with wrong username, but no error");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_web_handlers() {
        use base64::{engine::general_purpose::STANDARD, Engine as _};

        let temp_dir = assert_fs::TempDir::with_prefix("favorites_sql_web_handlers").unwrap();
        let fav_sql = Storage::new(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();
        let fav_sql_data = web::Data::new(fav_sql);
        let auth_header = format!("Basic {}", &STANDARD.encode("root:taosdata"));
        // add sql
        let req = actix_web::test::TestRequest::default()
            .insert_header((AUTHORIZATION, auth_header.clone()))
            .to_http_request();
        let add_sql = web::Json(AddSql {
            sql: "select 1".to_string(),
            description: Some("test sql".to_string()),
        });
        let res = add_favorites_sql(fav_sql_data.clone(), req, add_sql)
            .await
            .unwrap();
        assert_eq!(res.code, 0);

        // get page
        let req = actix_web::test::TestRequest::default()
            .insert_header((AUTHORIZATION, auth_header.clone()))
            .to_http_request();
        let search = web::Query(SearchParams {
            page: 1,
            page_size: 10,
            sql_desc_fuzzy: None,
            is_public: None,
        });
        let res = get_favorites_sql_page(req, fav_sql_data.clone(), search)
            .await
            .unwrap();
        assert_eq!(res.code, 0);
        assert_eq!(res.data.unwrap().total, 1);
    }
}
