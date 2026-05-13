use std::cell::UnsafeCell;
use std::ffi::{c_char, CStr, CString};
use std::mem::ManuallyDrop;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use once_cell::sync::OnceCell;
use raw::{ApiEntry, BlockState, RawRes, RawTaos};
use taos_query::prelude::{Field, Precision, RawBlock, RawMeta, RawResult};
use taos_query::util::Edition;
use taos_query::RawError;
use tokio::sync::oneshot;
use tokio::{select, task, time};
use tracing::{warn, Instrument};

const MAX_CONNECT_RETRIES: u8 = 2;

mod into_c_str;
mod raw;
mod stmt;
pub use stmt::Stmt;
mod stmt2;
pub use stmt2::Stmt2;

#[allow(non_camel_case_types)]
pub(crate) mod types;

pub mod tmq;
pub use tmq::{Consumer, TmqBuilder};

pub mod prelude {
    pub use taos_query::prelude::*;

    pub use super::{Consumer, ResultSet, Stmt, Stmt2, Taos, TaosBuilder, TmqBuilder};

    pub mod sync {
        pub use taos_query::prelude::sync::*;

        pub use crate::{Consumer, ResultSet, Stmt, Stmt2, Taos, TaosBuilder, TmqBuilder};
    }
}

#[macro_export(local_inner_macros)]
macro_rules! err_or {
    ($res:ident, $code:expr, $ret:expr) => {
        unsafe {
            let code: Code = { $code }.into();
            if code.success() {
                Ok($ret)
            } else {
                Err(taos_query::RawError::new(code, $res.err_as_str()))
            }
        }
    };
    ($res:ident, $code:expr) => {
        err_or!($res, $code, ())
    };
    ($code:expr, $ret:expr) => {
        unsafe {
            let code: Code = { $code }.into();
            if code.success() {
                Ok($ret)
            } else {
                Err(RawError::from_code(code))
            }
        }
    };
    ($code:expr) => {
        err_or!($code, ())
    };
}

#[derive(Debug)]
pub struct Taos {
    raw: RawTaos,
}

impl Drop for Taos {
    fn drop(&mut self) {
        self.raw.close();
    }
}

impl taos_query::Queryable for Taos {
    type ResultSet = ResultSet;

    fn query<T: AsRef<str>>(&self, sql: T) -> RawResult<Self::ResultSet> {
        tracing::trace!("Query with SQL: {}", sql.as_ref());
        self.raw.query(sql.as_ref()).map(ResultSet::new)
    }

    fn query_with_req_id<T: AsRef<str>>(&self, sql: T, req_id: u64) -> RawResult<Self::ResultSet> {
        tracing::trace!("Query with SQL: {}", sql.as_ref());
        self.raw
            .query_with_req_id(sql.as_ref(), req_id)
            .map(ResultSet::new)
    }

    fn write_raw_meta(&self, meta: &RawMeta) -> RawResult<()> {
        let raw = meta.as_raw_data_t();
        self.raw.write_raw_meta(raw)
    }

    fn write_raw_block(&self, raw: &RawBlock) -> RawResult<()> {
        self.raw.write_raw_block(raw)
    }

    fn write_raw_block_with_req_id(&self, raw: &RawBlock, req_id: u64) -> RawResult<()> {
        self.raw.write_raw_block_with_req_id(raw, req_id)
    }

    fn put(&self, data: &taos_query::common::SmlData) -> RawResult<()> {
        self.raw.put(data)
    }

    fn table_vgroup_id(&self, db: &str, table: &str) -> Option<i32> {
        self.raw.get_table_vgroup_id(db, table).ok()
    }

    fn tables_vgroup_ids<T: AsRef<str>>(&self, db: &str, tables: &[T]) -> Option<Vec<i32>> {
        self.raw.get_tables_vgroup_ids(db, tables).ok()
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncQueryable for Taos {
    type AsyncResultSet = ResultSet;

    #[tracing::instrument(level = "trace", skip_all)]
    async fn query<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<Self::AsyncResultSet> {
        tracing::trace!("Async query with SQL: {}", sql.as_ref());
        match self.raw.query_async(sql.as_ref()).await {
            Err(err) if err.code() == 0x2603 => {
                self.raw.query_async(sql.as_ref()).await.map(ResultSet::new)
            }
            Err(err) => Err(err),
            Ok(raw) => Ok(ResultSet::new(raw)),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn exec<T: AsRef<str> + Send + Sync>(&self, sql: T) -> RawResult<usize> {
        let sql = sql.as_ref();
        self.raw.exec_async(sql).await
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn query_with_req_id<T: AsRef<str> + Send + Sync>(
        &self,
        _sql: T,
        _req_id: u64,
    ) -> RawResult<Self::AsyncResultSet> {
        self.raw
            .query_with_req_id(_sql.as_ref(), _req_id)
            .map(ResultSet::new)
    }

    #[allow(clippy::redundant_pub_crate)]
    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_raw_meta(&self, meta: &taos_query::common::RawMeta) -> RawResult<()> {
        let raw = meta.as_raw_data_t();
        let slf = self.raw.clone();
        let mut h = task::spawn_blocking(move || slf.write_raw_meta(raw));
        let mut interval = time::interval(Duration::from_secs(60));
        const MAX_WAIT_TICKS: usize = 5; // means 5 minutes
        const TIMEOUT_ERROR: &str = "Write raw timeout, maybe the connection has been lost";
        let mut ticks = 0;
        loop {
            select! {
                _ = interval.tick() => {
                    ticks += 1;
                    if ticks >= MAX_WAIT_TICKS {
                        tracing::warn!("{}", TIMEOUT_ERROR);
                        return Err(RawError::new(
                            0xE002, // Connection closed
                            TIMEOUT_ERROR,
                        ));
                    }
                    if let Err(err) = time::timeout(Duration::from_secs(30), self.exec("select server_version()").in_current_span()).await {
                        tracing::warn!(error = format!("{err:#}"), TIMEOUT_ERROR);
                        return Err(RawError::new(
                            0xE002, // Connection closed
                            TIMEOUT_ERROR,
                        ));
                    }
                }
                res = &mut h => {
                    return res.map_err(|err| RawError::from_string(format!("Write raw data join error: {err}")))?;
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_raw_block(&self, block: &RawBlock) -> RawResult<()> {
        self.raw.write_raw_block(block)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn write_raw_block_with_req_id(&self, block: &RawBlock, req_id: u64) -> RawResult<()> {
        let slf = self.raw.clone();
        let block_ptr =
            unsafe { ManuallyDrop::new(Box::from_raw(block as *const RawBlock as *mut RawBlock)) };
        time::timeout(
            Duration::from_secs(60),
            task::spawn_blocking(move || {
                slf.write_raw_block_with_req_id(block_ptr.as_ref(), req_id)
            })
            .in_current_span(),
        )
        .in_current_span()
        .await
        .map_err(|_| {
            tracing::warn!("Write raw data timeout, maybe the connection has been lost");
            RawError::new(
                0xE002, // Connection closed
                "Write raw data timeout, maybe the connection has been lost",
            )
        })?
        .map_err(|err| RawError::from_string(format!("Write raw data join error: {err}")))?
    }

    async fn put(&self, data: &taos_query::common::SmlData) -> RawResult<()> {
        self.raw.put(data)
    }

    async fn table_vgroup_id(&self, db: &str, table: &str) -> Option<i32> {
        self.raw.get_table_vgroup_id(db, table).ok()
    }

    async fn tables_vgroup_ids<T: AsRef<str> + Sync>(
        &self,
        db: &str,
        tables: &[T],
    ) -> Option<Vec<i32>> {
        self.raw.get_tables_vgroup_ids(db, tables).ok()
    }
}

/// Connection builder.
///
/// ## Examples
///
/// ### Synchronous
///
/// ```rust
/// use taos_optin::prelude::sync::*;
///
/// fn main() -> anyhow::Result<()> {
///     let builder = TaosBuilder::from_dsn("taos://localhost:6030")?;
///     let taos = builder.build()?;
///     let mut query = taos.query("show databases")?;
///     for row in query.rows() {
///         println!("{:?}", row?.into_values());
///     }
///     Ok(())
/// }
/// ```
///
/// ### Async
///
/// ```rust
/// use taos_optin::prelude::*;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let builder = TaosBuilder::from_dsn("taos://localhost:6030")?;
///     let taos = builder.build().await?;
///     let mut query = taos.query("show databases").await?;
///
///     while let Some(row) = query.rows().try_next().await? {
///         println!("{:?}", row.into_values());
///     }
///     Ok(())
/// }
/// #
/// ```
#[derive(Debug)]
pub struct TaosBuilder {
    auth: Auth,
    lib: Arc<ApiEntry>,
    inner_conn: OnceCell<Taos>,
    server_version: OnceCell<String>,
}

impl TaosBuilder {
    fn inner_connection(&self) -> RawResult<&Taos> {
        if let Some(taos) = self.inner_conn.get() {
            Ok(taos)
        } else {
            let ptr = self
                .lib
                .connect_with_retries(&self.auth, self.auth.max_retries())?;

            let raw = RawTaos::new(self.lib.clone(), ptr)?;
            let taos = Ok(Taos { raw });
            self.inner_conn.get_or_try_init(|| taos)
        }
    }

    async fn async_inner_connection(&self) -> RawResult<&Taos> {
        if let Some(taos) = self.inner_conn.get() {
            Ok(taos)
        } else {
            let taos = self.async_connect().await;
            self.inner_conn.get_or_try_init(|| taos)
        }
    }

    async fn async_connect(&self) -> RawResult<Taos> {
        let api = self.lib.clone();
        let auth = self.auth.clone();

        let (tx, rx) = oneshot::channel::<()>();
        let join = task::spawn_blocking(move || {
            tracing::trace!("Async connecting to the server");
            let ptr = api.connect_with_retries(&auth, auth.max_retries())?;
            RawTaos::new(api, ptr).map(|raw| Taos { raw })
        });
        let abort = join.abort_handle();
        task::spawn(async move {
            let _ = rx.await;
            if abort.is_finished() {
                return;
            }
            tracing::trace!("Abort the connecting");
            abort.abort();
        });
        let res = join.await.map_err(|err| {
            tracing::error!("Failed to join threads {err:#}: {:?}", err);
            taos_query::RawError::from_string("Failed to connect to the server").with_code(0x000B)
        })?;
        drop(tx);
        res
    }
}

#[derive(Debug, Default, Clone)]
struct Auth {
    host: Option<CString>,
    user: Option<CString>,
    pass: Option<CString>,
    totp: Option<CString>,
    token: Option<CString>,
    db: Option<CString>,
    port: u16,
    max_retries: u8,
}

impl Auth {
    pub(crate) fn host(&self) -> Option<&CStr> {
        self.host.as_deref()
    }

    pub(crate) fn host_as_ptr(&self) -> *const c_char {
        self.host().map_or_else(std::ptr::null, CStr::as_ptr)
    }

    pub(crate) fn user(&self) -> Option<&CStr> {
        self.user.as_deref()
    }

    pub(crate) fn user_as_ptr(&self) -> *const c_char {
        self.user().map_or_else(std::ptr::null, CStr::as_ptr)
    }

    pub(crate) fn password(&self) -> Option<&CStr> {
        self.pass.as_deref()
    }

    pub(crate) fn password_as_ptr(&self) -> *const c_char {
        self.password().map_or_else(std::ptr::null, CStr::as_ptr)
    }

    pub(crate) fn totp(&self) -> Option<&CStr> {
        self.totp.as_deref()
    }

    pub(crate) fn totp_as_ptr(&self) -> *const c_char {
        self.totp().map_or_else(std::ptr::null, CStr::as_ptr)
    }

    pub(crate) fn token(&self) -> Option<&CStr> {
        self.token.as_deref()
    }

    pub(crate) fn token_as_ptr(&self) -> *const c_char {
        self.token().map_or_else(std::ptr::null, CStr::as_ptr)
    }

    pub(crate) fn database(&self) -> Option<&CStr> {
        self.db.as_deref()
    }

    pub(crate) fn database_as_ptr(&self) -> *const c_char {
        self.database().map_or_else(std::ptr::null, CStr::as_ptr)
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn max_retries(&self) -> u8 {
        self.max_retries
    }
}

impl taos_query::TBuilder for TaosBuilder {
    type Target = Taos;

    fn available_params() -> &'static [&'static str] {
        const PARAMS: &[&str] = &[
            "configDir",
            "libraryPath",
            "maxRetries",
            "totpCode",
            "totp_code",
            "bearerToken",
            "bearer_token",
        ];
        PARAMS
    }

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> RawResult<Self> {
        let mut dsn = dsn.into_dsn()?;

        let lib = if let Some(path) = dsn.params.remove("libraryPath") {
            tracing::trace!("using library path: {path}");
            ApiEntry::dlopen(path).map_err(taos_query::RawError::any)?
        } else {
            tracing::trace!("using default library of taos");
            ApiEntry::open_default().map_err(taos_query::RawError::any)?
        };

        let mut auth = Auth::default();
        if let Some(addr) = dsn.addresses.first() {
            if let Some(host) = &addr.host {
                auth.host.replace(CString::new(host.as_str()).unwrap());
            }
            if let Some(port) = addr.port {
                auth.port = port;
            }
        }
        if let Some(db) = dsn.subject.as_deref() {
            auth.db.replace(CString::new(db).unwrap());
        }
        if let Some(user) = dsn.username.as_deref() {
            auth.user.replace(CString::new(user).unwrap());
        }
        if let Some(pass) = dsn.password.as_deref() {
            auth.pass.replace(CString::new(pass).unwrap());
        }

        let params = &dsn.params;
        if let Some(totp) = params.get("totpCode").or_else(|| params.get("totp_code")) {
            let totp = CString::new(totp.as_str())
                .map_err(|e| RawError::from_string(format!("Invalid totp code: {e}")))?;
            auth.totp.replace(totp);
        }
        if let Some(token) = params
            .get("bearerToken")
            .or_else(|| params.get("bearer_token"))
        {
            let token = CString::new(token.as_str())
                .map_err(|e| RawError::from_string(format!("Invalid bearer token: {e}")))?;
            auth.token.replace(token);
        }

        if let Some(dir) = params.get("configDir") {
            lib.options(types::TSDB_OPTION::ConfigDir, dir);
        }

        lib.options(types::TSDB_OPTION::ShellActivityTimer, "120");

        if let Some(max_retries) = params.get("maxRetries") {
            auth.max_retries = max_retries.parse().unwrap_or(MAX_CONNECT_RETRIES);
        } else {
            auth.max_retries = MAX_CONNECT_RETRIES;
        }

        Ok(Self {
            auth,
            lib: Arc::new(lib),
            inner_conn: OnceCell::new(),
            server_version: OnceCell::new(),
        })
    }

    fn client_version() -> &'static str {
        "dynamic"
    }

    fn ping(&self, conn: &mut Self::Target) -> RawResult<()> {
        let mut res = conn.raw.query("select server_version()")?;
        res.free_result();
        Ok(())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        let ptr = self
            .lib
            .connect_with_retries(&self.auth, self.auth.max_retries())?;

        let raw = RawTaos::new(self.lib.clone(), ptr)?;
        Ok(Taos { raw })
    }

    fn server_version(&self) -> RawResult<&str> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = self.inner_connection()?;
            use taos_query::prelude::sync::Queryable;
            let v: String = Queryable::query_one(conn, "select server_version()")?.unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) | Err((v, _)) => v.as_str(),
            })
        }
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        let taos = self.inner_connection()?;
        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            taos,
            "select version, (expire_time < now) as valid from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition.is_enterprise_edition())
    }

    fn get_edition(&self) -> RawResult<Edition> {
        let taos = self.inner_connection()?;
        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            taos,
            "select version, (expire_time < now) as valid from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    !(expired.trim() == "false" || expired.trim() == "unlimited"),
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TaosBuilder {
    type Target = Taos;

    fn from_dsn<D: taos_query::IntoDsn>(dsn: D) -> RawResult<Self> {
        let mut dsn = dsn.into_dsn()?;

        let lib = if let Some(path) = dsn.params.remove("libraryPath") {
            tracing::trace!("using library path: {path}");
            ApiEntry::dlopen(path).map_err(taos_query::RawError::any)?
        } else {
            tracing::trace!("using default library of taos");
            ApiEntry::open_default().map_err(taos_query::RawError::any)?
        };

        let mut auth = Auth::default();
        if let Some(addr) = dsn.addresses.first() {
            if let Some(host) = &addr.host {
                auth.host.replace(CString::new(host.as_str()).unwrap());
            }
            if let Some(port) = addr.port {
                auth.port = port;
            }
        }
        if let Some(db) = dsn.subject.as_deref() {
            auth.db.replace(CString::new(db).unwrap());
        }
        if let Some(user) = dsn.username.as_deref() {
            auth.user.replace(CString::new(user).unwrap());
        }
        if let Some(pass) = dsn.password.as_deref() {
            auth.pass.replace(CString::new(pass).unwrap());
        }

        let params = &dsn.params;
        if let Some(totp) = params.get("totpCode").or_else(|| params.get("totp_code")) {
            let totp = CString::new(totp.as_str())
                .map_err(|e| RawError::from_string(format!("Invalid totp code: {e}")))?;
            auth.totp.replace(totp);
        }
        if let Some(token) = params
            .get("bearerToken")
            .or_else(|| params.get("bearer_token"))
        {
            let token = CString::new(token.as_str())
                .map_err(|e| RawError::from_string(format!("Invalid bearer token: {e}")))?;
            auth.token.replace(token);
        }

        if let Some(dir) = params.get("configDir") {
            lib.options(types::TSDB_OPTION::ConfigDir, dir);
        }

        lib.options(types::TSDB_OPTION::ShellActivityTimer, "3600");

        if let Some(max_retries) = params.get("maxRetries") {
            auth.max_retries = max_retries.parse().unwrap_or(MAX_CONNECT_RETRIES);
        } else {
            auth.max_retries = MAX_CONNECT_RETRIES;
        }

        Ok(Self {
            auth,
            lib: Arc::new(lib),
            inner_conn: OnceCell::new(),
            server_version: OnceCell::new(),
        })
    }

    fn client_version() -> &'static str {
        "dynamic"
    }

    async fn ping(&self, _: &mut Self::Target) -> RawResult<()> {
        // use taos_query::prelude::AsyncQueryable;
        // conn.query("select server_version()").await?;
        Ok(())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        self.async_connect().await
    }

    async fn server_version(&self) -> RawResult<&str> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = self.async_inner_connection().await?;
            use taos_query::prelude::AsyncQueryable;
            let v: String = AsyncQueryable::query_one(conn, "select server_version()")
                .await?
                .unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) | Err((v, _)) => v.as_str(),
            })
        }
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        let taos = self.async_inner_connection().await?;
        use taos_query::prelude::AsyncQueryable;

        // the latest version of 3.x should work
        let grant: RawResult<Option<(String, bool)>> = time::timeout(
            Duration::from_secs(60),
            AsyncQueryable::query_one(
                taos,
                "select version, (expire_time < now) as valid from information_schema.ins_cluster",
            ),
        )
        .await
        .context("Check cluster edition timeout")?;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> = time::timeout(
                Duration::from_secs(60),
                AsyncQueryable::query_one(taos, "show grants"),
            )
            .await
            .context("Check legacy grants timeout")?;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition.is_enterprise_edition())
    }

    async fn get_edition(&self) -> RawResult<Edition> {
        let taos = self.inner_connection()?;
        use taos_query::prelude::AsyncQueryable;

        // the latest version of 3.x should work
        let grant: RawResult<Option<(String, bool)>> = time::timeout(
            Duration::from_secs(60),
            AsyncQueryable::query_one(
                taos,
                "select version, (expire_time < now) as valid from information_schema.ins_cluster",
            ),
        )
        .await
        .context("Check cluster edition timeout")?;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> = time::timeout(
                Duration::from_secs(60),
                AsyncQueryable::query_one(taos, "show grants"),
            )
            .await
            .context("Check legacy grants timeout")?;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    !(expired.trim() == "false" || expired.trim() == "unlimited"),
                )
            } else {
                warn!("Can't check enterprise edition with either \"show cluster\" or \"show grants\"");
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[derive(Debug)]
pub struct ResultSet {
    raw: RawRes,
    fields: OnceCell<Vec<Field>>,
    summary: UnsafeCell<(usize, usize)>,
    state: Rc<UnsafeCell<BlockState>>,
}

impl ResultSet {
    fn new(raw: RawRes) -> Self {
        Self {
            raw,
            fields: OnceCell::new(),
            summary: UnsafeCell::new((0, 0)),
            state: Rc::new(UnsafeCell::new(BlockState::default())),
        }
    }

    fn precision(&self) -> Precision {
        self.raw.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields.get_or_init(|| self.raw.fetch_fields())
    }

    fn update_summary(&mut self, nrows: usize) {
        let summary = self.summary.get_mut();
        summary.0 += 1;
        summary.1 += nrows;
    }

    pub(crate) fn summary(&self) -> &(usize, usize) {
        unsafe { &*self.summary.get() }
    }

    pub(crate) fn affected_rows(&self) -> i32 {
        self.raw.affected_rows() as _
    }
}

impl taos_query::Fetchable for ResultSet {
    fn affected_rows(&self) -> i32 {
        self.affected_rows()
    }

    fn precision(&self) -> Precision {
        self.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields()
    }

    fn summary(&self) -> (usize, usize) {
        *self.summary()
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows);
    }

    fn fetch_raw_block(&mut self) -> RawResult<Option<RawBlock>> {
        self.raw.fetch_raw_block(self.fields())
    }
}

impl taos_query::AsyncFetchable for ResultSet {
    fn affected_rows(&self) -> i32 {
        self.affected_rows()
    }

    fn precision(&self) -> Precision {
        self.precision()
    }

    fn fields(&self) -> &[Field] {
        self.fields()
    }

    fn summary(&self) -> (usize, usize) {
        *self.summary()
    }

    fn fetch_raw_block(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<RawResult<Option<RawBlock>>> {
        self.raw
            .fetch_raw_block_async(self.fields(), self.precision(), &self.state, cx)
    }

    fn update_summary(&mut self, nrows: usize) {
        self.update_summary(nrows);
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        self.raw.free_result();
    }
}

unsafe impl Send for ResultSet {}
unsafe impl Sync for ResultSet {}

#[cfg(test)]
pub(crate) mod constants {
    // pub const DSN_V2: &str = "taos://localhost:16030?libraryPath=tests/libs/libtaos.so.2.6.0.16";
    // pub const DSN_V3: &str = "taos://localhost:26030?libraryPath=tests/libs/libtaos.so.3.0.1.5";
    pub const DSN_V2: &str = "taos://localhost:6030";
    pub const DSN_V3: &str = "taos://localhost:6030";
}

#[cfg(test)]
mod tests {
    use taos_query::common::{SchemalessPrecision, SchemalessProtocol, SmlDataBuilder};

    use super::*;
    use crate::constants::{DSN_V2, DSN_V3};

    #[test]
    fn show_databases() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set.blocks() {
            let raw = raw?;
            for (col, view) in raw.columns().enumerate() {
                for (row, value) in view.iter().enumerate().take(10) {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate().take(10) {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[test]
    fn long_query() -> RawResult<()> {
        use taos_query::prelude::sync::*;
        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set.blocks() {
            let raw = raw?;
            for (col, view) in raw.columns().enumerate() {
                for (row, value) in view.iter().enumerate().take(10) {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate().take(10) {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn builder_retry_once() -> RawResult<()> {
        use taos_query::prelude::*;

        let builder = TaosBuilder::from_dsn("taos://localhost:6041?maxRetries=1")?;
        assert!(builder.ready().await);

        let res = builder.build().await;
        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn builder_retry_default() -> RawResult<()> {
        use taos_query::prelude::*;

        let builder = TaosBuilder::from_dsn("taos://localhost:6041")?;
        assert!(builder.ready().await);

        let res = builder.build().await;
        assert!(res.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn long_query_async() -> RawResult<()> {
        use taos_query::prelude::*;

        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build().await?;
        let mut set = taos.query("show databases").await?;

        set.blocks()
            .try_for_each_concurrent(10, |block| async move {
                println!("{}", block.pretty_format());
                Ok(())
            })
            .await?;
        println!("summary: {:?}", set.summary());

        let mut set = taos.query("show databases").await?;

        set.rows()
            .try_for_each_concurrent(10, |row| async move {
                println!(
                    "{}",
                    row.map(|(_, value)| value.to_string().unwrap()).join(",")
                );
                Ok(())
            })
            .await?;

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[tokio::test]
    async fn show_databases_async() -> RawResult<()> {
        use taos_query::prelude::*;

        unsafe { std::env::set_var("RUST_LOG", "debug") };

        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build().await?;
        let mut set = taos.query("show databases").await?;

        let mut rows = set.rows();
        let mut nrows = 0;
        while let Some(row) = rows.try_next().await? {
            for (col, (name, value)) in row.enumerate() {
                println!("[{}, {}] (named `{:>4}`): {}", nrows, col, name, value);
            }
            nrows += 1;
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }
    #[tokio::test]
    async fn error_async() -> RawResult<()> {
        use taos_query::prelude::*;

        unsafe { std::env::set_var("RUST_LOG", "debug") };

        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos = builder.build().await?;
        let err = taos
            .exec("create table test.`abc.` (ts timestamp, val int)")
            .await
            .unwrap_err();
        println!("{:?}", err);
        assert!(err.code() == 0x2617);
        let err_str = err.to_string();
        assert!(err_str.contains("0x2617"));
        assert!(err_str.contains("The table name cannot contain '.'"));
        Ok(())
    }

    #[tokio::test]
    async fn error_fetch_async() -> RawResult<()> {
        use taos_query::prelude::*;

        unsafe { std::env::set_var("RUST_LOG", "debug") };

        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos = builder.build().await?;
        let err = taos
            .query("select * from testxxxx.meters")
            .await
            .unwrap_err();

        tracing::trace!("{:?}", err);

        assert!(err.code() == 0x0388);
        let err_str = err.to_string();
        assert!(err_str.contains("0x0388"));
        assert!(err_str.contains("Database not exist"));

        Ok(())
    }

    #[tokio::test]
    async fn error_sync() -> RawResult<()> {
        use taos_query::prelude::sync::*;

        unsafe { std::env::set_var("RUST_LOG", "debug") };

        let builder = TaosBuilder::from_dsn("taos:///")?;
        let taos = builder.build()?;
        let err = taos
            .exec("create table test.`abc.` (ts timestamp, val int)")
            .unwrap_err();

        assert!(err.code() == 0x2617);
        let err_str = err.to_string();
        assert!(err_str.contains("0x2617"));
        assert!(err_str.contains("The table name cannot contain '.'"));
        println!("{:?}", err);

        Ok(())
    }

    #[test]
    fn show_databases_v2() -> RawResult<()> {
        use taos_query::prelude::sync::*;

        let builder = TaosBuilder::from_dsn(crate::constants::DSN_V2)?;
        let taos = builder.build()?;
        let mut set = taos.query("show databases")?;

        for raw in &mut set.blocks() {
            let raw = raw?;
            for (col, view) in raw.columns().enumerate() {
                for (row, value) in view.iter().enumerate().take(10) {
                    println!("Value at (row: {}, col: {}) is: {}", row, col, value);
                }
            }

            for (row, view) in raw.rows().enumerate().take(10) {
                for (col, value) in view.enumerate() {
                    println!("Value at (row: {}, col: {}) is: {:?}", row, col, value);
                }
            }
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[tokio::test]
    async fn show_databases_async_v2() -> RawResult<()> {
        use taos_query::prelude::*;

        let builder = TaosBuilder::from_dsn(DSN_V2)?;
        let taos = builder.build().await?;
        let mut set = taos.query("show databases").await?;

        let mut rows = set.rows();
        let mut nrows = 0;
        while let Some(row) = rows.try_next().await? {
            for (col, (name, value)) in row.enumerate() {
                println!("[{}, {}] (named `{:>4}`): {}", nrows, col, name, value);
            }
            nrows += 1;
        }

        println!("summary: {:?}", set.summary());

        Ok(())
    }

    #[tokio::test]
    async fn exec_async() -> RawResult<()> {
        use taos_query::prelude::*;

        let builder = TaosBuilder::from_dsn(DSN_V3)?;
        let taos = builder.build().await?;
        let affected_rows = taos
            .exec_many([
                "drop database if exists test_exec_async",
                "create database test_exec_async",
                "use test_exec_async",
                "create table test_exec_async.t1 (ts timestamp, val int)",
            ])
            .await?;

        assert_eq!(affected_rows, 0);
        let affected_rows = taos
            .exec("insert into test_exec_async.t1 values(now, 1)")
            .await?;
        assert_eq!(affected_rows, 1);

        let affected_rows = taos
            .exec("insert into test_exec_async.t1 values(now, 2)(now+1s, 3)")
            .await?;
        assert_eq!(affected_rows, 2);

        assert_eq!(taos.exec("drop database test_exec_async").await?, 0);

        Ok(())
    }

    #[test]
    fn test_put_line() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        unsafe { std::env::set_var("RUST_LOG", "taos=debug") };

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        tracing::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless_optin";

        client.exec(format!("drop database if exists {db}"))?;

        client.exec(format!("create database if not exists {db}"))?;

        // should specify database before insert
        client.exec(format!("use {db}"))?;

        let data = [
            "measurement,host=host1 field1=2i,field2=2.0 1577837300000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837400000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837500000",
            "measurement,host=host1 field1=2i,field2=2.0 1577837600000",
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(100u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Line)
            .data(data)
            .req_id(103u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        client.exec(format!("drop database if exists {db}"))?;

        Ok(())
    }

    #[test]
    fn test_put_telnet() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        unsafe { std::env::set_var("RUST_LOG", "taos=debug") };

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        tracing::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless_telnet_optin";

        client.exec(format!("drop database if exists {db}"))?;

        client.exec(format!("create database if not exists {db}"))?;

        // should specify database before insert
        client.exec(format!("use {db}"))?;

        let data = [
            "meters.current 1648432611249 10.3 location=California.SanFrancisco group=2",
            "meters.current 1648432611250 12.6 location=California.SanFrancisco group=2",
            "meters.current 1648432611249 10.8 location=California.LosAngeles group=3",
            "meters.current 1648432611250 11.3 location=California.LosAngeles group=3",
            "meters.voltage 1648432611249 219 location=California.SanFrancisco group=2",
            "meters.voltage 1648432611250 218 location=California.SanFrancisco group=2",
            "meters.voltage 1648432611249 221 location=California.LosAngeles group=3",
            "meters.voltage 1648432611250 217 location=California.LosAngeles group=3",
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(100u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .req_id(101u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Telnet)
            .data(data)
            .req_id(103u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        client.exec(format!("drop database if exists {db}"))?;

        Ok(())
    }

    #[test]
    fn test_put_json() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        unsafe { std::env::set_var("RUST_LOG", "taos=debug") };

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        tracing::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_schemaless_json_optin";

        client.exec(format!("drop database if exists {db}"))?;

        client.exec(format!("create database if not exists {db}"))?;

        // should specify database before insert
        client.exec(format!("use {db}"))?;

        // SchemalessProtocol::Json
        let data = [
            r#"[{"metric": "meters.current", "timestamp": 1681345954000, "value": 10.3, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611249, "value": 219, "tags": {"location": "California.LosAngeles", "groupid": 1}}, {"metric": "meters.current", "timestamp": 1648432611250, "value": 12.6, "tags": {"location": "California.SanFrancisco", "groupid": 2}}, {"metric": "meters.voltage", "timestamp": 1648432611250, "value": 221, "tags": {"location": "California.LosAngeles", "groupid": 1}}]"#
        ]
        .map(String::from)
        .to_vec();

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .precision(SchemalessPrecision::Millisecond)
            .data(data.clone())
            .ttl(1000)
            .req_id(300u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .ttl(1000)
            .req_id(301u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .req_id(302u64)
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        let sml_data = SmlDataBuilder::default()
            .protocol(SchemalessProtocol::Json)
            .data(data.clone())
            .build()?;
        assert_eq!(client.put(&sml_data)?, ());

        client.exec(format!("drop database if exists {db}"))?;

        Ok(())
    }

    #[test]
    fn test_error_details() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        unsafe { std::env::set_var("RUST_LOG", "taos=debug") };

        let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
        tracing::debug!("dsn: {:?}", &dsn);

        let client = TaosBuilder::from_dsn(dsn)?.build()?;

        let db = "test_tmq_err_details";

        client.exec(format!("drop database if exists {db}"))?;

        client.exec(format!("create database if not exists {db}"))?;

        // should specify database before insert
        client.exec(format!("use {db}"))?;

        client.exec("create table t1 (ts timestamp, val int)")?;

        let views = vec![
            ColumnView::from_millis_timestamp(vec![164000000000]),
            ColumnView::from_bools(vec![true]),
        ];
        let mut block = RawBlock::from_views(&views, Precision::Millisecond);
        block.with_table_name("t1");

        let err = client.write_raw_block(&block).unwrap_err();
        dbg!(&err);

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_blob() -> anyhow::Result<()> {
        use serde::Deserialize;
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec_many([
            "drop database if exists test_1753066385",
            "create database test_1753066385",
            "use test_1753066385",
            "create table t0(ts timestamp, c1 blob)",
            "insert into t0 values(1752218982761, null)",
            "insert into t0 values(1752218982762, '')",
            "insert into t0 values(1752218982763, 'hello')",
            "insert into t0 values(1752218982764, '\\x12345678')",
        ])?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: i64,
            c1: Option<Vec<u8>>,
        }

        let records: Vec<Record> = taos
            .query("select * from t0")?
            .deserialize()
            .try_collect()?;

        assert_eq!(records.len(), 4);

        assert_eq!(records[0].ts, 1752218982761);
        assert_eq!(records[1].ts, 1752218982762);
        assert_eq!(records[2].ts, 1752218982763);
        assert_eq!(records[3].ts, 1752218982764);

        assert_eq!(records[0].c1, None);
        assert_eq!(records[1].c1, Some(vec![]));
        assert_eq!(records[2].c1, Some(vec![0x68, 0x65, 0x6C, 0x6C, 0x6F]));
        assert_eq!(records[3].c1, Some(vec![0x12, 0x34, 0x56, 0x78]));

        taos.exec("drop database test_1753066385")?;

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[test]
    fn test_blob_all_types() -> anyhow::Result<()> {
        use bytes::Bytes;
        use serde::Deserialize;
        use taos_query::prelude::sync::*;
        use taos_query::util::hex::hex_string_to_bytes;

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec_many([
            "drop database if exists test_1752220387",
            "create database test_1752220387",
            "use test_1752220387",
            "create table t0 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, \
                c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, \
                c9 bigint unsigned, c10 float, c11 double, c12 varchar(10), c13 nchar(10), \
                c14 varbinary(10), c15 geometry(50), c16 decimal(10, 5), c17 decimal(20, 5), \
                c18 blob)",
            "insert into t0 values (1741780784752, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1.1, 1.1, \
                'hello', 'hello', 'hello', 'POINT(1 1)', 12345.12345, 123456789012345.12345, \
                '\\x12345678')",
            "insert into t0 values (1741780784753, null, null, null, null, null, null, \
                null, null, null, null, null, null, null, null, null, null, null, null)",
        ])?;

        #[derive(Debug, Deserialize)]
        struct Record {
            ts: i64,
            c1: Option<bool>,
            c2: Option<i8>,
            c3: Option<i16>,
            c4: Option<i32>,
            c5: Option<i64>,
            c6: Option<u8>,
            c7: Option<u16>,
            c8: Option<u32>,
            c9: Option<u64>,
            c10: Option<f32>,
            c11: Option<f64>,
            c12: Option<String>,
            c13: Option<String>,
            c14: Option<Bytes>,
            c15: Option<Bytes>,
            c16: Option<String>,
            c17: Option<String>,
            c18: Option<Bytes>,
        }

        let records: Vec<Record> = taos
            .query("select * from t0")?
            .deserialize()
            .try_collect()?;

        assert_eq!(records.len(), 2);

        assert_eq!(records[0].ts, 1741780784752);
        assert_eq!(records[1].ts, 1741780784753);

        assert_eq!(records[0].c1, Some(true));
        assert_eq!(records[1].c1, None);

        assert_eq!(records[0].c2, Some(1));
        assert_eq!(records[1].c2, None);

        assert_eq!(records[0].c3, Some(1));
        assert_eq!(records[1].c3, None);

        assert_eq!(records[0].c4, Some(1));
        assert_eq!(records[1].c4, None);

        assert_eq!(records[0].c5, Some(1));
        assert_eq!(records[1].c5, None);

        assert_eq!(records[0].c6, Some(1));
        assert_eq!(records[1].c6, None);

        assert_eq!(records[0].c7, Some(1));
        assert_eq!(records[1].c7, None);

        assert_eq!(records[0].c8, Some(1));
        assert_eq!(records[1].c8, None);

        assert_eq!(records[0].c9, Some(1));
        assert_eq!(records[1].c9, None);

        assert_eq!(records[0].c10, Some(1.1));
        assert_eq!(records[1].c10, None);

        assert_eq!(records[0].c11, Some(1.1));
        assert_eq!(records[1].c11, None);

        assert_eq!(records[0].c12, Some("hello".to_string()));
        assert_eq!(records[1].c12, None);

        assert_eq!(records[0].c13, Some("hello".to_string()));
        assert_eq!(records[1].c13, None);

        assert_eq!(records[0].c14, Some(Bytes::from("hello")));
        assert_eq!(records[1].c14, None);

        assert_eq!(
            records[0].c15,
            Some(hex_string_to_bytes(
                "0101000000000000000000f03f000000000000f03f"
            ))
        );
        assert_eq!(records[1].c15, None);

        assert_eq!(records[0].c16, Some("12345.12345".to_string()));
        assert_eq!(records[1].c16, None);

        assert_eq!(records[0].c17, Some("123456789012345.12345".to_string()));
        assert_eq!(records[1].c17, None);

        assert_eq!(records[0].c18, Some(Bytes::from("\x12\x34\x56\x78")));
        assert_eq!(records[1].c18, None);

        taos.exec("drop database test_1752220387")?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_connect_with_totp() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;
        use taos_query::util::totp::*;

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;

        let totp_seed = generate_totp_seed(64);
        taos.exec(format!(
            "create user nts_totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"
        ))?;

        let totp_secret = generate_totp_secret(totp_seed.as_bytes());
        let totp_code = generate_totp_code(&totp_secret);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nts_totp_user:totp_pass_1@localhost:6030?totp_code={totp_code}"
        ))?
        .build()?;

        let mut rs = taost.query("select 1")?;
        let rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nts_totp_user:totp_pass_1@localhost:6030?totpCode={totp_code}"
        ))?
        .build()?;

        let mut rs = taost.query("select 1")?;
        let rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user nts_totp_user")?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[test]
    fn test_connect_with_token() -> anyhow::Result<()> {
        use taos_query::prelude::sync::*;

        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?.build()?;
        taos.exec("create user nts_token_user pass 'token_pass_1'")?;

        let mut rs = taos.query("create token test_nts_bearer_token from user nts_token_user")?;
        let rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows.len(), 1);
        let token = &rows[0];

        let taost = TaosBuilder::from_dsn(format!("taos://localhost:6030?bearer_token={token}"))?
            .build()?;

        let mut rs = taost.query("select 1")?;
        let rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!("taos://localhost:6030?bearer_token={token}"))?
            .build()?;

        let mut rs = taost.query("select 1")?;
        let rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost =
            TaosBuilder::from_dsn(format!("taos://localhost:6030?bearerToken={token}"))?.build()?;

        let mut rs = taost.query("select 1")?;
        let rows: Vec<String> = rs.deserialize().try_collect()?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user nts_token_user")?;

        Ok(())
    }
}

#[cfg(feature = "test-enterprise")]
#[cfg(test)]
mod totp_async_tests {
    use taos_query::prelude::*;
    use taos_query::util::totp::*;

    use super::*;

    #[tokio::test]
    async fn test_connect_with_totp() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec("drop user nt_totp_user").await.ok();

        let totp_seed = generate_totp_seed(64);
        taos.exec(format!(
            "create user nt_totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"
        ))
        .await?;

        let totp_secret = generate_totp_secret(totp_seed.as_bytes());
        let totp_code = generate_totp_code(&totp_secret);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nt_totp_user:totp_pass_1@localhost:6030?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nt_totp_user:totp_pass_1@localhost:6030?totpCode={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user nt_totp_user").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_with_totp_twice() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec("drop user nt_tw_totp_user").await.ok();

        let totp_seed = generate_totp_seed(64);
        taos.exec(format!(
            "create user nt_tw_totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"
        ))
        .await?;

        let totp_secret = generate_totp_secret(totp_seed.as_bytes());
        let totp_code = generate_totp_code(&totp_secret);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nt_tw_totp_user:totp_pass_1@localhost:6030?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nt_tw_totp_user:totp_pass_1@localhost:6030?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user nt_tw_totp_user").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_with_invalid_totp_code() -> anyhow::Result<()> {
        let err = TaosBuilder::from_dsn("taos://localhost:6030?totp_code=xxx")?
            .build()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Invalid TOTP code"));
        Ok(())
    }

    #[tokio::test]
    async fn test_connect_with_token() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec("drop user nt_token_user").await.ok();
        taos.exec("create user nt_token_user pass 'token_pass_1'")
            .await?;

        let mut rs = taos
            .query("create token test_nt_bearer_token from user nt_token_user")
            .await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = &rows[0];

        let taost = TaosBuilder::from_dsn(format!("taos://localhost:6030?bearer_token={token}"))?
            .build()
            .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!("taos://localhost:6030?bearer_token={token}"))?
            .build()
            .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!("taos://localhost:6030?bearerToken={token}"))?
            .build()
            .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user nt_token_user").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_with_invalid_token() -> anyhow::Result<()> {
        let err = TaosBuilder::from_dsn("taos://localhost:6030?bearer_token=xxx")?
            .build()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Invalid token"));
        Ok(())
    }

    #[tokio::test]
    async fn test_connect_with_totp_and_token() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("taos://localhost:6030")?
            .build()
            .await?;

        taos.exec("drop user nt_tt_totp_user").await.ok();

        let totp_seed = generate_totp_seed(255);
        taos.exec(format!(
            "create user nt_tt_totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"
        ))
        .await?;

        let totp_secret = generate_totp_secret(totp_seed.as_bytes());
        let totp_code = generate_totp_code(&totp_secret);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nt_tt_totp_user:totp_pass_1@localhost:6030?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user nt_tt_token_user").await.ok();
        taos.exec("create user nt_tt_token_user pass 'token_pass_1'")
            .await?;

        let mut rs = taos
            .query("create token test_nt_tt_bearer_token from user nt_tt_token_user")
            .await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = &rows[0];

        let taost = TaosBuilder::from_dsn(format!("taos://localhost:6030?bearer_token={token}"))?
            .build()
            .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nt_tt_token_user:token_pass_1@localhost:6030?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "taos://nt_tt_totp_user:totp_pass_1@localhost:6030?bearer_token={token}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
             "taos://nt_tt_totp_user:totp_pass_1@localhost:6030?totp_code={totp_code}&bearer_token={token}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec_many(["drop user nt_tt_totp_user", "drop user nt_tt_token_user"])
            .await?;

        Ok(())
    }
}
