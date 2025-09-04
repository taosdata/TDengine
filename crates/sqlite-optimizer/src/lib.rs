use std::{
    fmt::{Display, Formatter},
    path::Path,
};

use sqlx::{sqlite::SqlitePool, Row};
use tokio::fs;

pub struct SqliteOptimizer {
    pub path: String,
    pub pool: SqlitePool,
}

impl SqliteOptimizer {
    /// 打开数据库连接
    ///
    /// 如果数据库不存在，则返回 Ok(None)
    ///
    /// # Errors
    ///
    /// 如果数据库连接失败，则返回 Err
    pub async fn open(path: &str) -> Result<Option<Self>, sqlx::Error> {
        let exists = match fs::metadata(path).await {
            Ok(metadata) => metadata.is_file(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(e.into()),
        };
        if exists {
            let database_url = format!("sqlite:{}", path);
            let pool = SqlitePool::connect(&database_url).await?;
            Ok(Some(Self {
                path: path.into(),
                pool,
            }))
        } else {
            Ok(None)
        }
    }

    /// 获取数据库文件大小信息
    pub async fn get_database_size(&self) -> anyhow::Result<DatabaseSizeInfo> {
        let db_metadata = fs::metadata(&self.path).await?;
        let db_size = db_metadata.len();

        // 检查 WAL 文件
        let wal_path = format!("{}-wal", self.path);
        let wal_size = if Path::new(&wal_path).exists() {
            fs::metadata(&wal_path).await?.len()
        } else {
            0
        };

        // 检查 SHM 文件
        let shm_path = format!("{}-shm", self.path);
        let shm_size = if Path::new(&shm_path).exists() {
            fs::metadata(&shm_path).await?.len()
        } else {
            0
        };

        Ok(DatabaseSizeInfo {
            db_size,
            wal_size,
            shm_size,
            total_size: db_size + wal_size + shm_size,
        })
    }

    /// 执行 WAL 检查点操作
    pub async fn wal_checkpoint(&self) -> Result<WalCheckpointResult, sqlx::Error> {
        let row = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .fetch_one(&self.pool)
            .await?;

        let busy = row.get::<i64, _>(0);
        let log = row.get::<i64, _>(1);
        let checkpointed = row.get::<i64, _>(2);

        Ok(WalCheckpointResult {
            busy,
            log,
            checkpointed,
        })
    }

    /// 执行 VACUUM 操作
    pub async fn vacuum(&self) -> Result<(), sqlx::Error> {
        sqlx::query("VACUUM").execute(&self.pool).await?;
        Ok(())
    }

    /// 执行优化操作
    pub async fn pragma_optimize(&self) -> Result<(), sqlx::Error> {
        sqlx::query("PRAGMA optimize").execute(&self.pool).await?;
        Ok(())
    }

    /// 检查数据库完整性
    pub async fn integrity_check(&self) -> Result<String, sqlx::Error> {
        let row = sqlx::query("PRAGMA integrity_check")
            .fetch_one(&self.pool)
            .await?;

        Ok(row.get::<String, _>(0))
    }

    /// 获取数据库统计信息
    pub async fn get_database_stats(&self) -> Result<DatabaseStats, sqlx::Error> {
        let page_count: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(&self.pool)
            .await?;

        let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
            .fetch_one(&self.pool)
            .await?;

        let freelist_count: i64 = sqlx::query_scalar("PRAGMA freelist_count")
            .fetch_one(&self.pool)
            .await?;

        Ok(DatabaseStats {
            page_count,
            page_size,
            freelist_count,
            used_pages: page_count - freelist_count,
            database_size_bytes: page_count * page_size,
            free_space_bytes: freelist_count * page_size,
        })
    }

    /// 完整的数据库优化流程
    pub async fn optimize(&self) -> anyhow::Result<OptimizationReport> {
        tracing::info!("Start optimizing...");

        // 获取优化前的大小
        let size_before = self.get_database_size().await?;
        tracing::info!(
            "Before optimizing: {:.2} MB",
            size_before.total_size as f64 / 1024.0 / 1024.0
        );

        // 1. WAL 检查点
        tracing::debug!("Executing WAL checkpoint...");
        let checkpoint_result = self.wal_checkpoint().await?;
        tracing::debug!("WAL checkpoint completed: {:?}", checkpoint_result);

        // 2. VACUUM 操作
        tracing::debug!("Executing VACUUM...");
        self.vacuum().await?;
        tracing::debug!("VACUUM completed");

        // 3. 优化操作
        tracing::debug!("Executing optimization...");
        self.pragma_optimize().await?;
        tracing::debug!("Optimization completed");

        // 4. 完整性检查
        tracing::debug!("Checking database integrity...");
        let integrity_result = self.integrity_check().await?;
        tracing::debug!("Integrity check result: {}", integrity_result);

        // 获取优化后的大小和统计信息
        let size_after = self.get_database_size().await?;
        let stats = self.get_database_stats().await?;

        let space_saved = size_before.total_size.saturating_sub(size_after.total_size);
        let space_saved_percentage = if size_before.total_size > 0 {
            (space_saved as f64 / size_before.total_size as f64) * 100.0
        } else {
            0.0
        };

        tracing::info!(
            "After optimized: {:.2} MB, space saved: {:.2} MB ({:.1}%)",
            size_after.total_size as f64 / 1024.0 / 1024.0,
            space_saved as f64 / 1024.0 / 1024.0,
            space_saved_percentage
        );

        Ok(OptimizationReport {
            size_before,
            size_after,
            checkpoint_result,
            integrity_check: integrity_result,
            stats,
            space_saved,
            space_saved_percentage,
        })
    }
}

#[derive(Debug)]
pub struct DatabaseSizeInfo {
    pub db_size: u64,
    pub wal_size: u64,
    pub shm_size: u64,
    pub total_size: u64,
}

#[derive(Debug)]
pub struct WalCheckpointResult {
    pub busy: i64,
    pub log: i64,
    pub checkpointed: i64,
}

#[derive(Debug)]
pub struct DatabaseStats {
    pub page_count: i64,
    pub page_size: i64,
    pub freelist_count: i64,
    pub used_pages: i64,
    pub database_size_bytes: i64,
    pub free_space_bytes: i64,
}

#[derive(Debug)]
pub struct OptimizationReport {
    pub size_before: DatabaseSizeInfo,
    pub size_after: DatabaseSizeInfo,
    pub checkpoint_result: WalCheckpointResult,
    pub integrity_check: String,
    pub stats: DatabaseStats,
    pub space_saved: u64,
    pub space_saved_percentage: f64,
}

impl Display for OptimizationReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            writeln!(f, "== Sqlite Optimizer Report ==")?;
            writeln!(f, "Before:")?;
            writeln!(
                f,
                "  Database Size: {:.2} MB",
                self.size_before.db_size as f64 / 1024.0 / 1024.0
            )?;
            writeln!(
                f,
                "  WAL Size: {:.2} MB",
                self.size_before.wal_size as f64 / 1024.0 / 1024.0
            )?;
            writeln!(
                f,
                "  SHM Size: {:.2} MB",
                self.size_before.shm_size as f64 / 1024.0 / 1024.0
            )?;
            writeln!(
                f,
                "  Total Size: {:.2} MB",
                self.size_before.total_size as f64 / 1024.0 / 1024.0
            )?;
            writeln!(f, "After:")?;
            writeln!(
                f,
                "  Database Size: {:.2} MB",
                self.size_after.db_size as f64 / 1024.0 / 1024.0
            )?;
            writeln!(
                f,
                "  WAL Size: {:.2} MB",
                self.size_after.wal_size as f64 / 1024.0 / 1024.0
            )?;
            writeln!(
                f,
                "  SHM Size: {:.2} MB",
                self.size_after.shm_size as f64 / 1024.0 / 1024.0
            )?;
            writeln!(
                f,
                "  Total Size: {:.2} MB",
                self.size_after.total_size as f64 / 1024.0 / 1024.0
            )?;

            writeln!(
                f,
                "Space Saved: {:.2} MB ({:.1}%)",
                self.space_saved as f64 / 1024.0 / 1024.0,
                self.space_saved_percentage
            )?;

            writeln!(f, "Stats:")?;
            writeln!(f, "  Page Count: {}", self.stats.page_count)?;
            writeln!(f, "  Page Size: {} bytes", self.stats.page_size)?;
            writeln!(f, "  Free List Count: {}", self.stats.freelist_count)?;
            writeln!(f, "  Used Pages: {}", self.stats.used_pages)?;
            writeln!(
                f,
                "  Database Size: {} bytes",
                self.stats.database_size_bytes
            )?;
            writeln!(f, "  Free Space: {} bytes", self.stats.free_space_bytes)?;

            writeln!(f, "Integrity Check: {}", self.integrity_check)?;
            Ok(())
        } else {
            write!(
	            f,
	            "Integrate check: {}, before: {:.2}+{:.2}+{:.2}={:.2} MB, after: {:.2}+{:.2}+{:.2}={:.2} MB, saved: {:.2} MB ({:.1}%)",
	            self.integrity_check,
	            self.size_before.db_size as f64 / 1024.0 / 1024.0,
	            self.size_before.wal_size as f64 / 1024.0 / 1024.0,
	            self.size_before.shm_size as f64 / 1024.0 / 1024.0,
							self.size_before.total_size as f64 / 1024.0 / 1024.0,
	            self.size_after.db_size as f64 / 1024.0 / 1024.0,
	            self.size_after.wal_size as f64 / 1024.0 / 1024.0,
	            self.size_after.shm_size as f64 / 1024.0 / 1024.0,
	            self.size_after.total_size as f64 / 1024.0 / 1024.0,
	            self.space_saved as f64 / 1024.0 / 1024.0,
	            self.space_saved_percentage
	        )
        }
    }
}
#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqliteConnectOptions;

    use super::*;
    #[tokio::test]
    async fn test_optimizer() -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();

        let db_path = "./non-exist.db";

        async fn create_db(db_path: &'static str) -> anyhow::Result<()> {
            let options = SqliteConnectOptions::new()
                .create_if_missing(true)
                .filename(db_path);
            let pool = SqlitePool::connect_with(options).await?;
            sqlx::query("create table if not exists tb1(id int, val double)")
                .execute(&pool)
                .await?;
            Ok(())
        }
        create_db(db_path).await?;

        // let db_path = "tests/taosx.db"; // try a exist db
        assert!(fs::try_exists(db_path).await.unwrap_or_default());

        // Create optimizer instance
        let optimizer = SqliteOptimizer::open(db_path)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Database file {} not found", db_path))?;

        // Execute full optimization
        let report = optimizer.optimize().await?;

        println!("{}", report);

        println!("=== Report ===");
        println!("{:#}", report);

        let _ = fs::remove_file(db_path).await;
        Ok(())
    }
}
