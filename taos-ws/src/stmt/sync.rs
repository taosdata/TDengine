use tokio::runtime::Runtime;

pub use super::*;

pub struct WsSyncStmtClient {
    rt: Arc<Runtime>,
    client: WsStmtClient,
}

impl WsSyncStmtClient {
    pub(crate) fn new(info: &TaosBuilder, rt: Arc<Runtime>) -> Result<Self> {
        let client = rt.block_on(WsStmtClient::from_wsinfo(info))?;
        Ok(Self { rt, client })
    }

    pub fn stmt_init(&self) -> Result<WsSyncStmt> {
        let stmt = self.rt.block_on(self.client.stmt_init())?;
        Ok(WsSyncStmt {
            rt: self.rt.clone(),
            stmt,
            affected_rows: 0,
        })
    }
}

pub struct WsSyncStmt {
    rt: Arc<Runtime>,
    stmt: WsAsyncStmt,
    affected_rows: usize,
}

impl WsSyncStmt {
    pub fn prepare(&mut self, sql: &str) -> Result<()> {
        self.affected_rows = 0;
        self.rt.block_on(self.stmt.prepare(sql))
    }

    pub fn set_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.stmt.set_timeout(timeout);
        self
    }
    pub fn add_batch(&mut self) -> Result<()> {
        self.rt.block_on(self.stmt.add_batch())
    }
    pub fn bind(&mut self, columns: Vec<serde_json::Value>) -> Result<()> {
        self.rt.block_on(self.stmt.bind(columns))
    }

    /// Call bind and add batch.
    pub fn bind_all(&mut self, columns: Vec<serde_json::Value>) -> Result<()> {
        self.rt.block_on(self.stmt.bind_all(columns))
    }

    pub fn set_tbname(&mut self, name: &str) -> Result<()> {
        self.rt.block_on(self.stmt.set_tbname(name))
    }

    pub fn set_tags(&mut self, tags: Vec<serde_json::Value>) -> Result<()> {
        self.rt.block_on(self.stmt.set_tags(tags))
    }

    pub fn set_tbname_tags(&mut self, name: &str, tags: Vec<serde_json::Value>) -> Result<()> {
        self.set_tbname(name)?;
        self.set_tags(tags)
    }

    pub fn exec(&mut self) -> Result<usize> {
        let rows = self.rt.block_on(self.stmt.exec())?;
        self.affected_rows += rows;
        Ok(rows)
    }

    pub fn affected_rows(&mut self) -> usize {
        self.affected_rows
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use taos_query::{Dsn, TBuilder};

    
    #[test]
    fn test_stmt_stable() -> anyhow::Result<()> {
        
        use taos_query::Queryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;
        dbg!(&dsn);
        let taos = crate::sync::WsClient::from_dsn(&dsn)?;

        taos.exec("drop database if exists stmt_s")?;
        taos.exec("create database stmt_s")?;
        taos.exec("create table stmt_s.stb (ts timestamp, v int) tags(t1 binary(100))")?;

        std::env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();

        let mut stmt = taos.stmt_init()?;

        stmt.prepare("insert into ? using stmt_s.stb tags(?) values(?, ?)")?;

        stmt.set_tbname("stmt_s.tb1")?;

        // stmt.set_tags(vec![json!({"name": "value"})]).await?;

        // stmt.set_tags(vec![json!(json!({"name": "value"}))])?;
        stmt.set_tags(vec![json!(r#"{"a":"b"}"#)])?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])?;
        let res = stmt.exec()?;

        assert_eq!(res, 2);
        taos.exec("drop database stmt_s")?;
        Ok(())
    }

    #[test]
    fn test_stmt_table() -> anyhow::Result<()> {
        
        use taos_query::Queryable;

        let dsn = Dsn::try_from("taos://localhost:6041")?;
        dbg!(&dsn);
        let taos = crate::sync::WsClient::from_dsn(&dsn)?;

        taos.exec("drop database if exists stmt_c")?;
        taos.exec("create database stmt_c")?;
        taos.exec("create table stmt_c.tb1 (ts timestamp, v int)")?;

        std::env::set_var("RUST_LOG", "debug");
        pretty_env_logger::init();

        let mut stmt = taos.stmt_init()?;

        stmt.prepare("insert into ? values(?, ?)")?;

        stmt.set_tbname("stmt_c.`tb1`")?;

        stmt.bind_all(vec![
            json!([
                "2022-06-07T11:02:44.022450088+08:00",
                "2022-06-07T11:02:45.022450088+08:00"
            ]),
            json!([2, 3]),
        ])?;
        let res = stmt.exec()?;

        assert_eq!(res, 2);
        taos.exec("drop database stmt_c")?;
        Ok(())
    }
}
