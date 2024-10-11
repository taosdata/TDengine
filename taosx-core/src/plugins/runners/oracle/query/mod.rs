use std::time::Duration;

use crate::runners::oracle::appender;
use crate::runners::oracle::config::connect::ConnectConfig;
use arrow::record_batch::RecordBatch;
use linked_hash_map::LinkedHashMap;
use oracle::{
    pool::{Pool, PoolBuilder},
    sql_type::OracleType,
};

#[derive(Clone)]
pub struct OracleQuery {
    pub pool: Pool,
    time_zone: String,
}

impl OracleQuery {
    pub fn try_new(config: ConnectConfig, time_zone: String) -> anyhow::Result<Self> {
        let pool = Self::connect(
            &config.host,
            config.port,
            &config.subject,
            &config.username,
            &config.password,
            time_zone.clone(),
        )
        .map_err(|err| {
            anyhow::anyhow!("failed to connect to oracle, cause: {}", err.to_string())
        })?;
        Ok(Self { pool, time_zone })
    }

    fn connect(
        host: &String,
        port: u16,
        subject: &String,
        username: &String,
        password: &String,
        _time_zone: String,
    ) -> anyhow::Result<Pool> {
        let addr = format!("//{}:{}/{}", host, port, subject);
        let mut pool_builder = PoolBuilder::new(username, password, addr);
        // connection pool settings
        pool_builder.min_connections(5);
        pool_builder.max_connections(20);
        pool_builder.timeout(Duration::from_secs(20))?;
        // TODO timezone
        Ok(pool_builder.build()?)
    }

    pub fn get_conn(&self) -> anyhow::Result<oracle::Connection> {
        for i in 1..4 {
            match self.pool.get() {
                Ok(conn) => {
                    // modify session timezone
                    let _ = conn.execute(
                        format!("ALTER SESSION SET TIME_ZONE='{}'", self.time_zone).as_str(),
                        &[],
                    )?;
                    conn.commit()?;
                    return Ok(conn);
                }
                Err(err) => {
                    tracing::warn!(
                        "migrate oracle, failed to get connection from pool, cause: {}, retrying {i} times...",
                        err
                    );
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
        anyhow::bail!("migrate oracle, failed to get connection from pool")
    }

    pub fn select_distinct_values(
        &mut self,
        sql: &str,
    ) -> anyhow::Result<(LinkedHashMap<String, OracleType>, Vec<oracle::Row>)> {
        let conn = self.get_conn()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        let mut rows = Vec::new();
        match result {
            Ok(rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
                for row in rs {
                    match row {
                        Ok(row) => {
                            rows.push(row);
                        }
                        Err(err) => {
                            anyhow::bail!(
                                "failed to select distinct values, cause: {}",
                                err.to_string()
                            )
                        }
                    }
                }
            }
            Err(err) => anyhow::bail!(
                "failed to select distinct values, cause: {}",
                err.to_string()
            ),
        }
        Ok((col_map, rows))
    }

    pub fn select_for_schema(
        &mut self,
        sql: &str,
    ) -> anyhow::Result<LinkedHashMap<String, OracleType>> {
        let conn = self.get_conn()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        match result {
            Ok(rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
        Ok(col_map)
    }

    #[allow(dead_code)]
    pub fn select_all(
        &mut self,
        sql: &str,
    ) -> anyhow::Result<(LinkedHashMap<String, OracleType>, Vec<oracle::Row>)> {
        let conn = self.get_conn()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        let mut rows = Vec::new();
        match result {
            Ok(rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
                for row in rs {
                    match row {
                        Ok(row) => {
                            rows.push(row);
                        }
                        Err(err) => {
                            anyhow::bail!("failed to select data, cause: {}", err.to_string())
                        }
                    }
                }
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
        Ok((col_map, rows))
    }

    pub fn select_all_and_to_record_batches(
        &mut self,
        sql: &str,
        batch_size: usize,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let conn = self.get_conn()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        let mut rows = Vec::new();
        match result {
            Ok(rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
                for row in rs {
                    match row {
                        Ok(row) => {
                            rows.push(row);
                        }
                        Err(err) => {
                            anyhow::bail!("failed to select data, cause: {}", err.to_string())
                        }
                    }
                }
                let batch =
                    appender::to_record_batches(col_map, rows, batch_size, self.time_zone.clone())?;
                Ok(batch)
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
        // Ok((col_map, rows))
    }

    pub fn top_n(
        &mut self,
        sql: &str,
        top_n: u32,
    ) -> anyhow::Result<(LinkedHashMap<String, OracleType>, Vec<oracle::Row>)> {
        let conn = self.get_conn()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        let mut rows = Vec::new();
        match result {
            Ok(rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
                for row in rs {
                    match row {
                        Ok(row) => {
                            if rows.len() >= top_n as usize {
                                break;
                            }
                            rows.push(row);
                        }
                        Err(err) => {
                            anyhow::bail!("failed to select data, cause: {}", err.to_string())
                        }
                    }
                }
            }
            Err(err) => anyhow::bail!("failed to select data, cause: {}", err.to_string()),
        }
        Ok((col_map, rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use taos::Dsn;

    fn test_create_table() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                let sql_create_table = "create table t_metric (id NUMBER(10, 0) PRIMARY KEY, name VARCHAR2(255), value NUMBER(10, 2), ts timestamp)";
                let x = conn.execute(sql_create_table, &[]);
                println!("create table: {:?}", x);
                let y = conn.commit();
                println!("commit: {:?}", y);
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    fn test_insert_data(len: usize) {
        test_create_table();

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                for i in 0..len {
                    let sql_insert_data = format!("insert into t_metric (id, name, value, ts) values ({}, 'cpu', 0.8, sysdate)", i);
                    let _ = conn.execute(sql_insert_data.as_str(), &[]);
                }
                let _ = conn.commit();
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    fn test_clear_data() {
        test_create_table();

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(query) => {
                let conn = query.get_conn().unwrap();
                let sql = "delete from t_metric where 1 = 1";
                let _ = conn.execute(sql, &[]);
                let _ = conn.commit();
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }

    #[ignore]
    #[test]
    fn test_connect() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1");
        let config = ConnectConfig::from_dsn(&dsn.unwrap()).unwrap();
        dbg!(&config);

        let query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();
        assert!(query.get_conn().is_ok());
    }

    #[ignore]
    #[test]
    fn test_select_distinct_values() {
        // prepare data
        test_create_table();
        test_insert_data(7);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let (col_map, rows) = query
            .select_distinct_values("select distinct name,value from t_metric")
            .unwrap();
        dbg!(&col_map);
        dbg!(&rows);
        // clear data
        test_clear_data();
    }

    #[ignore]
    #[test]
    fn test_select_for_schema() {
        // prepare data
        test_create_table();
        test_insert_data(1);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(mut query) => {
                let query_result = query.select_for_schema("select * from t_metric");
                match query_result {
                    Ok(col_map) => {
                        dbg!(&col_map);
                        assert_eq!(col_map.len(), 4);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        test_clear_data();
    }

    #[ignore]
    #[test]
    fn test_select_all() {
        // prepare data
        test_create_table();
        test_insert_data(7);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(mut query) => {
                let query_result = query.select_all("select * from t_metric");
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(col_map);
                        dbg!(&rows);
                        assert_eq!(rows.len(), 7);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        test_clear_data();
    }

    #[ignore]
    #[test]
    fn test_select_all_and_to_record_batches() {
        // prepare data
        test_create_table();
        test_insert_data(7);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(mut query) => {
                let query_result =
                    query.select_all_and_to_record_batches("select * from t_metric", 3);
                match query_result {
                    Ok(batches) => {
                        dbg!(&batches);
                        assert_eq!(batches.len(), 3);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        test_clear_data();
    }

    #[ignore]
    #[test]
    fn test_top_n() {
        // prepare data
        test_create_table();
        test_insert_data(3);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+08:00"));
        match result {
            Ok(mut query) => {
                let query_result = query.top_n("select * from t_metric", 5);
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(col_map);
                        dbg!(&rows);
                        assert_eq!(rows.len(), 3);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
        // clear data
        test_clear_data();
    }

    #[ignore]
    #[test]
    fn test_top_n_with_tz() {
        // prepare data
        test_create_table();
        test_insert_data(3);

        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();

        let result = OracleQuery::try_new(config, String::from("+06:00"));
        match result {
            Ok(mut query) => {
                let query_result = query.top_n("select * from t_metric", 5);
                match query_result {
                    Ok((col_map, rows)) => {
                        dbg!(col_map);
                        dbg!(&rows);
                        assert_eq!(rows.len(), 3);
                    }
                    Err(e) => {
                        println!("error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {:?}", e);
            }
        }
    }
}
