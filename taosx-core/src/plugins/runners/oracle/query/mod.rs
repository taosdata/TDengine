use crate::runners::oracle::appender;
use crate::runners::oracle::config::connect::ConnectConfig;
use arrow::record_batch::RecordBatch;
use linked_hash_map::LinkedHashMap;
use oracle::{
    pool::{Pool, PoolBuilder},
    sql_type::OracleType,
};

pub struct OracleQuery {
    pool: Pool,
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
        let pool_builder = PoolBuilder::new(username, password, addr);
        // TODO timezone
        Ok(pool_builder.build()?)
    }

    pub fn select_for_schema(
        &mut self,
        sql: &str,
    ) -> anyhow::Result<LinkedHashMap<String, OracleType>> {
        let conn = self.pool.get()?;
        // modify session timezone
        let _ = conn.execute(
            format!("ALTER SESSION SET TIME_ZONE='{}'", self.time_zone).as_str(),
            &[],
        )?;
        let _ = conn.commit()?;
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
        let conn = self.pool.get()?;
        // modify session timezone
        let _ = conn.execute(
            format!("ALTER SESSION SET TIME_ZONE='{}'", self.time_zone).as_str(),
            &[],
        )?;
        let _ = conn.commit()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        let mut rows = Vec::new();
        match result {
            Ok(mut rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
                while let Some(row) = rs.next() {
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
        let conn = self.pool.get()?;
        // modify session timezone
        let _ = conn.execute(
            format!("ALTER SESSION SET TIME_ZONE='{}'", self.time_zone).as_str(),
            &[],
        )?;
        let _ = conn.commit()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        let mut rows = Vec::new();
        match result {
            Ok(mut rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
                while let Some(row) = rs.next() {
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
        let conn = self.pool.get()?;
        // modify session timezone
        let _ = conn.execute(
            format!("ALTER SESSION SET TIME_ZONE='{}'", self.time_zone).as_str(),
            &[],
        )?;
        let _ = conn.commit()?;
        // select data
        let result = conn.query(sql, &[]);
        let mut col_map = LinkedHashMap::new();
        let mut rows = Vec::new();
        match result {
            Ok(mut rs) => {
                let cols = rs.column_info();
                for col in cols {
                    col_map.insert(col.name().to_string(), col.oracle_type().clone());
                }
                while let Some(row) = rs.next() {
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

    #[test]
    #[ignore]
    fn test_connect() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1");
        let config = ConnectConfig::from_dsn(&dsn.unwrap()).unwrap();
        dbg!(&config);

        let query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();
        assert!(query.pool.get().is_ok());
    }

    #[test]
    #[ignore]
    fn test_select_for_schema() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let col_map = query.select_for_schema("select * from TEST").unwrap();
        dbg!(col_map);
    }

    #[test]
    #[ignore]
    fn test_select_all() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let (col_map, rows) = query.select_all("select * from TEST").unwrap();
        dbg!(col_map);
        dbg!(rows);
    }

    #[test]
    #[ignore]
    fn test_select_all_and_to_record_batches() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let batches = query
            .select_all_and_to_record_batches("select * from TEST", 2)
            .unwrap();
        dbg!(batches);
    }

    #[test]
    #[ignore]
    fn test_top_n() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+08:00")).unwrap();

        let (col_map, rows) = query.top_n("select * from TEST", 1).unwrap();
        dbg!(col_map);
        dbg!(&rows);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    #[ignore]
    fn test_top_n_with_tz() {
        let dsn = Dsn::from_str("oracle://test_user:123456@192.168.1.40:1521/ORCLPDB1").unwrap();
        let config = ConnectConfig::from_dsn(&dsn).unwrap();
        let mut query = OracleQuery::try_new(config, String::from("+06:00")).unwrap();

        let (col_map, rows) = query.top_n("select * from TEST", 1).unwrap();
        dbg!(col_map);
        dbg!(&rows);
        assert_eq!(rows.len(), 1);
    }
}
