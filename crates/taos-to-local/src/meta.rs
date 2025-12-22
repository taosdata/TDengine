use std::{collections::BTreeMap, fmt::Display};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use taos::{taos_query::common::Describe, *};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub db_meta: DbMeta,
    pub metas: Vec<MetaUnit>,
}

impl Schema {
    pub fn meta_create_iter(&self) -> impl Iterator<Item = &MetaCreate> {
        self.metas.iter().filter_map(|m: &MetaUnit| {
            if let MetaUnit::Create(mc) = m {
                return Some(mc);
            }
            None
        })
    }

    pub async fn stable_count(&self, taos: &Taos, db: &str, stb: &str) -> anyhow::Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM `{db}`.`{stb}`");
        let count: i64 = taos
            .query_one(sql)
            .await
            .context(format!("failed to count stable '{stb}'"))?
            .unwrap_or(0);
        Ok(count)
    }
}

pub async fn fetch_database_meta(taos: &Taos, db: &str) -> anyhow::Result<DbMeta> {
    let sql = format!("SELECT * FROM information_schema.ins_databases WHERE name = '{db}'");
    let db_meta: Option<DbMeta> = taos.query_one(sql).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to fetch database meta from information_schema, cause: {:?}",
            e
        )
    })?;
    let db_meta = db_meta
        .ok_or_else(|| anyhow::anyhow!("database '{}' not found in information_schema", db))?;

    Ok(db_meta)
}

pub async fn fetch_tables_meta(
    taos: &Taos,
    db: &str,
    stables: &[String],
) -> anyhow::Result<Vec<MetaUnit>> {
    taos.exec(format!("USE `{db}`")).await?;

    let mut metas = vec![];
    // 超级表
    let stable_metas = fetch_stables_meta(taos, db, stables).await?;
    metas.extend(stable_metas);

    // 子表
    let child_metas = fetch_ctables_meta(taos, db, stables).await?;
    metas.extend(child_metas);

    // 普通表
    let normal_metas = fetch_ntables_meta(taos, db).await?;
    metas.extend(normal_metas);

    Ok(metas)
}

pub async fn fetch_stables_meta(
    taos: &Taos,
    db: &str,
    stables: &[String],
) -> anyhow::Result<Vec<MetaUnit>> {
    let mut metas = vec![];

    let sql = format!("SHOW `{db}`.stables");
    tracing::debug!("fetch super tables meta sql: {}", sql);
    let mut res = taos
        .query(sql)
        .await
        .context("failed to fetch super tables meta")?;
    let mut rows = res.deserialize::<String>();
    while let Some(stb) = rows
        .try_next()
        .await
        .context("failed to fetch super tables meta")?
    {
        // 过滤不需要的超级表
        if !stables.is_empty() && !stables.contains(&stb) {
            continue;
        }

        let describe = taos
            .describe(&stb)
            .await
            .context(format!("failed to describe stable '{stb}'"))?;

        let mut meta_unit: serde_json::Value = json!({
            "type": "create",
            "tableType": "super",
            "tableName": stb,
            "columns": [],
            "tags": []
        });

        // fill columns' name and type
        for col in describe.iter() {
            let col_json = json!({
                "name": col.field(),
                "type": col.ty(),
                "length": col.length(),
                "compression": col.compression,
            });
            if col.is_tag() {
                meta_unit["tags"].as_array_mut().unwrap().push(col_json);
            } else {
                meta_unit["columns"].as_array_mut().unwrap().push(col_json);
            }
        }

        metas.push(serde_json::from_value(meta_unit)?);
    }

    Ok(metas)
}

async fn fetch_ctables_meta(
    taos: &Taos,
    db: &str,
    stables: &[String],
) -> anyhow::Result<Vec<MetaUnit>> {
    // 缓存超级表的 Describe
    let mut tag_cache: BTreeMap<String, Describe> = BTreeMap::new();
    let sql = format!("SHOW `{db}`.stables");
    tracing::debug!("fetch super tables for tag cache, sql: {}", sql);
    let mut res = taos
        .query(sql)
        .await
        .context("failed to fetch super tables for tag cache")?;
    let mut rows = res.deserialize::<String>();
    while let Some(stb) = rows
        .try_next()
        .await
        .context("failed to fetch super tables for tag cache")?
    {
        // 过滤不需要的超级表
        if !stables.is_empty() && !stables.contains(&stb) {
            continue;
        }
        let describe = taos
            .describe(&stb)
            .await
            .context(format!("failed to describe stable '{stb}' for tag cache"))?;
        tag_cache.insert(stb.clone(), describe);
    }

    // 查所有子表的 schema
    let mut ctb_meta_map: BTreeMap<String, Vec<(String, serde_json::Value, Describe)>> =
        BTreeMap::new();
    let sql = format!(
        "SELECT stable_name,table_name FROM information_schema.ins_tables WHERE db_name = '{db}' AND type = 'CHILD_TABLE' ORDER BY stable_name,table_name"
    );
    tracing::debug!("fetch child tables meta, sql: {}", sql);
    let mut res = taos
        .query(sql)
        .await
        .context("failed to fetch child tables meta")?;
    let mut rows = res.deserialize::<(String, String)>();
    while let Some((stb, ctb)) = rows
        .try_next()
        .await
        .context("failed to fetch child tables meta")?
    {
        // 过滤不需要的超级表
        if !stables.is_empty() && !stables.contains(&stb) {
            continue;
        }

        tracing::debug!(
            "fetch child table meta, stable: {}, child table: {}",
            stb,
            ctb
        );
        let ctb_desc = tag_cache
            .get(&stb)
            .cloned()
            .ok_or(anyhow::anyhow!("stable '{}' not found in tag cache", stb))?;

        let mut meta_unit = json!({
            "type": "create",
            "tableType": "child",
            "tableName": ctb,
            "using": stb,
            "tagNum": ctb_desc.iter().filter(|c| c.is_tag()).count(),
            "tags": [],
        });

        // tag 列的类型
        for col in ctb_desc.iter().filter(|c| c.is_tag()) {
            let col_json = json!({
                "name": col.field(),
                "type": col.ty(),
                "length": col.length(),
            });
            meta_unit["tags"].as_array_mut().unwrap().push(col_json);
        }

        if ctb_meta_map.contains_key(&stb) {
            ctb_meta_map
                .get_mut(&stb)
                .unwrap()
                .push((ctb, meta_unit, ctb_desc));
        } else {
            ctb_meta_map.insert(stb.clone(), vec![(ctb, meta_unit, ctb_desc)]);
        }
    }

    // 查所有子表的 tag 值
    let stable_names = ctb_meta_map.keys().cloned().collect::<Vec<_>>();
    for stb in stable_names {
        let stb_desc = taos.describe(&stb).await.context(format!(
            "failed to describe stable '{stb}' to get tag value",
        ))?;
        let sql = format!(
            "SELECT tags {},tbname FROM `{stb}` ORDER BY tbname",
            stb_desc
                .iter()
                .filter(|c| c.is_tag())
                .map(|c| c.field())
                .join(",")
        );
        tracing::debug!("fetch child tables' tag values, sql: {}", sql);
        let mut res = taos
            .query(sql)
            .await
            .context("failed to fetch child tables tag values, query child tables")?;

        let mut rows = res.rows();
        let mut ctb_idx = 0;
        while let Some(row) = rows
            .try_next()
            .await
            .context("failed to fetch child tables tag values, iter child tables")?
        {
            let (_ctb, meta_unit, desc) = &mut ctb_meta_map.get_mut(&stb).unwrap()[ctb_idx];
            // println!("stb: {}, meta_unit: {:?}, desc: {:?}", stb, meta_unit, desc);

            for (tag_idx, (_name, val)) in row.enumerate() {
                if tag_idx == desc.iter().filter(|c| c.is_tag()).count() {
                    // 最后一个字段是 tbname
                    break;
                }

                let tag_col = &mut meta_unit["tags"].as_array_mut().unwrap()[tag_idx];
                tag_col["value"] = to_json_value(val);
            }

            ctb_idx += 1;
        }
    }

    let mut metas = vec![];
    for (_stb, ctbs) in ctb_meta_map {
        for (_ctb, meta_unit, _desc) in ctbs {
            let meta_unit =
                serde_json::from_value(meta_unit).context("failed to serialize meta unit")?;
            metas.push(meta_unit);
        }
    }

    Ok(metas)
}

fn to_json_value(val: BorrowedValue) -> Value {
    match val {
        BorrowedValue::Null(_) => Value::Null,
        BorrowedValue::Bool(b) => Value::Bool(b),
        BorrowedValue::TinyInt(i) => Value::Number(i.into()),
        BorrowedValue::SmallInt(i) => Value::Number(i.into()),
        BorrowedValue::Int(i) => Value::Number(i.into()),
        BorrowedValue::BigInt(i) => Value::Number(i.into()),
        BorrowedValue::VarChar(s) => Value::String(s.to_string()),
        BorrowedValue::Timestamp(ts) => Value::Number(ts.as_raw_i64().into()),
        BorrowedValue::NChar(s) => Value::String(s.to_string()),
        BorrowedValue::UTinyInt(i) => Value::Number(i.into()),
        BorrowedValue::USmallInt(i) => Value::Number(i.into()),
        BorrowedValue::UInt(i) => Value::Number(i.into()),
        BorrowedValue::UBigInt(i) => Value::Number(i.into()),
        BorrowedValue::Float(f) => serde_json::Number::from_f64(f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        BorrowedValue::Double(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        BorrowedValue::Json(s) => {
            // s is Cow<'_, [u8]>; try to parse JSON from bytes, otherwise fall back to UTF-8 string
            match serde_json::from_slice::<Value>(s.as_ref()) {
                Ok(v) => v,
                Err(_) => Value::String(String::from_utf8_lossy(s.as_ref()).into_owned()),
            }
        }
        BorrowedValue::VarBinary(b) => {
            Value::String(String::from_utf8_lossy(b.as_ref()).into_owned())
        }
        _ => unimplemented!("unsupported type: {:?}", val),
    }
}

async fn fetch_ntables_meta(taos: &Taos, db: &str) -> anyhow::Result<Vec<MetaUnit>> {
    let mut metas = vec![];
    let sql = format!(
        "SELECT table_name FROM information_schema.ins_tables WHERE db_name = '{db}' AND type = 'NORMAL_TABLE'"
    );
    let mut res = taos
        .query(sql)
        .await
        .context("failed to fetch normal tables meta from information_schema")?;
    let mut rows = res.deserialize::<String>();
    while let Some(ntb) = rows
        .try_next()
        .await
        .context("failed to fetch normal tables meta")?
    {
        let describe = taos
            .describe(&ntb)
            .await
            .context(format!("failed to describe normal table '{ntb}'"))?;

        let mut meta_unit = json!({
            "type": "create",
            "tableType": "normal",
            "tableName": ntb,
            "columns": [],
            "tags": []
        });
        for col in describe.iter() {
            let col_json = json!({
                "name": col.field(),
                "type": col.ty(),
                "length": col.length(),
                "compression": col.compression,
            });
            meta_unit["columns"].as_array_mut().unwrap().push(col_json);
        }

        metas.push(serde_json::from_value(meta_unit)?);
    }
    Ok(metas)
}

/*         field              |          type          |   length    |        note        |
=============================================================================================
 name                           | VARCHAR                |          64 |                    |
 create_time                    | TIMESTAMP              |           8 |                    |
 vgroups                        | INT                    |           4 |                    |
 ntables                        | BIGINT                 |           8 |                    |
 replica                        | TINYINT                |           1 |                    |
 strict                         | VARCHAR                |           4 |                    |
 duration                       | VARCHAR                |          10 |                    |
 keep                           | VARCHAR                |          32 |                    |
 buffer                         | INT                    |           4 |                    |
 pagesize                       | INT                    |           4 |                    |
 pages                          | INT                    |           4 |                    |
 minrows                        | INT                    |           4 |                    |
 maxrows                        | INT                    |           4 |                    |
 comp                           | TINYINT                |           1 |                    |
 precision                      | VARCHAR                |           2 |                    |
 status                         | VARCHAR                |          10 |                    |
 retentions                     | VARCHAR                |          60 |                    |
 single_stable                  | BOOL                   |           1 |                    |
 cachemodel                     | VARCHAR                |          11 |                    |
 cachesize                      | INT                    |           4 |                    |
 wal_level                      | TINYINT                |           1 |                    |
 wal_fsync_period               | INT                    |           4 |                    |
 wal_retention_period           | INT                    |           4 |                    |
 wal_retention_size             | BIGINT                 |           8 |                    |
 stt_trigger                    | SMALLINT               |           2 |                    |
 table_prefix                   | SMALLINT               |           2 |                    |
 table_suffix                   | SMALLINT               |           2 |                    |
 tsdb_pagesize                  | INT                    |           4 |                    |
 keep_time_offset               | INT                    |           4 |                    |
 ss_chunkpages                  | INT                    |           4 |                    |
 ss_keeplocal                   | VARCHAR                |          10 |                    |
 ss_compact                     | TINYINT                |           1 |                    |
 with_arbitrator                | TINYINT                |           1 |                    |
 encrypt_algorithm              | VARCHAR                |          16 |                    |
 compact_interval               | VARCHAR                |          12 |                    |
 compact_time_range             | VARCHAR                |          24 |                    |
 compact_time_offset            | VARCHAR                |           4 |                    |
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbMeta {
    pub name: String,
    pub create_time: i64,                  // TIMESTAMP(8)
    pub ntables: i64,                      // BIGINT(8)
    pub strict: String,                    // VARCHAR(4)
    pub status: String,                    // VARCHAR(10)
    pub retentions: Option<String>,        // VARCHAR(60)
    pub ss_chunkpages: Option<i32>,        // INT(4), available since TDengine v3.3.8
    pub ss_keeplocal: Option<String>,      // VARCHAR(10), available since TDengine v3.3.8
    pub ss_compact: Option<i8>,            // TINYINT(1), available since TDengine v3.3.8
    pub with_arbitrator: Option<i8>,       // TINYINT(1)
    pub encrypt_algorithm: Option<String>, // VARCHAR(16)
    #[serde(flatten)]
    pub opts: Option<DatabaseOptions>, // database options
}

impl Display for DbMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE DATABASE IF NOT EXISTS `{}`", self.name)?;

        if let Some(o) = &self.opts {
            write!(f, " BUFFER {}", o.buffer)?;
            write!(f, " CACHESIZE {}", o.cachesize)?;
            write!(f, " CACHEMODEL '{}'", o.cachemodel)?;
            write!(f, " COMP {}", o.comp)?;
            write!(f, " DURATION {}", o.duration)?;
            write!(f, " WAL_FSYNC_PERIOD {}", o.wal_fsync_period)?;
            write!(f, " MAXROWS {}", o.maxrows)?;
            write!(f, " MINROWS {}", o.minrows)?;
            write!(f, " STT_TRIGGER {}", o.stt_trigger)?;
            write!(f, " KEEP {}", o.keep)?;
            write!(f, " PAGES {}", o.pages)?;
            write!(f, " PAGESIZE {}", o.pagesize)?;
            write!(f, " PRECISION '{}'", o.precision)?;
            write!(f, " REPLICA {}", o.replica)?;
            write!(f, " WAL_LEVEL {}", o.wal_level)?;
            write!(f, " VGROUPS {}", o.vgroups)?;
            write!(f, " SINGLE_STABLE {}", if o.single_stable { 1 } else { 0 })?;
            write!(f, " TABLE_PREFIX {}", o.table_prefix)?;
            write!(f, " TABLE_SUFFIX {}", o.table_suffix)?;
            write!(f, " TSDB_PAGESIZE {}", o.tsdb_pagesize)?;
            write!(f, " WAL_RETENTION_PERIOD {}", o.wal_retention_period)?;
            write!(f, " WAL_RETENTION_SIZE {}", o.wal_retention_size)?;
            write!(f, " KEEP_TIME_OFFSET {}", o.keep_time_offset)?;
            write!(f, " COMPACT_INTERVAL {}", o.compact_interval)?;
            write!(f, " COMPACT_TIME_RANGE {}", o.compact_time_range)?;
            write!(f, " COMPACT_TIME_OFFSET {}", o.compact_time_offset)?;
        }

        if let Some(v) = &self.retentions {
            write!(f, " RETENTIONS '{}'", v)?;
        }
        if let Some(v) = &self.with_arbitrator {
            write!(f, " WITH_ARBITRATOR {}", v)?;
        }
        if let Some(v) = &self.encrypt_algorithm {
            write!(f, " ENCRYPT_ALGORITHM '{}'", v)?;
        }
        if let Some(v) = self.ss_chunkpages {
            write!(f, " SS_CHUNKPAGES {}", v)?;
        }
        if let Some(v) = &self.ss_keeplocal {
            write!(f, " SS_KEEPLOCAL {}", v)?;
        }
        if let Some(v) = self.ss_compact {
            write!(f, " SS_COMPACT {}", v)?;
        }

        Ok(())
    }
}

/*
database_option: {
    VGROUPS value
  | PRECISION {'ms' | 'us' | 'ns'}
  | REPLICA value
  | BUFFER value
  | PAGES value
  | PAGESIZE  value
  | CACHEMODEL {'none' | 'last_row' | 'last_value' | 'both'}
  | CACHESIZE value
  | COMP {0 | 1 | 2}
  | DURATION value
  | MAXROWS value
  | MINROWS value
  | KEEP value
  | KEEP_TIME_OFFSET value
  | STT_TRIGGER value
  | SINGLE_STABLE {0 | 1}
  | TABLE_PREFIX value
  | TABLE_SUFFIX value
  | DNODES value
  | TSDB_PAGESIZE value
  | WAL_LEVEL {1 | 2}
  | WAL_FSYNC_PERIOD value
  | WAL_RETENTION_PERIOD value
  | WAL_RETENTION_SIZE value
  | COMPACT_INTERVAL value
  | COMPACT_TIME_RANGE value
  | COMPACT_TIME_OFFSET value
}
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseOptions {
    vgroups: i32,
    precision: String,
    replica: i8,
    buffer: i32,
    pages: i32,
    pagesize: i32,
    cachemodel: String,
    cachesize: i32,
    comp: i8,
    duration: String,
    maxrows: i32,
    minrows: i32,
    keep: String,
    keep_time_offset: i32,
    stt_trigger: i16,
    single_stable: bool,
    table_prefix: i16,
    table_suffix: i16,
    // dnodes: Option<i32>, 这个参数在 information_schema.ins_databases 中没有
    tsdb_pagesize: i32,
    wal_level: i8,
    wal_fsync_period: i32,
    wal_retention_period: i32,
    wal_retention_size: i64,
    compact_interval: String,
    compact_time_range: String,
    compact_time_offset: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_fetch_database_meta() {
        let dsn = "taos+ws://192.168.2.139:6041".into_dsn().unwrap();
        let taos = TaosBuilder::from_dsn(&dsn).unwrap().build().await.unwrap();

        taos.exec_many(vec!["CREATE DATABASE IF NOT EXISTS zyyang"])
            .await
            .unwrap();

        let db_meta = fetch_database_meta(&taos, "zyyang").await.unwrap();
        println!("db_meta: {:#?}", db_meta);
        println!("create db sql: {}", db_meta);

        let res = taos.exec(db_meta.to_string()).await;
        assert!(res.is_ok());
    }

    #[ignore]
    #[tokio::test]
    async fn test_fetch_table_meta() {
        let dsn = "taos+ws://192.168.2.139:6041".into_dsn().unwrap();
        let taos = TaosBuilder::from_dsn(&dsn).unwrap().build().await.unwrap();

        let schema = fetch_tables_meta(&taos, "zyyang", &[]).await.unwrap();
        schema.iter().for_each(|m| {
            // println!("{:#?}", m);
            println!("{m};");
        });
    }
}
