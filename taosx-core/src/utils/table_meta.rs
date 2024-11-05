use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, Taos, TaosBuilder};
use taos::{Dsn, TryStreamExt};

#[derive(Debug, Clone, Serialize)]
pub enum TableType {
    // #[serde(rename = "CHILD_TABLE")]
    ChildTable,
    // #[serde(rename = "SUPER_TABLE")]
    SuperTable,
    // #[serde(rename = "NORMAL_TABLE")]
    NormalTable,
    // #[serde(rename = "SYSTEM_TABLE")]
    SystemTable,
}

impl<'de> Deserialize<'de> for TableType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TableType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl FromStr for TableType {
    type Err = anyhow::Error;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name.to_uppercase().as_str() {
            "CHILD_TABLE" => Ok(TableType::ChildTable),
            "SUPER_TABLE" => Ok(TableType::SuperTable),
            "NORMAL_TABLE" => Ok(TableType::NormalTable),
            "SYSTEM_TABLE" => Ok(TableType::SystemTable),
            _ => Err(anyhow::anyhow!("invalid table type: {}", name)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub tbname: String,
    pub stable: Option<String>,
    pub db_name: String,
    pub r#type: TableType,
    pub comment: Option<String>,
    // pub create_time: u64,
    pub columns: Option<Vec<ColumnMeta>>,
    pub tags: Option<Vec<TagMeta>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub field: String,
    pub r#type: String,
    pub length: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagMeta {
    pub field: String,
    pub r#type: String,
    pub length: u32,
    pub note: Option<String>,
    /// 超级表没有 tag value, 子表有
    pub value: Option<String>,
}

pub struct TableMetaQueryBuilder {
    dsn: Dsn,
}

impl TableMetaQueryBuilder {
    pub fn new<D: IntoDsn>(dsn: D) -> anyhow::Result<Self> {
        let dsn = dsn.into_dsn()?;
        Ok(Self { dsn })
    }

    pub async fn build(&self) -> anyhow::Result<TableMetaQuerier> {
        let querier = TableMetaQuerier::from_dsn(&self.dsn).await?;

        let dbname = querier.load_current_database().await?;
        let super_tables = querier.load_stables(dbname.as_str()).await.ok();
        let child_tables = querier.load_child_tables(dbname.as_str()).await.ok();

        Ok(TableMetaQuerier {
            loaded: true,
            taos: querier.taos,
            current_db: Some(dbname),
            super_tables,
            child_tables,
        })
    }
}

pub struct TableMetaQuerier {
    loaded: bool,
    taos: Taos,
    current_db: Option<String>,
    super_tables: Option<Vec<TableMeta>>,
    child_tables: Option<Vec<TableMeta>>,
}

impl TableMetaQuerier {
    /// return super table meta by stable name
    pub async fn get_stable_meta(&self, stable: &str) -> anyhow::Result<Option<&TableMeta>> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(stables) = self.super_tables.as_ref() {
            return Ok(stables.iter().find(|t| t.tbname == stable));
        }

        Ok(None)
    }

    /// return child table meta by table name
    pub async fn get_child_table_meta(&self, tbname: &str) -> anyhow::Result<Option<&TableMeta>> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(child_tables) = self.child_tables.as_ref() {
            return Ok(child_tables.iter().find(|t| t.tbname == tbname));
        }

        Ok(None)
    }

    /// return true if tbname is a child table of stable
    pub async fn is_child_of_stable(&self, stable: &str, tbname: &str) -> anyhow::Result<bool> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(child_tables) = self.child_tables.as_ref() {
            return Ok(child_tables
                .iter()
                .any(|t| t.stable == Some(stable.to_string()) && t.tbname == tbname));
        }

        Ok(false)
    }

    /// return true if col_name is an existed column of stable
    pub fn is_stable_column_exist(&self, stable: &str, col_name: &str) -> anyhow::Result<bool> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(stables) = self.super_tables.as_ref() {
            if let Some(stable) = stables.iter().find(|t| t.tbname == stable) {
                if let Some(columns) = stable.columns.as_ref() {
                    return Ok(columns.iter().any(|c| c.field == col_name));
                }
            }
        }

        Ok(false)
    }

    /// return true if tag_name and tag_type match the tag of stable in database
    pub fn is_stable_tag_exist(
        &self,
        stable: &str,
        tag_name: &str,
        tag_type: &str,
    ) -> anyhow::Result<bool> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(stables) = self.super_tables.as_ref() {
            if let Some(stable) = stables.iter().find(|t| t.tbname == stable) {
                if let Some(tags) = stable.tags.as_ref() {
                    return Ok(tags.iter().any(|t| {
                        t.field.eq_ignore_ascii_case(tag_name)
                            && t.r#type.eq_ignore_ascii_case(tag_type)
                    }));
                }
            }
        }

        Ok(false)
    }

    /// 从 DSN 创建一个 TableMetaQuerier, 没有加载任何 meta
    async fn from_dsn<D: IntoDsn>(dsn: D) -> anyhow::Result<Self> {
        let dsn = dsn.into_dsn()?;
        // dbg!(&dsn);
        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;

        Ok(Self {
            loaded: false,
            taos,
            current_db: None,
            super_tables: None,
            child_tables: None,
        })
    }

    /// get current database
    pub fn current_database(&self) -> anyhow::Result<Option<String>> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        Ok(self.current_db.as_ref().map(|s| s.to_string()))
    }

    /// Short for 'select database()'
    async fn load_current_database(&self) -> anyhow::Result<String> {
        let sql = "select database() as db_name";
        tracing::debug!("sql: {}", sql);

        let dbname: String = self
            .taos
            .query_one(sql)
            .await?
            .ok_or(anyhow::anyhow!("failed to get current database"))?;

        if dbname.is_empty() {
            bail!("failed to get current database");
        }

        Ok(dbname)
    }

    /// get table meta of all stable
    async fn load_stables(&self, dbname: &str) -> anyhow::Result<Vec<TableMeta>> {
        let sql = format!(
            "select {},{},{},{},{} from information_schema.ins_stables where db_name = '{}' order by `tbname`",
            "stable_name as `tbname`",
            "stable_name as `stable`",
            "db_name",
            "table_comment as `comment`",
            "'SUPER_TABLE' as type",
            dbname
        );
        tracing::debug!("sql: {}", sql);

        let mut stable_meta_vec: Vec<TableMeta> = self
            .taos
            .query(sql)
            .await?
            .deserialize()
            .try_collect()
            .await?;
        if stable_meta_vec.is_empty() {
            bail!("no stable found in database: {}", dbname);
        }

        for stable_meta in stable_meta_vec.iter_mut() {
            let columns = self
                .load_columns(&stable_meta.db_name, &stable_meta.tbname)
                .await?;
            stable_meta.columns = Some(columns);

            let tags = self
                .load_tags_of_super_table(&stable_meta.db_name, &stable_meta.tbname)
                .await?;
            stable_meta.tags = Some(tags);
        }

        Ok(stable_meta_vec)
    }

    /// get columns of a table
    async fn load_columns(&self, dbname: &str, tbname: &str) -> anyhow::Result<Vec<ColumnMeta>> {
        let sql = format!(
            "select {},{},{} from information_schema.ins_columns where db_name = '{}' and table_name = '{}'",
            "col_name as `field`",
            "col_type as `type`",
            "col_length as `length`",
            dbname,
            tbname,
        );
        tracing::debug!("sql: {}", sql);

        let columns: Vec<ColumnMeta> = self
            .taos
            .query(sql)
            .await?
            .deserialize()
            .try_collect()
            .await?;

        Ok(columns)
    }

    /// 查超级表的 tag 元数据，超级表的 tag 只有 tag_name 和 tag_type，没有 tag_value
    /// 在 information_schema.ins_tags 中只有子表的 tag schema，没有超级表的
    async fn load_tags_of_super_table(
        &self,
        dbname: &str,
        stable: &str,
    ) -> anyhow::Result<Vec<TagMeta>> {
        let sql = format!("describe `{}`.`{}`", dbname, stable);
        tracing::debug!("sql: {}", sql);

        let tags: Vec<TagMeta> = self
            .taos
            .query(sql)
            .await?
            .deserialize()
            .try_collect()
            .await?;

        Ok(tags
            .iter()
            .filter(|t| match t.note.as_ref() {
                None => false,
                Some(note) => note == "TAG",
            })
            .map(|t| t.clone())
            .collect_vec())
    }

    /// 查子表的 tag 元数据，子表的 tag 有 tag_name, tag_type 和 tag_value
    async fn load_tags_of_child_table(
        &self,
        dbname: &str,
        tbname: &str,
    ) -> anyhow::Result<Vec<TagMeta>> {
        let sql = format!(
            "select {},{},{} from information_schema.ins_tags where db_name = '{}' and table_name = '{}'",
            "tag_name as `field`", "tag_type as `type`", "tag_value as `value`", dbname, tbname
        );
        tracing::debug!("sql: {}", sql);

        let tags: Vec<TagMeta> = self
            .taos
            .query(sql)
            .await?
            .deserialize()
            .try_collect()
            .await?;

        Ok(tags)
    }

    /// 查所有子表的元数据
    async fn load_child_tables(&self, dbname: &str) -> anyhow::Result<Vec<TableMeta>> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::clear_database;
    use taos::{AsyncQueryable, AsyncTBuilder, TaosBuilder};

    async fn prepare_data(dsn: &str, db_name: &str) {
        let taos = TaosBuilder::from_dsn(dsn).unwrap().build().await.unwrap();

        taos.exec_many(vec![
            format!("drop database if exists `{}`", db_name),
            format!("create database `{}`", db_name),
            format!("use `{}`", db_name),
            "create table `Stb1`(ts timestamp, val int) tags(t int)".to_string(),
            "create table `TB_1_1` using `Stb1` tags(1)".to_string(),
            "create table `TB_1_2` using `Stb1` tags(2)".to_string(),
            "create stable `Stb2`(ts timestamp, val int) tags(t int)".to_string(),
            "create table `TB_2_1` using `Stb2` tags(3)".to_string(),
            "create table `TB_2_2` using `Stb2` tags(4)".to_string(),
            "create table ttt (ts timestamp, val int)".to_string(),
        ])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_get_stables() {
        // given
        // let dsn = "taos:///";
        let dsn = "taos+ws://192.168.0.201:6041/";
        let db_name = "taosx_core_utils_table_meta";
        prepare_data(dsn, db_name).await;

        // when
        let dsn = format!("{dsn}{db_name}").into_dsn().unwrap();
        // dbg!(&dsn);
        let querier = TableMetaQuerier::from_dsn(&dsn).await.unwrap();
        let stables = querier.load_stables(db_name).await;

        println!("{:?}", stables);

        let stables = stables.unwrap();

        // then
        assert_eq!(stables.len(), 2);
        let stb1 = stables
            .iter()
            .find(|t| t.stable == Some("Stb1".to_string()))
            .unwrap();
        let columns = stb1.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        let tags = stb1.tags.as_ref().unwrap();
        assert_eq!(tags.len(), 1);

        clear_database(&dsn).await.unwrap();
    }
}
