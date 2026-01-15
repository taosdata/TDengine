use anyhow::bail;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use taos::{AsyncFetchable, AsyncQueryable, AsyncTBuilder, IntoDsn, Taos, TaosBuilder};
use taos::{Dsn, TryStreamExt};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Serialize, PartialEq)]
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
    /// 超级表的 tbname 和 stable 相同
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
    /// 超级表有 length，但子表没有，子表的length 在 type中
    pub length: Option<u32>,
    /// 超级表有 note，但子表没有
    pub note: Option<String>,
    /// 超级表没有 tag value, 但子表有
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
        let mut table_metas = querier.load_super_tables(dbname.as_str()).await?;
        let child_tables = querier.load_tables(dbname.as_str()).await?;
        table_metas.extend(child_tables);

        Ok(TableMetaQuerier {
            loaded: true,
            taos: querier.taos,
            current_db: Some(dbname),
            table_metas: Some(table_metas),
        })
    }
}

pub struct TableMetaQuerier {
    taos: Taos,
    /// 是否已加载 schema
    loaded: bool,
    /// 当前数据库
    current_db: Option<String>,
    /// 所有超级表/子表/普通表的 TableMeta
    table_metas: Option<Vec<TableMeta>>,
}

impl TableMetaQuerier {
    /// return super table meta by stable name
    pub fn super_table_meta(&self, stable: &str) -> anyhow::Result<Option<&TableMeta>> {
        self.table_meta_with_filter(|t| t.r#type == TableType::SuperTable && t.tbname == stable)
    }

    /// return child table meta by tbname
    pub fn child_table_meta(&self, tbname: &str) -> anyhow::Result<Option<&TableMeta>> {
        self.table_meta_with_filter(|t| t.r#type == TableType::ChildTable && t.tbname == tbname)
    }

    /// return normal table meta by tbname
    pub fn normal_table_meta(&self, tbname: &str) -> anyhow::Result<Option<&TableMeta>> {
        self.table_meta_with_filter(|t| t.r#type == TableType::NormalTable && t.tbname == tbname)
    }

    /// return table meta with filter
    pub fn table_meta_with_filter<F>(&self, filter: F) -> anyhow::Result<Option<&TableMeta>>
    where
        F: Fn(&TableMeta) -> bool,
    {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(tables) = self.table_metas.as_ref() {
            return Ok(tables.iter().find(|t| filter(t)));
        }

        Ok(None)
    }

    /// return true if tbname is a child table of stable
    pub fn is_child_of_stable(&self, stable: &str, tbname: &str) -> anyhow::Result<bool> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(child_tables) = self.table_metas.as_ref() {
            return Ok(child_tables
                .iter()
                .any(|t| t.stable == Some(stable.to_string()) && t.tbname == tbname));
        }

        Ok(false)
    }

    /// return true if col_name is an existed column of stable
    pub fn is_stable_col_exist(&self, stable: &str, col_name: &str) -> anyhow::Result<bool> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        if let Some(stables) = self.super_table_meta(stable)?
            && let Some(columns) = stables.columns.as_ref()
        {
            return Ok(columns.iter().any(|c| c.field == col_name));
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

        if let Some(stable) = self.super_table_meta(stable)?
            && let Some(tags) = stable.tags.as_ref()
        {
            return Ok(tags.iter().any(|t| {
                t.field.eq_ignore_ascii_case(tag_name) && t.r#type.eq_ignore_ascii_case(tag_type)
            }));
        }

        Ok(false)
    }

    /// get current database
    pub fn current_database(&self) -> anyhow::Result<Option<String>> {
        if !self.loaded {
            bail!("querier is not loaded");
        }

        Ok(self.current_db.as_ref().map(|s| s.to_string()))
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
            table_metas: None,
        })
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

    /// get table meta of all super tables
    /// Short for: select * from information_schema.ins_stables where db_name = '$DB_NAME'
    async fn load_super_tables(&self, dbname: &str) -> anyhow::Result<Vec<TableMeta>> {
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

        // load columns and tags of each stable
        for stable_meta in stable_meta_vec.iter_mut() {
            let columns = self
                .load_columns(&stable_meta.db_name, &stable_meta.tbname)
                .await?;
            stable_meta.columns = Some(columns);

            let tags = self
                .load_tags_of_stable(&stable_meta.db_name, &stable_meta.tbname)
                .await?;
            stable_meta.tags = Some(tags);
        }

        if stable_meta_vec.is_empty() {
            tracing::warn!("no stable found in database: {}", dbname);
        }

        Ok(stable_meta_vec)
    }

    /// 查所有子表和普通表的元数据
    /// Short for: select * from information_schema.ins_tables where db_name = '$DB_NAME'
    async fn load_tables(&self, dbname: &str) -> anyhow::Result<Vec<TableMeta>> {
        let sql = format!(
            "select {},{},{},{},{} from information_schema.ins_tables where db_name = '{}' order by table_name",
            "table_name as `tbname`",
            "stable_name as `stable`",
            "db_name",
            "table_comment as `comment`",
            "type",
            dbname
        );
        tracing::debug!("sql: {}", sql);

        let mut table_metas: Vec<TableMeta> = self
            .taos
            .query(sql)
            .await?
            .deserialize()
            .try_collect()
            .await?;

        // load columns and tags of each table
        for table_meta in table_metas.iter_mut() {
            let columns = self
                .load_columns(&table_meta.db_name, &table_meta.tbname)
                .await?;
            table_meta.columns = Some(columns);

            let tags = self
                .load_tags_of_table(&table_meta.db_name, &table_meta.tbname)
                .await?;
            table_meta.tags = Some(tags);
        }

        if table_metas.is_empty() {
            tracing::warn!("no child table found in database: {}", dbname);
        }

        Ok(table_metas)
    }

    /// return column meta of table
    /// 表 information_schema.ins_columns 中既有超级表的 column meta，也有子表和普通表的 column meta
    /// Short for: select * from information_schema.ins_columns where db_name = '$DB_NAME' and table_name = '$TB_NAME'
    async fn load_columns(&self, dbname: &str, tbname: &str) -> anyhow::Result<Vec<ColumnMeta>> {
        let sql = format!(
            "select {},{},{} from information_schema.ins_columns where db_name = '{}' and table_name = '{}'",
            "col_name as `field`", "col_type as `type`", "col_length as `length`", dbname, tbname,
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

    /// return tag meta of super table
    /// short for: describe $DB_NAME.$STABLE_NAME
    async fn load_tags_of_stable(
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
            .cloned()
            .collect_vec())
    }

    /// 查子表的 tag 元数据，子表的 tag 有 tag_name, tag_type 和 tag_value
    /// 在 information_schema.ins_tags 中只有 CHILD_TABLE 和 NORMAL_TABLE 的 tag schema，没有 SUPER_TABLE
    /// short for: select * from information_schema.ins_tags where db_name = '$DB_NAME' and table_name = '$TB_NAME'
    async fn load_tags_of_table(&self, dbname: &str, tbname: &str) -> anyhow::Result<Vec<TagMeta>> {
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
    async fn drop_database(dsn: &str, db_name: &str) {
        let taos = TaosBuilder::from_dsn(dsn).unwrap().build().await.unwrap();
        taos.exec(format!("drop database if exists `{}`", db_name))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_table_meta_querier_with_taos() {
        // given
        let dsn = "taos:///";
        let db_name = "taosx_core_utils_table_meta";
        prepare_data(dsn, db_name).await;

        // build
        let dsn = format!("{dsn}{db_name}").into_dsn().unwrap();
        let querier = TableMetaQueryBuilder::new(&dsn)
            .unwrap()
            .build()
            .await
            .unwrap();

        // super table
        let stables = querier.super_table_meta("Stb1").unwrap().unwrap();
        assert_eq!(stables.tbname, "Stb1");
        assert_eq!(stables.stable, Some("Stb1".to_string()));
        assert_eq!(stables.db_name, db_name);
        assert_eq!(stables.r#type, TableType::SuperTable);
        assert_eq!(stables.comment, None);
        let columns = stables.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        let tags = stables.tags.as_ref().unwrap();
        assert_eq!(tags.len(), 1);

        // is_stable_column_exist
        let is_exist = querier.is_stable_col_exist("Stb1", "ts").unwrap();
        assert!(is_exist);
        let is_exist = querier.is_stable_col_exist("Stb1", "val").unwrap();
        assert!(is_exist);
        let is_exist = querier.is_stable_col_exist("Stb1", "not_exist").unwrap();
        assert!(!is_exist);

        // is_stable_tag_exist
        let is_exist = querier.is_stable_tag_exist("Stb1", "t", "int").unwrap();
        assert!(is_exist);
        let is_exist = querier.is_stable_tag_exist("Stb1", "t", "bigint").unwrap();
        assert!(!is_exist);

        // child table
        let tb_2_1 = querier.child_table_meta("TB_2_1").unwrap().unwrap();
        assert_eq!(tb_2_1.tbname, "TB_2_1");
        assert_eq!(tb_2_1.stable, Some("Stb2".to_string()));
        assert_eq!(tb_2_1.db_name, db_name);
        assert_eq!(tb_2_1.r#type, TableType::ChildTable);
        assert_eq!(tb_2_1.comment, None);
        let columns = tb_2_1.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        let tags = tb_2_1.tags.as_ref().unwrap();
        assert_eq!(tags.len(), 1);
        let t = tags.first().unwrap();
        assert_eq!(t.value, Some("3".to_string()));

        // is_child_of_stable
        let is_child = querier.is_child_of_stable("Stb2", "TB_2_1").unwrap();
        assert!(is_child);
        let is_child = querier.is_child_of_stable("Stb1", "TB_2_1").unwrap();
        assert!(!is_child);

        clear_database(&dsn).await.unwrap();

        drop_database("taos:///", db_name).await;
    }
}
