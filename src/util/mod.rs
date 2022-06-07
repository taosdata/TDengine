use std::collections::HashMap;

use futures::prelude::*;
use log::info;
use serde::Deserialize;
use taos::prelude::{Error, Taos};

pub async fn sync_schema(from: &Taos, to: &Taos) -> Result<(), Error> {
    use taos::prelude::*;
    let stables: Vec<String> = from
        .query("show stables")
        .await?
        .deserialize_stream()
        .try_collect()
        .await?;
    let mut stable_fields = HashMap::new();
    for stable in stables {
        // todo: use "show create" sql?
        // from.query(format!("show create stable {stable}")).await?;
        let desc = from.describe(&stable).await?;
        let sql = desc.to_create_table_sql(&stable);
        stable_fields.insert(stable, desc);
        to.exec(sql).await?;
    }

    #[derive(Deserialize)]
    struct Table {
        table_name: String,
        db_name: String,
        stable_name: Option<String>,
    }
    let tables: Vec<Table> = from
        .query("show tables")
        .await?
        .deserialize_stream()
        .try_collect()
        .await?;
    use itertools::Itertools;
    for table in tables {
        let table_name = &table.table_name;
        if let Some(stable) = table.stable_name {
            let fields = &stable_fields[&stable];
            let tags = fields.tag_names().collect_vec();
            let names = fields.tag_names().join(",");
            let fields: Vec<Value> = from
                .query_one(format!(
                    "select {names} from {stable} where tbname = '{table_name}'"
                ))
                .await?
                .unwrap();

            let tags_values = fields.into_iter().map(|v| v.to_sql_value()).join(",");
            to.exec(format!(
                "create table if not exists {table_name} using {stable} tags({tags_values})"
            ))
            .await?;
            // tags.iter().zip(fields).map(|(name, value)| format!(""))
            // let tags_stmt = tags.map(|_| '?').join(",");
            // let mut stmt = to.stmt(format!("create table if not exists ? using ({tags_stmt})"))?;
            // stmt.set_tbname_tags(&table.table_name, &fields);
        } else {
            let desc = from.describe(table_name).await?;
            let sql = desc.to_create_table_sql(table_name);
            to.exec(sql).await?;
        }
    }

    // let tables: Vec<
    Ok(())
}

pub fn sync_table(from: &Taos, to: &Taos, db: &str, table: &str) -> Result<(), Error> {
    use taos::prelude::sync::*;

    let stable: Option<String> = from
        .query_one(format!(
            "select stable_name from information_schema.user_tables where db_name = '{db}' and table_name = \"{table}\""
        ))?
        .unwrap();
    use itertools::Itertools;

    if let Some(stable) = stable {
        let desc = from.describe(&format!("{db}.`{stable}`"))?;
        let sql = desc.to_create_table_sql(&stable);
        to.exec(sql)?;

        let names = desc.tag_names().join(",");
        let children: Vec<Vec<Value>> = from
            .query(format!("select tbname,{names} from {stable}"))?
            .deserialize()
            .try_collect()?;

        // todo: use par_iter to speed up tables creation.
        // todo: single table not work, blocked by https://jira.taosdata.com:18080/browse/TD-16117
        for child in children {
            let tbname = child[0].to_string().unwrap();
            let tags_values = child[1..].into_iter().map(|v| v.to_sql_value()).join(",");
            to.exec(format!(
                "create table if not exists {tbname} using {stable} tags({tags_values})"
            ))?;
        }

        // let fields: Vec<Value> = from
        //     .query_one(format!(
        //         "select {names} from {stable} where tbname = '{table}'"
        //     ))?
        //     .unwrap();

        // let tags_values = fields.into_iter().map(|v| v.to_sql_value()).join(",");
        // to.exec(format!(
        //     "create table if not exists {table} using {stable} tags({tags_values})"
        // ))?;
    } else {
        info!("describe table {table}");
        let desc = from.describe(&format!("{db}.`{table}`"))?;
        info!("table {table}: {desc:?}");
        let sql = desc.to_create_table_sql(&table);
        info!("exec sql: {sql}");
        to.exec(sql)?;
    }
    Ok(())
}
