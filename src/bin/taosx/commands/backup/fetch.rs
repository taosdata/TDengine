use std::{ops::Not, rc::Rc, sync::Arc};

use futures::TryStreamExt;
use libtaos::{ColumnMeta, Field, Taos};
use taos::TaosOptions;
use taosx::Database;

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub tag_buffer: Option<String>,
    pub tags: Option<Vec<ColumnMeta>>,
    pub cols: Vec<ColumnMeta>,
}
#[derive(::serde::Deserialize, Debug)]
pub struct Table {
    pub table_name: String,
    pub stable_name: String,
}

impl TableInfo {
    pub async fn new(
        database: String,
        stable_list: Vec<String>,
        common_list: Vec<String>,
    ) -> Vec<Self> {
        let mut tableinfo_list = vec![];
        let taos = Taos::new("10.72.136.169", "root", "taosdata", database.as_str(), 6030).unwrap();
        for stable in stable_list {
            let describe = taos.describe(stable.as_str()).await.unwrap();
            let mut tag_buffer = String::from("");
            for tag in &describe.tags {
                tag_buffer += ",";
                tag_buffer += tag.name.as_str();
            }
            tableinfo_list.push(TableInfo {
                name: stable,
                tag_buffer: Some(tag_buffer),
                tags: Some(describe.tags),
                cols: describe.cols,
            });
        }
        for common_table in common_list {
            let describe = taos.describe(common_table.as_str()).await.unwrap();
            tableinfo_list.push(TableInfo {
                name: common_table,
                tags: None,
                cols: describe.cols,
                tag_buffer: None,
            });
        }
        tableinfo_list
    }
}

pub async fn fetch_stable_list(database: String) -> Vec<String> {
    let mut stable_list = vec![];
    let taos = Taos::new("10.72.136.169", "root", "taosdata", database, 6030).unwrap();
    let res = taos.query("show stables").await.unwrap();
    if res.rows.len() == 0 {
        unreachable!("no stable found");
    } else {
        for row in res.rows {
            assert!(row.len() > 1);
            match row[0].clone() {
                Field::Binary(v) => stable_list.push(v.to_string()),
                _ => continue,
            }
        }
    }
    stable_list
}

// pub async fn fetch_table_list(database: String) -> (Vec<String>, u32) {
//     let mut table_list = vec![];
//     let mut count = 0;
//     let taos = Taos::new("10.72.136.169", "root", "taosdata", database, 6030).unwrap();
//     let res = taos.query("show tables").await.unwrap();
//     if res.rows.len() == 0 {
//         unreachable!("no stable found");
//     } else {
//         for row in res.rows {
//             assert!(row.len() > 1);
//             match row[0].clone() {
//                 Field::Binary(v) => {
//                     count += 1;
//                     table_list.push(v.to_string())
//                 }
//                 _ => continue,
//             }
//         }
//     }
//     (table_list, count)
// }

pub async fn fetch_table_list(database: String) -> (Vec<String>, Vec<String>, u32) {
    let mut select_list = vec![];
    let mut describe_list = vec![];
    let taos = TaosOptions::new().database(database).build().unwrap();
    let tables: Vec<Table> = taos
        .query("show tables")
        .await
        .unwrap()
        .rows_de_stream()
        .try_collect()
        .await
        .unwrap();
    let mut count = 0;
    for table in tables {
        count += 1;
        select_list.push(table.table_name.clone());
        if table.stable_name == "" {
            describe_list.push(table.table_name);
        } else if !describe_list.contains(&table.stable_name) {
            describe_list.push(table.stable_name)
        }
    }
    (select_list, describe_list, count)
}

pub async fn fetch_database_info(database: String) -> Database {
    let taos = TaosOptions::new().build().unwrap();
    let dbs: Vec<Database> = taos
        .query("show databases")
        .await
        .unwrap()
        .rows_de_stream()
        .try_collect()
        .await
        .unwrap();
    for db in dbs {
        if db.name == database {
            return db;
        }
    }
    panic!("cannot find database {}", database);
}
