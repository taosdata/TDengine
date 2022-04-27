use futures::TryStreamExt;
use taos::TaosOptions;
use taosx::Database;
use tokio::runtime::Builder;
#[derive(::serde::Deserialize, Debug)]
pub struct Table {
    pub table_name: String,
    pub stable_name: String,
}

pub fn fetch_table_list(database: String) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut select_list = vec![];
    let mut describe_list = vec![];
    let mut stable_list = vec![];
    let taos = TaosOptions::new().database(database).build().unwrap();
    let tables: Vec<Table> = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            taos.query("show tables")
                .await
                .unwrap()
                .rows_de_stream()
                .try_collect()
                .await
                .unwrap()
        });
    for table in tables {
        select_list.push(table.table_name.clone());
        if table.stable_name.is_empty() {
            describe_list.push(table.table_name);
        } else if !describe_list.contains(&table.stable_name) {
            stable_list.push(table.stable_name.clone());
            describe_list.push(table.stable_name);
        }
    }
    (select_list, describe_list, stable_list)
}

pub fn fetch_database_info(database: String) -> Database {
    let taos = TaosOptions::new()
        .database(database.clone())
        .build()
        .unwrap();
    let dbs: Vec<Database> = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            taos.query("show databases")
                .await
                .unwrap()
                .rows_de_stream()
                .try_collect()
                .await
                .unwrap()
        });
    for db in dbs {
        if db.name == database {
            return db;
        }
    }
    panic!("cannot find database {}", database);
}

pub async fn fetch_stable_tag_buffer(database: String, stable: String) -> String {
    let mut tag_buffer = String::from("");
    let taos = TaosOptions::new()
        .database(database.clone())
        .build()
        .unwrap();
    let describe = taos
        .describe(format!("{}.{}", database.clone(), stable).as_str())
        .await
        .unwrap();
    for col in describe {
        if col.is_tag() {
            tag_buffer += ",";
            tag_buffer += col.field();
        }
    }
    tag_buffer
}
