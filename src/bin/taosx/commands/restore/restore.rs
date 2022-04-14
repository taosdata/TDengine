use clap::Args;
use futures::Future;
use std::path::PathBuf;
use taos::{TaosOptions, Value};
use taos_sys::TaosDataType;
use taosx::{TaosBlock, TaosDescribe, TaosOpts, TaosTag};
use tokio::runtime::Builder;
pub(crate) mod deserialize;

use self::deserialize::{deserialize_database, Deserialize};
#[derive(Debug, Args)]
/// Restore from a backup output directory.

pub(crate) struct App {
    #[clap(short, long)]
    database: Option<String>,
    #[clap(short, long)]
    input: Option<String>,
    #[clap(short, long)]
    thread: Option<u32>,
}

fn allocate_task<Fut: 'static>(
    db: String,
    mut dir: PathBuf,
    path: &str,
    mut threads: u32,
    f: impl Fn(Vec<String>, String) -> Fut,
) where
    Fut: Future<Output = ()> + std::marker::Send,
{
    dir.push(path);
    let mut filelist = vec![];
    for element in dir.read_dir().unwrap() {
        let filename = element.unwrap().path();
        if let Some(extension) = filename.extension() {
            if extension == "parquet" {
                filelist.push(filename.to_str().unwrap().to_string());
            }
        }
    }
    if threads > filelist.len() as _ {
        threads = filelist.len() as _;
    }
    let file_per_threads = if filelist.len() as u32 % threads == 0 {
        filelist.len() as u32 / threads
    } else {
        filelist.len() as u32 / threads + 1
    };

    let mut handles = Vec::with_capacity(threads as _);
    let chunks: Vec<&[String]> = filelist.chunks(file_per_threads as _).collect();
    threads = chunks.len() as _;

    log::info!(
        "{} threads each deal with at most {} files",
        threads,
        file_per_threads
    );

    let runtime = Builder::new_multi_thread()
        .worker_threads(threads as usize)
        .enable_all()
        .build()
        .unwrap();
    for chunk in chunks {
        handles.push(runtime.spawn(f(chunk.to_owned(), db.clone())))
    }
    for handle in handles {
        runtime.block_on(handle).unwrap();
    }
}

impl App {
    pub fn run_with_taos_opts(&self, _opts: &TaosOpts) {
        let threads = self.thread.unwrap_or(1);
        let input = self.input.as_deref().unwrap_or("./");
        let path = PathBuf::from(input);

        let db = restore_database(path.clone());

        allocate_task(
            db.clone(),
            path.clone(),
            "table.info",
            threads,
            restore_table_info,
        );

        allocate_task(db.clone(), path.clone(), "tags", threads, restore_tags);

        allocate_task(db, path, "chunk", threads, restore_data);
    }
}

fn restore_database(mut path: PathBuf) -> String {
    path.push("db.info");
    let taos = TaosOptions::new().build().unwrap();
    let database = deserialize_database(path);
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            taos.query(format!("create database if not exists {}", database.name))
                .await
                .unwrap()
        });

    database.name
}

async fn restore_table_info(filelist: Vec<String>, db: String) {
    let taos = TaosOptions::new().database(db).build().unwrap();
    for file in filelist {
        let mut deserialize = Deserialize::<TaosDescribe>::new(file);
        deserialize.deserialize().await;
        let describes = deserialize.output;
        for describe in describes {
            let mut col_buffer = String::from("");
            let mut tag_buffer = String::from("");
            for col in describe.describe {
                match col {
                    taosx::TaosColumnMeta::Column(des) => {
                        if des.r#type == TaosDataType::Binary || des.r#type == TaosDataType::NChar {
                            col_buffer += format!(
                                "{} {}({}),",
                                des.field.as_str(),
                                des.r#type.as_str(),
                                des.length
                            )
                            .as_str();
                        } else {
                            col_buffer +=
                                format!("{} {},", des.field.as_str(), des.r#type.as_str(),)
                                    .as_str();
                        }
                    }
                    taosx::TaosColumnMeta::Tag(des) => {
                        if des.r#type == TaosDataType::Binary || des.r#type == TaosDataType::NChar {
                            tag_buffer += format!(
                                "{} {}({}),",
                                des.field.as_str(),
                                des.r#type.as_str(),
                                des.length
                            )
                            .as_str();
                        } else {
                            tag_buffer +=
                                format!("{} {},", des.field.as_str(), des.r#type.as_str(),)
                                    .as_str();
                        }
                    }
                }
            }
            col_buffer.pop();
            tag_buffer.pop();
            let sql = format!(
                "create table {} ({}) tags ({})",
                describe.name, col_buffer, tag_buffer
            );
            taos.stmt(sql).unwrap().execute().unwrap();
        }
    }
}

async fn restore_tags(filelist: Vec<String>, db: String) {
    let taos = TaosOptions::new().database(db).build().unwrap();
    for file in filelist {
        let mut deserialize = Deserialize::<TaosTag>::new(file);
        deserialize.deserialize().await;
        let taostags = deserialize.output;
        for taostag in taostags {
            let stbname = taostag.name;
            for tag in taostag.tags {
                let mut sql = String::new();
                for (index, value) in tag.into_iter().enumerate() {
                    if index == 0 {
                        if let Value::Binary(b) = value {
                            sql = format!(
                                "create table {} using {} tags (",
                                std::str::from_utf8(&b).unwrap(),
                                &stbname
                            );
                        }
                    } else {
                        match value {
                            Value::Null => sql += "NULL,",
                            Value::Bool(v) => sql += format!("{},", v).as_str(),
                            Value::TinyInt(v) => sql += format!("{},", v).as_str(),
                            Value::SmallInt(v) => sql += format!("{},", v).as_str(),
                            Value::Int(v) => sql += format!("{},", v).as_str(),
                            Value::BigInt(v) => sql += format!("{},", v).as_str(),
                            Value::Float(v) => sql += format!("{},", v).as_str(),
                            Value::Double(v) => sql += format!("{},", v).as_str(),
                            Value::Binary(v) => {
                                sql +=
                                    format!("\'{}\',", std::str::from_utf8(&v).unwrap()).as_str();
                            }
                            Value::Timestamp(v) => sql += format!("{},", v.as_raw_i64()).as_str(),
                            Value::NChar(v) => sql += format!("\'{}\',", v).as_str(),
                            Value::UTinyInt(v) => sql += format!("{},", v).as_str(),
                            Value::USmallInt(v) => sql += format!("{},", v).as_str(),
                            Value::UInt(v) => sql += format!("{},", v).as_str(),
                            Value::UBigInt(v) => sql += format!("{},", v).as_str(),
                            Value::Json(v) => sql += format!("{},", v).as_str(),
                            _ => todo!(),
                        }
                    }
                }
                sql.pop();
                sql += ")";
                taos.stmt(sql).unwrap().execute().unwrap();
            }
        }
    }
}

async fn restore_data(filelist: Vec<String>, db: String) {
    let taos = TaosOptions::new().database(db).build().unwrap();
    for file in filelist {
        let mut deserialize = Deserialize::<TaosBlock>::new(file);
        deserialize.deserialize().await;
        let taos_blocks = deserialize.output;
        for taos_block in taos_blocks {
            let col_num = taos_block.data.len();
            let mut prepare = format!("insert into {} values (", taos_block.name);
            for _ in 0..col_num {
                prepare += "?,";
            }
            prepare.pop();
            prepare += ")";
            let mut stmt = taos.stmt(prepare).expect("prepare");
            let col_vec = taos_block.to_column_vec();
            let bind: Vec<_> = col_vec.iter().map(|v| v.to_multi_bind()).collect();
            stmt.multi_bind(&bind).expect("bind erro");
            stmt.execute().expect("execute error");
        }
    }
}
