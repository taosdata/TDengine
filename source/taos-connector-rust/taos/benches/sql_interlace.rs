use std::time::{Instant, SystemTime, UNIX_EPOCH};

use chrono::Local;
use rand::Rng;
use taos::*;

const DSN: &str = "ws://localhost:6041";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "SQL: interlace=1, one million subtables, each with one hundred records, \
        a total of one hundred million records."
    );

    let subtable_cnt = 1000000;
    let record_cnt = 100; // number of records per subtable

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let db = &format!("db_{}", ts);

    let taos = TaosBuilder::from_dsn(DSN)?.build().await?;
    taos.exec_many([
        &format!("drop database if exists {db}"),
        &format!("create database {db} vgroups 10"),
        &format!("use {db}"),
        "create stable s0 (ts timestamp, c1 int, c2 float, c3 float) tags(t1 int)",
    ])
    .await?;

    create_subtables(db, subtable_cnt).await;

    let sqls = produce_sqls(subtable_cnt, record_cnt).await;

    consume_sqls(db, sqls).await;

    check_count(&taos, subtable_cnt * record_cnt).await?;

    taos.exec(format!("drop database {db}")).await?;

    Ok(())
}

async fn create_subtables(db: &str, subtable_cnt: usize) {
    println!("Creating subtables start");

    let start = Instant::now();
    let batch_cnt = 10000;
    let thread_cnt = 10;
    let thread_subt_cnt = subtable_cnt / thread_cnt;
    let mut tasks = vec![];

    for i in (0..subtable_cnt).step_by(thread_subt_cnt) {
        let db = db.to_owned();

        let task = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn(DSN).unwrap().build().await.unwrap();
            taos.exec(format!("use {db}")).await.unwrap();

            for j in (0..thread_subt_cnt).step_by(batch_cnt) {
                // creata table d0 using s0 tags(0) d1 using s0 tags(0) ...
                let mut sql = String::with_capacity(25 * batch_cnt);
                sql.push_str("create table ");
                for k in 0..batch_cnt {
                    sql.push_str(&format!("d{} using s0 tags(0) ", i + j + k));
                }
                taos.exec(sql).await.unwrap();
            }
        });

        tasks.push(task);
    }

    for task in tasks {
        task.await.unwrap();
    }

    println!("Creating subtables end, elapsed = {:?}", start.elapsed());
}

async fn produce_sqls(subtable_cnt: usize, record_cnt: usize) -> Vec<String> {
    println!("Producing sqls start");

    let start = Instant::now();
    let batch_cnt = 10000;
    let thread_cnt = 10;
    let thread_subt_cnt = subtable_cnt / thread_cnt;
    let mut tasks = Vec::with_capacity(thread_cnt);

    for i in 0..thread_cnt {
        let task = tokio::spawn(async move {
            println!("Producer thread[{i}] starts producing data");

            let mut rng = rand::thread_rng();
            let mut sqls = Vec::with_capacity(thread_subt_cnt * record_cnt);

            let start = i * thread_subt_cnt;
            let end = start + thread_subt_cnt;

            for _ in 0..record_cnt {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                for j in (start..end).step_by(batch_cnt) {
                    // insert into d0 values() d1 values() ...
                    let mut sql = String::with_capacity(100 * batch_cnt);
                    sql.push_str("insert into ");
                    for k in 0..batch_cnt {
                        let tbname = format!("d{}", j + k);
                        let c1 = rng.gen::<i32>();
                        let c2: f32 = rng.gen_range(0.0..10000000.);
                        let c2 = (c2 * 100.0).round() / 100.0;
                        let c3: f32 = rng.gen_range(0.0..10000000.);
                        let c3 = (c3 * 100.0).round() / 100.0;
                        sql.push_str(&format!("{tbname} values({ts},{c1},{c2},{c3}) "));
                    }
                    sqls.push(sql);
                }
            }

            println!("Producer thread[{i}] ends producing data");

            sqls
        });

        tasks.push(task);
    }

    let mut sqls = Vec::with_capacity(subtable_cnt * record_cnt);
    for task in tasks {
        sqls.extend(task.await.unwrap());
    }

    println!("Producing {} sqls", sqls.len());
    println!("Producing sqls end, elapsed = {:?}", start.elapsed());

    sqls
}

async fn consume_sqls(db: &str, sqls: Vec<String>) {
    let now = Local::now();
    let time = now.format("%Y-%m-%d %H:%M:%S").to_string();
    println!("Consuming data start, time = {time}");

    let thread_cnt = 4;
    let thread_sql_cnt = sqls.len() / thread_cnt;
    let mut tasks = Vec::with_capacity(thread_cnt);

    for (i, sqls) in sqls.chunks(thread_sql_cnt).enumerate() {
        let db = db.to_owned();
        let sqls = sqls.to_vec();

        let task = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn(DSN).unwrap().build().await.unwrap();
            taos.exec(format!("use {db}")).await.unwrap();

            println!("Consumer thread[{i}] starts consuming data");

            let start = Instant::now();
            for sql in sqls {
                taos.exec(sql).await.unwrap();
            }

            println!(
                "Consumer thread[{i}] ends consuming data, elapsed = {:?}",
                start.elapsed()
            );

            start.elapsed().as_secs()
        });

        tasks.push(task);
    }

    let mut total_time = 0;
    for task in tasks {
        total_time += task.await.unwrap();
    }

    println!(
        "Consuming data end, speed(single thread) = {:?}\n",
        sqls.len() * 10000 / total_time as usize
    );
}

async fn check_count(taos: &Taos, cnt: usize) -> anyhow::Result<()> {
    #[derive(Debug, serde::Deserialize)]
    struct Record {
        cnt: usize,
    }

    let res: Vec<Record> = taos
        .query("select count(*) as cnt from s0")
        .await?
        .deserialize()
        .try_collect()
        .await?;

    assert_eq!(res.len(), 1);
    assert_eq!(res[0].cnt, cnt);

    Ok(())
}
