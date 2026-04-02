use std::mem;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::Local;
use flume::{Receiver, Sender};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use taos::*;

const DSN: &str = "ws://localhost:6041";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!(
        "Stmt: interlace=1, one million subtables, each with one hundred records, \
        a total of one hundred million records."
    );

    let thread_cnt = 4;
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

    let mut senders = Vec::with_capacity(thread_cnt);
    let mut receivers = Vec::with_capacity(thread_cnt);

    for _ in 0..thread_cnt {
        let (sender, receiver) = flume::bounded(64);
        senders.push(sender);
        receivers.push(receiver);
    }

    produce_data(senders, subtable_cnt, record_cnt).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    consume_data(db, receivers, subtable_cnt * record_cnt).await;

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

async fn produce_data(
    senders: Vec<Sender<Vec<(String, Vec<ColumnView>)>>>,
    subtable_cnt: usize,
    record_cnt: usize,
) {
    let batch_cnt = 10000;
    let thread_cnt = senders.len();
    let thread_subt_cnt = subtable_cnt / thread_cnt;

    for i in 0..thread_cnt {
        let sender = senders[i].clone();
        tokio::spawn(async move {
            println!("Producer thread[{i}] starts producing data");

            let mut rng = StdRng::from_entropy();
            let mut datas = Vec::with_capacity(batch_cnt);

            let start = i * thread_subt_cnt;
            let end = start + thread_subt_cnt;

            for _ in 0..record_cnt {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                for k in start..end {
                    let c1 = rng.gen::<i32>();
                    let c2: f32 = rng.gen_range(0.0..10000000.);
                    let c2 = (c2 * 100.0).round() / 100.0;
                    let c3: f32 = rng.gen_range(0.0..10000000.);
                    let c3 = (c3 * 100.0).round() / 100.0;

                    let tbname = format!("d{}", k);
                    let cols = vec![
                        ColumnView::from_millis_timestamp(vec![ts]),
                        ColumnView::from_ints(vec![c1]),
                        ColumnView::from_floats(vec![c2]),
                        ColumnView::from_floats(vec![c3]),
                    ];

                    datas.push((tbname, cols));
                    if datas.len() == batch_cnt {
                        sender.send_async(mem::take(&mut datas)).await.unwrap();
                    }
                }
            }

            if !datas.is_empty() {
                sender.send_async(datas).await.unwrap();
            }

            println!("Producer thread[{i}] ends producing data");
        });
    }
}

async fn consume_data(
    db: &str,
    mut receivers: Vec<Receiver<Vec<(String, Vec<ColumnView>)>>>,
    total_record_cnt: usize,
) {
    let thread_cnt = receivers.len();

    let now = Local::now();
    let time = now.format("%Y-%m-%d %H:%M:%S").to_string();
    println!("Consuming data start, time = {time}");

    let mut tasks = vec![];

    for i in 0..thread_cnt {
        let db = db.to_owned();
        let receiver = receivers.pop().unwrap();

        let task = tokio::spawn(async move {
            let taos = TaosBuilder::from_dsn(DSN).unwrap().build().await.unwrap();
            taos.exec(format!("use {db}")).await.unwrap();

            let mut stmt = Stmt::init(&taos).await.unwrap();

            let sql = "insert into ? values(?, ?, ?, ?)";
            stmt.prepare(sql).await.unwrap();

            println!("Consumer thread[{i}] starts consuming data");

            let start = Instant::now();
            while let Ok(datas) = receiver.recv_async().await {
                for (tbname, cols) in datas {
                    stmt.set_tbname(&tbname).await.unwrap();
                    stmt.bind(&cols).await.unwrap();
                    stmt.add_batch().await.unwrap();
                }
                stmt.execute().await.unwrap();
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
        total_record_cnt / total_time as usize
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
