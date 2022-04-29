use std::sync::atomic;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use taos::prelude::sync::*;
use taos::tmq::*;

use anyhow::Result;
use taos_sys::*;

fn init_env(taos: &Taos) -> Result<()> {
    taos.query("create stable if not exists st1 (ts timestamp, k int) tags(a int)")?;
    taos.query("create table if not exists tu1 using st1 tags(1)")?;
    taos.query("create table if not exists tu2 using st1 tags(2)")?;
    Ok(())
}

fn create_topic(taos: &Taos) -> Result<()> {
    println!("create topic");
    taos.create_topic("test_stb_topic_1", "select * from tu1")?;
    println!("create topic ok");
    Ok(())
}

fn build_consumer(taos: &Taos) -> Result<Consumer> {
    println!("consumer config");
    let mut conf = TmqConf::new();
    conf.set("group.id", "tg2")?;
    unsafe extern "C" fn tmq_commit_callback(
        _tmq: *mut tmq_t,
        resp: tmq_resp_err_t,
        _topic: *mut tmq_topic_vgroup_list_t,
    ) {
        log::info!("commit {resp:?}");
    }
    conf.set_offset_commit_cb(tmq_commit_callback);
    println!("build consumer");
    Ok(conf.consumer()?)
}

fn build_topic_list() -> Result<TmqList> {
    println!("build topic list");
    let mut topic = TmqList::new();
    topic.append("test_stb_topic_1")?;
    Ok(topic)
}

fn process_message(msg: &mut ResultSet) {
    let rows = msg.to_rows_vec();

    for row in rows {
        println!("{row:?}");
    }
}
fn sync_consume_loop(database: &str, consumer: &Consumer, topics: &TmqList) -> Result<()> {
    println!("consume loop");
    // start subscription
    consumer.subscribe(topics)?;
    let running = Arc::new(atomic::AtomicBool::new(true));
    let msg_count = atomic::AtomicUsize::new(0);
    let running2 = running.clone();

    fn insert(database: &str) -> Result<()> {
        println!("connect taos in a spawned thread");
        let taos = TaosOptions::new().database(database).build()?;
        println!("start to insert 10 rows");
        for i in 0..10 {
            use taos::prelude::sync::*;
            taos.query(&format!("insert into tu1 values (now, {i})"))?;
            println!("- {i} rows inserted");
            thread::sleep(Duration::from_millis(1));
        }
        drop(taos);
        println!("write data thread finish");
        Ok(())
    }

    let database = database.to_string();
    thread::spawn(move || match insert(&database) {
        Ok(_) => {
            running2.store(false, atomic::Ordering::SeqCst);
        }
        Err(err) => {
            running2.store(false, atomic::Ordering::SeqCst);
            eprintln!("{}", err.to_string());
        }
    });

    println!("inserting thread spawned.");
    while running.load(atomic::Ordering::SeqCst) || msg_count.load(atomic::Ordering::SeqCst) < 10 {
        println!("looping...");
        if let Some(Ok(mut msg)) = consumer.poll(10) {
            println!("msg: {}", msg_count.load(atomic::Ordering::SeqCst));
            process_message(&mut msg);
            msg_count.fetch_add(1, atomic::Ordering::SeqCst);

            consumer.commit(None, 0)?;
            println!("msg summary: {:?}", msg.summary());
        }
    }
    println!("loop done");
    Ok(())
}

fn main() -> Result<()> {
    let database = "tmq_consume_test";
    let taos = TaosOptions::new().database(database).build()?;
    println!("connected");
    init_env(&taos)?;
    println!("env inited");
    create_topic(&taos)?;
    println!("topic created");
    let consumer = build_consumer(&taos)?;
    println!("consumer created");
    let topics = build_topic_list()?;
    println!("topics created");
    sync_consume_loop(database, &consumer, &topics)?;
    dbg!(consumer.subscription()?);
    println!("finished");
    Ok(())
}
