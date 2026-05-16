use std::str::FromStr;
use std::time::Duration;

use taos::sync::*;

// #[test]
fn _test_tmq_meta_sync() -> anyhow::Result<()> {
    // pretty_env_logger::formatted_timed_builder()
    //     .filter_level(tracing::LevelFilter::Debug)
    //     .init();
    use taos_query::prelude::sync::*;
    let dsn = std::env::var("TEST_DSN").unwrap_or("taos://localhost:6030".to_string());
    let mut dsn = Dsn::from_str(&dsn)?;

    let taos = TaosBuilder::from_dsn(&dsn)?.build()?;
    taos.exec_many([
        "drop topic if exists t_tmq_meta_sync",
        "drop database if exists t_tmq_meta_sync",
        "create database t_tmq_meta_sync wal_retention_period 3600",
        "create topic t_tmq_meta_sync with meta as database t_tmq_meta_sync",
        "use t_tmq_meta_sync",
        // kind 1: create super table using all types
        "create table stb1(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(16),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 json)",
        // kind 2: create child table with json tag
        "create table tb0 using stb1 tags('{\"name\":\"value\"}')",
        "create table tb1 using stb1 tags(NULL)",
        "insert into tb0 values(now, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)
            tb1 values(now, true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
        // kind 3: create super table with all types except json (especially for tags)
        "create table stb2(ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint,\
            c6 timestamp, c7 float, c8 double, c9 varchar(10), c10 nchar(10),\
            c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned)\
            tags(t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint,\
            t6 timestamp, t7 float, t8 double, t9 varchar(10), t10 nchar(16),\
            t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)",
        // kind 4: create child table with all types except json
        "create table tb2 using stb2 tags(true, -2, -3, -4, -5, \
            '2022-02-02 02:02:02.222', -0.1, -0.12345678910, 'abc 和我', 'Unicode + 涛思',\
            254, 65534, 1, 1)",
        "create table tb3 using stb2 tags( NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL)",
        // kind 5: create common table
        "create table `table` (ts timestamp, v int)",
        // kind 6: column in super table
        "alter table stb1 add column new1 bool",
        "alter table stb1 add column new2 tinyint",
        "alter table stb1 add column new10 nchar(16)",
        "alter table stb1 modify column new10 nchar(32)",
        "alter table stb1 drop column new10",
        "alter table stb1 drop column new2",
        "alter table stb1 drop column new1",
        // kind 7: add tag in super table
        "alter table `stb2` add tag new1 bool",
        "alter table `stb2` rename tag new1 new1_new",
        "alter table `stb2` modify tag t10 nchar(32)",
        "alter table `stb2` drop tag new1_new",
        // kind 8: column in common table
        "alter table `table` add column new1 bool",
        "alter table `table` add column new2 tinyint",
        "alter table `table` add column new10 nchar(16)",
        "alter table `table` modify column new10 nchar(32)",
        "alter table `table` rename column new10 new10_new",
        "alter table `table` drop column new10_new",
        "alter table `table` drop column new2",
        "alter table `table` drop column new1",
        // kind 9: drop normal table
        "drop table `table`",
        // kind 10: drop child table
        "drop table `tb2`, `tb1`",
        // kind 11: drop super table
        // "drop table `stb2`",
        // "drop table `stb1`",
    ])?;

    taos.exec_many([
        "drop database if exists t_tmq_meta_sync2",
        "create database if not exists t_tmq_meta_sync2 wal_retention_period 3600",
        "use t_tmq_meta_sync2",
    ])?;

    dsn.params.insert("group.id".to_string(), "abc".to_string());

    dsn.params
        .insert("auto.offset.reset".to_string(), "earliest".to_string());

    let builder = TmqBuilder::from_dsn(&dsn)?;
    let mut consumer = builder.build()?;
    tracing::info!("consumer started");
    consumer.subscribe(["t_tmq_meta_sync"])?;
    tracing::info!("start subscribe");

    while let Some((offset, message)) = consumer.recv()? {
        // Offset contains information for topic name, database name and vgroup id,
        //  similar to kafka topic/partition/offset.
        let _ = offset.topic();
        let _ = offset.database();
        let _ = offset.vgroup_id();
        tracing::info!("");

        // Different to kafka message, TDengine consumer would consume two kind of messages.
        //
        // 1. meta
        // 2. data

        match message {
            MessageSet::Meta(meta) => {
                let raw = meta.as_raw_meta()?;
                taos.write_raw_meta(&raw)?;

                // meta data can be write to an database seamlessly by raw or json (to sql).
                let json = meta.as_json_meta()?;
                let sql = json.iter().next().unwrap().to_string();
                if let Err(err) = taos.exec(sql) {
                    println!("maybe error: {}", err);
                }
            }
            MessageSet::Data(data) => {
                // data message may have more than one data block for various tables.
                for block in data {
                    let block = block?;
                    dbg!(block.table_name());
                    dbg!(block);
                }
            }
            MessageSet::MetaData(meta, data) => {
                let raw = meta.as_raw_meta()?;
                taos.write_raw_meta(&raw)?;

                // meta data can be write to an database seamlessly by raw or json (to sql).
                let json = meta.as_json_meta()?;
                let sql = json.iter().next().unwrap().to_string();
                if let Err(err) = taos.exec(sql) {
                    println!("maybe error: {}", err);
                }
                // data message may have more than one data block for various tables.
                for block in data {
                    let block = block?;
                    dbg!(block.table_name());
                    dbg!(block);
                }
            }
        }
        consumer.commit(offset)?;
    }

    consumer.unsubscribe();

    std::thread::sleep(Duration::from_secs(2));

    taos.exec_many([
        "drop database t_tmq_meta_sync2",
        "drop topic t_tmq_meta_sync",
        "drop database t_tmq_meta_sync",
    ])?;
    Ok(())
}
