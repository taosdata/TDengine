use crate::serve::tests::{generate_scheduler_for_test, tracing_subscriber_init};

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_agent() -> anyhow::Result<()> {
    // std::env::set_var("RUST_LOG", "debug");
    tracing_subscriber_init()?;
    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
    dbg!(&controller);
    let new: AgentProps = serde_json::from_str(
        r#"
        {
            "dsn": "",
            "name": "agent1",
            "cluster_id": "xxx",
            "user_id":"root",
            "expire_date": "2024-01-01",
            "connectors": ["opc"]
        }
        "#,
    )
    .unwrap();
    dbg!(&new);
    let agent = controller.create_agent(new).await?;
    dbg!(&agent);
    let detail = controller.get_agent_by_id(agent.id).await?;
    dbg!(&detail);

    let found = controller.get_agent_with_token(&agent.token).await?;
    dbg!(&found);

    let res = controller
        .agent_connect_with_token(&agent.token, "127.0.0.1:8080".parse().ok().as_ref())
        .await?;
    dbg!(res);

    let task: NewTask = serde_json::from_str(&format!(
        r#"
        {{
            "from": "tmq:///test", "to":"taos:///test", "via": {}
        }}
        "#,
        agent.id
    ))
    .unwrap();

    let task = controller.create(task).await;
    assert!(task.is_err()); // agent is not alive.

    let activities = controller
        .agent_activities(agent.id, &Default::default())
        .await?;
    dbg!(activities);

    controller.delete_agent(agent.id).await?;

    // let deleted_task = controller.get(task.id).await?;
    // dbg!(&deleted_task);
    // assert!(deleted_task.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_patch() -> anyhow::Result<()> {
    // std::env::set_var("RUST_LOG", "taos=debug");
    tracing_subscriber_init()?;
    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;

    let new: AgentProps = serde_json::from_str(
        r#"
        {
            "dsn": "",
            "name": "代理1",
            "cluster_id": "xxx",
            "user_id":"root",
            "expire_date": "2024-01-01",
            "connectors": ["opc"]
        }
        "#,
    )
    .unwrap();
    dbg!(&new);
    let agent = controller.create_agent(new).await?;

    let detail = controller.get_agent_by_id(agent.id).await?;
    dbg!(&detail);

    let patch: AgentUpdates = serde_json::from_str(
        r#"{
            "name": "代理2",
            "connectors": ["opc", "modbus"]
        }
        "#,
    )
    .unwrap();

    let _agent = controller.update_agent(agent.id, patch).await?;

    let detail = controller.get_agent_by_id(agent.id).await?;
    dbg!(&detail);

    controller.delete_agent(agent.id).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_task_when_agent_not_alive() -> anyhow::Result<()> {
    tracing_subscriber_init()?;
    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
    let agent = controller
        .create_agent(AgentProps {
            dsn: "".to_string(),
            name: "a1".to_string(),
            cluster_id: "".to_string(),
            user_id: "".to_string(),
        })
        .await?;
    dbg!(&agent);

    let task_props: NewTask = serde_json::from_str(
        r#"
        {
            "from": "mqtt:///db2",
            "to":"taos:///db2",
            "via": 1
        }
        "#,
    )
    .unwrap();

    let task = controller.create(task_props).await;
    assert!(task.is_err());
    dbg!(&task);
    assert!(
        task.unwrap_err()
            .to_string()
            .contains("Agent 1 is not alive")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_task_offset_with_taos() -> anyhow::Result<()> {
    unsafe {
        std::env::set_var("RUST_LOG", "taos=info");
    }
    tracing_subscriber_init()?;

    let dsn = "taos://localhost:6030".to_string();
    tracing::info!("dsn: {}", dsn);

    let taos = taos::TaosBuilder::from_dsn(&dsn)?.build().await?;
    taos.exec_many([
        "drop topic if exists ws_abc1",
        "drop database if exists ws_abc1",
        "create database ws_abc1 wal_retention_period 3600",
        "create topic ws_abc1 with meta as database ws_abc1",
        "use ws_abc1",
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
    ])
    .await?;

    taos.exec_many([
        "drop database if exists ws_abc2",
        "create database if not exists ws_abc2",
    ])
    .await?;

    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;

    let task_props: NewTask = serde_json::from_str(
        r#"
        {
            "from": "tmq:///ws_abc1",
            "to":"taos:///ws_abc2",
            "force": true
        }
        "#,
    )
    .unwrap();

    let task = controller.create(task_props).await?;
    // dbg!(&task);

    // let tasks = controller.tasks(TaskFilter::default()).await?;

    controller.start_task(&task).await?;

    // sleep to wait for task started.
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // let task_after_start = controller.get(task.id).await?;
    // dbg!(&task_after_start);

    controller.stop(task.id).await?;
    let offset = controller.offsets(task.id).await?.unwrap();
    dbg!(&offset);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_max_activities_per_entity() -> anyhow::Result<()> {
    tracing_subscriber_init()?;
    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
    let agent = controller
        .create_agent(AgentProps {
            dsn: "".to_string(),
            name: "a1".to_string(),
            cluster_id: "".to_string(),
            user_id: "".to_string(),
        })
        .await?;
    dbg!(&agent);
    let pool = controller.pool.clone();
    for _i in 0..1000 {
        let _ = push_agent_activity(
            &pool,
            &Activity::agent_transferring(agent.id, "test".to_string()),
        )
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    keep_max_activities(&pool, 100).await?;

    let len = sqlx::query_scalar::<_, i64>("select count(*) from agent_activities")
        .fetch_one(&pool)
        .await?;
    assert_eq!(len, 100);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_task_summaries() -> anyhow::Result<()> {
    tracing_subscriber_init()?;
    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
    let _ = controller.get_task_summaries(10).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_edition_check_with_taos() -> anyhow::Result<()> {
    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
    let from = Dsn::from_str("taos://")?;
    let to = Dsn::from_str("taos+ws://localhost:6041")?;
    license::validate_task(&from, &to, Some(&controller.pool)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn active_active_edition_check_with_taos() -> anyhow::Result<()> {
    let _ = tracing_subscriber_init();
    let from = Dsn::from_str("tmq+ws://localhost:16041/test?replica")?;
    let to = Dsn::from_str("taos+ws://localhost:6041/test")?;
    license::validate_task(&from, &to, None).await?;
    let from = Dsn::from_str("tmq:///test?replica")?;
    let to = Dsn::from_str("taos:///test")?;
    license::validate_task(&from, &to, None).await?;

    let from = Dsn::from_str("tmq+ws://localhost:16041/test?replica")?;
    let to = Dsn::from_str("taos+ws://localhost:6041/test")?;
    let res = license::validate_task(&from, &to, None).await;
    dbg!(&res);
    assert!(res.is_err());
    Ok(())
}

#[test]
fn test_parse_csv() {
    let dsn = Dsn::from_str("csv:./ab.csv,./cd.csv?param=1").unwrap();
    dbg!(&dsn);
    assert_eq!(dsn.path.unwrap(), "./ab.csv,./cd.csv");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_replica_docker() -> anyhow::Result<()> {
    std::thread::spawn(move || {
        std::env::set_current_dir("tests/active-active").unwrap();
        let _ = std::process::Command::new("docker")
            .args(["compose", "up", "-d", "--remove-orphans"])
            .output();
    })
    .join()
    .unwrap();
    let source = TaosBuilder::from_dsn("http://localhost:7041")?
        .build()
        .await?;
    let sink = TaosBuilder::from_dsn("http://localhost:8041")?
        .build()
        .await?;
    {
        // prepare data
        source
            .exec_many([
                "drop topic if exists __replica__rep1",
                "drop database if exists rep1",
                "create database if not exists rep1",
                "create table if not exists rep1.t1 (ts timestamp, c1 int)",
                "insert into rep1.t1 values(now, 1)",
                "drop topic if exists __replica__rep2",
                "drop database if exists rep2",
                "create database if not exists rep2",
                "create table if not exists rep2.st1 (ts timestamp, c1 int) tags(t1 int)",
                "insert into rep2.t1 using rep2.st1 tags(1) values(now, 2)",
            ])
            .await?;
        sink.exec_many([
            "drop database if exists rep1",
            "drop database if exists rep2",
        ])
        .await?;
    }
    let _ = tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .pretty()
        .try_init();
    let (controller, _scheduler, _agent_notify_sender) = generate_scheduler_for_test().await?;
    let opts = ReplicaOpts {
        source: "http://localhost:7041".to_string(),
        sink: "http://localhost:8041".to_string(),
        new_databases_checking_interval: Some(1),
        ..Default::default()
    };
    let arc = Arc::new(controller);
    let replica = arc.start_replica_monitor(opts).await?;
    assert!(replica.id.is_some(), "replica id is none: {replica:?}");
    let reps = arc.list_replicas().await?;
    assert!(
        reps.is_empty() || reps[0].1.is_empty(),
        "replicas is not empty: {reps:?}"
    );

    {
        sink.exec("create database if not exists rep1").await?;
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let reps = arc.list_replicas().await?;
    assert_eq!(reps[0].1.len(), 1, "replicas should have 1 task: {reps:?}");

    {
        sink.exec("create database if not exists rep2").await?;
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    let reps = arc.list_replicas().await?;
    assert_eq!(reps[0].1.len(), 2, "replicas should have 2 task: {reps:?}");

    let del = arc
        .remove_replica_monitor(replica.id.as_deref().unwrap())
        .await?;
    assert!(del.is_some());
    Ok(())
}
