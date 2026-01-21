use anyhow::{Context, bail};
use ha_core::types::{HaTask, SplitJobResult, SplitJobTask};

pub async fn plan_task(task: HaTask) -> anyhow::Result<SplitJobResult> {
    let split_task: SplitJobTask = task.try_into().context("build split task req error")?;

    let from_driver = split_task.from.driver.as_str();
    let to_driver = split_task.to.driver.as_str();
    let task_value = match (from_driver, to_driver) {
        ("kafka", _) => source_kafka::split_job::split_job(split_task)
            .await
            .context("kafka split job error")?,
        ("mqtt", _) => source_mqtt::split_job::split_job(split_task)
            .await
            .context("mqtt split job error")?,
        ("tmq" | "sync", "taos") => tmq_to_td::split_job::split_job(split_task)
            .await
            .context("tmq to taos split job error")?,
        ("taos", "taos") => legacy_to_taos::split_job::split_job(split_task)
            .await
            .context("legacy to taos split job error")?,
        ("tmq" | "sync", "local")
        | ("local", "taos" | "tmq")
        | ("taos", "csv")
        | ("taos", "parquet")
        | ("pi" | "pibackfill", "taos")
        | ("opc" | "opcda" | "opcua", "taos")
        | ("tmq", "mqtt")
        | ("sparkplugb", "taos")
        | ("influxdb", "taos")
        | ("opentsdb", "taos")
        | ("csv", "taos")
        | ("tmq", "kafka")
        | ("avevaHistorian", "taos")
        | ("orc", "taos")
        | ("mongodb", _)
        | ("mysql", _)
        | ("postgres", _)
        | ("oracle", _)
        | ("mssql", _) => {
            return Ok(SplitJobResult {
                from: serde_json::Value::String(split_task.from.to_string()),
                to: split_task.to.to_string(),
                parser: split_task.parser,
            });
        }
        _ => {
            bail!("unsupported split job from `{from_driver}` to `{to_driver}`")
        }
    };
    Ok(task_value)
}
