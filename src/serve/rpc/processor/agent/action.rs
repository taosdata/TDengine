use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array};
use ha_core::utils::next_req_id;
use taos::Dsn;
use taosx_core::utils::get_string_content_from_param_value;
use taosx_utils::dsn::json_to_dsn;
use tracing::instrument;
use uuid::Uuid;

use crate::serve::{
    controller::{AgentAction, Task, TaskControllerRef},
    rpc::{DataSetsSenders, DsvSenders, StringSenders},
    utils::csv::encode_csv_config_file,
};

pub async fn action_to_arrow(
    datasets_senders: &DataSetsSenders,
    dsv_senders: &DsvSenders,
    string_senders: &StringSenders,
    controller: &TaskControllerRef,
    action: AgentAction,
) -> anyhow::Result<Option<RecordBatch>> {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter_values([
        chrono::Utc::now().timestamp_millis(),
    ]));
    let req_id = next_req_id();

    match action {
        AgentAction::Run(task_id, job_id, jid, rid) => {
            tracing::info!(
                task.id = task_id,
                task.job_id = job_id,
                task.jid = %jid,
                task.rid = rid,
                "Send run action"
            );
            let task = controller.get_task(task_id, job_id).await;
            if let Some(mut task) = task {
                // handle dsn(from) params contains file(@)
                if let Err(err) = modify_task_dsn_params(&mut task).await {
                    tracing::error!(
                        task.id = task_id,
                        job.id = job_id,
                        "Failed to modify task dsn params: {err:#}"
                    );
                    return Err(err);
                }
                #[derive(serde::Serialize)]
                struct TaskInAgent {
                    #[serde(flatten)]
                    task: Task,
                    jid: Uuid,
                    rid: u64,
                }
                let context: ArrayRef =
                    Arc::new(StringArray::from_iter_values([serde_json::to_string(
                        &TaskInAgent { task, jid, rid },
                    )
                    .unwrap()]));
                let action: ArrayRef = Arc::new(StringArray::from_iter_values(["run".to_string()]));
                let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("ts", ts),
                    ("action", action),
                    ("context", context),
                    ("req_id", req_id),
                ])
                .context("failed to build record batch")?;
                Ok(Some(batch))
            } else {
                tracing::warn!(
                    "Received Run action for task ({task_id},{job_id}) but currently not found"
                );
                Ok(None)
            }
        }
        AgentAction::Stop(task_id, job_id) => {
            tracing::info!(
                task.id = task_id,
                job.id = job_id,
                "Send stop action to task ({task_id},{job_id})"
            );
            let task = controller.get_task(task_id, job_id).await;
            if let Some(task) = task {
                let context: ArrayRef =
                    Arc::new(StringArray::from_iter_values([serde_json::to_string(
                        &task,
                    )
                    .unwrap()]));
                let action: ArrayRef =
                    Arc::new(StringArray::from_iter_values(["stop".to_string()]));
                let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("ts", ts),
                    ("action", action),
                    ("context", context),
                    ("req_id", req_id),
                ])
                .context("failed to build record batch")?;
                Ok(Some(batch))
            } else {
                tracing::warn!(
                    "Received Stop action for task ({task_id},{job_id}) but currently not found"
                );
                Ok(None)
            }
        }
        AgentAction::Cancel(task_id, job_id) => {
            tracing::info!(
                task.id = task_id,
                job.id = job_id,
                "Send suspend action to task ({task_id},{job_id})"
            );
            let task = controller.get_task(task_id, job_id).await;
            if let Some(task) = task {
                let context: ArrayRef =
                    Arc::new(StringArray::from_iter_values([serde_json::to_string(
                        &task,
                    )
                    .unwrap()]));
                let action: ArrayRef =
                    Arc::new(StringArray::from_iter_values(["cancel".to_string()]));
                let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
                let batch = RecordBatch::try_from_iter(vec![
                    ("ts", ts),
                    ("action", action),
                    ("context", context),
                    ("req_id", req_id),
                ])
                .context("failed to build record batch")?;
                Ok(Some(batch))
            } else {
                tracing::warn!(
                    "Received Cancel action for task ({task_id},{job_id}) but currently not found"
                );
                Ok(None)
            }
        }
        AgentAction::ListDataSets(dataset, sender) => {
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                    &dataset,
                )
                .unwrap()]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values(["list".to_string()]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;

            let datasets_senders = datasets_senders.clone();
            let mut senders = datasets_senders.write();
            senders.insert(req_id, sender);
            Ok(Some(batch))
        }
        AgentAction::Check(dsn, sender) => {
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([
                serde_json::to_string(&dsn).unwrap(),
            ]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values(["check".to_string()]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;

            let mut senders = dsv_senders.write();
            senders.insert(req_id, sender);
            Ok(Some(batch))
        }
        AgentAction::GetSample(dsn, sender) => {
            let action: ArrayRef = Arc::new(StringArray::from_iter_values(["sample".to_string()]));
            // modify dsn params
            let dsn = modify_dsn_params_for_get_samples(&dsn).await?.to_string();
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(&dsn)
                    .map_err(|err| {
                        anyhow::format_err!("failed to serialize dsn: {err:#}")
                    })?]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build GetSample message")?;

            let mut senders = string_senders.write();
            senders.insert(req_id, sender);
            Ok(Some(batch))
        }
        AgentAction::PutFile(put_file_req, sender) => {
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                    &put_file_req,
                )
                .unwrap()]));
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values(["put-file".to_string()]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;
            let mut senders = string_senders.write();
            senders.insert(req_id, sender);
            Ok(Some(batch))
        }
        AgentAction::QueryDataSource(query_data_source_req, sender) => {
            let context: ArrayRef =
                Arc::new(StringArray::from_iter_values([serde_json::to_string(
                    &query_data_source_req,
                )
                .unwrap()]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([
                "query-data-source".to_string()
            ]));
            let req_id_array: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", ts),
                ("action", action),
                ("context", context),
                ("req_id", req_id_array),
            ])
            .context("failed to build record batch")?;
            let mut senders = string_senders.write();
            senders.insert(req_id, sender);
            Ok(Some(batch))
        }
    }
}

#[instrument(skip(task))]
async fn modify_task_dsn_params(task: &mut Task) -> anyhow::Result<()> {
    let dsn = modify_dsn_params(&task.from).await?;
    task.from = dsn.to_string();
    Ok(())
}

#[instrument(skip(dsn))]
async fn modify_dsn_params(dsn: &str) -> anyhow::Result<Dsn> {
    let mut dsn = json_to_dsn(&serde_json::Value::String(dsn.to_string()))?;
    tracing::debug!("dsn before modify: {}", &dsn);

    if let Some(v) = dsn.params.get("csv_config_file") {
        let csv_path = &v[1..];
        dsn.params
            .insert("csv_config_file_origin".to_string(), csv_path.to_string());
    }

    for (k, v) in dsn.params.iter_mut() {
        if k == "csv_config_file" {
            *v = encode_csv_config_file(v.clone()).await?;
            continue;
        }
        if k == "transform_config_file" {
            continue;
        }
        if v.contains("@")
            && let Some(new_value) = get_string_content_from_param_value(v, false, false)?
        {
            *v = new_value;
        }
    }

    tracing::debug!("dsn after modify: {}", &dsn);
    Ok(dsn)
}

/// For GetSample: only inline csv_config_file for drivers that need to parse CSV immediately
/// (opcda/opcua/opc). Other drivers keep the original @path to avoid large logs and unnecessary I/O.
#[instrument(skip(dsn))]
async fn modify_dsn_params_for_get_samples(dsn: &str) -> anyhow::Result<Dsn> {
    let mut dsn = Dsn::from_str(dsn).context("invalid modify list action dsn")?;
    let driver = dsn.driver.clone();
    tracing::debug!("dsn before modify (driver={}): {}", driver, &dsn);
    let need_inline = matches!(driver.to_lowercase().as_str(), "opcda" | "opcua" | "opc");

    if let Some(csv_config_file) = dsn.params.get("csv_config_file").cloned() {
        if csv_config_file.starts_with('@') && csv_config_file.len() > 1 {
            let csv_path = &csv_config_file[1..];
            dsn.params
                .insert("csv_config_file_origin".to_string(), csv_path.to_string());
        }
        if need_inline {
            // Inline only for OPC drivers.
            let new_v = encode_csv_config_file(csv_config_file).await?;
            dsn.params.insert("csv_config_file".to_string(), new_v);
        }
    }

    if need_inline {
        // Inline other @file params (except transform_config_file) to keep previous behavior.
        for (k, v) in dsn.params.clone().into_iter() {
            if k == "csv_config_file" || k == "transform_config_file" {
                continue;
            }
            if v.contains('@')
                && let Some(new_value) = get_string_content_from_param_value(&v, false, false)?
            {
                dsn.params.insert(k, new_value);
            }
        }
        tracing::debug!("dsn after modify (inlined): {}", &dsn);
    } else {
        tracing::debug!("dsn after modify (no inline needed): {}", &dsn);
    }

    Ok(dsn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_modify_dsn_params() {
        // modify the csv_config_file
        let dsn = "opcda://192.168.2.16/Matrikon.OPC.Simulation.1?csv_config_file=%40.%2Ftests%2Fopc%2Fopcda-utf8.csv";
        let new_dsn = modify_dsn_params(dsn).await.unwrap();
        let csv_config = new_dsn.params.get("csv_config_file").unwrap();
        assert_eq!(
            "MCx0YWdfbmFtZSxlbmFibGVkLHN0YWJsZSx0Ym5hbWUsdmFsdWVfY29sLHZhbHVlX3RyYW5zZm9ybSx0eXBlLHF1YWxpdHlfY29sLHRzX2NvbCxyZWNlaXZlZF90c19jb2wsdHNfdHJhbnNmb3JtLHJlY2VpdmVkX3RzX3RyYW5zZm9ybSx0YWc6OlZBUkNIQVIoMjAwKTo6bmFtZQ0KMSxyb290LnBhcmVudC50ZW1wZXJhdHVyZSwxLG9wY197dHlwZX0sdF97dGFnX25hbWV9LHZhbCx2YWwgKjEuOCArIDMyLGludCxxdWFsaXR5LHRzLHJ0cywscnRzICsgOGgs5YWl5bqT5rip5bqmDQoyLHJvb3QucGFyZW50LnByZXNzdXJlLDAsb3BjX3t0eXBlfSx0X3t0YWdfbmFtZX0sdmFsLHZhbCArIDEwLCxxdWFsaXR5LHRzLHJ0cyx0cyArIDhoLCzlh4/ljovpmIDljovlipsNCjMscm9vdC5wYXJlbnQuY3VycmVudCwxLG9wY19kYV9lbGVjLHRfY3VzdG9tX2N1cnJlbnQsdmFsLCwscXVhbGl0eSx0cyxydHMsdHMgLSA2cyxydHMgLSA2cyzmgLvnur/nlLXmtYENCg==",
            csv_config
        );

        // do not modify the transform_config_file
        let dsn = "pi://192.168.0.34/ci_test?transform_config_file=%40.%2Ftaosx-core%2Ftests%2Fpi%2Fpi_singlecol_point.csv";
        let new_dsn = modify_dsn_params(dsn).await.unwrap();
        let config_file = new_dsn.params.get("transform_config_file").unwrap();
        assert_eq!("@./taosx-core/tests/pi/pi_singlecol_point.csv", config_file);
    }
}
