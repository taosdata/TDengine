#[cfg(windows)]
use anyhow::Context;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;
#[cfg(windows)]
use arrow::{
    array::ArrayRef, array::BinaryArray, array::BooleanArray, array::Float32Array,
    array::Float64Array, array::Int8Array, array::Int16Array, array::Int32Array, array::Int64Array,
    array::StringArray, array::TimestampMicrosecondArray, array::TimestampMillisecondArray,
    array::TimestampNanosecondArray, array::TimestampSecondArray, array::UInt8Array,
    array::UInt16Array, array::UInt32Array, array::UInt64Array,
};
#[cfg(windows)]
use chrono::{DateTime, Local};
#[cfg(windows)]
use kinghistorian_sys::windows::Data as KhData;
#[cfg(windows)]
use kinghistorian_sys::windows::HistoryDataType;
#[cfg(windows)]
use kinghistorian_sys::windows::TagFields;
#[cfg(windows)]
use kinghistorian_sys::windows::TagProperties;
#[cfg(windows)]
use kinghistorian_sys::windows::Value as KhValue;
#[cfg(windows)]
use kinghistorian_sys::windows::{ConnectionOptions, DataCriteria, ServerConnection, TagCriteria};
use std::collections::HashMap;
use std::str::FromStr;
#[cfg(windows)]
use std::sync::Arc;
use taos::Itertools;
use taosx_core::DataSet;
use taosx_core::sink::point::model::PointModelConfig;
use taosx_ipc::prelude::IpcDataType;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
use crate::KingHistTagOption;
#[cfg(windows)]
use crate::KingHistVarGroup;
use crate::ListCriteria;
use crate::{
    build_point_schema,
    config::{HistQueryCriteria, KingHistConfig, KingHistConnectConfig, KingHistMode},
    ensure_api,
};

#[cfg(windows)]
fn create_server_connection(connect: &KingHistConnectConfig) -> anyhow::Result<ServerConnection> {
    // connect to kinghistorian server
    let prot = connect.port.to_string();
    let mut builder =
        ConnectionOptions::builder(&connect.host, &prot, &connect.username, &connect.password);
    if let Some(timeout) = &connect.timeout_ms {
        builder = builder.network_timeout_ms(*timeout);
    }
    let opts = builder.build();
    let conn =
        ServerConnection::new(opts).context("failed to create kinghistorian server connection")?;

    tracing::info!(
        "kinghistorian connected at {}:{}",
        &connect.host,
        connect.port
    );

    Ok(conn)
}

#[cfg(windows)]
fn query_tag_values_with_retry(
    conn: &mut ServerConnection,
    conn_config: &KingHistConnectConfig,
    points: &[String],
    start: DateTime<Local>,
    end: DateTime<Local>,
    max_retries: usize,
    retry_interval_sec: usize,
    cancel: &CancellationToken,
) -> anyhow::Result<HashMap<String, Vec<KhData>>> {
    let mut attempt: usize = 0;
    loop {
        if cancel.is_cancelled() {
            tracing::info!("kinghist_to_taos collect history cancelled during retry, aborting...");
            return Ok(HashMap::new());
        }

        let filter = DataCriteria::builder(points)
            .start_time(start.to_utc())
            .end_time(end.to_utc())
            .row_count(MAX_ROWS_PER_DAY)
            .build();
        let result = conn
            .query_tag_values(filter)
            .context("failed to query kinghistorian tag values")
            .inspect_err(|e| {
                tracing::error!("kinghistorian query_tag_values error: {e:#?}");
            });

        match result {
            Ok(tags) => return Ok(tags),
            Err(err) => {
                attempt += 1;
                if attempt > max_retries {
                    return Err(err.context(format!(
                        "kinghistorian query_tag_values failed after {} retries",
                        max_retries
                    )));
                }

                tracing::warn!(
                    "kinghistorian query_tag_values failed (attempt {}/{}) for window [{} - {}], will retry after {}s: {:#?}",
                    attempt,
                    max_retries,
                    start,
                    end,
                    retry_interval_sec,
                    err
                );

                let sleep_ms = (retry_interval_sec as u64).saturating_mul(1000);
                let step = std::time::Duration::from_millis(100);
                let mut waited = 0u64;
                while waited < sleep_ms {
                    if cancel.is_cancelled() {
                        tracing::info!(
                            "kinghist_to_taos collect history cancelled during backoff, aborting..."
                        );
                        return Ok(HashMap::new());
                    }
                    std::thread::sleep(step);
                    waited = waited.saturating_add(step.as_millis() as u64);
                }

                // recreate connection before next attempt
                *conn = create_server_connection(conn_config)?;
                tracing::info!("recreating kinghistorian connection before next retry...");
            }
        }
    }
}

// use the sql repr as key
pub fn type_key_of(val: &IpcDataType) -> String {
    val.sql_repr_display()
}

// 每个 collector 负责一种数据类型的 point 数据
pub async fn run_collectors(
    task_config: &KingHistConfig,
    model_config: &PointModelConfig,
    sender_map: &HashMap<String, flume::Sender<RecordBatch>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut handles = vec![];

    for (key, tx) in sender_map.iter() {
        let key = key.clone();
        // 需要采集的点位
        let points = model_config
            .point_config_map
            .iter()
            .filter(|(_pid, pcfg)| {
                pcfg.value_type
                    .as_ref()
                    .map(|ty| type_key_of(ty) == key.clone())
                    .unwrap_or(false)
            })
            .map(|(pid, _)| pid.clone())
            .collect_vec();
        // 使用 sender 发送数据到 IPC
        let tx_clone = tx.clone();
        // 查询条件
        let task_config = task_config.clone();
        // 处理 cancel
        let cancel_inner = cancel.clone();
        // On Windows, the underlying kinghistorian bindings use !Send types.
        // Run each collector on a dedicated current-thread runtime inside a blocking task
        // to avoid the Send bound required by tokio::spawn.
        #[cfg(windows)]
        let h = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            tracing::info!("collector[{key}] start, points: {}", points.len());

            // Build schema for this type key
            let ipc_ty = IpcDataType::from_str(&key)
                .map_err(|e| anyhow::anyhow!("parse ipc type from key[{key}] error: {e}"))?;
            let value_dtype: ArrowDataType = ipc_ty.arrow_data_type();
            let schema = build_point_schema(value_dtype);

            match task_config.mode {
                KingHistMode::History => {
                    let query = task_config.query_criteria.as_ref().ok_or(anyhow::anyhow!(
                        "kinghist_to_taos collector missing historian query criteria"
                    ))?;

                    collect_history(
                        &task_config.connect,
                        points,
                        query,
                        &schema,
                        &ipc_ty,
                        tx_clone,
                        cancel_inner,
                        task_config.max_retries,
                        task_config.retry_interval_sec,
                    )?;
                }
                KingHistMode::RealTime => {
                    let min_elapsed = task_config.min_elapsed.ok_or(anyhow::anyhow!(
                        "kinghist_to_taos collect missing min_elapsed"
                    ))?;

                    collect_realtime(
                        &task_config.connect,
                        points,
                        min_elapsed,
                        &schema,
                        &ipc_ty,
                        tx_clone,
                        cancel_inner,
                    )?;
                }
            }

            Ok(())
        });

        #[cfg(not(windows))]
        let h = tokio::spawn(async move {
            tracing::info!("collector[{key}] start, points: {:?}", points);

            // Build schema for this type key
            let ipc_ty = IpcDataType::from_str(&key)
                .map_err(|e| anyhow::anyhow!("parse ipc type from key[{key}] error: {e}"))?;
            let value_dtype: ArrowDataType = ipc_ty.arrow_data_type();
            let schema = build_point_schema(value_dtype);

            match task_config.mode {
                KingHistMode::History => {
                    let query = task_config.query_criteria.as_ref().ok_or(anyhow::anyhow!(
                        "kinghist_to_taos collector missing historian query criteria"
                    ))?;

                    collect_history(
                        &task_config.connect,
                        points,
                        query,
                        &schema,
                        &ipc_ty,
                        tx_clone,
                        cancel_inner,
                        task_config.max_retries,
                        task_config.retry_interval_sec,
                    )?;
                }
                KingHistMode::RealTime => {
                    let min_elapsed = task_config.min_elapsed.ok_or(anyhow::anyhow!(
                        "kinghist_to_taos collect missing min_elapsed"
                    ))?;

                    collect_realtime(
                        &task_config.connect,
                        points,
                        min_elapsed,
                        &schema,
                        &ipc_ty,
                        tx_clone,
                        cancel_inner,
                    )?;
                }
            }

            Ok(())
        });

        handles.push(h);
    }

    // Wait for all collectors to finish or cancellation to occur
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        // Join all and map any join errors
        let mut res: anyhow::Result<()> = Ok(());
        for h in handles {
            match h.await {
                Ok(inner) => {
                    if let Err(e) = inner {
                        res = Err(e);
                        break;
                    }
                }
                Err(join_err) => {
                    res = Err(anyhow::anyhow!(
                        "kinghist_to_taos collector tasks join error: {join_err:#?}"
                    ));
                    break;
                }
            }
        }
        let _ = done_tx.send(res);
    });

    tokio::select! {
        res = &mut done_rx => {
            match res {
                Ok(r) => r,
                Err(_canceled) => Ok(()),
            }
        }
        _ = cancel.cancelled() => {
            tracing::info!("kinghist_to_taos collectors received cancellation, aborting...");
            Ok(())
        }
    }
}

#[cfg(windows)]
const MAX_ROWS_PER_DAY: u32 = 24 * 3600 * 1000;
#[cfg(windows)]
const BATCH: usize = 10000;

#[allow(unused_variables)]
fn collect_history(
    conn_config: &KingHistConnectConfig,
    points: Vec<String>,
    query: &HistQueryCriteria,
    schema: &arrow_schema::Schema,
    ipc_ty: &IpcDataType,
    sender: flume::Sender<RecordBatch>,
    cancel: CancellationToken,
    max_retries: usize,
    retry_interval_sec: usize,
) -> anyhow::Result<()> {
    ensure_api()?;

    #[cfg(not(windows))]
    {
        anyhow::bail!("kinghistorian collector is only supported on Windows");
    }

    #[cfg(windows)]
    {
        // create initial connection and reuse it across windows
        let mut conn = create_server_connection(conn_config)?;

        // generate query windows
        let windows = gen_windows(query);

        for (start, end) in windows {
            if cancel.is_cancelled() {
                tracing::info!("kinghist_to_taos collect history cancelled, aborting...");
                return Ok(());
            }

            tracing::debug!(
                "kinghist_to_taos querying data from {} to {}, points: {}",
                start,
                end,
                points.len()
            );

            for chunk in points.chunks(100) {
                // request time in milliseconds
                let request_ts_ms = Local::now().timestamp_millis();
                let tags = query_tag_values_with_retry(
                    &mut conn,
                    conn_config,
                    chunk,
                    start,
                    end,
                    max_retries,
                    retry_interval_sec,
                    &cancel,
                )?;
                // received time in milliseconds
                let received_ts_ms = Local::now().timestamp_millis();

                let tag_cnt = tags.len();
                let mut row_cnt = 0;
                for (key, rows) in tags {
                    if rows.is_empty() {
                        continue;
                    }
                    // Build column vectors first
                    let n = rows.len();
                    row_cnt += n;

                    // id and name: both are the key
                    let ids: Vec<Option<String>> =
                        std::iter::repeat(Some(key.clone())).take(n).collect();
                    let names: Vec<Option<String>> =
                        std::iter::repeat(Some(key.clone())).take(n).collect();
                    // ts
                    let ts_vals: Vec<Option<i64>> = rows
                        .iter()
                        .map(|d| d.timestamp.map(|t| t.timestamp_millis()))
                        .collect();
                    // received ts (ms)
                    let rts_vals: Vec<i64> = std::iter::repeat(received_ts_ms).take(n).collect();
                    // request ts (ms)
                    let qts_vals: Vec<i64> = std::iter::repeat(request_ts_ms).take(n).collect();
                    // quality
                    let status_vals: Vec<i64> = rows.iter().map(|d| d.quality as i64).collect();
                    // value column according to ipc_ty
                    let value_arr: ArrayRef = build_value_array(ipc_ty, &rows)?;

                    // Construct arrays
                    let id_arr = Arc::new(StringArray::from(ids)) as ArrayRef;
                    let name_arr = Arc::new(StringArray::from(names)) as ArrayRef;
                    let ts_arr = Arc::new(TimestampMillisecondArray::from(ts_vals)) as ArrayRef;
                    let received_arr =
                        Arc::new(TimestampMillisecondArray::from(rts_vals)) as ArrayRef;
                    let status_arr = Arc::new(Int64Array::from(status_vals)) as ArrayRef;
                    let request_arr =
                        Arc::new(TimestampMillisecondArray::from(qts_vals)) as ArrayRef;

                    let batch = RecordBatch::try_new(
                        Arc::new(schema.clone()),
                        vec![
                            id_arr,
                            name_arr,
                            ts_arr,
                            received_arr,
                            value_arr,
                            status_arr,
                            request_arr,
                        ],
                    )?;

                    // Send to IPC stream
                    if let Err(e) = sender.send(batch) {
                        anyhow::bail!("failed to send record batch to IPC: {}", e);
                    }
                    tracing::debug!("kinghist_to_taos sent {} rows for point {}", n, key);
                }

                tracing::debug!(
                    "kinghistorian query returned {} tags {} rows",
                    tag_cnt,
                    row_cnt
                );
            }

            // interval between queries
            if query.interval > 0 {
                std::thread::sleep(std::time::Duration::from_millis(query.interval as u64));
            }
        }

        Ok(())
    }
}

#[cfg(windows)]
fn build_value_array(ipc_ty: &IpcDataType, rows: &[KhData]) -> anyhow::Result<ArrayRef> {
    // Helper to convert KH value to desired primitive
    fn to_bool(v: &KhValue) -> Option<bool> {
        match v {
            KhValue::Bool(b) => Some(*b),
            KhValue::I8(x) => Some(*x != 0),
            KhValue::I16(x) => Some(*x != 0),
            KhValue::I32(x) => Some(*x != 0),
            KhValue::I64(x) => Some(*x != 0),
            KhValue::U8(x) => Some(*x != 0),
            KhValue::U16(x) => Some(*x != 0),
            KhValue::U32(x) => Some(*x != 0),
            KhValue::U64(x) => Some(*x != 0),
            KhValue::F32(x) => Some(*x != 0.0),
            KhValue::F64(x) => Some(*x != 0.0),
            KhValue::Str(s) => s
                .parse::<i64>()
                .ok()
                .map(|x| x != 0)
                .or_else(|| s.parse::<bool>().ok()),
            _ => None,
        }
    }

    fn to_i64(v: &KhValue) -> Option<i64> {
        match v {
            KhValue::I8(x) => Some(*x as i64),
            KhValue::I16(x) => Some(*x as i64),
            KhValue::I32(x) => Some(*x as i64),
            KhValue::I64(x) => Some(*x),
            KhValue::U8(x) => Some(*x as i64),
            KhValue::U16(x) => Some(*x as i64),
            KhValue::U32(x) => Some(*x as i64),
            KhValue::U64(x) => {
                if *x > i64::MAX as u64 {
                    tracing::warn!(
                        "Value {} is too large to fit in i64, capping at i64::MAX",
                        *x
                    );
                    Some(i64::MAX)
                } else {
                    Some(*x as i64)
                }
            }
            KhValue::F32(x) => Some(*x as i64),
            KhValue::F64(x) => Some(*x as i64),
            KhValue::Str(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    fn to_u64(v: &KhValue) -> Option<u64> {
        match v {
            KhValue::I8(x) => {
                if *x < 0 {
                    tracing::warn!(
                        "negative i8 value {} converted to u64 will be discarded",
                        *x
                    );
                    None
                } else {
                    Some(*x as u64)
                }
            }
            KhValue::I16(x) => {
                if *x < 0 {
                    tracing::warn!(
                        "negative i16 value {} converted to u64 will be discarded",
                        *x
                    );
                    None
                } else {
                    Some(*x as u64)
                }
            }
            KhValue::I32(x) => {
                if *x < 0 {
                    tracing::warn!(
                        "negative i32 value {} converted to u64 will be discarded",
                        *x
                    );
                    None
                } else {
                    Some(*x as u64)
                }
            }
            KhValue::I64(x) => {
                if *x < 0 {
                    tracing::warn!(
                        "negative i64 value {} converted to u64 will be discarded",
                        *x
                    );
                    None
                } else {
                    Some(*x as u64)
                }
            }
            KhValue::U8(x) => Some(*x as u64),
            KhValue::U16(x) => Some(*x as u64),
            KhValue::U32(x) => Some(*x as u64),
            KhValue::U64(x) => Some(*x),
            KhValue::F32(x) => {
                if *x < 0.0 {
                    tracing::warn!(
                        "negative f32 value {} converted to u64 will be saturated to 0",
                        *x
                    );
                }
                Some((*x).max(0.0) as u64)
            }
            KhValue::F64(x) => {
                if *x < 0.0 {
                    tracing::warn!(
                        "negative f64 value {} converted to u64 will be saturated to 0",
                        *x
                    );
                }
                Some((*x).max(0.0) as u64)
            }
            KhValue::Str(s) => {
                match s.parse::<u64>() {
                    Ok(v) => Some(v),
                    Err(_) => {
                        // try parse as signed to emit warning for negatives
                        if let Ok(neg) = s.parse::<i64>() {
                            if neg < 0 {
                                tracing::warn!(
                                    "negative string value '{}' converted to u64 is discarded",
                                    s
                                );
                            }
                        }
                        None
                    }
                }
            }
            _ => None,
        }
    }

    fn to_f64(v: &KhValue) -> Option<f64> {
        match v {
            KhValue::I8(x) => Some(*x as f64),
            KhValue::I16(x) => Some(*x as f64),
            KhValue::I32(x) => Some(*x as f64),
            KhValue::I64(x) => Some(*x as f64),
            KhValue::U8(x) => Some(*x as f64),
            KhValue::U16(x) => Some(*x as f64),
            KhValue::U32(x) => Some(*x as f64),
            KhValue::U64(x) => Some(*x as f64),
            KhValue::F32(x) => Some(*x as f64),
            KhValue::F64(x) => Some(*x),
            KhValue::Str(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    fn to_string_opt(v: &KhValue) -> Option<String> {
        match v {
            KhValue::Str(s) => Some(s.clone()),
            KhValue::WStr(ws) => Some(ws.into_iter().join("")),
            KhValue::Bool(x) => Some(x.to_string()),
            KhValue::I8(x) => Some(x.to_string()),
            KhValue::I16(x) => Some(x.to_string()),
            KhValue::I32(x) => Some(x.to_string()),
            KhValue::I64(x) => Some(x.to_string()),
            KhValue::U8(x) => Some(x.to_string()),
            KhValue::U16(x) => Some(x.to_string()),
            KhValue::U32(x) => Some(x.to_string()),
            KhValue::U64(x) => Some(x.to_string()),
            KhValue::F32(x) => Some(x.to_string()),
            KhValue::F64(x) => Some(x.to_string()),
            KhValue::Timestamp(t) => Some(t.timestamp_millis().to_string()),
            _ => None,
        }
    }

    let arr: ArrayRef = match ipc_ty {
        IpcDataType::Bool => {
            let vals: Vec<Option<bool>> = rows.iter().map(|d| to_bool(&d.value)).collect();
            Arc::new(BooleanArray::from(vals))
        }
        IpcDataType::Int8 => {
            let vals: Vec<Option<i8>> = rows
                .iter()
                .map(|d| to_i64(&d.value).map(|x| x as i8))
                .collect();
            Arc::new(Int8Array::from(vals))
        }
        IpcDataType::Int16 => {
            let vals: Vec<Option<i16>> = rows
                .iter()
                .map(|d| to_i64(&d.value).map(|x| x as i16))
                .collect();
            Arc::new(Int16Array::from(vals))
        }
        IpcDataType::Int32 => {
            let vals: Vec<Option<i32>> = rows
                .iter()
                .map(|d| to_i64(&d.value).map(|x| x as i32))
                .collect();
            Arc::new(Int32Array::from(vals))
        }
        IpcDataType::Int64 => {
            let vals: Vec<Option<i64>> = rows.iter().map(|d| to_i64(&d.value)).collect();
            Arc::new(Int64Array::from(vals))
        }
        IpcDataType::UInt8 => {
            let vals: Vec<Option<u8>> = rows
                .iter()
                .map(|d| to_u64(&d.value).map(|x| x as u8))
                .collect();
            Arc::new(UInt8Array::from(vals))
        }
        IpcDataType::UInt16 => {
            let vals: Vec<Option<u16>> = rows
                .iter()
                .map(|d| to_u64(&d.value).map(|x| x as u16))
                .collect();
            Arc::new(UInt16Array::from(vals))
        }
        IpcDataType::UInt32 => {
            let vals: Vec<Option<u32>> = rows
                .iter()
                .map(|d| to_u64(&d.value).map(|x| x as u32))
                .collect();
            Arc::new(UInt32Array::from(vals))
        }
        IpcDataType::UInt64 => {
            let vals: Vec<Option<u64>> = rows.iter().map(|d| to_u64(&d.value)).collect();
            Arc::new(UInt64Array::from(vals))
        }
        IpcDataType::Float32 => {
            let vals: Vec<Option<f32>> = rows
                .iter()
                .map(|d| to_f64(&d.value).map(|x| x as f32))
                .collect();
            Arc::new(Float32Array::from(vals))
        }
        IpcDataType::Float64 => {
            let vals: Vec<Option<f64>> = rows.iter().map(|d| to_f64(&d.value)).collect();
            Arc::new(Float64Array::from(vals))
        }
        IpcDataType::Timestamp(unit) => {
            let vals: Vec<Option<i64>> = rows
                .iter()
                .map(|d| match &d.value {
                    KhValue::Timestamp(t) => Some(match unit {
                        arrow::datatypes::TimeUnit::Second => t.timestamp(),
                        arrow::datatypes::TimeUnit::Millisecond => t.timestamp_millis(),
                        arrow::datatypes::TimeUnit::Microsecond => t.timestamp_micros(),
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            t.timestamp_nanos_opt().unwrap_or(0)
                        }
                    }),
                    KhValue::I64(x) => Some(*x),
                    KhValue::U64(x) => (*x).try_into().ok(),
                    KhValue::Str(s) => s.parse::<i64>().ok(),
                    _ => None,
                })
                .collect();
            match unit {
                arrow::datatypes::TimeUnit::Second => {
                    Arc::new(TimestampSecondArray::from(vals)) as ArrayRef
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    Arc::new(TimestampMillisecondArray::from(vals)) as ArrayRef
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    Arc::new(TimestampMicrosecondArray::from(vals)) as ArrayRef
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    Arc::new(TimestampNanosecondArray::from(vals)) as ArrayRef
                }
            }
        }
        IpcDataType::VarChar(_) | IpcDataType::NChar(_) | IpcDataType::Json => {
            let vals: Vec<Option<String>> = rows.iter().map(|d| to_string_opt(&d.value)).collect();
            Arc::new(StringArray::from(vals))
        }
        IpcDataType::VarBinary(_) | IpcDataType::Blob => {
            let vals: Vec<Option<&[u8]>> = rows
                .iter()
                .map(|d| match &d.value {
                    KhValue::Blob(b) => Some(b.as_slice()),
                    KhValue::Str(s) => Some(s.as_bytes()),
                    _ => None,
                })
                .collect();
            Arc::new(BinaryArray::from_opt_vec(vals))
        }
        IpcDataType::Decimal(_, _) | IpcDataType::Null => {
            // Not supported yet, fill nulls matching length
            let len = rows.len();
            let vals: Vec<Option<i64>> = std::iter::repeat(None).take(len).collect();
            Arc::new(Int64Array::from(vals))
        }
    };

    Ok(arr)
}

// 从 HistoryQueryConfig.start 开始，到 HistoryQueryConfig.end 结束的数据，
// 每次查询使用time_range 作为步长，并向前回溯restro长的时间间隔。两次查询之间间隔 interval 毫秒。
// 例如：查询 2025-10-01T01:00:00Z 到 2025-10-01T03:00:00Z的数据，restro 为 30 分钟，time_range 为 1 小时
// 则，分为2次查询：[2025-10-01T00:30:00, 2025-10-01T02:00:00) 和 [2025-10-01T01:30:00, 2025-10-01T03:00:00)
#[cfg(windows)]
fn gen_windows(query: &HistQueryCriteria) -> Vec<(DateTime<Local>, DateTime<Local>)> {
    let mut windows = vec![];

    let start = query.start;
    let end = query.end;

    if end <= start {
        return windows;
    }

    let step = match chrono::Duration::from_std(query.time_range) {
        Ok(d) => d,
        Err(_) => {
            return windows;
        }
    };
    let restro = match chrono::Duration::from_std(query.restro) {
        Ok(d) => d,
        Err(_) => chrono::Duration::zero(),
    };

    if step.is_zero() {
        let left = start - restro;
        windows.push((left, end));
        return windows;
    }

    let mut i: i32 = 0;
    loop {
        let mut right = start + step * (i + 1);
        if right > end {
            right = end;
        }

        let left = start + step * i - restro;
        windows.push((left, right));

        if right >= end {
            break;
        }
        i += 1;
    }

    windows
}

#[allow(unused_variables)]
/// 实时数据同步
fn collect_realtime(
    conn_config: &KingHistConnectConfig,
    points: Vec<String>,
    min_elapsed: usize,
    schema: &arrow_schema::Schema,
    ipc_ty: &IpcDataType,
    tx: flume::Sender<RecordBatch>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    ensure_api()?;

    #[cfg(not(windows))]
    {
        anyhow::bail!("kinghistorian realtime data collection only supported on Windows")
    }

    #[cfg(windows)]
    {
        tracing::info!(
            "kinghistorian starting realtime data collection for {} points",
            points.len()
        );

        // connect to kinghistorian server
        let mut conn = create_server_connection(conn_config)?;
        tracing::info!(
            "kinghistorian connected to {}:{}",
            &conn_config.host,
            conn_config.port
        );

        let (sender, receiver) = flume::bounded(128);
        // subscribe points
        let res = conn
            .data_subscribe(&points, min_elapsed as u32, Some(sender))
            .inspect_err(|e| {
                tracing::error!("kinghistorian subscribe error: {e:#?}");
            })
            .context("failed to subscribe kinghistorian data")?;

        // 检查订阅结果，如果有错误则返回
        let mut sub_err = None;
        for (id, r) in res.into_iter().enumerate() {
            if let Err(e) = r {
                let pid = points.get(id).cloned().unwrap_or_default();
                tracing::error!("kinghistorian subscribe point: {}, error: {:#?}", pid, e);
                sub_err =
                    Some(anyhow::Error::from(e).context("failed to subscribe kinghistorian tags"));
                break;
            }
        }
        if let Some(e) = sub_err {
            return Err(e);
        }
        tracing::info!(
            "kinghistorian realtime subscription established for {} tags",
            points.len()
        );

        // Receive loop until cancelled
        loop {
            if cancel.is_cancelled() {
                tracing::info!("kinghist_to_taos collect realtime cancelled, aborting...");
                break;
            }
            let received_res = match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                Err(flume::RecvTimeoutError::Timeout) => {
                    continue;
                }
                Err(err) => {
                    tracing::warn!("kinghistorian realtime subscription channel closed: {err:#?}");
                    break;
                }
                Ok(res) => res,
            };

            let data_record = match received_res {
                Ok(v) => v,
                Err(err) => {
                    tracing::error!("failed to get subscribed data rows: {:#?}", err);
                    continue;
                }
            };

            // request and receive timestamps (ms)
            // For realtime, use the time we received as both request and receive markers
            let received_ts_ms = Local::now().timestamp_millis();
            let request_ts_ms = received_ts_ms;

            // process data_record to RecordBatch
            let name = data_record
                .tag_name()
                .context("failed to get tag name from data record")?;
            let rows = data_record.data;
            if rows.is_empty() {
                continue;
            }

            let n = rows.len();
            // id/name are the same as tag name
            let ids: Vec<Option<String>> = std::iter::repeat(Some(name.clone())).take(n).collect();
            let names: Vec<Option<String>> =
                std::iter::repeat(Some(name.clone())).take(n).collect();
            // ts column
            let ts_vals: Vec<Option<i64>> = rows
                .iter()
                .map(|d| d.timestamp.map(|t| t.timestamp_millis()))
                .collect();
            // received and request columns
            let rts_vals: Vec<i64> = std::iter::repeat(received_ts_ms).take(n).collect();
            let qts_vals: Vec<i64> = std::iter::repeat(request_ts_ms).take(n).collect();
            // quality/status
            let status_vals: Vec<i64> = rows.iter().map(|d| d.quality as i64).collect();
            // value column
            let value_arr: ArrayRef = build_value_array(ipc_ty, &rows)?;

            // Build arrays
            let id_arr = Arc::new(StringArray::from(ids)) as ArrayRef;
            let name_arr = Arc::new(StringArray::from(names)) as ArrayRef;
            let ts_arr = Arc::new(TimestampMillisecondArray::from(ts_vals)) as ArrayRef;
            let received_arr = Arc::new(TimestampMillisecondArray::from(rts_vals)) as ArrayRef;
            let status_arr = Arc::new(Int64Array::from(status_vals)) as ArrayRef;
            let request_arr = Arc::new(TimestampMillisecondArray::from(qts_vals)) as ArrayRef;

            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    id_arr,
                    name_arr,
                    ts_arr,
                    received_arr,
                    value_arr,
                    status_arr,
                    request_arr,
                ],
            )?;

            if let Err(e) = tx.send(batch) {
                tracing::error!("failed to send record batch to IPC: {:#?}", e);
                return Err(anyhow::Error::from(e).context("failed to send record batch to IPC"));
            }

            tracing::debug!(
                "kinghistorian realtime subscribed data received for {} rows (tag: {})",
                n,
                name
            );
        }

        tracing::info!("kinghistorian realtime data collection stopped");

        Ok(())
    }
}

pub fn list_groups(conn_config: KingHistConnectConfig) -> anyhow::Result<Vec<DataSet>> {
    tracing::info!(
        "kinghistorian list var groups from {}:{}",
        &conn_config.host,
        conn_config.port
    );
    ensure_api()?;

    #[cfg(not(windows))]
    {
        anyhow::bail!("kinghist_to_taos list groups only supported on Windows")
    }

    #[cfg(windows)]
    {
        let mut conn = create_server_connection(&conn_config)?;

        // Only list groups meta without fetching var names to speed up
        let groups =
            fetch_groups_bfs(&mut conn, None, false).context("failed to fetch all groups")?;
        let mut datasets = vec![];
        for g in &groups {
            datasets.push(g.to_dataset());
        }

        Ok(datasets)
    }
}

/// 列出 KingHistorian 中的所有数据集（Tag）
pub fn list_datasets(
    connect: KingHistConnectConfig,
    criteria: ListCriteria,
) -> anyhow::Result<Vec<DataSet>> {
    tracing::info!(
        "kinghistorian listing datasets from {}:{}, criteria: {:?}",
        &connect.host,
        connect.port,
        &criteria
    );
    ensure_api()?;

    #[cfg(not(windows))]
    {
        anyhow::bail!("kinghist_to_taos list datasets only supported on Windows")
    }

    #[cfg(windows)]
    {
        use crate::csv;
        let mut conn = create_server_connection(&connect)?;

        let mut datasets = vec![];

        // 获取变量组列表
        let groups = match &criteria.groups {
            Some(required_groups) => fetch_groups_by_ids(&mut conn, required_groups)
                .context("failed to fetch groups by ids")?,
            None => fetch_groups(&mut conn).context("failed to fetch all groups")?,
        };
        // 将 group 添加到 DataSet 列表中
        for g in &groups {
            datasets.push(g.to_dataset());
        }

        // 构建 var_name -> var_group 的 map，用于从 var_name 找到所属的 var_group
        let mut var_to_group: HashMap<String, KingHistVarGroup> = {
            // 预估容量，减少 rehash 次数
            let total_vars: usize = groups.iter().map(|g| g.var_names.len()).sum();
            HashMap::with_capacity(total_vars)
        };
        for g in &groups {
            for v in &g.var_names {
                let var_group = KingHistVarGroup {
                    id: g.id,
                    name: g.name.clone(),
                    path: g.path.clone(),
                    var_names: vec![], // 不需要存储变量名列表
                };
                var_to_group.insert(v.clone(), var_group);
            }
        }

        // 获取变量列表：如果指定 groups，复用已获取的 groups（含 var_names）避免重复 RPC
        let tags = match &criteria.groups {
            Some(_) => fetch_tags_by_groups(&mut conn, &groups, &criteria.point_mask)
                .context("failed to fetch tags by group")?,
            None => fetch_tags(&mut conn).context("failed to fetch all tags")?,
        };

        // Apply pagination on tags list
        if criteria.limit == 0 {
            return Ok(vec![]);
        }
        let total = tags.len();
        if criteria.offset >= total {
            return Ok(vec![]);
        }

        // 将 tag 添加到 DataSet 列表中
        for tag_props in tags.into_iter().skip(criteria.offset).take(criteria.limit) {
            // 跳过 KingHistorian 的系统标签
            if let Some(tag_name) = &tag_props.tag_name {
                if tag_name.starts_with("@@") || tag_name.starts_with("$") {
                    tracing::debug!("kinghistorian skipping system tag: {} ", &tag_name);
                    continue;
                }
            }
            let ds = var_to_dataset(&var_to_group, tag_props, &criteria.tags)?;
            datasets.push(ds);
        }

        // 加入 __CSV_HEADER
        for i in 0..csv::DEFAULT_CSV_HEADERS.len() {
            let header = csv::DEFAULT_CSV_HEADERS[i];
            let ds = DataSet {
                id: i.to_string(),
                name: Some(header.to_string()),
                category: Some("__CSV_HEADER".to_string()),
                r#type: None,
                options: None,
                format: None,
            };
            datasets.push(ds);
        }
        for (i, tag) in criteria.tags.iter().enumerate() {
            let idx = csv::DEFAULT_CSV_HEADERS.len() + i;
            let ds = tag.to_csv_header_dataset(idx as i32);
            datasets.push(ds);
        }

        return Ok(datasets);
    }
}

/// 广度优先遍历所有变量组
#[cfg(windows)]
fn fetch_groups(conn: &mut ServerConnection) -> anyhow::Result<Vec<KingHistVarGroup>> {
    // Backward-compatible: include var names by default
    fetch_groups_bfs(conn, None, true)
}

/// 广度优先遍历变量组，只返回指定 group_ids 的变量组
#[cfg(windows)]
fn fetch_groups_by_ids(
    conn: &mut ServerConnection,
    group_ids: &[u32],
) -> anyhow::Result<Vec<KingHistVarGroup>> {
    // For callers that need groups with their variables (e.g., list_datasets_impl)
    fetch_groups_bfs(conn, Some(group_ids), true)
}

/// 广度优先遍历获取变量组，如果 filter_ids 为 Some，则只返回在 filter_ids 中的 group
#[cfg(windows)]
fn fetch_groups_bfs(
    conn: &mut ServerConnection,
    filter_ids: Option<&[u32]>,
    include_vars: bool,
) -> anyhow::Result<Vec<KingHistVarGroup>> {
    use std::collections::{HashSet, VecDeque};

    let mut result = Vec::new();
    let mut visited = HashSet::new();

    // 队列中存储 (group_id, parent_path)
    // parent_path 是父组的路径，不包含当前组
    let mut queue: VecDeque<(u32, Option<String>)> = VecDeque::new();

    // 从根组（ID = 1）开始
    queue.push_back((1, None));

    while let Some((group_id, parent_path)) = queue.pop_front() {
        // 避免重复访问
        if visited.contains(&group_id) {
            continue;
        }
        visited.insert(group_id);

        // 如果设置了过滤条件，检查当前 group_id 是否在过滤列表中
        let should_include = filter_ids.map_or(true, |ids| ids.contains(&group_id));

        // 获取当前组的属性
        let props = conn
            .get_tag_group_properties(group_id)
            .inspect_err(|e| {
                tracing::error!(
                    "failed to get tag group properties for group: {group_id}, error: {e:#?}",
                );
            })
            .with_context(|| format!("failed to get tag group properties for group: {group_id}"))?;

        let group_name = props.group_name.clone().unwrap_or_else(|| {
            tracing::warn!("group {} has no name, using empty string", group_id);
            String::new()
        });

        // 构建当前组的路径
        let current_path = match &parent_path {
            Some(parent) if !parent.is_empty() => {
                if group_name.is_empty() {
                    Some(parent.clone())
                } else {
                    Some(format!("{}.{}", parent, group_name))
                }
            }
            _ => {
                // 根组的子组路径就是它自己的名字
                if group_id == 1 {
                    None
                } else if group_name.is_empty() {
                    None
                } else {
                    Some(group_name.clone())
                }
            }
        };

        // 如果当前组应该被包含，则添加到结果中
        if should_include {
            // 获取该组包含的变量名列表（可选，避免在仅列组场景下的额外开销）
            let (var_names, var_count) = if include_vars {
                let names = conn.tag_group_get_tags(group_id).inspect_err(|err| {
                    tracing::error!("failed to get tags for group {}, err: {:#?}", group_id, err);
                })?;
                let cnt = names.len();
                (names, cnt)
            } else {
                (Vec::new(), 0)
            };

            result.push(KingHistVarGroup {
                id: group_id,
                name: group_name.clone(),
                path: current_path.clone(),
                var_names,
            });

            tracing::debug!(
                "fetched group: id={}, name={}, path={:?}, var_count={}",
                group_id,
                group_name,
                current_path,
                var_count
            );
        }

        // 获取子组并加入队列
        // 即使当前组不在过滤列表中，也要遍历其子组（因为子组可能在过滤列表中）
        match conn.tag_group_get_children(group_id) {
            Ok(children) => {
                for child_id in children {
                    if !visited.contains(&child_id) {
                        queue.push_back((child_id, current_path.clone()));
                    }
                }
            }
            Err(err) => {
                tracing::warn!(
                    "failed to get children for group {}: {:#?}, continuing...",
                    group_id,
                    err
                );
            }
        }
    }

    tracing::info!("fetched {} groups in total", result.len());
    Ok(result)
}

/// 根据已经获取的变量组（含 var_names）筛选变量，避免重复拉取组下变量名
#[cfg(windows)]
fn fetch_tags_by_groups(
    conn: &mut ServerConnection,
    groups: &[KingHistVarGroup],
    point_mask: &Option<String>,
) -> anyhow::Result<Vec<TagProperties>> {
    // 汇总所有变量组的变量名
    let mut required_tag_names: Vec<String> =
        Vec::with_capacity(groups.iter().map(|g| g.var_names.len()).sum());
    for g in groups {
        required_tag_names.extend(g.var_names.iter().cloned());
    }
    // 去重，防止跨组重复出现（如果允许）
    required_tag_names.sort();
    required_tag_names.dedup();

    // 如果没有任何变量名，直接返回空结果，避免后续无意义的 RPC
    if required_tag_names.is_empty() {
        tracing::debug!("fetch_tags_by_groups: no var names aggregated, returning empty list");
        return Ok(Vec::new());
    }

    let mut tags: Vec<TagProperties> = Vec::with_capacity(required_tag_names.len());
    match &point_mask {
        // 如果设置了 point_mask，则直接按 mask 查询，不再按变量名分 chunk，避免在服务端多次全量扫描
        Some(mask) => {
            let filter = TagCriteria::builder().tag_name_mask(mask).build();
            let fields = TagFields::builder()
                .tag_id()
                .tag_name()
                .description()
                .data_type()
                .data_length()
                .last_modified()
                .last_modified_user()
                .build();

            tags = conn
                .query_tag_properties(filter, fields)
                .inspect_err(|e| {
                    tracing::error!("kinghistorian list datasets error (mask only): {e:#?}");
                })
                .context("kinghistorian list datasets error (mask only)")?;
        }
        // 未设置 point_mask 时，按变量名精确查询，并分 chunk 降低单次 RPC 负载
        None => {
            for chunk in required_tag_names.chunks(BATCH) {
                let filter = TagCriteria::builder().tag_names(chunk).build();
                let fields = TagFields::builder()
                    .tag_id()
                    .tag_name()
                    .description()
                    .data_type()
                    .data_length()
                    .last_modified()
                    .last_modified_user()
                    .build();

                let mut batch_tags = conn
                    .query_tag_properties(filter, fields)
                    .inspect_err(|e| {
                        tracing::error!("kinghistorian list datasets error: {e:#?}");
                    })
                    .context("kinghistorian list datasets error")?;

                let fetched = batch_tags.len();
                tags.append(&mut batch_tags);
                tracing::debug!(
                    "fetch_tags_by_groups: fetched {} tag properties in current batch (total so far: {})",
                    fetched,
                    tags.len()
                );
            }
        }
    }

    // 返回汇总结果
    tracing::info!(
        "fetch_tags_by_groups: aggregated {} tag properties",
        tags.len()
    );

    Ok(tags)
}

/// 从 KingHistorian Server 中查所有变量
#[cfg(windows)]
fn fetch_tags(conn: &mut ServerConnection) -> anyhow::Result<Vec<TagProperties>> {
    let filter = TagCriteria::builder().tag_name_mask("*").build();
    let fields = TagFields::builder()
        .tag_id() // tag_id：变量ID
        .tag_name() // tag_name: 变量名
        .description() // tag_description：变量描述
        .data_type() // data_type：变量类型
        .data_length() // data_length：变量数据长度
        .last_modified() // last_modified：上次修改变量配置时间
        .last_modified_user() // last_modified_user：上次修改变量配置的用户
        .build();
    let tags = conn
        .query_tag_properties(filter, fields)
        .inspect_err(|e| {
            tracing::error!("kinghistorian list datasets error: {e:#?}");
        })
        .context("kinghistorian list datasets error")?;
    Ok(tags)
}

/// KingHistorian Var -> DataSet
/// groups: 变量组
/// tag_propes: 变量
#[cfg(windows)]
fn var_to_dataset(
    var_to_group_id: &HashMap<String, KingHistVarGroup>,
    tag_props: TagProperties,
    required_tags: &Vec<KingHistTagOption>,
) -> anyhow::Result<DataSet> {
    let tag_name = tag_props
        .tag_name
        .clone()
        .ok_or(anyhow::anyhow!("TagProperties.tag_name cannot be None"))?;
    let id = tag_props
        .tag_id
        .map(|i| i.to_string())
        .unwrap_or(tag_name.clone());
    let data_type = tag_props
        .data_type
        .ok_or(anyhow::anyhow!("TagProperties.data_type cannot be None"))?;
    let data_length = tag_props.data_length;
    let tag_type = to_ipc_data_type(data_type, data_length)?;

    // 使用预构建的索引在 O(1) 内找到变量所属的组
    // get() 返回 Option<&KingHistVarGroup>，而下游期望的是 Option<KingHistVarGroup>
    // 这里进行一次浅拷贝（KingHistVarGroup 已实现 Clone）
    let var_group = var_to_group_id.get(&tag_name).cloned();
    let mut options = Vec::with_capacity(required_tags.len());
    // KingHistTag 中的标签，添加到 DataSet.options 中
    for tag in required_tags.iter() {
        let option = tag.to_optionset(&tag_props, &var_group)?;
        options.push(option);
    }
    let options = if options.is_empty() {
        None
    } else {
        Some(options)
    };

    Ok(DataSet {
        id,                           // var's tag_id
        name: Some(tag_name.clone()), // var's tag_name
        category: Some("__TAG".to_string()),
        r#type: Some(tag_type.sql_repr_display()), // var's data_type
        options,                                   // var's tag options
        format: None,
    })
}

#[cfg(windows)]
pub fn to_ipc_data_type(
    data_type: HistoryDataType,
    data_length: Option<i32>,
) -> anyhow::Result<IpcDataType> {
    match data_type {
        HistoryDataType::Empty => Ok(IpcDataType::Null),
        HistoryDataType::Boolean => Ok(IpcDataType::Bool),
        HistoryDataType::Int8 => Ok(IpcDataType::Int8),
        HistoryDataType::Int16 => Ok(IpcDataType::Int16),
        HistoryDataType::Int32 => Ok(IpcDataType::Int32),
        HistoryDataType::Int64 => Ok(IpcDataType::Int64),
        HistoryDataType::Float32 => Ok(IpcDataType::Float32),
        HistoryDataType::Float64 => Ok(IpcDataType::Float64),
        HistoryDataType::Timestamp => Ok(IpcDataType::Timestamp(
            arrow::datatypes::TimeUnit::Millisecond,
        )),
        HistoryDataType::Char => Ok(IpcDataType::VarChar(1)),
        HistoryDataType::Varchar => {
            let len = data_length.unwrap_or(128);
            Ok(IpcDataType::VarChar(len as u32))
        }
        HistoryDataType::Nchar => {
            let len = data_length.unwrap_or(128);
            Ok(IpcDataType::NChar(len as u32))
        }
        HistoryDataType::Nvarchar => {
            let len = data_length.unwrap_or(128);
            Ok(IpcDataType::NChar(len as u32))
        }
        HistoryDataType::Binary => {
            let len = data_length.unwrap_or(128);
            Ok(IpcDataType::VarBinary(len as u32))
        }
        HistoryDataType::Varbinary => {
            let len = data_length.unwrap_or(128);
            Ok(IpcDataType::VarBinary(len as u32))
        }
        HistoryDataType::Digital => Ok(IpcDataType::VarChar(128)),
        HistoryDataType::Float16 => Ok(IpcDataType::Float32),
        _ => Err(anyhow::anyhow!(
            "unsupported kinghistorian data type: {:?}",
            data_type
        )),
    }
}

#[cfg(test)]
mod tests {

    #[cfg(windows)]
    #[test]
    fn test_gen_windows() {
        use super::*;
        use std::time::Duration;

        let query = HistQueryCriteria {
            start: DateTime::parse_from_rfc3339("2025-10-01T01:00:00+08:00")
                .unwrap()
                .with_timezone(&Local),
            end: DateTime::parse_from_rfc3339("2025-10-01T03:00:00+08:00")
                .unwrap()
                .with_timezone(&Local),
            time_range: Duration::from_secs(3600),
            restro: Duration::from_secs(1800),
            interval: 1000,
        };

        let windows = gen_windows(&query);
        dbg!(&windows);
        assert_eq!(windows.len(), 2);
        assert_eq!(
            windows[0],
            (
                DateTime::parse_from_rfc3339("2025-10-01T00:30:00+08:00")
                    .unwrap()
                    .with_timezone(&Local),
                DateTime::parse_from_rfc3339("2025-10-01T02:00:00+08:00")
                    .unwrap()
                    .with_timezone(&Local),
            )
        );
        assert_eq!(
            windows[1],
            (
                DateTime::parse_from_rfc3339("2025-10-01T01:30:00+08:00")
                    .unwrap()
                    .with_timezone(&Local),
                DateTime::parse_from_rfc3339("2025-10-01T03:00:00+08:00")
                    .unwrap()
                    .with_timezone(&Local),
            )
        );
    }
}
