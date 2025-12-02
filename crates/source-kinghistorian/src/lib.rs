use anyhow::Context;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
};
use core::str;
#[cfg(windows)]
use kinghistorian_sys::windows as kdb;
#[cfg(windows)]
use kinghistorian_sys::windows::TagProperties;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::net::Shutdown;
use std::net::TcpStream;
use std::sync::Arc;
#[cfg(windows)]
use std::sync::OnceLock;
use std::vec;
use taos::*;
use taosx_core::DataSet;
use taosx_core::core_metrics::{CoreMetrics, get_metrics_arc_or, insert_metrics};
use taosx_core::sink::ipc_metric::IpcMetrics;
use taosx_core::sink::point::csv::CsvParser;
use taosx_core::sink::point::model::PointModelConfig;
use taosx_core::sink::point::model::SourceType;
use taosx_core::utils;
use taosx_core::{
    TaskNotifySender, build_ipc, dsv::DataSourceValidation, utils::port_pool::PortPool,
};
use taosx_ipc::ack::AckReaderBuilder;
use taosx_ipc::prelude::IpcDataType;
use taosx_ipc::types::OptionSet;
use tokio_util::sync::CancellationToken;

#[cfg(windows)]
use crate::collector::to_ipc_data_type;
use crate::collector::{run_collectors, type_key_of};
use crate::config::KingHistConfig;
use crate::config::KingHistConnectConfig;

mod collector;
mod config;
mod csv;

pub const KING_HIST_ID: &str = "kinghist";

/// This function is registered into taosx-core at startup to avoid circular dependencies.
/// Inputs:
/// - from: parsed Dsn with driver == "kinghist"
///   Returns:
/// - Vec<DataSet>: list of datasets that match the request. For now, this is a stub.
///
pub fn kinghist_datasets_lister(
    from: &Dsn,
    req: &taosx_core::DataSetsReq,
) -> anyhow::Result<Vec<taosx_core::DataSet>> {
    list_datasets(from, req)
}

/// KingHistorian -> TDengine
pub async fn kinghist_to_taos(
    task_id: Option<i64>,
    from: Dsn,
    to: Dsn,
    port_pool: &PortPool,
    cancel: CancellationToken,
    with_agent: Option<(i64, String, String)>,
    notify: TaskNotifySender,
) -> anyhow::Result<()> {
    tracing::info!("kinghist_to_taos start");

    let mut context = KingHistContext::new(task_id, &from, &to);
    tracing::info!("kinghist_to_taos create context: {:#?}", context);

    // metrics
    let metrics = get_metrics_arc_or(task_id, || {
        // task_id is None if taosx run
        Arc::new(CoreMetrics::IPC(IpcMetrics::new(
            format!("taosx_task_{}", KING_HIST_ID),
            task_id.unwrap_or(-1),
            None,
        )))
    })
    .await;
    if task_id.is_none() {
        insert_metrics(-1, metrics.clone()).await;
    }
    context.metrics = Some(metrics.clone());
    tracing::info!("kinghist_to_taos metrics initialized");

    // 解析配置参数
    let config = Arc::new(KingHistConfig::try_from_dsn(&from)?);
    tracing::info!("kinghist_to_taos job config: {:#?}", &config);

    let csv_content = config.csv_content.clone().ok_or(anyhow::anyhow!(
        "kinghist_to_taos missing csv_content in config"
    ))?;
    // 解析 csv 中的点位映射，直接解析 csv_content
    let parser = CsvParser::try_from_content(SourceType::KingHistorian, &csv_content)?;
    let model_config = parser.parse().await?;
    context.task_config = Some(config);
    check_csv_type(&model_config)?;
    tracing::info!(
        "kinghist_to_taos csv config file parsed with {} points",
        model_config.point_config_map.len()
    );
    context.model_config = Some(Arc::new(model_config));

    // 获取 IPC 端口
    let ipc_port = port_pool
        .get()
        .await
        .context("No available port for kinghist_to_taos task")?;
    let ipc_port = ipc_port.get();
    let socket = format!("127.0.0.1:{ipc_port}");
    context.ipc_socket = Some(socket.clone());
    tracing::info!("kinghist_to_taos build ipc socket: {}", &socket);

    // 创建 IPC
    let ipc_cancel = cancel.child_token();
    let (mut ipc_handler, _) = build_ipc(
        Some(socket.as_str()),
        None,
        &to,
        Some(KING_HIST_ID),
        context.model_config.clone(),
        None,
        &ipc_cancel,
        with_agent,
        None,
        task_id,
        notify,
        None,
    )
    .await?;
    tracing::info!("kinghist_to_taos ipc handlers created");

    // 启动数据采集
    let collect_cancel = cancel.child_token();
    let mut collect_handler =
        tokio::spawn(async move { kinghist_collect(context, collect_cancel).await });
    tracing::info!("kinghist_to_taos collect task spawned");

    // 等待采集结束 / 错误 / 取消
    tokio::select! {
        res = &mut collect_handler => {
            match res {
                Ok(Ok(())) => {
                    tracing::info!("kinghist_to_taos collect task finished");
                    // cancel ipc listener child to stop background loops
                    ipc_cancel.cancel();
                }
                Ok(Err(err)) => {
                    // 采集线程返回错误，结束任务
                    tracing::error!("kinghist_to_taos collect task error: {:#}", err);
                    ipc_cancel.cancel();
                    // 关闭 IPC 后返回错误
                    let _ = ipc_handler.close().await;
                    anyhow::bail!("kinghist_to_taos collect task error: {err:#}");
                }
                Err(join_err) => {
                    tracing::error!("kinghist_to_taos panicked: {:#?}", join_err);
                    ipc_cancel.cancel();
                    let _ = ipc_handler.close().await;
                    anyhow::bail!("kinghist_to_taos panicked: {join_err:#?}");
                }
            }
        }
        err = ipc_handler.recv_error() => {
            if let Some(err) = err {
                tracing::error!("kinghist_to_taos IPC writer error: {:#}", err);
                // 停止采集线程并关闭 IPC
                collect_handler.abort();
                ipc_cancel.cancel();
                let _ = ipc_handler.close().await;
                anyhow::bail!("kinghist_to_taos IPC writer error: {err}");
            }
        }
        _ = cancel.cancelled() => {
            tracing::info!("kinghist_to_taos job cancelled");
        }
    }

    // 关闭 IPC
    tracing::info!("kinghist_to_taos wait for IPC handlers finished");
    // ensure ipc listener is cancelled
    ipc_cancel.cancel();
    let _ = ipc_handler.close().await;
    tracing::info!("kinghist_to_taos all IPC handlers have been finished");

    Ok(())
}

// 检查 point_config_map 中是否有 value_type 未指定的点位
fn check_csv_type(model_config: &PointModelConfig) -> anyhow::Result<()> {
    let missing_ids: Vec<String> = model_config
        .point_config_map
        .iter()
        .filter_map(|(pid, pcfg)| {
            if pcfg.value_type.is_none() {
                Some(pid.clone())
            } else {
                None
            }
        })
        .collect();

    if !missing_ids.is_empty() {
        anyhow::bail!(
            "kinghist_to_taos point config missing value_type for ids: [{}], please specify in csv file",
            missing_ids.join(", ")
        );
    }
    Ok(())
}

// 数据采集任务
async fn kinghist_collect(
    context: KingHistContext,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    tracing::info!("kinghist_to_taos collect task start");

    let task_config = &context.task_config.ok_or(anyhow::anyhow!(
        "kinghist_to_taos missing task_config in context"
    ))?;
    let model_config = &context.model_config.ok_or(anyhow::anyhow!(
        "kinghist_to_taos missing model_config in context"
    ))?;
    let socket = &context.ipc_socket.ok_or(anyhow::anyhow!(
        "kinghist_to_taos missing ipc_socket in context"
    ))?;

    // 按照 IpcDataType 将点位分组
    let mut uniq_types: HashMap<String, IpcDataType> = HashMap::new();
    for (pid, pcfg) in model_config.point_config_map.iter() {
        let ipc_ty = pcfg.value_type.clone().ok_or(anyhow::anyhow!(
            "kinghist_to_taos missing value_type in point config for id: {}",
            pid
        ))?;
        let key = type_key_of(&ipc_ty);
        uniq_types.entry(key).or_insert(ipc_ty);
    }

    let mut is_cancelled = false;
    // 为每种类型创建独立的 IPC 连接、Writer 线程与 ACK 线程
    let mut streams: Vec<TypeStream> = Vec::new();
    let mut sender_map: HashMap<String, flume::Sender<RecordBatch>> = HashMap::new();
    for (key, ipc_ty) in uniq_types.iter() {
        // 如果在建立连接前收到取消信号，提前退出
        if cancel.is_cancelled() {
            tracing::info!("kinghist_to_taos cancel received before establishing all IPC streams");
            is_cancelled = true;
            break;
        }

        // 建立 IPC 连接
        let (ipc_stream, ack_stream) = connect_ipc(socket.clone()).await?;
        let value_dtype = ipc_ty.arrow_data_type();
        tracing::info!(
            "kinghist_to_taos connected ipc for DataType: {:?}",
            value_dtype
        );

        let schema = build_point_schema(value_dtype);
        let (tx, rx) = flume::bounded::<RecordBatch>(0);
        //
        let semaphore = Arc::new(tokio::sync::Semaphore::new(100));

        // 启动 writer（创建即发送 schema header，避免对端读不到头部）
        let writer_stream = ipc_stream.try_clone()?;
        let schema_for_writer = schema.clone();
        let writer_semaphore = semaphore.clone();
        let datetype = key.clone();
        let writer = tokio::task::spawn(async move {
            let mut writer = StreamWriter::try_new(writer_stream, &schema_for_writer)?;
            let mut batch_cnt = 0usize;
            while let Ok(batch) = rx.recv() {
                // 限流，避免内存占用过高
                let _permit = writer_semaphore.acquire().await.unwrap();
                writer.write(&batch)?;
                batch_cnt += 1;
                tracing::debug!(datetype, "kinghist_to_taos wrote batch: {batch_cnt}");
            }
            writer.finish()?;
            Ok(())
        });

        // 启动 IPC ACK 读取线程
        let ack_reader_stream = ack_stream
            .try_clone()
            .context("kinghist_to_taos failed to clone ack stream for reader")?;
        let datetype = key.clone();
        let ack = tokio::task::spawn_blocking(move || {
            let ack_reader = AckReaderBuilder::new(taosx_ipc::prelude::AckType::Lush)
                .open(&ack_reader_stream)
                .context("kinghist_to_taos failed to open ack stream")?;
            let mut ack_cnt = 0usize;
            for ack in ack_reader {
                if !ack.success() {
                    tracing::error!("kinghist_to_taos failed to handle message error: {ack:?}");
                    if let Some(message) = ack.message() {
                        anyhow::bail!("kinghist_to_taos IPC writer error: {message}");
                    }
                }
                ack_cnt += 1;
                tracing::debug!(datetype, "kinghist_to_taos received ACK: {ack_cnt}");
            }
            tracing::info!("kinghist_to_taos ACK reader finished for [{datetype}]");
            Ok(())
        });

        // sender_map key: stream_type, value: stream_sender
        sender_map.insert(key.clone(), tx.clone());
        streams.push(TypeStream {
            tx: Some(tx),
            writer,
            ack,
            ipc_stream,
            ack_stream,
        });
    }

    if !is_cancelled {
        // 为每种类型启动一个 collector 线程
        run_collectors(task_config, model_config, &sender_map, cancel.clone()).await?;
    }

    // 关闭所有发送端以通知 writer 完成
    for (_, tx) in sender_map.into_iter() {
        drop(tx);
    }

    // 关闭所有 stream 并等待线程结束
    for mut ts in streams.into_iter() {
        // drop per-type sender to let writer exit recv loop
        if let Some(tx) = ts.tx.take() {
            drop(tx);
        }
        // wait writer to finish and flush IPC EOS
        ts.writer.await??;
        // then close streams and wait ACK reader
        let _ = ts.ipc_stream.shutdown(Shutdown::Both);
        let _ = ts.ack_stream.shutdown(Shutdown::Both);
        ts.ack.await??;
    }

    tracing::info!("kinghist_to_taos collect task stopped");
    Ok(())
}

struct KingHistContext {
    pub task_id: Option<i64>,
    pub from: String,
    pub to: String,
    pub task_config: Option<Arc<KingHistConfig>>,
    pub model_config: Option<Arc<PointModelConfig>>,
    pub ipc_socket: Option<String>,
    pub metrics: Option<Arc<CoreMetrics>>,
}

impl std::fmt::Debug for KingHistContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("KingHistContext");
        d.field("task_id", &self.task_id);
        if std::env::var("TAOSX_DEBUG_DSN").is_ok() {
            d.field("from", &self.from);
            d.field("to", &self.to);
        }

        if let Some(ref config) = self.task_config {
            d.field("task_config", config);
        }
        if let Some(ref model_config) = self.model_config {
            d.field("model_config", model_config);
        }
        if let Some(ref ipc_socket) = self.ipc_socket {
            d.field("ipc_socket", ipc_socket);
        }
        // metrics 不打印

        d.finish()
    }
}

impl KingHistContext {
    pub fn new(task_id: Option<i64>, from: &Dsn, to: &Dsn) -> Self {
        Self {
            task_id,
            from: from.to_string(),
            to: to.to_string(),
            task_config: None,
            model_config: None,
            ipc_socket: None,
            metrics: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingHistOptions {
    pub groups: Vec<KingHistGroupOption>,
    pub tags: Vec<KingHistTagOption>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingHistGroupOption {
    pub id: u32,              // 变量组 ID
    pub name: String,         // 变量组名称
    pub path: Option<String>, // 变量组路径
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingHistTagOption {
    pub id: i32,               // 标签 ID
    pub name: String,          // 标签名
    pub name_cn: String,       // 标签中文名
    pub data_type: String,     // 标签类型
    pub value: Option<String>, // 标签值
}

impl KingHistTagOption {
    // "变量名","变量类型","变量数据长度","变量描述","变量组"，"上次修改变量配置时间"，"上次修改变量配置的用户", "变量组", "变量组路径"
    pub fn default_all() -> Vec<KingHistTagOption> {
        vec![
            KingHistTagOption {
                id: 1,
                name: "tag_name".to_string(),
                name_cn: "变量名".to_string(),
                data_type: "varchar(128)".to_string(),
                value: None,
            },
            KingHistTagOption {
                id: 2,
                name: "data_type".to_string(),
                name_cn: "变量类型".to_string(),
                data_type: "varchar(64)".to_string(),
                value: None,
            },
            KingHistTagOption {
                id: 3,
                name: "data_length".to_string(),
                name_cn: "变量数据长度".to_string(),
                data_type: "int".to_string(),
                value: None,
            },
            KingHistTagOption {
                id: 4,
                name: "description".to_string(),
                name_cn: "变量描述".to_string(),
                data_type: "varchar(1024)".to_string(),
                value: None,
            },
            KingHistTagOption {
                id: 5,
                name: "last_modified".to_string(),
                name_cn: "上次修改变量配置时间".to_string(),
                data_type: "timestamp".to_string(),
                value: None,
            },
            KingHistTagOption {
                id: 6,
                name: "last_modified_user".to_string(),
                name_cn: "上次修改变量配置的用户".to_string(),
                data_type: "varchar(128)".to_string(),
                value: None,
            },
            KingHistTagOption {
                id: 7,
                name: "group_name".to_string(),
                name_cn: "变量组".to_string(),
                data_type: "varchar(128)".to_string(),
                value: None,
            },
            KingHistTagOption {
                id: 8,
                name: "group_path".to_string(),
                name_cn: "变量组路径".to_string(),
                data_type: "varchar(1024)".to_string(),
                value: None,
            },
        ]
    }

    // TagProperties -> OptionSet
    // OptionSet.name 对应 KingHistTag.name
    // OptionSet.display 对应 TagProperties中的字段
    // OptionSet.description 对应 KingHistTag.value
    #[cfg(windows)]
    pub fn to_optionset(
        &self,
        tag_props: &TagProperties,
        tag_group: &Option<KingHistVarGroup>,
    ) -> anyhow::Result<OptionSet> {
        let opts = match self.name.as_str() {
            "tag_name" => {
                let tag_name = tag_props
                    .tag_name
                    .clone()
                    .ok_or(anyhow::anyhow!("TagProperties.tag_name cannot be none"))?;

                OptionSet {
                    name: self.name.clone(),
                    display: tag_name,
                    description: self.value.clone(),
                    required: true,
                }
            }
            "data_type" => {
                let tag_type = tag_props
                    .data_type
                    .ok_or(anyhow::anyhow!("TagProperties.data_type cannot be none"))?;
                let data_type = to_ipc_data_type(tag_type, tag_props.data_length)?;

                OptionSet {
                    name: self.name.clone(),
                    display: data_type.sql_repr_display(),
                    description: self.value.clone(),
                    required: true,
                }
            }
            "data_length" => {
                let data_length = tag_props
                    .data_length
                    .ok_or(anyhow::anyhow!("TagProperties.data_length cannot be none"))?;

                OptionSet {
                    name: self.name.clone(),
                    display: data_length.to_string(),
                    description: self.value.clone(),
                    required: true,
                }
            }
            "description" => {
                let description = tag_props
                    .description
                    .clone()
                    .ok_or(anyhow::anyhow!("TagProperties.description cannot be none"))?;

                OptionSet {
                    name: self.name.clone(),
                    display: description,
                    description: self.value.clone(),
                    required: true,
                }
            }
            "last_modified" => {
                let last_modified = tag_props.last_modified.ok_or(anyhow::anyhow!(
                    "TagProperties.last_modified cannot be none"
                ))?;

                OptionSet {
                    name: self.name.clone(),
                    display: last_modified.to_rfc3339(),
                    description: self.value.clone(),
                    required: true,
                }
            }
            "last_modified_user" => {
                let last_modified_user = tag_props.last_modified_user.clone().ok_or(
                    anyhow::anyhow!("TagProperties.last_modified_user cannot be none"),
                )?;

                OptionSet {
                    name: self.name.clone(),
                    display: last_modified_user,
                    description: self.value.clone(),
                    required: true,
                }
            }
            "group_name" => {
                let group_name = tag_group
                    .as_ref()
                    .map(|g| g.name.clone())
                    .unwrap_or("".to_string());

                OptionSet {
                    name: self.name.clone(),
                    display: group_name,
                    description: self.value.clone(),
                    required: true,
                }
            }
            "group_path" => {
                // tag_group.path 是 Option<String>，需先扁平化再提供默认空字符串
                let group_path = tag_group
                    .as_ref()
                    .and_then(|tag_group| tag_group.path.clone())
                    .unwrap_or_else(|| "".to_string());

                OptionSet {
                    name: self.name.clone(),
                    display: group_path,
                    description: self.value.clone(),
                    required: false,
                }
            }
            _ => {
                anyhow::bail!("KingHistTag.to_optionset unsupported for: {:?}", self.value);
            }
        };
        Ok(opts)
    }

    pub fn to_csv_header_dataset(&self, id: i32) -> DataSet {
        let name = format!("tag::{}::{}", self.data_type, self.name_cn);

        DataSet {
            id: id.to_string(),
            name: Some(name),
            category: Some("__CSV_HEADER".to_string()),
            r#type: None,
            options: None,
            format: None,
        }
    }
}

// 变量组
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KingHistVarGroup {
    pub id: u32,                // 变量组 ID
    pub name: String,           // 变量组名称
    pub path: Option<String>,   // 变量组路径
    pub var_names: Vec<String>, // 当前变量组包含的变量名列表
}

impl KingHistVarGroup {
    pub fn to_dataset(&self) -> DataSet {
        let mut options = Vec::new();
        // 每个变量名作为一个 OptionSet
        for var_name in self.var_names.iter() {
            options.push(OptionSet {
                name: "__TAG_NAME".to_string(),
                display: var_name.clone(),
                description: None,
                required: true,
            });
        }
        let options = if options.is_empty() {
            None
        } else {
            Some(options)
        };

        DataSet {
            id: self.id.to_string(),
            name: Some(self.name.clone()),
            category: Some("__GROUPS".to_string()),
            r#type: None,
            options,
            format: self.path.clone(),
        }
    }
}

pub struct KingHistVar {
    pub id: i32,
    pub name: String,
    pub data_type: IpcDataType,
    pub length: usize,
    pub description: String,
    pub last_modified_time: String,
    pub last_modified_user: String,
    pub group_name: String,
    pub group_path: Option<String>,
}

// 根据点位 value 的数据类型，准备对应的 IPC 流与 writer/ack 线程
// key: type-key (e.g. "int32"/"double"/"varchar(128)")
pub struct TypeStream {
    pub tx: Option<flume::Sender<RecordBatch>>,
    pub writer: tokio::task::JoinHandle<anyhow::Result<()>>,
    pub ack: tokio::task::JoinHandle<anyhow::Result<()>>,
    pub ipc_stream: TcpStream,
    pub ack_stream: TcpStream,
}

/// 创建 IPC 连接
pub async fn connect_ipc(socket: String) -> anyhow::Result<(TcpStream, TcpStream)> {
    let stream = {
        let stream = std::net::TcpStream::connect(socket)?;
        taosx_core::runners::set_tcp_keepalive(&stream)?;
        stream.set_nonblocking(false)?;
        stream
    };
    let ack_stream = {
        let ack_stream = stream.try_clone()?;
        taosx_core::runners::set_tcp_keepalive(&ack_stream)?;
        ack_stream.set_read_timeout(None)?;
        ack_stream
    };
    Ok((stream, ack_stream))
}

pub fn build_point_schema(value_dtype: ArrowDataType) -> ArrowSchema {
    let fields = vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new(
            "ts",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        ),
        ArrowField::new(
            "received",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        ),
        ArrowField::new("value", value_dtype, true),
        ArrowField::new("status", ArrowDataType::Int64, false),
        ArrowField::new(
            "request",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        ),
    ];
    let mut meta = HashMap::new();
    meta.insert("version".to_string(), "1.0".to_string());
    meta.insert("stream".to_string(), "point".to_string());
    meta.insert("ack".to_string(), "lush".to_string());
    ArrowSchema::new_with_metadata(fields, meta)
}

/// 连通性检查
pub async fn is_valid(dsn: &Dsn) -> DataSourceValidation {
    match is_valid_impl(dsn).await {
        Ok(()) => DataSourceValidation::valid(KING_HIST_ID.to_string(), None),
        Err(err) => DataSourceValidation::invalid(KING_HIST_ID.to_string(), format!("{err:#}")),
    }
}

pub async fn is_valid_impl(dsn: &Dsn) -> anyhow::Result<()> {
    // Initialize underlying API only once per process
    ensure_api()?;

    // windows check kinghistorian connection
    #[cfg(windows)]
    {
        use crate::config::KingHistConnectConfig;

        let connect = KingHistConnectConfig::try_from(dsn)?;
        let host = connect.host.as_str();
        let port = connect.port;
        let username = connect.username.as_str();
        let password = connect.password.as_str();

        let opts = kdb::ConnectionOptions::builder(host, &port.to_string(), username, password)
            .network_timeout_ms(3_000)
            .build();

        let mut conn = kdb::ServerConnection::new(opts).map_err(|e| {
            anyhow::anyhow!(
                "failed to connect to kinghistorian server at {}:{}: {}",
                host,
                port,
                e
            )
        })?;

        if !conn
            .is_connected()
            .context("kinghistorian connection check failed")?
        {
            anyhow::bail!(
                "failed to connect to kinghistorian server at {}:{}",
                host,
                port
            );
        }

        // Optional: ensure a simple API call works
        let _ = conn
            .get_server_time()
            .map_err(|e| anyhow::anyhow!("connected but get_server_time failed: {}", e))?;

        Ok(())
    }

    #[cfg(not(windows))]
    {
        Err(anyhow::anyhow!(
            "KingHistorian is only supported on Windows platform, dsn: {}",
            dsn
        ))
    }
}

/// CSV 配置文件的模版
pub fn get_template() -> String {
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/example/template.csv")).to_string()
}

/// CSV 合法性检查
pub async fn is_csv_valid(dsn: &Dsn) -> anyhow::Result<()> {
    csv::is_csv_valid_impl(dsn).await
}

/// 查询 KingHistorian 的点位
pub fn list_datasets(dsn: &Dsn, req: &taosx_core::DataSetsReq) -> anyhow::Result<Vec<DataSet>> {
    tracing::info!("kinghistorian list tags for dsn: {}", dsn);

    let connect = KingHistConnectConfig::try_from(dsn)
        .context("failed to parse KingHistConnectConfig when listing datasets")?;

    let only_groups = utils::parse_key_in_dsn::<String>(dsn, "only_groups")?;
    let datasets = match only_groups {
        Some(_) => {
            let groups = collector::list_groups(connect)?;
            tracing::info!(
                "kinghistorian list variable groups success, count: {}",
                groups.len()
            );
            groups
        }
        None => {
            let criteria = ListCriteria::try_from(dsn, req.limit, req.offset)
                .context("failed to build ListCriteria when listing datasets")?;

            let datasets = collector::list_datasets(connect, criteria)?;
            tracing::info!(
                "kinghistorian list datasets success, count: {}",
                datasets.len()
            );
            datasets
        }
    };

    Ok(datasets)
}

/// 点位列表的查询条件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCriteria {
    pub groups: Option<Vec<u32>>,   // 变量组 ID 列表
    pub point_mask: Option<String>, // 变量名掩码，如 "OPC*"，替代按ID过滤
    pub tags: Vec<KingHistTagOption>,
    pub limit: usize,
    pub offset: usize,
}

impl ListCriteria {
    pub fn try_from(dsn: &Dsn, limit: usize, offset: usize) -> anyhow::Result<Self> {
        // parse groups from dsn
        let groups = utils::parse_key_in_dsn::<String>(dsn, "groups")?;
        let mut group_ids = vec![];
        if let Some(gids) = groups
            && gids != "all"
        {
            for gid in gids.split(',') {
                let gid = gid.parse::<u32>().map_err(|e| {
                    anyhow::anyhow!("invalid group id '{}' in dsn: {}, error: {}", gid, dsn, e)
                })?;
                group_ids.push(gid);
            }
        }
        let groups = if group_ids.is_empty() {
            None
        } else {
            Some(group_ids)
        };

        // parse point name mask from dsn, e.g. tag_name_mask=OPC*
        let point_mask = utils::parse_key_in_dsn::<String>(dsn, "tag_name_mask")?;

        let tids: Option<String> = utils::parse_key_in_dsn::<String>(dsn, "tags")?;
        // use all KingHistTag if tags is None, or tags=all
        let mut tags = KingHistTagOption::default_all();
        if let Some(tids) = tids {
            // filter tags by tids
            if tids != "all" {
                let mut required_tags = Vec::new();
                for tid in tids.split(',') {
                    let tid = tid.parse::<i32>().map_err(|e| {
                        anyhow::anyhow!("invalid tag id '{}' in dsn: {}, error: {}", tid, dsn, e)
                    })?;
                    if let Some(tag) = tags.iter().find(|t| t.id == tid) {
                        required_tags.push(tag.clone());
                    }
                }
                tags = required_tags;
            }
        }

        Ok(Self {
            groups,
            point_mask,
            tags,
            limit,
            offset,
        })
    }
}

// 将从 KingHistorian 查询到的点位 dataset ，生成 PointOptions 需要的 JSON 格式
pub fn to_point_options(datasets: Vec<DataSet>) -> anyhow::Result<serde_json::Value> {
    tracing::info!("kinghistorian to_point_option");

    // 从 datasets 中找 category = "__GROUPS" 的 DataSet 转换为 KingHistGroupOption 列表
    let mut groups = Vec::new();
    for ds in &datasets {
        if let Some(category) = &ds.category
            && category != "__GROUPS"
        {
            continue;
        }

        let group = KingHistGroupOption {
            id: ds.id.parse::<u32>().unwrap_or(1),
            name: ds.name.clone().unwrap_or_else(|| ds.id.clone()),
            path: ds.format.clone(),
        };
        groups.push(group);
    }

    let options = KingHistOptions {
        groups,
        tags: KingHistTagOption::default_all(), // 默认全部标签
    };

    Ok(serde_json::to_value(options)?)
}

/// 使用 KingHistorian 查询到的点位列表，生成 CSV 配置文件的内容
pub async fn to_csv_context(datasets: Vec<DataSet>) -> anyhow::Result<String> {
    let mut wtr = csv_async::AsyncWriter::from_writer(vec![]);

    // 从 datasets 中读取 category="__CSV_HEADER" 的行，作为 CSV header
    let mut cols = vec![];
    // 记录额外 tag 列（超出默认列）的顺序；这些需要从 DataSet.options 中取值
    let mut extra_tag_headers: Vec<String> = Vec::new();
    for p in &datasets {
        if let Some(category) = &p.category
            && category != "__CSV_HEADER"
        {
            continue;
        }

        if p.name.is_none() {
            tracing::warn!("CSV header point id: {} has no name, skipping", p.id);
            continue;
        }

        let name = p.name.as_ref().unwrap();
        cols.push(name.as_str());
    }
    wtr.write_record(&cols).await?;
    wtr.flush().await?;

    // 计算 header 中额外的 tag 列（默认 14 列之后的列）
    if cols.len() > crate::csv::DEFAULT_CSV_HEADERS.len() {
        let start = crate::csv::DEFAULT_CSV_HEADERS.len();
        for h in &cols[start..] {
            extra_tag_headers.push(h.to_string());
        }
    }

    // 构建 header 文本 -> 选项键名 的映射，如：
    //   "tag::varchar(128)::变量名" -> "tag_name"
    let mut header_to_optkey = std::collections::HashMap::new();
    for t in KingHistTagOption::default_all().into_iter() {
        let header = format!("tag::{}::{}", t.data_type, t.name_cn);
        header_to_optkey.insert(header, t.name);
    }

    // 写入每个点位的配置行
    for p in datasets {
        if let Some(category) = &p.category
            && category != "__TAG"
        {
            continue;
        }

        if p.name.is_none() || p.r#type.is_none() {
            tracing::warn!("point id: {} has no name or type, skipping", p.id);
            continue;
        }

        let name = p.name.unwrap();
        let data_type = p.r#type.unwrap();

        let mut row: Vec<String> = vec![
            &name,
            format!("kinghist_{}", &data_type).as_str(),
            &name.replace('.', "_"),
            "1",
            "val",
            "",
            &data_type,
            "quality",
            "ts",
            "",
            "qts",
            "",
            "rts",
            "",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect();

        // 追加额外 tag 列的值，顺序与 header 一致
        if !extra_tag_headers.is_empty() {
            // 将当前点的 options 映射为 name -> display
            let mut opt_map: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
            if let Some(options) = p.options.as_ref() {
                for opt in options.iter() {
                    opt_map.insert(opt.name.clone(), opt.display.clone());
                }
            }

            for hdr in &extra_tag_headers {
                // 将 header 文本映射回选项键名
                if let Some(opt_key) = header_to_optkey.get(hdr) {
                    // 按键名从 options 中取值，缺省为空
                    let val = opt_map.get(opt_key).cloned().unwrap_or_default();
                    row.push(val);
                } else {
                    // 未知 header（理论上不会发生），补空以保证列数一致
                    row.push(String::new());
                }
            }
        }

        wtr.write_record(&row).await?;
        wtr.flush().await?;
    }

    let data = wtr.into_inner().await?;
    let csv_content = String::from_utf8(data)?;
    Ok(csv_content)
}

#[cfg(windows)]
struct ApiGuard;
#[cfg(windows)]
impl Drop for ApiGuard {
    fn drop(&mut self) {
        #[cfg(windows)]
        {
            let _ = kdb::api_cleanup();
        }
    }
}
#[cfg(windows)]
static API_GUARD: OnceLock<Result<ApiGuard, anyhow::Error>> = OnceLock::new();
fn ensure_api() -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        match API_GUARD.get_or_init(|| {
            kdb::api_start_up()
                .map_err(|e| anyhow::anyhow!(e.to_string()))
                .map(|_| ApiGuard)
        }) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!(e.to_string())),
        }
    }

    #[cfg(not(windows))]
    {
        Err(anyhow::anyhow!(
            "KingHistorian is only supported on Windows platform"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Local;
    use taos::IntoDsn;
    use taosx_core::utils::port_pool::PortPool;

    #[tokio::test]
    async fn test_to_csv_context() {
        let mut datasets = vec![];
        // header: 默认列
        for (i, h) in crate::csv::DEFAULT_CSV_HEADERS.iter().enumerate() {
            datasets.push(DataSet {
                id: i.to_string(),
                name: Some((*h).to_string()),
                category: Some("__CSV_HEADER".to_string()),
                r#type: None,
                options: None,
                format: None,
            });
        }
        // header: 追加两个 tag 列（使用默认的 tag 定义）
        let tags = KingHistTagOption::default_all();
        for (i, t) in tags.iter().take(2).enumerate() {
            datasets
                .push(t.to_csv_header_dataset((crate::csv::DEFAULT_CSV_HEADERS.len() + i) as i32));
        }

        // 一条 __TAG 点，包含与 header 对应的 options
        let opts = vec![
            OptionSet {
                name: "tag_name".to_string(),
                display: "point_0".to_string(),
                description: None,
                required: true,
            },
            OptionSet {
                name: "data_type".to_string(),
                display: "varchar(128)".to_string(),
                description: None,
                required: true,
            },
            // 为追加的两个 tag 提供值
            OptionSet {
                name: tags[0].name.clone(),
                display: "tag_name_val".to_string(),
                description: None,
                required: true,
            },
            OptionSet {
                name: tags[1].name.clone(),
                display: "data_type_val".to_string(),
                description: None,
                required: true,
            },
        ];
        datasets.push(DataSet {
            id: "0".to_string(),
            name: Some("point_0".to_string()),
            category: Some("__TAG".to_string()),
            r#type: Some("varchar(128)".to_string()),
            options: Some(opts),
            format: None,
        });

        let csv_content = to_csv_context(datasets).await.unwrap();
        // 简单校验：header 与数据行字段数一致
        let mut lines = csv_content.lines();
        let header = lines.next().unwrap_or("");
        let row = lines.next().unwrap_or("");
        let hcnt = header.split(',').count();
        let rcnt = row.split(',').count();
        assert_eq!(hcnt, rcnt, "header and row column counts should match");
    }

    /**
     *
     * | Parameter | Type   | Description  |
     * | :-------: | :----: | ------------ |
     * | username  | String | 用户登录名   |
     * | password  | String | 密码         |
     *
     */
    #[test]
    fn test_to_point_options() {
        let mut datasets = vec![];
        for i in 0..3 {
            let ds = DataSet {
                id: i.to_string(),
                name: Some(format!("point_{}", i)),
                category: None,
                r#type: Some("int".to_string()),
                options: Some(vec![OptionSet {
                    name: "groups".to_string(),
                    display: "1".to_string(),
                    description: Some("变量组".to_string()),
                    required: false,
                }]),
                format: None,
            };
            datasets.push(ds);
        }

        let options_value = to_point_options(datasets).unwrap();
        let options_json = serde_json::to_string_pretty(&options_value).unwrap();
        println!("Generated Point Options JSON:\n{}", options_json);
    }

    /// 连通性检查
    /// ```shell
    /// DSN="kinghist://sa:sa@192.168.2.122:5678" cargo nextest run -p source-kinghistorian test_is_valid --retries 0 --nocapture
    /// ```
    #[tokio::test]
    async fn test_is_valid() {
        let dsn = std::env::var("DSN")
            .ok()
            .unwrap_or("kinghist://sa:sa@127.0.0.1:5678".to_string())
            .into_dsn()
            .unwrap();
        dbg!(&dsn);

        #[cfg(not(windows))]
        {
            let result = is_valid(&dsn).await;
            assert!(
                !result.valid,
                "DSN should be invalid on non-windows platform: {:?}",
                result
            );
            assert_eq!(
                result.message.as_deref(),
                Some("KingHistorian is only supported on Windows platform")
            );
        }

        #[cfg(windows)]
        {
            let result = is_valid(&dsn).await;
            assert!(result.valid, "DSN should be valid: {:?}", result);
        }
    }

    #[test]
    fn test_context_display() {
        let from: Dsn = "kinghist://sa:sa@127.0.0.1:5678".into_dsn().unwrap();
        let to: Dsn = "taos:///".into_dsn().unwrap();

        let context = KingHistContext::new(Some(1), &from, &to);
        let display = format!("{:?}", context);

        assert!(display.contains("task_id: Some(1)"));
    }

    /// TAOS_DSN="taos+ws://192.168.2.139:6041/test" cargo nextest run -p source-kinghistorian test_kinghist_to_taos --retries 0 --nocapture
    #[tokio::test()]
    #[cfg_attr(not(windows), ignore)]
    async fn test_kinghist_to_taos() {
        let _ = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .try_init();

        let from = std::env
            ::var("KINGHIST_DSN")
            .ok()
            .unwrap_or({
                let csv_path = concat!(env!("CARGO_MANIFEST_DIR"), "/example/kinghist.csv");
                let start = "2025-10-01T00:00:00+08:00";
                let end = Local::now().to_rfc3339();
                format!(
                    "kinghist://sa:sa@127.0.0.1:5678?csv_config_file=@{csv}&mode=history&start={start}&end={end}&time_range=1d",
                    csv = csv_path
                )
            })
            .into_dsn()
            .unwrap();
        let to = std::env::var("TAOS_DSN")
            .unwrap_or_else(|_| "taos:///".to_string())
            .into_dsn()
            .unwrap();
        let port_pool = PortPool::default();
        let cancel = CancellationToken::new();
        let (notify_tx, _notify_rx) =
            flume::unbounded::<taosx_core::task_set::prelude::TaskNotify>();

        // Run task. It should finish for History mode.
        let res = kinghist_to_taos(None, from, to, &port_pool, cancel, None, notify_tx).await;
        assert!(res.is_ok(), "kinghist_to_taos failed: {:#?}", res.err());
    }

    #[test]
    fn test_var_group_to_dataset() {
        let var_group = KingHistVarGroup {
            id: 10,
            name: "Test Group".to_string(),
            path: Some("abc.123.ABC".to_string()),
            var_names: vec![
                "OPC_数据类型示例.16 位设备.R 寄存器.Float1".to_string(),
                "OPC_数据类型示例.16 位设备.R 寄存器.Float2".to_string(),
            ],
        };

        let dataset = var_group.to_dataset();

        println!("{}", serde_json::to_string_pretty(&dataset).unwrap());

        assert_eq!(dataset.id, "10");
        assert_eq!(dataset.name.unwrap(), "Test Group");
        assert_eq!(dataset.category.unwrap(), "__GROUPS");
        assert_eq!(dataset.format.unwrap(), "abc.123.ABC");
        assert!(dataset.options.is_some());
        let options = dataset.options.unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].name, "__TAG_NAME");
        assert_eq!(
            options[0].display,
            "OPC_数据类型示例.16 位设备.R 寄存器.Float1"
        );
        assert_eq!(options[1].name, "__TAG_NAME");
        assert_eq!(
            options[1].display,
            "OPC_数据类型示例.16 位设备.R 寄存器.Float2"
        );
    }
}
