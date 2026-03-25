# 数据管道工具-Design Spec

## 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-20 | 1.0 | 霍琳贺 | 编写文档 | 初始化文档 |
| 2026-02-06 | 1.1 | 张贵川 | 编写文档 | 增加高可用 |

## 引言

### 2.1 目的

本设计文档旨在详细描述 taosX 高可用架构的设计目标、技术架构和实现细节，为开发、部署及维护 taosX 高可用版本提供指导。同时，本文档将为后续的功能扩展和性能优化提供设计依据，确保 taosX 能持续高效地支持 TDengine 生态系统。

### 2.2 范围

taosX 高可用版本是一个分布式数据管道系统，本文档旨在描述其整体运行架构和核心设计。包括：
- 高可用架构设计（MNode + XNode 模式）。
- 节点管理模块设计。
- 任务分片和负载均衡设计。
- 数据同步模块设计。
- 数据迁移模块设计。
- 数据库或（超级）表的增量备份/恢复。
- 离线数据文件的导出或导入。
- TDengine 的双活部署和管理。
- 外部数据源的导入管道通用流程设计。
- 共享存储设计。

### 2.3 受众

本设计文档的目标读者包括：
- **开发人员**：负责实现和优化 taosX 高可用版本的工程师。
- **系统架构师**：需要理解 taosX 高可用整体架构和技术决策。
- **运维工程师**：负责部署和维护 taosX 高可用集群的人员。

## 术语

1. **TDengine**: 一种开源的时序数据库，专为处理物联网、大数据和实时分析场景下的大规模数据采集、存储和查询而设计。
2. **taosX/XNode**: 为 TDengine 提供数据管道功能的分布式工作节点。
3. **MNode**: TDengine 的管理节点，存储集群元数据并执行管理操作。
4. **xnoded**: 运行在 MNode Leader 节点上的守护进程，负责管理 XNode 状态和任务调度。
5. **sdb**: TDengine 的元数据存储系统，基于 RAFT 协议保证数据一致性。
6. **Shard（分片）**: 任务被拆分后的子任务单元，可在不同 XNode 上独立执行。
7. **Job（分片任务）**: Shard 在 XNode 上的具体执行实例。
8. **DSN**: Data Source Name，数据源的字符串表示。
9. **RESTful API**: 一种基于 HTTP 的架构风格，用于构建分布式系统。
10. **Arrow Flight**: Apache Arrow 的 RPC 框架，用于高效数据传输。

## 概述

### 4.1 架构

整体高可用架构图：高可用架构通过复用 TDengine 的 MNode 来保证数据一致性并进行主体任务的调度，架构图如下：
![](./images/wb_BSjowqxkBhU7UIb4sPxclqVvnNe.png)

整体高可用架构图：高可用架构通过复用 TDengine 的 MNode 来保证数据一致性并进行主体任务的调度，架构图如下：
![](./images/img_TkSBbePA6onTNqx8pTxc1TEOnSf.png)

架构要点：
1. **MNode 功能**：
  - 任务元数据的存储一致性和持久化：通过 taosd 自身的 mnode 高可用，实现存储的分布式一致性。
  - 任务的分片与负载均衡：在任务启动后，由 MNode 生成分片任务，并将分片按照负载均衡策略派发到不同的 XNode 节点执行。
1. **元数据操作**：所有元数据操作均通过 MNode 执行：
  - 添加或删除节点：`CREATE/DROP XNODE`
  - 修改节点状态：`ALTER XNODE SET key = value`
  - 创建数据接入任务：`CREATE XNODE TASK 'name' FROM 'dsn://source' TO DATABASE db`
  - 更新数据接入任务：`ALTER XNODE TASK SET FROM 'dsn://source' TO DATABASE db`
  - 删除任务：`DROP XNODE TASK [FORCE] id | 'name'`
  - 创建或删除 Agent：`CREATE/DROP XNODE AGENT 'name'`
1. **XNode 节点**：
  - XNode 节点之间是平等的，无论 TDengine 包含几个节点，XNode 都可以任意添加或删除。
  - 所有 XNode 节点定期上报状态到 MNode。
  - XNode 不存储元数据，仅接收 MNode 的指令并执行任务。

### 4.2 技术

该分布式架构的核心在实现集群节点管理和任务分片：
1. 开发语言：Rust
2. 元数据存储：复用 TDengine MNode 的 sdb，使用 TDengine SQL 进行节点管理
3. 通信协议：
  - MNode/XNoded 与 Xnode：Arrow Flight gRPC
  - XNode 与 Agent：Arrow Flight gRPC
1. 任务分片：提供任务分片（Sharding）接口，为支持的数据源添加分片能力
2. 共享存储：多节点共享 NAS/SAN 存储，用于任务检查点和数据缓存
3. 其他相关技术栈：
- 异步运行时：Tokio - https://crates.io/crates/tokio
- HTTP 框架: Actix - https://crates.io/crates/actix-web
- GRPC 框架：Tonic - https://crates.io/crates/tonic
- 数据库框架：sqlx - https://crates.io/crates/sqlx
- 本地数据库：sqlite - https://crates.io/crates/sqlx-sqlite
- 内存列式存储数据类型：Arrow - https://crates.io/crates/arrow
- 日志框架：Tracing - https://crates.io/crates/tracing
- 配置及命令行参数解析：Clap - https://crates.io/crates/clap
- 序列化框架：serde - https://crates.io/crates/serde
- 连接池：deadpool - https://crates.io/crates/deadpool

### 4.3 开发环境

1. Clang 12+ 或 GCC 10+
2. Rust 1.81.0
3. Go 1.20+ （OPC、MQTT）
4. JDK 8+ （InfluxDB、OpenTSDB）
5. 第三方依赖项
  - JSON 库：Serde JSON - https://crates.io/crates/serde_json
  - Kafka 驱动：rdkafka - https://crates.io/crates/rdkafka
  - MySQL 驱动：sqlx mysql -  - https://crates.io/crates/sqlx-mysql
  - Microsoft SQL Server 驱动：tiberius - https://crates.io/crates/tiberius

### 4.4 运行时依赖

- TDengine 客户端动态库：当需要原生连接时依赖。
- Krb5 相关库和工具（kinit）：当 Kafka 连接需要支持 SASL 时依赖。
- Java Runtime Environment 8 及以上：当需要连接 OpenTSDB 和 InfluxDB 数据源时依赖。
- PI Asset Framework SDK: 当需要连接 PI System 时依赖。
- OPCDA Driver：当需要连接 OPC-DA 数据源时依赖。

## 设计考虑

### 5.1 假设和限制

假设:
- TDengine 依赖：taosX 必须在 TDengine 正常工作时才允许配置数据接入任务。
- 单实例限制：一个 TDengine 集群仅允许一套 taosX 实例，这意味着 TDengine 与 taosX 必须绑定部署。
- 共享存储：多个节点 taosX 必须配置数据目录为同一共享存储路径，且存储可靠。
- 数据一致性：数据可以重复写入 TDengine，不会影响查询结果和数据一致性。
限制:
- TDengine 宕机影响：TDengine 宕机时，taosX 不能创建、修改、或启停任务。删除 TDengine 数据，会影响 taosX 集群使用。
- 数据源分片限制：PI、OPC-UA/DA、InfluxDB、OpenTSDB 等数据源暂不支持分片。
- 网络要求：XNode 节点之间需要可靠的网络连接，以保证 RPC 通信正常。
- taosX 支持使用 taosc 客户端连接 TDengine，需要安装 TDengine 客户端。
- taosX 支持 WebSocket 连接 TDengine，需要服务端部署 taosAdapter 且 HTTP 连接可用。
- taosX 支持 OPC-DA 数据源，但仅支持 Windows 系统（使用 taosX Windows 版本，或者 Windows 上的 Agent 服务）。
- taosX 支持 PI 数据源，需要安装 PI AF SDK，仅支持 Windows 系统或 Windows 上的 Agent 服务。
- toasX 支持 MQTT 数据源 3.1 和 5.0 协议。
- taosX 支持 Kafka 0.10.0 及以上版本。
- taosX 支持 Oracle ：
   - 连接 Oracle 数据库需要安装 [**ODPI-C**](https://oracle.github.io/odpi/)** **库。
   - 支持 Oracle 客户端 11.2 以上。
   - 不支持自定义类型、XML 类型、JSON 类型。
- taosX 支持 MySQL/PostgreSQL 不含 socket 直连。
- taosX 支持 InfluDB > 1.8.0，且 < 3.0.0。

### 5.2 设计模式和原则

设计模式和原则：
- **迭代器模式** - taosX 中大量使用了迭代器模式进行链式调用和相关优化。
- **状态模式** - taosX 使用状态模式进行任务和执行器管理。
- **访问者模式** -  taosX 使用访问者模式对指标进行读写操作。
- **观察者模式** - taosX 使用观察者模式对事件状态进行广播和监听。
- **生成器模式** - taosX 使用生成器模式进行各项配置。
- **单例模式** -  taosX 使用单例模式进行全局配置、环境变量等的管理。
设计原则：
- **模块化设计**: 各功能模块分离，便于扩展和维护。
- **接口隔离原则**: 各模块之间通过明确的接口交互，减少耦合。
- **高内聚低耦合**: 各模块专注于自身的功能，减少对其他模块的依赖。

### 5.3 风险和缓解措施

- 风险： 计算型任务在异步运行时下容易导致运行时阻塞。
  - 缓解措施：
      - 对于 CPU 计算型任务使用 tokio::spawn_blocking 方式在单独线程池中执行。
      - 对于数据订阅任务使用独立线程执行持续订阅操作，使用 Channel 与运行时进行数据交互。
- 风险：数据源任务耗时长且调用频繁，API 响应容易受到影响。
  - 缓解措施：
      - 对 GRPC API 使用独立运行时进行隔离。
      - 使用 Channel 和 Arc 智能指针在各个运行时之间共享变量和进行数据交互。
- 风险：写入数据频繁且较大时积压占用大量内存，容易产生 OOM。
  - 缓解措施：
      - 使用 Bounded Channel 进行队列限制和反压，缓解下游数据压力。
      - 使用 并行化措施 提升下游处理能力。
- 风险：XNode 单点故障
  - 缓解措施
    - 自动故障检测和分片迁移
    - 任务分片可重新调度到其他节点

## 详细设计

### 配置

1. 组件设计：
   - 支持配置文件、环境变量和命令行参数三种配置方式，命令行参数优先于环境变量优先于配置文件
   - 默认配置文件路径：
      - Linux / macOS ： `/etc/taos/taosx.toml`；
      - Windows ：`C:\TDengine\cfg\taosx.toml`；
   - 配置文件使用 [clap](https://crates.io/clap) 配置命令行参数 + [twelf](https://crates.io/twelf) 加载配置文件和环境变量，使用 git-like 的子命令方案提供 CLI 接口。
2. 列出系统中的关键数据结构：
   - 子命令
    ```rust
    #[derive(Subcommand, Debug)]
    enum Commands {
        Run(run::Cli),
        Serve(serve::Cli),
        Replica(replica::Cli),
        Privileges(privileges::Cli),
    }
    ```

  子命令参数如下：
   - Serve 服务模式
  ```rust
  
  #[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
  #[serde(default)]
  pub(super) struct Cli {
      /// Listen to ip:port address.
      #[clap(short = 'l', long, env = "LISTEN")]
      pub listen: Option<String>,
  
      /// Grpc listen to ip:port address.
      ///
      #[clap(short = 'g', long, env = "TAOSX_GRPC")]
      pub grpc: Option<String>,
  
      /// Database URL.
      #[clap(short = 'D', long, env = "DATABASE_URL")]
      pub database_url: Option<String>,
  
      #[clap(hide = true)]
      pub secret_prefix: Option<String>,
  
      #[clap(long)]
      pub do_not_resume: Option<bool>,
  
      #[clap(hide = true)]
      pub replica: Option<Vec<String>>,
  
      #[clap(flatten)]
      #[serde(skip)]
      pub verbose: Option<Verbosity<InfoLevel>>,
  
      #[clap(long, env = "REPEAT_INTERVAL")]
      pub repeat_interval: Option<u64>,
  
      #[clap(long, env = "TAOSX_REQUEST_TIMEOUT")]
      pub request_timeout: Option<u64>,
  }
  
  ```

   - Run 一次性任务
  ```rust
  
  #[derive(Parser, Debug)]
  pub(super) struct Cli {
      /// Input DSN(Data Source Name) string.
      ///
      /// Supported:
      ///
      /// ─ TMQ: TDengine message queue data stream, use as:
      ///  ** `tmq://host:port/topics?group.id=STR&client.id=STR&timeout`.
      ///
      /// ─ Legacy query, use as:
      ///
      /// └── a) database input: `taos://localhost:6030/database`, this will output stable schemas and child tables.
      ///
      /// └── b) table input: `taos://host:port/db?query=select c1,c2,c3 from stb1`, this will be queried by sql `select c1,c2,c3 from stb1` and output as a plain table.
      ///
      /// ─ Local backup, use as `local:./path`.
      ///
      /// ─ CSV: `csv:/path/to/file.csv`.
      ///
      /// ─ Parquet: `parquet:/path/to/*.parquet`.
      ///
      #[clap(short, long, value_parser)]
      from: Dsn,
  
      /// Output DSN.
      #[clap(short, long, value_parser)]
      to: Dsn,
  
      /// Parser.
      #[clap(short, long)]
      parser: Option<String>,
      // parser: Option<taosx_core::Parser>,
      /// Transformer actions.
      ///
      /// Supported action format:
      ///
      /// - 'add-tag:tag1=value1': add a tag named `tag1`, and valued `value1`.
      ///
      /// - 'rename-table:prefix:v1_': rename all tables as `v1_{{ name }}`
      ///
      /// - 'rename-super-table:suffix:_stb': rename all super tables as suffixed '_stb'
      ///
      /// - 'rename-child-table:template:prefix_{{ name }}_stb': rename all super tables with prefix 'prefix_' and suffix '_stb'
      ///
      /// - 'rename-child-table:map:oldname1,newname1|oldname2::newname2': rename all child tables with oldname1 to newname1, oldname2 to newname2
      ///
      /// - 'rename-child-table:map:@./rename-old-new.csv': rename all child tables with oldname,newname pairs in csv file
      ///
      /// - 'rename-replace-with-regex:replace_with_regex:prefix(?<old>)::newprefix_$old': replace all tables prefix with new prefix
      #[clap(short = 'T', long)]
      transform: Vec<Action>,
      
      /// Task id, default is -1.
      #[clap(long, hide = true)]
      task_id: Option<i64>,
  }
  
  ```

   - Replica 双活管理
  ```rust
  
  /// Active-StandBy replication management commands
  #[derive(Debug, Args)]
  pub struct Cli {
      #[clap(subcommand)]
      command: ReplicaCommands,
  
      /// taosX server endpoint
      #[clap(flatten)]
      config: ReplicaConfig,
  }
  #[derive(clap::Args, Debug)]
  struct ReplicaConfig {
      /// The taosX server endpoint.
      ///
      /// Default to `http://localhost:6050`.
      #[clap(long, default_value = "http://localhost:6050", global = true)]
      server: String,
  
      /// Connection timeout in seconds.
      #[clap(long, default_value = "30", global = true)]
      timeouts: u64,
  }
  
  #[derive(Debug, Subcommand)]
  pub enum ReplicaCommands {
      /// Show the replication status
      Status {
          /// Replica ID list in positional arguments.
          ids: Vec<ReplicaId>,
      },
      /// Check the difference in the replication subscriptions.
      Diff {
          /// The replica id.
          id: ReplicaId,
          /// The databases to check.
          databases: Vec<String>,
      },
      /// Start replication to the specified endpoint
      Start {
          #[clap(short = 'f', long)]
          source: Option<String>,
          /// The endpoint to replicate to.
          #[clap(short = 't', long)]
          sink: Option<String>,
          /// The replica identity string.
          ///
          /// If not specified, the replica id will be generated automatically.
          #[clap(short, long)]
          id: Option<ReplicaId>,
          /// The databases to replicate.
          databases: Vec<String>,
  
          /// Custom topic template for replication.
          ///
          /// Replica task will use `{database}` as the topic name by default.
          #[clap(long, default_value = DEFAULT_TOPIC_PREFIX, alias = "topic-prefix")]
          topic_prefix: Option<String>,
  
          /// Whether to keep topic or not when remove replication.
          ///
          /// By default, the topic will be removed when remove replication.
          #[clap(long)]
          keep_topic_after_remove: bool,
  
          /// Custom consumer group for replication.
          ///
          /// Replica task will use `__replica__` as the consumer group by default.
          ///
          /// If set, the consumer group will be used as the consumer group name.
          #[clap(long, alias = "group.id")]
          group: Option<String>,
      },
      /// Stop replication with the specified databases or not
      Stop {
          /// The replica id.
          id: ReplicaId,
          /// The databases to replicate.
          databases: Vec<String>,
      },
      /// Restart replication with the specified databases or not
      Restart {
          /// The replica id.
          id: ReplicaId,
          /// The databases to replicate.
          databases: Vec<String>,
      },
  
      /// Remove replication with the specified databases
      Remove {
          /// The replica id.
          id: ReplicaId,
          /// The databases to replicate.
          #[clap()]
          databases: Vec<String>,
      },
  }
  
  ```

   - Privileges 权限导入导出
  ```rust
  #[derive(Debug, Args)]
  pub struct Cli {
      /// The source endpoint to replicate from.
      #[clap(short = 'f', long)]
      from: Option<Dsn>,
      /// The endpoint to replicate to.
      #[clap(short = 'i', long)]
      input: Option<PathBuf>,
      /// The endpoint to migrate to.
      #[clap(short = 't', long)]
      to: Option<Dsn>,
      /// Export data to a file.
      #[clap(short = 'o', long)]
      output: Option<PathBuf>,
  
      /// Scope
      #[clap(flatten)]
      scope: Option<Scope>,
  }
  
  ```

   - 全局设置
  ```rust
  #[derive(Parser, Debug, Deserialize, Serialize, Default)]
  #[serde(default)]
  struct Global {
      #[clap(long, env = "PLUGINS_HOME", global = true)]
      plugins_home: Option<String>,
  
      #[clap(long, env = "TAOSX_DATA_DIR", global = true)]
      data_dir: Option<String>,
  
      #[clap(long, global = true, env = "INSTANCE_ID")]
      #[serde(rename = "instanceId")]
      instance_id: Option<u8>,
  
      #[clap(long, env = "LOGS_HOME", global = true)]
      logs_home: Option<String>,
  
      /// For environment variable wised log level.
      #[clap(long, hide = true, env = "LOG_LEVEL", global = true)]
      log_level: Option<LevelFilter>,
  
      #[clap(flatten)]
      log: Option<LogOpts>,
  
      /// Enable debug will set the mod path as `file:line`.
      #[clap(short, long, global = true)]
      debug: bool,
  
      /// Log keep days.
      #[clap(long, env = "LOG_KEEP_DAYS", global = true)]
      log_keep_days: Option<i64>,
  
      /// Not log to files.
      #[clap(long, global = true)]
      no_log_to_files: bool,
  
      /// Disable non-blocking writer for log file appender.
      #[clap(long, global = true)]
      no_async_log: bool,
  
      /// Number of jobs, default to 0, will use `jobs` number of works for TMQ.
      #[clap(short, long, value_parser, default_value = "0", global = true)]
      jobs: usize,
  
      /// Enable OpenTelemetry tracing and metrics exporter.
      #[clap(long, action = clap::ArgAction::SetTrue, env = "ENABLE_OTEL", global = true)]
      otel: Option<bool>,
  
      /// Max activities per entity.
      max_activities_per_entity: Option<usize>,
  
      #[clap(long, action = clap::ArgAction::SetTrue, env = "DRY_RUN", global = true, hide = true)]
      dry_run: Option<bool>,
  
      #[clap(long, env = "SQL_TAG_CACHE_CAPACITY", global = true, hide = true)]
      sql_tag_cache_capacity: Option<usize>,
  }
  
  ```

   - 特殊命令行参数
  ```go
  #[derive(Parser, Debug)]
  struct OptArgs {
      #[clap(short = 'c', long, global = true)]
      config: Option<PathBuf>,
  
      /// For verbosity print.
      #[clap(flatten)]
      verbose: Verbosity<InfoLevel>,
  
      /// Be careful to use this, we suggest only use it when failed at first time.
      ///
      /// We'll warn you various kind of risks before really running a task.
      #[clap(short, long, global = true, default_value = "false", hide = true)]
      yes_i_really_mean_it: bool,
  
      #[clap(
      long,
      global = true,
      default_value = "none",
      value_parser = fmt_span_from_str,
      env = "TRACING_EVENTS"
      )]
      tracing_events: FmtSpan,
  }
  
  ```

   - 日志配置
  ```rust
  #[serde_as]
  #[derive(Parser, Debug, Serialize, Deserialize, Clone, Default)]
  #[serde(rename_all = "camelCase")]
  struct LogOpts {
      /// Log path.
      #[clap(id = "log.path", long = "log.path", env = "LOG_PATH")]
      path: Option<PathBuf>,
  
      /// Log level.
      #[clap(id = "log.level", long = "log.level", env = "LOG_LEVEL")]
      level: Option<LevelFilter>,
  
      /// Enable compress for log files.
      #[clap(
          id = "log.compress",
          long = "log.compress",
          env = "LOG_COMPRESS",
          num_args = 0..=1,
          default_missing_value = "true",
          value_parser = compress_arg_parser,
      )]
      /// Enable compress for log files.
      #[serde_as(as = "Option<FromInto<CompressType>>")]
      compress: Option<bool>,
  
      /// Rotation count for log files.
      #[clap(
          id = "log.rotationCount",
          long = "log.rotationCount",
          env = "LOG_ROTATION_COUNT"
      )]
      rotation_count: Option<u16>,
  
      /// Keep days for log files.
      #[clap(id = "log.keepDays", long = "log.keepDays", env = "LOG_KEEP_DAYS")]
      keep_days: Option<u16>,
  
      /// Rotation size for log files.
      #[clap(
          id = "log.rotationSize",
          long = "log.rotationSize",
          env = "LOG_ROTATION_SIZE"
      )]
      rotation_size: Option<String>,
  
      /// Reserved disk size for log files.
      #[clap(
          id = "log.reservedDiskSize",
          long = "log.reservedDiskSize",
          env = "LOG_RESERVED_DISK_SIZE"
      )]
      reserved_disk_size: Option<String>,
  
      /// Enable watching for loggers changes.
      #[clap(
          hide = true,
          env = "LOG_WATCHING",
          default_value_if("log.watching", "true", Some("true"))
      )]
      watching: Option<bool>,
  
      /// Enable watching for loggers changes.
      #[clap(long = "log.watching", id = "log.watching")]
      #[serde(skip)]
      _log_watching_helper: bool,
  
      /// Loggers.
      #[clap(skip)]
      loggers: Option<HashMap<String, String>>,
  }
  
  ```

   - 监控配置
  ```rust
  #[derive(Parser, Debug, Deserialize, Serialize, Default, Clone)]
  #[serde(default)]
  pub struct MonitorCfg {
      /// FQDN of taosKeeper service
      #[clap(long = "monitor-fqdn", env = "MONITOR_FQDN")]
      pub fqdn: Option<String>,
  
      /// Port of taosKeeper service
      #[clap(
          long = "monitor-port",
          env = "MONITOR_PORT",
          global = true,
          default_value = "6043"
      )]
      pub port: u16,
  
      #[clap(
          long = "monitor-interval",
          env = "MONITOR_INTERVAL",
          global = true,
          default_value = "10",
          value_parser=less_than_10
      )]
      pub interval: u64,
  }
  
  ```

### 日志

1. 组件设计：
   - 使用链路跟踪框架 tracing 。
   - 使用 Tracing-appender 输出到日志文件。
   - 使用 RollingFileAppender 进行日志文件滚动输出。
   - 使用 Tracing-subscriber Layers 支持同时输出 stdout 与配置文件，前台运行时可以通过命令行看到日志，后台运行时可以通过配置文件看到日志。
   - API 请求结束时打印 info 级别日志、记录 http 响应码、请求持续时间、客户端 ip、请求方法、请求的 uri
2. 列出系统中的关键数据结构
   - 滚动日志文件
  ```rust {wrap}
      RollingFileAppender::builder()
          .max_log_files(max_files as usize)
          .filename_prefix("taosx.log")
          .rotation(Rotation::DAILY)
          .build(log_dir)
          .expect("failed to initialize rolling file appender")
  ```

   - 多层级日志输出
  ```rust {wrap}
  if atty::is(atty::Stream::Stderr) {
          layers.push(
              tracing_subscriber::fmt::layer()
                  .with_timer(timer.clone())
                  .with_thread_names(true)
                  .with_writer(std::io::stderr)
                  .with_span_events(span_events)
                  .with_ansi(true)
                  .pretty()
                  .with_filter(env_filter_from(&tracing_level_filter)?)
                  .boxed(),
          );
      } else {
          layers.push(
              tracing_subscriber::fmt::layer()
                  .with_timer(timer.clone())
                  .with_thread_names(true)
                  .with_writer(std::io::stderr)
                  .with_span_events(span_events)
                  .with_ansi(false)
                  .with_filter(env_filter_from(&tracing_level_filter)?)
                  .boxed(),
          );
      }
      
  let layered = tracing_subscriber::registry().with(layers);
  layered.try_init()?;
  ```

### 任务调度器

1. 组件设计：
   - 任务调度器负责对任务进行调度，在程序运行开始时，创建任务调度器；
   - 用户创建数据采集任务时，通过 gRPC API 发起创建任务请求，修改任务的调度状态。
   - 任务调度器通过查看任务状态，调度当前任务队列中的任务。
2. 列出系统中的关键数据结构
   - 任务调度器
  ```rust {wrap}
  #[derive(Clone)]
  pub struct TaskScheduler {
      pub tasks: Arc<RwLock<MultiIndexTaskJobMap>>,
      pub global_state: Arc<GlobalState>,
      pub shutdown_handler: Arc<Mutex<Option<ShutdownHandler>>>,
      pub drop_notifier: Arc<Notify>,
      pub dropped_notifier: Arc<Notify>,
      // An Task-to-TableTagCache hashmap.
      pub lush_table_cache: Arc<RwLock<HashMap<i64, Arc<TableTagCache>>>>,
      pub task_breakpoint_db: Arc<RwLock<HashMap<i64, BreakpointDb>>>,
  }
  ```

   - 任务状态
  ```rust {wrap}
  #[derive(Debug, Clone)]
  pub struct TaskState {
      span: tracing::Span,
      /// Current job run times.
      runs: Arc<AtomicU64>,
      /// Task details.
      pub(crate) task: Arc<Task>,
  
      pub(crate) operator: TaskOperator,
  
      pub(crate) state: Arc<RwLock<InnerState>>,
  
      /// Job schedule.
      schedule: Arc<Schedule>,
      /// Stop condition of current job.
      stop_condition: StopCondition,
      /// Stop a running task by sending a cancellation signal.
      pub(crate) cancellation: CancellationToken,
  
      /// Agent state if task is running on agent.
      agent_waiter: Option<AgentWaiter>,
  
      /// Last state
      ///
      /// When task finished unexpectedly, the last state will be None.
      ///
      /// When task finished successfully, the last state will be one of
      /// `Done`, `Stopped` or `Error`.
      last_state: Arc<RwLock<Option<LastState>>>,
  
      /// Job listener.
      last_waiter: Arc<Mutex<Option<oneshot::Receiver<bool>>>>,
  }
  ```

   - 全局任务状态
  ```rust {wrap}
  #[derive(Clone)]
  pub struct GlobalState {
      /// Global aliveness flag.
      pub(crate) alive: Arc<AtomicBool>,
      /// Global job scheduler.
      pub(crate) scheduler: JobScheduler,
      /// Global task activities notify sender.
      pub(crate) notify_sender: NotifySender,
      /// Global port pool.
      pub(crate) port_pool: PortPool,
      /// Global Agent task manager
      pub(crate) agent_runtime: AgentRuntimeRef,
  }
  ```

### 任务执行器

1. 组件设计：
   - 响应任务调度器的任务调度请求；
   - 触发任务开始、中断、结束等；
2. 列出系统中的关键数据结构
   - 任务
  ```rust {wrap}
  #[derive(MultiIndexMap, Debug, Clone)]
  pub struct TaskJob {
      #[multi_index(hashed_unique)]
      pub task_id: i64,
      #[multi_index(hashed_unique)]
      pub job_id: Uuid,
      /// The task that is associated with this job and shared amount all ticks of this job.
      pub task: TaskState,
      /// Global shared state across all jobs/tasks.
      pub global: GlobalState,
  }
  ```

   - 任务执行
  ```rust {wrap}
  #[derive(Debug, Clone)]
  pub struct TaskOpts {
      pub from: Dsn,
      pub transform: Vec<Action>,
      pub to: Dsn,
      pub parser: Option<plugins::Parser>,
      pub health: Option<task_set::prelude::HealthOpts>,
      pub cancel: CancellationToken,
      pub with_agent: Option<(i64, String, String)>,
      // pub port_pool: OnceCell<PortPool>
      pub breakpoints: Option<String>,
      pub task_id: Option<String>,
      pub notify: TaskNotifySender,
  }
  ```

### 任务管理器

1. 组件设计：
   - 任务调度器响应 MNode/Xnoded 通过 gRPC API 发起的任务请求；
   - 管理任务、Agent 和它们的状态，包括：创建、更新、启动、停止、删除等；
   - 管理数据库连接池；
   - 使用任务调度器进行调度；
   - 管理 Agent。
2. 列出系统中的关键数据结构
任务管理器
```rust {wrap}
pub(crate) struct TaskController {
    pub pool: SqlitePool,
    pub tasks: RwLock<TaskMap>,
    pub secret: RwLock<Option<Bytes>>,
    /// An Agent-to-Tasks-Vector hashmap.
    // pub agent_tasks: RwLock<HashMap<i64, AgentTasks>>,
    // An Task-to-Assignments-Vector hashmap.
    #[allow(clippy::type_complexity)]
    pub offsets: RwLock<HashMap<i64, Arc<DashMap<String, Vec<Assignment>>>>>,
    // pub agent_workers: RwLock<HashMap<i64, AgentWorker>>
    // tasks: Mutex<Vec<Task>>,
    pub transferred: Transferred,
    /// Task scheduler
    pub scheduler: TaskScheduler,

    #[allow(dead_code)]
    pub ctl_alive: Arc<AtomicBool>,

    pub shutdown_notify: Arc<tokio::sync::Notify>,

    /// Max activities per task or agent.
    pub max_activities_per_entity: usize,

    pub max_activities_keep_interval: Duration,

    /// for lock, function can only be called once at a time.
    pub lock_flag: Arc<tokio::sync::Mutex<i32>>,
}
```

### gRPC API

1. 组件设计：
   - RpcConfig 配置 gRPC 服务器的 TCP 和 Unix 套接字地址；提供 serve_with_controller 方法启动 gRPC 服务器；
   - AgentRpcChannel 包含 agent_activity_receiver 和 agent_notify_sender，用于接收和发送代理活动通知；
   - FlightServiceImpl 实现 FlightService 接口，处理 gRPC 请求，如 handshake、do_put、do_exchange 等；
2. 列出系统中的关键数据结构
   - gRPC 地址
  ```rust {wrap}
  #[derive(Debug, Deserialize)]
  #[allow(dead_code)]
  pub struct RpcConfig {
      pub tcp: Option<SocketAddr>,
      pub unix: Option<PathBuf>,
  }
  ```

   - gRPC 接口
  ```rust {wrap}
  pub(super) struct FlightServiceImpl {
      controller: TaskControllerRef,
      notify_sender: AgentNotifySender,
      activity_receiver: Arc<AgentActionsReceiver>,
      agent_connections: Arc<RwLock<HashMap<AgentId, ConnectionId>>>,
      request_id: Arc<AtomicU64>,
      datasets_senders: Arc<RwLock<LinkedHashMap<u64, AgentDataSetsSender>>>,
      dsv_senders: Arc<RwLock<LinkedHashMap<u64, DsvSender>>>,
      string_senders: Arc<RwLock<LinkedHashMap<u64, StringSender>>>,
      spawn_sender: AgentSpawnSender,
      monitor: Monitor,
  }
  ```

   - Agent 的 gPRC 通道
  ```rust {wrap}
  pub struct AgentRpcChannel {
      agent_activity_receiver: AgentActionsReceiver,
      agent_notify_sender: AgentNotifySender,
  }
  ```

   - 文件传输的流
  ```rust {wrap}
  #[derive(Debug)]
  pub struct PutStream {
      req: Streaming<FlightData>,
      controller: TaskControllerRef,
      task_id: i64,
      remote: Option<std::net::SocketAddr>,
      notify_sender: AgentNotifySender,
      qid: Qid,
      spawn_sender: AgentSpawnSender,
      cluster_id: i64,
      agent_id: i64,
  }
  ```

   - Agent 通知
  ```rust {wrap}
  #[derive(Debug, Clone)]
  pub enum AgentNotify {
      /// RPC server stopped.
      ServerStopped,
      /// Agent connected to RPC server.
      AgentConnected(AgentId),
      /// Agent has been unexpectedly disconnected from RPC server.
      AgentDisconnected(AgentId),
      /// Agent closed by ctrl-c.
      AgentClosed(AgentId),
  
      /// Put stream writer error.
      ///
      /// This error is sent by agent when it encounters an error while writing data to TDengine.
      WriterError(AgentId, TaskId, String),
      /// Agent task activity.
      TaskActivity(AgentId, Activity),
      /// Agent activity.
      AgentActivity(AgentId, Activity),
  }
  ```

   - Agent 操作
  ```rust {wrap}
  #[derive(Debug, Clone)]
  pub enum AgentAction {
      /// Tuple for (TaskId, JobId, RunId)
      Run(TaskId, Uuid, u64),
      #[allow(dead_code)]
      Stop(i64),
      /// Equivalent to `Suspend`.
      Cancel(i64),
      /// Interrupt and do nothing.
      Interrupt(i64),
      ListDataSets(DataSetsReq, AgentDataSetsSender),
      #[allow(dead_code)]
      RetrieveDataSets(DataSetsReq, Vec<DataSet>),
      /// check data source validation
      Check(String, DsvSender),
      /// get sample data
      GetSample(String, StringSender),
      /// send file to agent
      PutFile(PutFileReq, StringSender),
      /// query data source via connectors
      QueryDataSource(QueryDataSourceReq, StringSender),
  }
  ```

### 6.7 Transformer

1. 组件设计
   - Transformer Trait 定义所有 transformer 必须实现的接口，Transformer 包括：filter，map，mutate，parse等。
   - PipleLine 将所有 transformer 组织成工作流；
   - HandlingStrategy 处理 transformer 的处理策略，包括：Archive、Skip、Break 等；
   - Modeler 处理到 TDengine 的关系映射；
   - Sample 处理示例数据。
2. 列出系统中的关键数据结构
   - Pipeline
  ```rust {wrap}
  #[derive(Debug, Deserialize, Serialize, Clone)]
  pub struct Pipeline {
      #[serde(default)]
      global: Arc<TableOptions>,
      #[serde(default, skip_serializing_if = "Option::is_none")]
      parse: Option<ParserImpl>,
      #[serde(default, skip_serializing_if = "Vec::is_empty")]
      mutate: Vec<Mutate>,
      #[serde(default, skip_serializing_if = "Option::is_none")]
      s_model: Option<STableModel>,
      #[serde(default, skip_serializing_if = "Option::is_none")]
      model: Option<Modeler>,
  }
  ```

   - Parser
  ```rust {wrap}
  #[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
  pub struct Parser {
      #[serde(default)]
      global: Arc<TableOptions>,
      parse: Option<ParserImpl>,
      #[serde(default, skip_serializing_if = "Vec::is_empty")]
      mutate: Vec<Mutate>,
      s_model: Option<STableModel>,
      model: Modeler,
  }
  ```

   - 处理策略
  ```rust {wrap}
  #[derive(Debug, Deserialize, Serialize, Default, Clone, Copy, PartialEq)]
  #[serde(rename_all = "snake_case")]
  pub enum HandlingStrategy {
      #[default]
      Archive,
      Skip,
      Break,
  }
  ```

   - Model
  ```rust {wrap}
  #[derive(Debug, Deserialize, Serialize, Clone)]
  pub struct ModeledField {
      pub name: String,
      pub scope: FieldScope,
      pub r#type: IpcDataType,
      pub arrow_type: DataType,
  }
  ```

   - Table
  ```rust {wrap}
  #[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
  pub struct Table {
      pub name: String,
      #[serde(default)]
      pub using: Option<String>,
      #[serde(default)]
      pub tags: Option<Vec<String>>,
      #[serde(default)]
      pub columns: Option<Vec<String>>,
      #[serde(default)]
      pub r#where: Option<BooleanExpr>,
      #[serde(default, with = "once_lock_serde")]
      pub global: std::sync::OnceLock<Arc<TableOptions>>,
  }
  ```

### 6.8 TDengine Writer

1. 组件设计
   - IpcStreamWriter 用于处理 IPC（进程间通信）流的写入，处理数据批次的写入和错误处理，发送通知和处理中止消息。
   - 处理 Point 类消息，将其写入 TDengine；
   - 处理 Flat 类消息，将其写入 TDengine；
   - 将其写入 TDengine；
2. 列出系统中的关键数据结构
   - StreamWorker
  ```rust {wrap}
  
  #[allow(dead_code)]
  pub struct IpcStreamWorker {
      pool: TaosPool,
      pub parser: IpcParser,
      lock: Arc<Mutex<()>>,
      task: Option<i64>,
      from: Dsn,
      config: Option<Arc<OPCConfig>>,
      opc_table_config: OnceCell<OpcModelConfig>,
      pub lush_model_config: OnceCell<Arc<LushModelConfig>>,
      pub lush_table_cache: Option<Arc<TableTagCache>>,
      breakpoint_db: Option<BreakpointDb>,
      license: Option<Arc<ConnectorLicense>>,
      transferred: Option<Arc<Transferred>>,
      taos: Cell<Option<deadpool::managed::Object<Manager<TaosBuilder>>>>,
      target_precision: taos::Precision,
      span: tracing::Span,
      cancel: CancellationToken,
  }
  ```

   - Point 类型数据
  ```rust {wrap}
  #[derive(Clone, Debug)]
  struct PointInsertion {
      column_configs: Vec<ColumnConfig>,
      tag_configs: Option<Vec<TagConfig>>,
      columns: Vec<(String, String)>, // column_name(original_ts/received_ts/value/quality), column_alias
      value_column_config: Option<ColumnConfig>,
      other_columns: String,
      tags: String,
  }
  ```

   - Flat 类型的 Record
  ```rust {wrap}
  #[derive(Debug)]
  #[allow(dead_code)]
  pub(crate) struct Records {
      pub stable: Option<String>,
      pub sql: String,
      pub tables: usize,
      pub records: usize,
  }
  ```

   - FlatSink
  ```rust {wrap}
  pub struct FlatSink {
      pool: TaosPool,
      taos: Option<TaosConnection>,
      parser: Arc<Parser>,
      target_precision: taos::Precision,
      db: String,
      senders: Vec<flume::Sender<FlatItem>>,
      set: Option<JoinSet<anyhow::Result<()>>>,
  }
  ```

   - LushModelConfig
  ```rust {wrap}
  #[derive(Clone, Debug, Serialize)]
  pub struct LushModelConfig {
      /// The name of the column that can uniquely represent a sub-table in the received RecordBatch.
      pub table_id_column: String,
  
      /// key:  super-table name .
      /// value: parser for the super-table.
      pub super_table_parsers: HashMap<String, Parser>,
  
      pub super_table_sqls: HashMap<String, String>,
  
      /// key: sub-table name in point mode, default super table name in element mode.
      /// value: super-table name.
      pub super_table_name_mapping: HashMap<String, String>,
      // 写入的时候是否跳过 null 值
      // 目前实现：PI backfill 不跳过 null 值，PI 实时数据跳过 null 值
      pub skip_null: bool,
  }
  ```

   - Lush 写入错误
  ```rust {wrap}
  #[derive(Debug, Error)]
  pub enum WriteError {
      #[error("Connection error")]
      ConnectionPoolError(#[from] deadpool::managed::PoolError<taos::Error>),
      #[error("Table not exists")]
      TableNotExits(String),
      #[error("Container length too short: {0:#}")]
      ContainerLengthTooShort(String),
      #[error("Write SQL error: {0:#}")]
      Taos(#[from] taos::Error),
      #[error("Arrow internal error: {0}")]
      Arrow(#[from] arrow::error::ArrowError),
      #[error("IO error: {0}")]
      Io(#[from] std::io::Error),
      #[error(transparent)]
      Anyhow(#[from] anyhow::Error),
  }
  ```

   - 写入的统计指标
  ```rust {wrap}
  #[derive(Serialize, Deserialize, Debug, Default)]
  #[serde(default)]
  pub struct IpcMetrics {
      #[serde(flatten)]
      pub com: CommonMetrics,
      pub total_received_batches: AtomicU64,
      pub total_processed_batches: AtomicU64,
      pub total_failed_batches: AtomicU64,
      pub total_processed_rows: AtomicU64,
      pub total_inserted_sqls: AtomicU64,
      pub total_failed_sqls: AtomicU64,
      pub total_created_stables: AtomicU64,
      pub total_created_tables: AtomicU64,
      pub total_failed_rows: AtomicU64,
      pub total_drained_rows: AtomicU64,
      pub total_failed_points: AtomicU64,
      pub total_written_raw_blocks: AtomicU64,
      pub total_failed_raw_blocks: AtomicU64,
      pub received_batches: AtomicU64,
      pub processed_batches: AtomicU64,
      pub failed_batches: AtomicU64,
      pub processed_rows: AtomicU64,
      pub inserted_sqls: AtomicU64,
      pub failed_sqls: AtomicU64,
      pub created_stables: AtomicU64,
      pub created_tables: AtomicU64,
      pub drained_rows: AtomicU64,
      pub failed_rows: AtomicU64,
      pub failed_points: AtomicU64,
      pub written_raw_blocks: AtomicU64,
      pub failed_raw_blocks: AtomicU64,
  
      #[serde(flatten)]
      pub extras: scc::HashIndex<FastStr, u64>,
  }
  ```

### 6.9 Agent

1. 组件设计：
   - 使用 Arrow Flight Client 进行客户端管理和 API 调用。
   - 使用独立任务管理器进行数据源采集任务管理。
   - 使用事件流进行 Agent 与 Xnode 之间的控制信息交互。
   - 使用 Channel 在任务管理器与控制流接口间通信。
2. 列出系统中的关键数据结构
   - Agent Flight Client：
    ```rust
    #[derive(Debug)]
    pub struct Client {
        pub endpoint: String,
        pub client: FlightClient,
        pub agent: Agent,
        pub req_id: Arc<AtomicU64>,
    }
    #[derive(Debug, Clone, Deserialize)]
    pub struct Agent {
        pub id: i64,
    
        pub dsn: String,
        pub name: String,
        pub cluster_id: String,
        pub user_id: String,
    }
    
    ```

   - Agent -> Server 回复信息
    ```rust
    pub type TaskMetricItem = (i64, FastStr, TaskMetricsVariant, u64);
    pub type TaskMetrics = Vec<TaskMetricItem>;
    pub enum RespAction {
        /// Heartbeat
        Heartbeat,
        /// Response for server heartbeat request
        HeartbeatOk(HeartbeatResponse),
        /// Task error for specific task ID
        TaskError(i64),
        /// ReqId, Resp
        ListOk(ListResponse),
        /// Data source connectivity check response.
        CheckOk(CheckResponse),
        /// Sample data from source response to server.
        SampleOk(SampleResponse),
        /// Ack for put file request from server.
        PutFileOk(PutFileResp),
        /// Agent activity item to server.
        AgentActivity(Activity),
        /// Task activity item to server.
        TaskActivity(Activity),
        /// Task metrics pushed to server.
        TaskMetrics(TaskMetrics),
        /// System metrics events to server.
        Metrics(MetricsEvents),
        /// Query data from specfici data source response to server.
        QueryDataSourceOk(QueryDataSourceResp),
    }
    ```

   - Task Action：Agent 收到的服务端任务请求
    ```rust
    #[allow(clippy::large_enum_variant)]
    pub enum Action {
        Run(Task),
        Stop(i64),
        Cancel(i64),
        Interrupt(i64),
    }
    ```

### 6.10 系统表设计

1. ins_xnodes 系统表
存储 XNode 节点信息：

| **名称** | **类型** | **默认值** | **取值说明** | **功能描述** |
| --- | --- | --- | --- | --- |
| id | int | 自增 |  | xnode 的 id |
| url | varchar | 必填 | 通用的 host:port 形式的节点表示。 | 通信地址 |
| status | varchar | NULL | online | offline | drain | 节点状态 |
| create_time | datetime | 入库时间 |  | XNode 创建时间 |
| update_time | datetime | 入库时间 |  | XNode 最近一次状态更新时间 |

1. ins_xnode_tasks 系统表
存储任务元数据：

| **名称** | **类型** | **默认值** | **取值说明** | **功能描述** |
| --- | --- | --- | --- | --- |
| id | int | 自增 |  | 任务id，从1开始计数 |
| name | varchar | 必填 |  | 任务名称，用于标识任务 |
| via | int | NULL |  | agent 的 id |
| from | varchar | NULL |  | dsn 格式 |
| to | varchar | NULL |  | dsn 格式 |
| parser | varchar | NULL |  | 解析器配置 |
| xnode_id | int | NULL | NULL 表示非分片或初始状态 | 需要查看 show jobs 查看具体分片信息 |
| status | varchar | NULL | Xnode 自行维护 | 任务状态 |
| reason | varchar | NULL |  | failed 时填写：最后出错消息，重试失败消息会覆盖此内容 |
| create_time | datetime | 入库时间 |  | 创建时间 |
| update_time | datetime | 入库时间 |  | 最近一次更新时间(status完成状态时，updateTime就是完成时间) |

1. ins_xnode_jobs 系统表
存储分片任务信息：

| 名称 | 类型 | 默认值 | 取值说明 | 功能描述 |
| --- | --- | --- | --- | --- |
| id | int | 必填 |  | 任务id |
| task_id | int | 必填 |  | 对应的任务id |
| xnode_id | int | NULL | 为 NULL 表示还未分配到 xnode 执行 |  |
| via | int | NULL | agent 的 id, 如果没有 agent 取值 NULL | 和 task 的相同 |
| config | varchar | NULL | "config": {"from":{..},"to":{..},"parser":{..}} | 包含:from, to, parser 信息 |
| status | varchar | NULL | Xnoded 维护 | job状态 |
| reason | varchar | NULL |  | failed 需要填写此内容：最后出错消息，重试失败消息会覆盖此内容 |
| create_time | datetime | 入库时间 |  |  |
| update_time | datetime | 入库时间 |  | 最近一次更新时间, status 为 succeeded 时候，就是完成时间 |

1. ins_xnode_agent 系统表

| 名称 | 类型 | 默认值 | 取值说明 | 功能描述 |
| --- | --- | --- | --- | --- |
| id | int | 必填 |  | agent 的 id |
| name | varchar | 必填 |  | agent 名称 |
| token | varchar | 必填 | 样例：eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjIsImlhdCI6MTc1NzU4MDQyOX0.3RYaJHRzCEixFdwuh9sa26XMg8bc4I7_OeQVXFrjhZY | 验证 token |
| status | varchar | NULL | online | offline | 节点状态; Xnoded 通过 alter 语句自行维护 |
| create_time | long | 入库时间 |  | agent 最近一次创建的时间 |
| update_time | long | 入库时间 |  | agent 最近一次更新的时间 |

### 6.10 节点管理

1. 创建 XNode 流程
![](./images/img_QNk9bmVWyoU57zxjyKycgRWPn6S.png)

创建流程说明：
1. 用户通过 SQL 发起 `CREATE XNODE` 请求
2. 系统进行权限检查（仅管理员可操作）
3. 如果是第一个 XNode，创建专用用户 `xnode`
4. 将节点信息写入 sdb 的 `ins_xnodes` 表
5. XNode 启动后与 MNode 建立 RPC 连接，注册并定期发送心跳
6. 删除 XNode 流程
![](./images/img_MU9abLrv9oja7mxODcucFPLanWd.png)

删除流程说明：
1. 用户发起 `DROP XNODE` 请求
2. MNode 接收请求后检查节点可用性
3. 如果是最后一个可用 XNode 且有运行任务，报错
4. 否则发送 DRAIN 请求到 XNode，等待任务迁移
5. 任务清理完毕后，从 sdb 删除节点记录

### 6.11 任务管理

1. 创建任务流程
![](./images/img_JrThbKZqAozhrTx9ULOcVcm8ngh.png)

1. 执行任务流程
![](./images/img_XNWcbUr7JoMg1mxmbhLckJnunMe.png)

### 6.12 任务分片（Sharding）

1. 分片策略接口：
```java {wrap}
/// 分片 ID
pub enum ShardId {
    Consumer {
        id: FastStr,
        paramters: LinkedHashMap<FastStr, FastStr>,
    },
    Markup {
        values: LinkedHashMap<FastStr, FastStr>
    },
}

/// 分片
pub struct Shard {
    pub task: Arc<Task>,         // 父任务
    pub id: ShardId,             // 分片标识
    pub checkpoint: Option<Bytes>, // 偏移量或标记范围
}

/// 待执行的任务
pub type AsyncJob = Box<dyn Future<Output=Result<Summary, Error>>>;

/// 数据源定义接口
pub trait ShardingTask {
    async fn shards(&self) -> impl FusedStream<Item=Result<Shard, ShardError>>;
    fn shard_job(&self, context: &Context, shard: &Shard) -> Result<AsyncJob, ShardError>;
}
```

1. 消费者策略
适用于 Kafka、TMQ、MQTT 等订阅类数据源：
- 任务启动后，消费者分区基本不变（或变动频率较低）
- 每个消费者分区可能包含所有时间的时序数据
- 分区的进度使用偏移量进行标记，通常可以使用偏移量可允许重复消费
- 每个分区可能都会持续运行，并不结束
1. 标记切割策略
适用于 TDengine 查询、关系型数据库等：
- 每个分区只包含一部分时序数据
- 分区可以通过标记进行重复拉取
- 分区随着任务进行逐步增多
- 每个分区预期将在有限的时间内结束

### 6.13 负载均衡

1. 负载均衡策略
每个分片的子任务，按照配置负载均衡策略在可用节点上分配：
- RoundRobin 策略
按轮询方式分配分片：
```sql {wrap}
X1: Shard1, Shard4, Shard7
X2: Shard2, Shard5
X3: Shard3, Shard6
```

- Range 策略
按范围分配分片：
```sql {wrap}
X1: Shard1, Shard2, Shard3
X2: Shard4, Shard5
X3: Shard6, Shard7
```

1. 故障迁移
当 XNode 宕机时的处理流程：
1. 节点宕机确认：
  - 当心跳超时无法找到 XNode 节点时，视为系统异常，报警
  - 当 XNode 无法连接到 MNode 时，不做任何操作，当前任务继续执行
1. 重新平衡：
  - 将 MNode 中的节点状态置为 Offline，不再进行新任务的消费
  - 宕机节点的分片任务视为失败，重新分配到其他节点上执行
```sql {wrap}
MNode 检测到 XNode 2 失联
    |
    v
+-----------+
| 确认宕机 | (通过其他心跳超时确认) +-----------+ |
    v
+-----------+
| 标记 Offline +-----------+ |
    v
+-----------+
| 获取故障   |
| 节点分片   |
+-----------+
    |
    v
+-----------+
| 重新分配   |
| 分片到其他 |
| 可用节点   |
+-----------+
    |
    v
+-----------+
| 更新       |
| ins_xnode_|
| jobs      |
+-----------+
```

### 6.14 XNode 服务

1. 架构变化
原来的 taosx 服务模式转为 xnode 服务：
- 新的 XNODE 服务不存储元数据，仅接收 mnode 的指令并进行交互
- XNode 启动后，向 MNode 注册并定期发送心跳
- 接收 MNode 派发的分片任务并执行
1. RPC 通信
Mnode leader 节点启动后，启动 xnoded，将与 xnodes 列表中的节点建立 RPC 连接：
- 功能包括：
   - 获取当前 XNODE 状态和运行的任务
   - 检查数据源在当前节点的可用性
   - 发送指定任务
   - KeepAlive 心跳
1. 关键数据结构
```rust {wrap}
/// XNode 配置
pub struct XNodeConfig {
    pub id: i64,
    pub url: String,
    pub data_dir: PathBuf,
    pub rpc_listen: SocketAddr,
}

/// XNode 状态
pub struct XNodeState {
    pub config: XNodeConfig,
    pub status: XNodeStatus,  // Online, Offline, Drain
    pub running_jobs: HashMap<String, JobHandle>,
    pub mnode_client: MNodeClient,
}

/// 分片任务执行器
pub struct ShardExecutor {
    pub shard: Shard,
    pub task: Arc<Task>,
    pub cancel: CancellationToken,
    pub metrics: Arc<Metrics>,
}
```

### 6.15 共享存储

1. 目录结构
```bash {wrap}
/taosx/data/
├── files/              # 上传文件目录
├── tasks/              # 任务目录
│   └── {task_id}/      # 任务目录，以任务 ID 命名
│       ├── x.lock      # 任务锁文件（防止重复执行）
│       ├── checkpoints/# 检查点数据
│       │   └── {shard_id}.chk
│       ├── metrics/    # 运行指标缓存
│       │   └── current.json
│       └── shards/     # 分片数据目录
│           └── {shard_id}/
│               ├── dump/     # 数据转储
│               ├── archived/ # 归档文件
│               └── cache/    # 缓存文件
└── logs/               # 日志目录
    └── taosx.log
```

1. 文件锁机制
主任务执行时创建文件锁 `x.lock`，防止冲突和重复执行：
```rust {wrap}
pub struct TaskLock {
    path: PathBuf,
    file: File,
}

impl TaskLock {
    pub fn acquire(task_id: i64, data_dir: &Path) -> Result<Self, Error> {
        let path = data_dir.join("tasks").join(task_id.to_string()).join("x.lock");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.lock_exclusive()?;
        Ok(Self { path, file })
    }
}
```

1. 定时备份
支持配置 CRON 表达式自动备份：
```rust {wrap}
pub struct BackupConfig {
    pub cron: String,        // CRON 表达式
    pub backup_dir: PathBuf, // 备份目录（支持 S3）
    pub retention: Duration, // 保留期限
}

impl BackupConfig {
    pub fn schedule(&self) -> Result<Schedule, Error> {
        Schedule::from_str(&self.cron)
    }
}
```

## 接口规范

### 7.1 SQL 接口

1. XNode 管理
```sql {wrap}
-- 创建 XNode
CREATE XNODE 'host:port' [USER user PASS 'password'];

-- 删除 XNode
DROP XNODE [FORCE] 'host:port' | id;

-- 查看 XNode
SHOW XNODES;

-- 设置 DRAIN 模式
DRAIN XNODE id;
```

1. 任务管理
```sql {wrap}
-- 创建任务
CREATE XNODE TASK 'name' FROM 'dsn' TO DATABASE db [WITH options];

-- 启动任务
START XNODE TASK id | 'name';

-- 停止任务
STOP XNODE TASK id | 'name' [WITH timeout = 10s];

-- 删除任务
DROP XNODE TASK [FORCE] id | 'name';

-- 查看任务
SHOW XNODE TASKS;

-- 查看分片
SHOW XNODE JOBS [WHERE conditions];
```

1. Agent 管理
```sql {wrap}
-- 创建 Agent
CREATE XNODE AGENT 'name' [WITH options];

-- 删除 Agent
DROP XNODE AGENT [FORCE] 'name';

-- 查看 Agent
SHOW XNODE AGENTS;
```

1. 负载均衡
```rust {wrap}
-- 重新平衡
REBALANCE XNODE JOBS WHERE conditions;

-- 手动切换分片节点
ALTER XNODE JOB 'jid' SET XNODE id;
```

### 7.2 RPC 接口

1. gRPC 使用 Arrow Flight RPC 规范。
  ```rust
  pub trait FlightService:
      Send
      + Sync
      + 'static {
      type HandshakeStream: Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static;
      type ListFlightsStream: Stream<Item = Result<FlightInfo, Status>> + Send + 'static;
      type DoGetStream: Stream<Item = Result<FlightData, Status>> + Send + 'static;
      type DoPutStream: Stream<Item = Result<PutResult, Status>> + Send + 'static;
      type DoExchangeStream: Stream<Item = Result<FlightData, Status>> + Send + 'static;
      type DoActionStream: Stream<Item = Result<Result, Status>> + Send + 'static;
      type ListActionsStream: Stream<Item = Result<ActionType, Status>> + Send + 'static;
  
      // Required methods
      fn handshake<'life0, 'async_trait>(
          &'life0 self,
          request: Request<Streaming<HandshakeRequest>>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<Self::HandshakeStream>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn list_flights<'life0, 'async_trait>(
          &'life0 self,
          request: Request<Criteria>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<Self::ListFlightsStream>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn get_flight_info<'life0, 'async_trait>(
          &'life0 self,
          request: Request<FlightDescriptor>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<FlightInfo>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn poll_flight_info<'life0, 'async_trait>(
          &'life0 self,
          request: Request<FlightDescriptor>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<PollInfo>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn get_schema<'life0, 'async_trait>(
          &'life0 self,
          request: Request<FlightDescriptor>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<SchemaResult>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn do_get<'life0, 'async_trait>(
          &'life0 self,
          request: Request<Ticket>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<Self::DoGetStream>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn do_put<'life0, 'async_trait>(
          &'life0 self,
          request: Request<Streaming<FlightData>>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<Self::DoPutStream>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn do_exchange<'life0, 'async_trait>(
          &'life0 self,
          request: Request<Streaming<FlightData>>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<Self::DoExchangeStream>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn do_action<'life0, 'async_trait>(
          &'life0 self,
          request: Request<Action>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<Self::DoActionStream>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
      fn list_actions<'life0, 'async_trait>(
          &'life0 self,
          request: Request<Empty>,
      ) -> Pin<Box<dyn Future<Output = Result<Response<Self::ListActionsStream>, Status>> + Send + 'async_trait>>
         where Self: 'async_trait,
               'life0: 'async_trait;
  }
  ```

1. 模块 API 需要使用 snake_case 命名：
   - 公共 API：`pub fn module_name_functio(param1, param2)`
   - 库级别公共 API：`pub(crate) fn module_name_functio(param1, param2)`
   - 仅父模块可见 API：`pub(super) fn module_name_functio(param1, param2)`
   - 仅当前模块及子模块可见：`fn module_name_functio(param1, param2)`

## 安全考虑

1. **认证授权**：
  - XNode 使用独立用户（`__xnode__`）与 MNode 通信
  - 用户密码随机生成，不对外暴露
  - SQL 接口继承 TDengine 的权限体系
1. **权限控制**：
  - 只有管理员可执行节点管理操作
  - 普通用户只能管理自己的任务
1. **通信安全**：
  - RPC 通信使用 TLS 加密
  - SQL 接口通过 TDengine 的安全通道
1. **数据隔离**：
  - 任务数据按 ID 隔离存储
  - 文件锁防止并发冲突

## 性能和可扩展性

### 9.1 性能要求

1. 数据迁移：1000 万数据点/秒
2. 数据同步：100 万数据点/秒
3. CSV 导入：100 万数据点/秒
4. 分片切换：< 30 秒（XNode 故障后）
5. MNode 切换恢复：< 10 秒

### 9.2 可扩展性

- **水平扩展**：通过添加 XNode 节点线性提升处理能力
- **分片并行**：单个任务可分片到多个节点并行执行
- **负载均衡**：自动在节点间分配任务分片
- **故障恢复**：节点故障后自动迁移任务，不影响整体服务

## 部署和配置

### 10. 1 部署流程

1. 部署 TDengine 集群
2. 配置共享存储（NAS/SAN）
3. 在所有数据节点安装 taosx
4. 配置 `taosx.toml`，设置相同的 `data_dir`
5. 启动 taosd 和 taosx 服务
6. 使用 `CREATE XNODE` 注册节点

### 10.2 配置管理

1. taos.cfg
```rust {wrap}

## taosX 数据目录（共享存储）

taosxDataDir /nas/taosx/data/

## 自动备份配置

xNodeBackupCron "0 15 10 ? * MON-FRI"
xNodeBackupDir "/backup/taosx/"
```

1. taosx.toml
```rust {wrap}
data_dir = "/nas/taosx/data/"
logs_home = "/var/log/taos"
log_level = "info"

[serve]
listen = "0.0.0.0:6050"
```

### 10.3 版本控制

- 向后兼容：保持 SQL 接口稳定
- 升级：使用 `taosx migrate` 迁移旧数据
- 降级：不删除新数据，允许降级后原数据保持不变

## 监控和维护

### 11.1 监控机制

1. **节点状态监控**：`SHOW XNODES`
2. **任务状态监控**：`SHOW XNODE TASKS/JOBS`
3. **指标上报**：通过 taosKeeper 写入 `log` 库
4. **告警**：节点宕机、任务失败等事件触发告警

### 11.2 日志记录

- 日志路径：`${data_dir}/logs/taosx.log`
- 11.3任务日志：可通过 `task.id:<id>` 过滤
- 节点日志：可通过 `xnode.id:<id>` 过滤

### 11.3 维护操作

```sql {wrap}
-- 查看节点状态
SHOW XNODES;

-- 查看任务状态
SHOW XNODE TASKS;

-- 查看分片状态
SHOW XNODE JOBS;

-- 设置节点为 DRAIN 模式（准备维护）
DRAIN XNODE 1;

-- 重新平衡负载
REBALANCE XNODE JOBS;
```

## 参考资料

1. 功能规格说明：新数据管道-FS.md
2. 需求规格说明：新数据管道-RS.md
3. TDengine SQL 手册：[https://docs.taosdata.com/reference/taos-sql/](https://docs.taosdata.com/reference/taos-sql/)
4. TDengine 数据接入文档：[https://docs.taosdata.com/reference/taos-sql/datain/](https://docs.taosdata.com/reference/taos-sql/datain/)
