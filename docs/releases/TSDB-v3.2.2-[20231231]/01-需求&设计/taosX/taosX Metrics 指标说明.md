# taosX Metrics 指标说明

## 1. 背景

Jira: 
TD-26761

metrics 是对我们感兴趣的某个观测值的度量。taosX 提供了很多 metrics。metrics 的描述将作为提示信息展示在页面上。本文档对所有 metrics 的名称、含义、局限和应用场景做梳理。

## 2. 中文描述

```plaintext
taosx_sys_cpus: 系统 CPU 核数
taosx_sys_total_memory: 系统总内存，单位：字节
taosx_sys_used_memory: 系统已用内存，单位：字节
taosx_sys_available_memory: 系统可用内存, 单位：字节
taosx_sys_uptime_in_seconds: 系统启动时长，单位：秒
taosx_process_cpu_percent: taosX 进程占用 CPU 百分比
taosx_process_mem_percent: taosX 进程占用内存百分比
taosx_process_io_read_bytes: 从上次更新到现在 taosX IO 读字节数
taosx_process_io_written_bytes: 从上次更新到现在 taosX IO 写字节数
taosx_process_tasks: taosx 进程的线程数
metrics.legacy.workers: 并发读取数据源的数据 worker 数
metrics.legacy.total_stables: 需要迁移的超级表数据数量
metrics.legacy.updated_tags: 更新 tag 数
metrics.legacy.updated_tables: 更新子表数
metrics.legacy.created_tables: 创建子表数
metrics.legacy.total_tables: 需要迁移的子表数量
metrics.legacy.tables: 完成数据迁移的子表数(任务中断重启可能大于实际值)
metrics.legacy.blocks: 写入成功的数据块数
metrics.legacy.records: 写入成功的行数
metrics.legacy.points: 写入成功的测点数
metrics.time_started_timestamp: 任务启动时的 UTC 时间对应的毫秒数
metrics.time_cost: 任务已经运行秒数
metrics.records_per_second: 平均每秒写入行数
ipc.stream.record_batches: taosX 通过 IPC Stream 收到的数据总批数
ipc.stream.batch_records: taosX 通过 IPC Stream 收到的数据总行数
ipc.stream.insert_sqls: taosX 执行的 INSERT SQL 总条数
ipc.stream.insert_sql_fails: taosX 执行失败的 INSERT SQL 总条数
ipc.stream.stable_created: 尝试创建超级表数（可能大于实际值）
ipc.stream.child_table_created: 尝试创建子表数(可能大于实际值)
ipc.stream.records: 成功写入 TDengine 的总行数（包括重复记录）
ipc.stream.record_fails: 写入失败的行数
ipc.stream.points: 写入成功的点数, 即写入成功的函数乘以列数（包括重复记录）
ipc.stream.point_fails: 写入失败的点数
ipc.stream.write_raw_blocks: 写人成功的 raw block 数
ipc.stream.write_raw_blocks_fails: 写入失败的 raw block 数
metrics.tmq.topics: 通过 TMQ 订阅的主题数
metrics.tmq.workers: TMQ 消费者数
metrics.tmq.messages: 通过 TMQ 收到的消息总数
metrics.tmq.messages_of_meta: 通过 TMQ 收到的 Meta 类型的消息总数
metrics.tmq.write_meta_fails: 写入 Raw Meta 失败次数
metrics.tmq.messages_of_data: 通过 TMQ 收到的 Data 和 MetaData 类型的消息总数
metrics.tmq.blocks: 写入成功的数据块数
metrics.tmq.records: 写入成功的行数（等于数据块包含的行数）
metrics.tmq.points: 写入成功点数(等于数据块包含的行数乘以数据块包含的列数)
```

## 3. 英文描述

```plaintext
taosx_sys_cpus: System CPU cores
taosx_sys_total_memory: Ttotal system memory in bytes
taosx_sys_used_memory: "System used memory, unit: bytes"
taosx_sys_available_memory: "System available memory, unit: bytes"
taosx_sys_uptime_in_seconds: system start-up duration, in seconds
taosx_process_cpu_percent: "% CPU usage of taosx process"
taosx_process_mem_percent: "% of memory occupied by taosx processes"
taosx_process_io_read_bytes:  Number of read bytes since the last refresh
taosx_process_io_written_bytes: Number of written bytes since the last refresh
taosx_process_tasks: Number of threads run by this process
metrics.legacy.workers: The number of workers concurrently reading data sources
metrics.legacy.total_stables: Total super tables to migrate
metrics.legacy.updated_tags: Updated tag count
metrics.legacy.updated_tables: Updated subtables count
metrics.legacy.created_tables: Number of child tables created
metrics.legacy.total_tables: Total talbes to migratie
metrics.legacy.tables: Number of sub-tables successfully migrated.(may greater than the acutal value if task was resumed from break point)
metrics.legacy.blocks: Number of data blocks successfully written
metrics.legacy.records: Number of rows written successfully
metrics.legacy.points: Number of points written successfully
metrics.time_started_timestamp: The number of milliseconds corresponding to the time when the task is started
metrics.time_cost: The number of seconds the task has been running
metrics.records_per_second: Average lines written per second
ipc.stream.record_batches: Number of data batches received by taosx via IPC stream
ipc.stream.batch_records: Number of data rows received by taosx via IPC stream
ipc.stream.insert_sqls: Number of INSERT SQLs execute by taosx
ipc.stream.insert_sql_fails: Number of INSERT SQLs failed to execute
ipc.stream.stable_created: Number of suber tables that taosX try to created.(may greater than successfully created super tables)
ipc.stream.child_table_created: Number of child tables that taosX try to created
ipc.stream.records: Number of rows written successfully, including duplicated rows
ipc.stream.record_fails: Number of rows failed to write
ipc.stream.points: Number of points written successfully.(equals number of rows multiply number of columns)
ipc.stream.point_fails: Number of points failed to write
ipc.stream.write_raw_blocks: Number of raw blocks written sucessfully
ipc.stream.write_raw_blocks_fails: Number of raw blocks failed to write
metrics.tmq.topics: Number of topics that taosX will try to subscribe to
metrics.tmq.workers: Number of consumbers that taosX created to cosume topics
metrics.tmq.messages: Number of messages that taosX received
metrics.tmq.messages_of_meta: Number of messages with type "Meta" that taosX received
metrics.tmq.write_meta_fails: Number of "Meta" messages that taosX failed to deal with
metrics.tmq.messages_of_data: Number of messages with type "Data" that taosX received
metrics.tmq.blocks: Number of data blocks successfully written
metrics.tmq.records: Number of rows successfully written
metrics.tmq.points: Number of points successfully written
```

## 4. 度量使用场景

taosx_* 度量与具体任务无关，是 taosx 整体的 metrics。 
metrics.legacy.* 度量适用于 legacy_to_taos 任务类型。
ipc.stream.* 度量适用于所有使用 plugin 采集数据的任务类型，如： influxdb_to_taos, mqtt_to_taos 。
metrics.tmq.*  度量适用于 tmq_to_local 和 tmq_to_td 两种任务类型。
