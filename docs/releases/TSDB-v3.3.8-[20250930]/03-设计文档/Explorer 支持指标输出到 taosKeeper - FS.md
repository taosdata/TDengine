# Explorer 支持指标输出到 taosKeeper - FS

## 1. 背景

目前监控数据支持部分需求功能实现：
1. cpu负载：可通过监控数据taosd_dnodes_info中的cpu_engine和cpu_system计算得出过去N天的cpu使用率最大、最小和平均值
2. 内存负载：可通过监控数据taosd_dnodes_info中的mem_engine、mem_free和mem_total计算得出过去N天的内存使用率最大、最小和平均值
3. adapter负载：已新增监控指标；
4. taosd状态：可通过监控数据taosd_dnodes_status中的status计算得出过去N天中dnode的offline状态持续时间区间
5. taosAdapter状态：已新增监控指标；
6. taosExplorer状态：目前监控功能未收集相关数据。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/9/15 | 1.0 | @霍琳贺 | 初稿 |
|  |  |  |  |

## 3. 定义

无

## 4. 行为说明

### 4.1 新增Explorer 参数和配置文件

| 参数 | 命令行参数 | 环境变量 | 配置文件 | 默认值 |
| --- | --- | --- | --- | --- |
| taosKeeper 地址 | --monitor-fqdn | MONITOR_FQDN | monitor.fqdn | None（表示不启用监控上报） |
| taosKeeper 端口 | --monitor-port | MONITOR_PORT | monitor.port | 6043 |
| 上报间隔(秒) | --monitor-interval | MONITOR_INTERVAL | monitor.interval | 10 |

### 4.2 监控指标

在监控数据库中新增超级表 `explorer_sys`：
```sql {wrap}
taos> desc log.explorer_sys;
             field              |          type          |   length    |        note        |     encode     |    compress    |     level      |
================================================================================================================================================
 _ts                            | TIMESTAMP              |           8 |                    | delta-i        | lz4            | medium         |
 process_id                     | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 process_cpu_percent            | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 sys_total_memory               | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 process_memory_percent         | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 sys_used_memory                | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 process_uptime                 | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 sys_cpu_cores                  | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 process_disk_written_bytes     | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 sys_available_memory           | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 process_disk_read_bytes        | DOUBLE                 |           8 |                    | delta-d        | lz4            | medium         |
 endpoint                       | NCHAR                  |          16 | TAG                | disabled       | disabled       | disabled       |
Query OK, 13 row(s) in set (0.003300s)
```

各指标说明如下：

| 指标名 | 说明 |
| --- | --- |
| sys_total_memory | 系统内存 |
| sys_used_memory | 系统已用内存 |
| sys_available_memory | 系统可用内存 |
| sys_cpu_cores | 系统 CPU 核数 |
| process_id | 进程 ID |
| process_cpu_percent | 进程 CPU 占比 |
| process_mem_percent | 进程内存占比 |
| process_uptime | 进程启动时长 |
| process_disk_written_bytes | 距上次上报写入大小 |
| process_disk_read_bytes | 距上次上报读取大小 |
| endpoint | Explorer 节点 ID，使用 host:port 表示 |

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

1. 监控 Explorer 存活状态
使用 process_uptime 监控 Explorer 存活时长和状态。

## 9. 约束和限制

无

## 10. 常见错误和排查

### 10.1 Address already in use

此错误一般表示 Explorer 6060 端口被占用，与监控配置无关。
```plaintext {wrap}
Error: Bind address 0.0.0.0:6060 error

Caused by:
    Address already in use (os error 98)
```

### 10.2 “Connection Refused" 错误

通常配置了无法访问的 taosKeeper 地址。

## 11. 可观测性

增强各组件的观测可见性。

## 12. 安装和卸载

`explorer.toml` 文件更新，添加新参数配置。

## 13. 文档

须在 Explorer 参考文档中说明（https://docs.taosdata.com/reference/components/explorer/）

## 14. 参考文档

无。

## 15. 附录

无。
