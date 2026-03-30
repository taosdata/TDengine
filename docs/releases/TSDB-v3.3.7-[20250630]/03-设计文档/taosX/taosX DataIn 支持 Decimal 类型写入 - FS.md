# taosX DataIn 支持 Decimal 类型写入 - FS

## 1. 背景

TDengine 自 v3.3.6.0 开始支持 Decimal 数据类型，taosX 需要支持 Decimal 数据类型。
相关的 JIRA：

TS-6175

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/5/26 | 0.1 | @杨志宇 | 初稿 |
|  |  |  |  |

## 3. 定义

无

## 4. 行为说明

### 4.1 taosX 处理 Decimal 数据类型的规则

taosX 需要支持以下场景：
1. 数据源原生不支持 Decimal 但支持 double/float 类型的，taosX 支持将 double/float 写入到 decimal 类型。
2. 数据源原生支持 Decimal，taosX 支持将 decimal 类型的数据映射到 decimal，同时，支持将 float/double 映射成 decimal。
3. 数据源原生支持 Decimal，taosX 如果映射到 float/double，写入时可能产生 data overflow，taosX 需要处理错误，对 decimal 数据进行类型转换，丢失精度，但保证写入成功。

### 4.2 各数据源使用 Decimal 的场景

| **数据源** | **是否原生支持 Decimal 类型** | **使用场景** |
| --- | --- | --- |
| TDengine Query | 是 | 1. 迁移，包含 Decimal 类型的数据，source 和 target 都支持 decimal。 1. 迁移，source 为 double 类型，target 为 decimal(10, 2) 类型，列名、表名都相同 1. 迁移，source 为 double 类型，target 为 decimal(22, 10) 类型，列名、表名都相同 1. 迁移，source 为 decimal(10, 2) 类型，target 为 double 类型，列名、表名都相同 1. 迁移，source 为 decimal(22, 10) 类型，target 为 double 类型，列名、表名都相同 1. 迁移，source 为 float 类型，target 为 decimal(10, 2) 类型，列名、表名都相同 1. 迁移，source 为 float 类型，target 为 decimal(22, 10) 类型，列名、表名都相同 1. 迁移，source 为 decimal(10, 2) 类型，target 为 float 类型，列名、表名都相同 1. 迁移，source 为 decimal(22, 10) 类型，target 为 float 类型，列名、表名都相同 |
| TDengine Subscription | 是 | 1. 同步包含 Decimal 类型的数据，source 和 target 都支持 decimal。 1. 同步，souce 为 double 类型，target 为 decimal(10, 2) 类型，列名、表名都相同 1. 同步，source 为 decimal(10, 2) 类型，target 为 double 类型，列名、表名都相同 |
| PI / PI backfill | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| OPC UA | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| OPC DA | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| InfluxDB | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| OpenTSDB | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| MQTT | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| Kafka | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| CSV | 否 | 1. parse decimal(22, 10) in csv, write to decimal(22, 10) column 1. parse decimal(10, 2) in csv, write to decimal(10, 2) column 1. parse decimal(10, 2) in csv, write to double column 1. parse double in csv, write to decimal(10, 2) column 1. parse double in csv, write to decimal(22, 10) column |
| AVEVA Histrian | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |
| MySQL | 是 | 1. 数据写入，decimal 类型 mapping 到 Decimal 类型 1. 数据写入，double 类型 mapping 到 Decimal 类型 |
| PostgreSQL | 是 | 1. 数据写入，decimal 类型 mapping 到 Decimal 类型 1. 数据写入，double 类型 mapping 到 Decimal 类型 |
| Oracle | 是，即 number 类型 | 1. 数据写入，decimal 类型 mapping 到 Decimal 类型 1. 数据写入，double 类型 mapping 到 Decimal 类型 |
| Microsoft SQL Server | 是 | 1. 数据写入，decimal 类型 mapping 到 Decimal 类型 1. 数据写入，double 类型 mapping 到 Decimal 类型 |
| MongoDB | 是，decimal128 | 1. 数据写入，decimal 类型 mapping 到 Decimal 类型 1. 数据写入，double 类型 mapping 到 Decimal 类型 |
| SparkplugB | 否 | 数据写入，double 类型 mapping 到 Decimal 类型 |

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

无

## 9. 约束和限制

无

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

无

## 14. 参考文档

无

## 15. 附录
