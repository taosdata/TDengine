# Test Spec - MQTT 支持 SparkplugB

## 1. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2024/06/09 | 1.0 | @闫宇星 | 初稿 |

## 2. 测试目标

<quote-container>
对新数据源 SparkplugB 进行测试，包括任务的增删改查，启动停止等
</quote-container>

## 3. 参考文档

<quote-container>
https://jira.taosdata.com:18080/browse/TS-6067
</quote-container>

## 4. 测试结论

<quote-container>
任务正常增删改查和复制，配置任务后可以正常写入
</quote-container>

## 5. 测试环境

- OS: Linux
- Browser: Chrome

## 6. 功能测试

### 6.1 任务运行

#### 6.1.1 测试要点

##### 6.1.1.1 使用 spb_pub 工具指定上报的 metric

```bash
cargo run -p taosx-tools --bin spb_pub -- --schema tests/tools/schema/spb.toml --host broker-cn.emqx.io
```

配置文件: 指定了一个 node 可以一个 device，分别对 spb 的各字段进行了随机赋值
```toml
group_id = "taosdata"

[timestamp]
start_time = 2025-10-01T00:00:00.888Z
interval = "1ms"

[[node_devices]]
node = "node1"

[[node_devices.metrics]]
name = "metric14"
is_transient = {}
is_historical = { fixed = false }
value = { type = "int8", range = { min = -100, max = 100 } }

[[node_devices.metrics]]
name = "metric15"
is_transient = {}
is_historical = { fixed = false }
value = { type = "boolean" }

[[node_devices.metrics]]
name = "metric16"
is_transient = {}
is_historical = { fixed = false }
value = { type = "int16", range = { min = -100, max = 100 } }

[[node_devices.metrics]]
name = "metric17"
is_transient = {}
is_historical = { fixed = false }
value = { type = "int32", range = { min = -100, max = 100 } }

[node_devices.metrics.metadata]
is_multi_part = { fixed = true }
content_type = { samples = ["application/json", "application/plain"] }
size = { fixed = 100 }
seq = { range = { min = 1, max = 100 } }
file_name = { fixed = "file_a" }
file_type = { samples = ["txt", "json", "toml"] }
md5 = { fixed = "cab31dc8b3704659c52bd581455e2dc1" }
description = { random = { length = { fixed = 10 } } }

[node_devices.metrics.properties]
propa = { type = "string", fixed = "propa_value" }
propb = { type = "uint64", range = { min = 10, max = 1000 } }

[[node_devices]]
node = "node1"
device = "device1"

[[node_devices.metrics]]
name = "metric2"
is_transient = {}
is_historical = { fixed = false }
value = { type = "int8", range = { min = -100, max = 100 } }

[[node_devices.metrics]]
name = "metric3"
is_transient = {}
is_historical = { fixed = false }
value = { type = "uint8", range = { min = 10, max = 100 } }

[[node_devices.metrics]]
name = "metric4"
is_transient = {}
is_historical = { fixed = false }
value = { type = "int16", range = { min = -100, max = 100 } }

[[node_devices.metrics]]
name = "metric5"
is_transient = {}
is_historical = { fixed = false }
value = { type = "uint16", range = { min = 10, max = 100 } }

[[node_devices.metrics]]
name = "metric6"
is_transient = {}
is_historical = { fixed = false }
value = { type = "int32", range = { min = -10, max = 100 } }

[[node_devices.metrics]]
name = "metric7"
is_transient = {}
is_historical = { fixed = false }
value = { type = "uint32", range = { min = 10, max = 100 } }

[[node_devices.metrics]]
name = "metric8"
is_transient = {}
is_historical = { fixed = false }
value = { type = "int64", range = { min = -10, max = 100 } }

[[node_devices.metrics]]
name = "metric9"
is_transient = {}
is_historical = { fixed = false }
value = { type = "uint64", range = { min = 10, max = 100 } }

[[node_devices.metrics]]
name = "metric10"
is_transient = {}
is_historical = { fixed = false }
value = { type = "string", samples = ["abc", "def"] }

[[node_devices.metrics]]
name = "metric11"
is_transient = {}
is_historical = { fixed = false }
value = { type = "datetime", start_time = 2024-01-01T00:00:00.888Z, interval = "1ms" }

[[node_devices.metrics]]
name = "metric12"
is_transient = {}
is_historical = { fixed = false }
value = { type = "float", range = { min = 10.0, max = 100.0 } }

[[node_devices.metrics]]
name = "metric13"
is_transient = {}
is_historical = { fixed = false }
value = { type = "double", range = { min = 10.0, max = 100.0 } }

```

##### 6.1.1.2 任务配置

由于 spb 的数据结构固定，因此提供一个默认的任务配置
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: CxTabafl8ohxrzxUfmoceY7Andc)

</view>

#### 6.1.2 用例列表

##### 6.1.2.1 正常用例

| 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- |
| 创建任务 | 将上述任务配置导入 explorer，任务正常创建 | 通过 |
| 启动任务 | 点击启动按钮，看任务是否正常启动 | 通过 |
| 任务写入 | 任务启动后，观察到数据库中按每种类型新增了超级表，并有数据不断写入 | 通过 |
| 任务编辑 | 点击编辑按钮，任务正常回显，再次保存后，任务仍可正常写入 | 通过 |
| 任务停止 | 点击任务停止按钮，任务可以快速停止 | 通过 |
| 任务复制 | 点击任务复制按钮，复制任务，需要修改任务名称和客户端 ID | 通过 |
| 删除任务 | 停止任务后，点击删除按钮删除任务 | 通过 |
| 任务指标 | 新增的 spb_fetched_acks, spb_sent_batches, spb_received_messages, spb_received_metrics 四个指标正常显示 | 通过 |

##### 6.1.2.2 异常用例

| 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- |
| 任务运行中，关闭 spb_pub | 数据库不再有消息写入 | 通过 |
| 关闭spb_pub 后，点击编辑任务 | 任务信息无法正常反显，原因是无法获取示例数据，从而无法进行正常解析 | 通过 |
| 关闭spb_pub 后，编辑任务，再打开 spb_pub | 任务在连通 spb_pub 后，正常显示任务配置信息，也可以正常解析 | 通过 |
| 任务运行中，关闭 MQTT broker | 任务报错，重试一段时间后重启任务，以此往复，在 MQTT broker 恢复正常后任务正常运行 | 通过 |
| 任务复制后没有修改 客户端 ID | 日志里任务反复重试，无法进入正常订阅流程 | 通过 |

由于 spb 和 mqtt 数据源使用的同一套代码进行 MQTT 协议连接和订阅，因此连接异常测试例可参考 MQTT 数据源

## 7. 性能测试

无

## 8. 安全测试

无

## 9. 兼容性测试

新增数据源，对旧有数据源没有影响
