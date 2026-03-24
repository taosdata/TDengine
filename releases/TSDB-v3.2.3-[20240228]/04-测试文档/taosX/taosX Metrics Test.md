# taosX Metrics Test

## 1. Objectives

- 验证taosX Metrics功能

## 2. Revision History

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2023.12.27 | 0.1 | @聂敏慧 | Initial Draft |
|  |  |  |  |


## 3. Scope

- 进程监控指标不在这次测试范围内，在监控 taosx 的测试任务中进行
- 数据源包括：TDengine 2.x 数据源，TDengine 3.x 数据源，使用IPC传输数据的数据源，包括 InfluxDB，OpenTSDB，OPC，PI，MQTT，Kafka，CSV，Historian
- 各类数据源的指标测试在 Explorer 上进行
- 获取某个任务的 metrics 的 REST 接口：GET /tasks/{id}/metrics 

## 4. 测试结论

1. 使用 explorer 数据写入 -> 数据源 -> 查看 可以查看各类数据源（包括TDengine 2.x 数据源，TDengine 3.x 数据源，使用IPC传输数据的数据源）的指标，验证通过
2. 使用 REST 接口： GET /api/x/tasks/{id}/metrics 获取某个任务的metrics，验证通过

## 5. Limitations and Known Issues

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
1. csv/kafka/mqtt 数据源，创建的子表个数没有被统计，值为 0，因为子表是使用自动建表语句
2. kafka 数据源 written_rows 可能不包含部分重复数据
3. OPC 数据源
processed_batches 统计的是处理成功的 batch 数，在停掉 taosAdapter 之后，recieved_batches继续增加，processed_batches 不变。
如果 OPC 任务没有使用 agent，batch 处理失败被直接丢弃，通过 failed_batches + processed_batches 和 recieved_batches 比较来查看taosx的数据积压情况
如果 OPC 任务使用了 agent，batch 处理失败会再重试，这种情况下不能使用这三个指标判断 taosx 数据积压情况
1. OPC 数据源，没有统计 failed_rows / failed_points (因为insert sql 中不知道有多少条记录数，所以没有统计)
2. opentsdb/influxdb/pi 数据源，如果采用 sql 写入 taosd 时，没有统计 failed_rows / failed_points， 如果采用 write_block 接口写入会统计失败的行数和点数。
3. InfluxDB/OpenTSDB 数据源，written_rows 和 written_points 可能统计不准确
连接器是按列读取，一条数据可能会分为多个sql写入。
written_rows 是按照 taosd 返回的影响行数来统计，同一时间戳的数据在不同的 sql 中写入会重复统计。而 written_points = written_rows * 超级表的列数， 因此也会不准确。
1. 功能发布后，如果修改指标名字，会有兼容性问题
2. 1.5.0 遗留问题
- TDengine 2 数据源的任务，使用 realtime 模式，查询间隔是1s， metrics 统计的 written_rows 和实际同步的数据差距大： [TD-28568](https://jira.taosdata.com:18080/browse/TD-28568) [taosX Metrics] legacy任务，written_rows和实际同步的数据差距很大
- kafka/mqtt 数据源任务，使用 agent 的时候，没有统计 failed_sql,failed_rows,failed_points，failed_raw_blocks 指标 ([TD-28407](https://jira.taosdata.com:18080/browse/TD-28407))
- [TD-28368](https://jira.taosdata.com:18080/browse/TD-28368) [taosX Metrics] 将同类型的指标放在一起

## 6. Environment

- OS: Windows, Linux
- Browser: Chrome

## 7. Test Data

N/A

## 8. Test Cases

### 8.1 Functional

#### 8.1.1 功能测试用例：

1. TDengine2.x 数据源的指标（包括通用指标和独有的指标）的测试用例
2. TDengine3.x 数据源的指标（包括通用指标和独有的指标）的测试用例
3. IPC 传输数据的数据源指标（包括通用指标和独有的指标）的测试用例

#### 8.1.2 验证场景：

以上三类数据源的测试用例都包含下列测试场景，验证指标值正确，metrics.json每10秒正确更新，REST 接口返回正确。
1. 任务在 Queue 状态，验证 metrics
- 任务首次运行，期望行为：metrics 为空
- 任务非首次运行，期望行为：可以查看任务的累计指标，所有指标正确，REST 接口返回正确。

1. （Sanity）任务在 running 状态，验证 metrics，和 metrics.json
- 任务首次运行，current等于total
- 任务非首次运行
期望行为：可以查看任务的最近一次执行指标和累计指标，所有指标正确，REST 接口返回正确。

1. 任务在 Stopping 状态，验证 metrics，和 metrics.json
期望行为：统计 metrics 的逻辑和 running 状态一致， 可以查看任务的最近一次执行指标和累计指标，所有指标正确，REST 接口返回正确。

1. 任务（包含 agent ） running->waiting->running ，验证 metrics，和 metrics.json
期望行为：任务最近一次执行指标不重置，继续统计任务的执行时间，可以查看任务的最近一次执行指标和累计指标，所有指标正确，REST 接口返回正确。

1. 任务 running->interrupted->running ，验证 metrics，和 metrics.json
期望行为：任务最近一次执行指标重置，可以查看任务的最近一次执行指标和累计指标，所有指标正确，REST 接口返回正确。

1. 任务在 Stopped/Completed/Failed 状态，验证 metrics，和 metrics.json
- 任务的运行时间<10s
- 任务的运行时间>=10s
期望行为：可以查看任务的最近一次执行指标和累计指标并验证，所有指标都不再变化。所有指标正确，验证metrics.json正确，REST 接口返回正确。

1. 删除任务
期望行为：文件 metrics.json 被清除

1. 重启 taosx 
期望行为：重启 taosx 后，状态为 running 的任务最近一次执行指标重置。可以查看所有任务的最近一次执行指标和累计指标，所有指标正确，REST 接口返回正确

1. taosx 异常退出后恢复
期望行为：重启 taosx 后，状态为 running 的任务最近一次执行指标重置。可以查看所有任务的最近一次执行指标和累计指标，所有指标正确，REST 接口返回正确

1. 修改配置 data_dir 后， 重启 taosx 
期望行为：重启 taosx 后，可以查看 running 状态的任务最近一次执行指标和累计指标,  current等于total。其他状态任务的 metrics 为空，REST 接口返回正确

1. 任务在运行过程中，metrics.json 被删除
期望行为：可以查看任务的最近一次执行指标和累计指标，metrics.json 在10s内重新生成，REST 接口返回正确

1. 任务一次运行完成后，删除 metrics.json ，再运行任务
期望行为：可以查看任务的最近一次执行指标和累计指标，metrics.json 在10s内重新生成，REST 接口返回正确

#### 8.1.3 测试记录：

|  | Description | Expected Results | Use Agent？ | Result |  |  |  |  |  |  |  |  |  | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  |  |  |  | TDengine 2.x 数据源 | TDengine 3.x 数据源 | InfluxDB | OpenTSDB | PI | OPC | MQTT | Kafka | CSV | Historian |  |  |  |
|  | 任务在 Queue 状态(任务首次运行)，验证 metrics | metrics 为空 | N | Pass | Pass | Pass | Pass | Pass | Pass | Pass | Pass | Pass | \ |  |  |  |
|  | 任务在 Queue 状态(任务非首次运行)，验证 metrics | 1. 可以查看任务的累计指标
1. 所有指标正确
2. REST 接口返回正确 | N | Pass | Pass | Pass | Pass | Pass | Pass | Pass | Pass | Pass | \ |  |  |  |
|  | 任务在 running 状态(任务首次运行)，验证 metrics 和 metrics.json | 1. current等于total， 可以查看任务的最近一次执行指标和累计指标
1. 所有指标正确
2. REST 接口返回正确 | Y | N/A | N/A | Pass | Pass |  | Pass | Pending | Pending | N/A | \ | [TD-28174](https://jira.taosdata.com:18080/browse/TD-28174)(Done)
[TD-28218](https://jira.taosdata.com:18080/browse/TD-28218)
[TD-28217](https://jira.taosdata.com:18080/browse/TD-28217)
[TD-28248](https://jira.taosdata.com:18080/browse/TD-28248)
influxdb: points计算包含了标签
opentsdb is blocked by [TD-28214](https://jira.taosdata.com:18080/browse/TD-28214) |  |  |
|  |  |  | N | Pending | Pass | Pass | Pass | N/A | Pass | Pass | Pass | Pass | \ |  |  |  |
|  | 任务在 running 状态(任务非首次运行)，验证 metrics 和 metrics.json | 1. 可以查看任务的最近一次执行指标和累计指标
1. 所有指标正确
2. REST 接口返回正确 | Y | N/A | N/A | Pass | Pass |  | Pass | Pass | Pass | N/A | \ |  |  |  |
|  |  |  | N | Pass | Pass | Pass | Pass | N/A | Pass | Pass | Pass | Pass | \ |  |  |  |
|  | 任务在 Stopping 状态，验证 metrics 和 metrics.json | 1. 统计 metrics 的逻辑和 running 状态一致， 可以查看任务的最近一次执行指标和累计指标
1. 所有指标正确。
2. REST 接口返回正确 | Y | N/A | N/A |  |  |  |  |  |  | N/A |  | related to [TD-27998](https://jira.taosdata.com:18080/browse/TD-27998) |  |  |
|  |  |  | N |  |  |  |  |  |  |  |  |  | Pass |  |  |  |
|  | 任务 running->waiting->running ，验证 metrics 和 metrics.json | 1. 任务最近一次执行指标不重置
1. 可以查看任务的最近一次执行指标和累计指标
2. 所有指标正确
3. REST 接口返回正确 | Y | N/A | N/A | \ | \ | \ | \ | Pass | \ | N/A | \ |  |  |  |
|  | 任务 running->interrupted->running ，验证 metrics 和 metrics.json | 1. 最近一次执行指标重置
1. 可以查看任务的最近一次执行指标和累计指标
2. 所有指标正确
3. REST 接口返回正确 | Y | N/A | N/A | Pass | \ | Pass | \ | Pass | Pass | N/A | \ |  |  |  |
|  |  |  | N | \ | Pass | Pass | Pass | \ | \ | Pass | Pass | \ | \ |  |  |  |
|  | 任务在 Stopped/Completed/Failed 状态，验证 metrics 和 metrics.json：任务的运行时间<10s | 1. 可以查看任务的最近一次执行指标和累计指标
1. 所有指标都不再变化。
2. 所有指标正确。
3. REST 接口返回正确 | Y | N/A | N/A | Pass | \ | \ | \ | Pass | \ | N/A | \ |  |  |  |
|  | 任务在 Stopped/Completed/Failed 状态，验证 metrics 和 metrics.json：任务的运行时间>=10s | 1. 可以查看任务的最近一次执行指标和累计指标
1. 所有指标都不再变化。
2. 所有指标正确。
3. REST 接口返回正确 | N | \ | Pass | Pass | \ | \ | Pass | Pass | \ | Pass | \ |  |  |  |
|  | 删除任务 | 文件 metrics.json 被清除 | N/A | Pass | Pass | Pass | Pass | Pass | Pass | Pass | Pass | Pass | \ |  |  |  |
|  | 重启 taosx | 1. 重启 taosx 后，状态为 running 的任务最近一次执行指标重置。
1. 可以查看所有任务的最近一次执行指标和累计指标
2. 所有指标正确
3. REST 接口返回正确 | Y | N/A | N/A | Pass | \ | \ | Pass | Pass | \ | N/A | \ |  |  |  |
|  |  |  | N | \ | Pass | Pass | \ | \ | Pass | Pass | \ | \ | \ |  |  |  |
|  | taosx 异常退出后恢复 | 1. 重启 taosx 后，状态为 running 的任务最近一次执行指标重置。
1. 可以查看所有任务的最近一次执行指标和累计指标
2. 所有指标正确
3. REST 接口返回正确 | Y | N/A | N/A | \ | \ | \ | Pass | \ | Pass | N/A | \ |  |  |  |
|  |  |  | N | \ | Pass | \ | \ | \ | Pass | \ | Pass | \ | \ |  |  |  |
|  | 修改配置 data_dir 后， 重启 taosx | 1. 重启 taosx 后，可以查看 running 状态的任务最近一次执行指标和累计指标, current等于total。
1. 其他状态任务的 metrics 为空
2. REST 接口返回正确 | N | \ | Pass | \ | \ | \ | \ | Pass | \ | \ | \ | 除running状态的任务，其他状态任务的metrics在查看的时候会一直pending状态，前端显示一直转圈 |  |  |
|  | 任务在运行过程中，metrics.json 被删除 | 1. 可以查看任务的最近一次执行指标和累计指标
1. metrics.json 在10s内重新生成
2. REST 接口返回正确 | N | \ | Pass | \ | \ | \ | Pass | Pass | \ | \ | \ |  |  |  |
|  | 任务一次运行完成后，删除 metrics.json ，再运行任务 | 1. 可以查看任务的最近一次执行指标和累计指标，
1. metrics.json 在10s内重新生成
2. REST 接口返回正确 | N | \ | Pass | \ | \ | \ | Pass | Pass | \ | \ | \ |  |  |  |


部分指标的验证说明：

| 分类 | 名称 | 描述 | 验证 |
| --- | --- | --- | --- |
| read_concurrency | 并发读取数据源的数据 worker 数, 也等于并发写入 TDengine 的 worker 数 | UI 上配置 |
| success_blocks | 本次写入成功的数据块数 |  |
| consumers | TMQ 消费者数 | 目标TDengine vgroup数 |
| messages | 本次运行通过 TMQ 收到的消息总数 | messages_of_meta + message_of_data |
| messages_of_meta | 本次运行通过 TMQ 收到的 Meta 类型的消息总数 | 源创建新的子表 |
| messages_of_data | 本次运行通过 TMQ 收到的 Data 和 MetaData 类型的消息总数 |
| success_blocks | 本次写入成功的数据块数 |
| write_meta_fails | 本次运行写入 meta 失败的次数 | 停止目标taosd/taosadapter |
| received_batches | 本次运行此任务通过 IPC Stream 收到的数据总批数 |
| received_records | 本次运行此任务通过 IPC Stream 收到的数据总行数 |
| processed_batches | 本次运行已处理批数 |
| processed_records | 本次处理的总行数（等于包含数据的 batch 包含的数据行数之和） |
| inserted_sqls | 本次运行此任务执行的 INSERT SQL 总条数 |
| failed_sqls | 本次运行此任务执行失败的 INSERT SQL 总条数 |
| failed_rows | 本次运行此任务写入失败的行数 |
| failed_points | 本次运行此任务写入失败的点数 |
| written_blocks | 本次运行此任务写人成功的 block 数 |
| failed_blocks | 本次运行此任务写入失败的 block 数 |


### 8.2 Usability

测试用例包括但不局限于：
- UI 是否美观，metrics 的展示使用分组折叠
- 交互是否合理
- 字体、字号是否合适
- 是否存在错别字
- 格式化显示时间，浮点数

### 8.3 Reliability

N/A

### 8.4 Performance

N/A

### 8.5 Security

N/A

### 8.6 Compatibility

测试用例包括但不局限于：
- 升级安装后，老版本（上一个版本）下创建的任务，升级后查看Metrics

### 8.7 Localization

测试用例包括但不局限于：
- 点击切换语言按钮后，metrics上的所有元素是否按照选择的语言，正确展示

## 9. Questions

这里用于记录在Review Metting上需要讨论的问题：

## 10. Jira

此feature相关的所有Jira, 标题中包含统一的标签: [taosX Metrics]
<!-- Unsupported block type: 999 -->

## 11. Schedule

这里用于计划此feature测试的开始和结束时间。

## 12. Notes

## 13. Summary

## 14. Reference

[taosX Metrics 说明](https://taosdata.feishu.cn/wiki/I5EawNL4ViT082k5RwTcoIEMnRc)
