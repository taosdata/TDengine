# Advanced Options Test Design Spec

## 1. Objectives

- 验证所有数据源的 Advanced Options 可按统一的方式配置
- 验证任务提交后，Advanced Options 中的配置可以生效

## 2. Revision History

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2023.12.22 | 0.1 | @王旭 | Initial Draft |
| 2024.01.04 | 0.2 | 测试 2 组 | 补充了不同数据源的测试用例 |

## 3. Scope

- taosX/taosExplorer 支持的所有内、外部数据源
- 测试均在 Explorer 上进行，不涉及命令行模式

## 4. 测试结论

在以前的版本中，功能相似的配置，在不同类型数据源的配置中，名称和位置都一些差异，对用户可能会造成一些困惑，影响用户体验。在这个版本中，将以下 4 种类型的配置，统一到了任务配置页面的 Advanced Options 类别下，包括：log level, read/write concurrency, batch size/timeout, save raw data. 以上配置并不是每个数据源都全部支持，例如，对于 InfluxDB 就不支持 save raw data, 详见 functional spec 中的 4.3 节。
我们对每一种数据源所支持的所有配置，均进行了测试，包括功能的正确性，默认值的合理性等，对于保存原始数据这个功能，还测试了一些异常场景，例如：磁盘写满，配置的路径没有写权限等，测试结果符合预期，提交的 bug 已全部修复，测试通过。

## 5. Limitations and Known Issues

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.

## 6. Environment

- OS: Windows, Linux, macOS
- Browser: Chrome

## 7. Test Data

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：
- field的数量、类型
- tag的数量、类型
- 数据量的大小

## 8. Test Cases

### 8.1 Functional

在提测时，开发应保证 basic 类型的用例全部通过。

根据参数自上而下传递的过程，验证点如下：
- 前端提交任务时，传递的参数是否正确？
- 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数是否正确？
- 最终生效的参数是否正确？

#### 8.1.1 ver1 （已废弃）

| Data Source Type | Test Data | Type | Use Agent ? | Description | Expected Results | Metrics Screenshot (metrics截屏图片) | Result | Automated | Jira | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| TDengine |  |  |  |  |  |  |  |  |  |  |
| PI |  |  |  |  |  |  |  |  |  |  |
| OPC |  |  |  |  |  |  |  |  |  |  |
| Influxdb | Address: 
OrgID:
Bucket:
BeginTime:
EndTime:

(使用同一数据源同一Bucket完成所有测试) | sanity | Yes | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录使用默认值完成同一数据源同步以后，所需要的时间，最好将metrics截屏保存，方便对比 |  |  |  |  | default value |
|  |  |  | No | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录使用默认值完成同一数据源同步以后，所需要的时间，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  | Special (边界值) | Yes | read_concurrency使用最小值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要大，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | write_concurrency使用最小值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要大，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | read_concurrency使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要小，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | write_concurrency使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要小，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_size使用最小值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要大，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_size使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要小，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_timeout使用最小值，其余使用默认值
（目前最小值就是默认值，所以可以不用再测） | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该小于等于全都使用默认值，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_timeout使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该大于等于全都使用默认值，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  | No | read_concurrency使用最小值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要大，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | write_concurrency使用最小值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要大，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | read_concurrency使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要小，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | write_concurrency使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要小，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_size使用最小值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要大，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_size使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该比全都使用默认值的要小，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_timeout使用最小值，其余使用默认值
（目前最小值就是默认值，所以可以不用再测） | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该小于等于全都使用默认值，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  |  |  | batch_timeout使用最大值，其余使用默认值 | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认）
2. 记录完成同一数据源同步以后，所需要的时间，并且该时间应该大于等于全都使用默认值，最好将metrics截屏保存，方便对比 |  |  |  |  |  |
|  |  | log_level 覆盖 | Yes | log_level分别设置error/debug 
（Info已经在所有值都使用默认值被覆盖） | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认） |  |  |  |  |  |
|  |  |  | No | log_level分别设置warn/trace 
（Info已经在所有值都使用默认值被覆盖） | 1. 任务能够成功提交，并成功完成，目标DB中的记录数正确
1. 最好能验证配置的值已生效，通过Log查看（这个待确认） |  |  |  |  |  |
| OpenTSDB |  |  |  |  |  |  |  |  |  |  |
| MQTT |  |  |  |  |  |  |  |  |  |  |
| Kafka |  |  |  |  |  |  |  |  |  |  |
| CSV |  |  |  |  |  |  |  |  |  |  |
| Historian |  |  |  |  |  |  |  |  |  |  |


#### 8.1.2 ver2

注：所有数据源的测试均在一个大表中，可以通过 Data Source Type 字段过滤感兴趣的数据源的用例。
| Data Source Type | Type | Use Agent ? | Description | Expected Results | Result | Automated | Jira | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PI | basic | Yes | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  | batch | Yes | batch_size 使用最小值, batch_timeout 使用最大值,
log_level 设置为 debug | 1. 实际的 batch 大小由 batch_size 决定
1. 日志中包含有 debug 级别的日志 | Pass |  |  |  |
| Influxdb | basic | Yes | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  | Special (边界值) | Yes | 所有配置使用最小值，log_level 设置为 error | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  |  | read_concurrency使用最大值，write_concurrency使用最小值，
log_level 设置为 debug | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | 所有配置使用最大值，log_level 设置为 warn | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  |  | batch_size使用最大值, batch_timeout使用最小值,
log_level 设置为 trace | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
| Kafka | basic | Yes | read_concurrency 使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | read_concurrency 使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  |  | Yes | read_concurrency 设置为2，partions>read_concurrency | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数为read_concurrency | Pass |  |  |  |
|  |  | No | read_concurrency 使用最大值1000，高于partition的数量 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数为partitions的数量 | Pass |  |  |  |
| OpenTSDB | basic | Yes | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  | Special (边界值) | Yes | 所有配置使用最小值，log_level 设置为 error | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  |  | read_concurrency使用最大值，write_concurrency使用最小值，
log_level 设置为 debug | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | 所有配置使用最大值，log_level 设置为 warn | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  |  | batch_size使用最大值, batch_timeout使用最小值,
log_level 设置为 trace | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
| MQTT | basic | Yes | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  | 原始数据保存 | Yes | 原始数据保存打开
使用默认保存天数和目录 | 1. 在默认路径下有数据保存文件
1. 过期后文件被清理 | Pass |  | 和OPC连接器一样，存在同样的问题：[TD-28596](https://jira.taosdata.com:18080/browse/TD-28596) |  |
|  |  | Yes | 设置不存在的路径为文件保存路径 | 1. 数据同步正常
1. 目录自动创建 | Pass |  |  |  |
|  |  | Yes | 权限测试：将文件保存路径设置在一个只读的目录下 | 1. 数据同步正常
1. 日志中报错 | Pass |  |  |  |
|  |  | Yes | 磁盘满测试：将文件保存路径设置在一个较小的分区，运行一段时间将这个分区写满 | 1. 数据同步正常
1. 日志中有提示信息：原始数据无法正常写入 |  |  |  |  |
|  | Special (Log_Level覆盖) | Yes | log_level 设置为 error | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  |  | log_level 设置为 debug | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | log_level 设置为 warn | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  |  | log_level 设置为 trace | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
| OPC | basic | Yes | 所有配置使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  | 连接器目前直接使用的配置文件还是在 /tmp/ 文件目录下，files 下的配置文件只做备份用。 |
|  |  | No | 所有配置使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  | concurrency | Yes | Write Concurrency配置为最大值1000
log_level是指为debug | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  | OPC 连接器目前仅支持 info/debug 两种日志级别。小于等于 debug 启用 debug，其他日志级别都是 info。
[https://jira.taosdata.com:18080/browse/TD-28636](https://jira.taosdata.com:18080/browse/TD-28636)

OPC DA 目前配置 concurrent 为最大的时候数据无法正常采集上传。
[https://jira.taosdata.com:18080/browse/TD-28636](https://jira.taosdata.com:18080/browse/TD-28636) |
|  |  | Yes | Write Concurrency配置为最小值1
log_level是指为warn | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  | batch | Yes | batch_size设置为最大值10000
batch_timeout设置为最小值1
log_level是设置为trace | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  |  | Yes | batch_size设置为最小值1
batch_timeout设置为最大值60
log_level是指为debug | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 连接器使用的配置文件(/usr/local/taosx/files/<task_id>/)中，参数正确
2. 最终生效的参数正确 | Pass |  |  |  |
|  | 原始数据保存 | Yes | 原始数据保存打开
使用默认保存天数和目录 | 1. 在默认路径下有数据保存文件
1. 过期后文件被清理 | Pass |  |  |  |
|  |  | Yes | 设置不存在的路径为文件保存路径 | 1. 数据同步正常
1. 目录自动创建 | Pass |  |  | [opc 的原始数据默认使用的保存目录与描述不符 https://jira.taosdata.com:18080/browse/TD-28596](https://jira.taosdata.com:18080/browse/TD-28596) |
|  |  | Yes | 权限测试：将文件保存路径设置在一个只读的目录下 | 1. 数据同步正常
1. 日志中报错 | Pass |  |  | 由于目前使用 taosx 的时候都是 root 用户，即使当前目录是只读权限，启动的 opc 连接器依旧可以在该目录创建文件并执行写入操作。不过使用 /snap/ 下的目录就不存在该问题，该目录不能新创建文件。 |
|  |  | Yes | 磁盘满测试：将文件保存路径设置在一个较小的分区，运行一段时间将这个分区写满 | 1. 数据同步正常
1. 日志中有提示信息：原始数据无法正常写入 | Pass |  |  |  |
| Historian | basic | Yes | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  | concurrency | Yes | 设置read_concurrency/write_concurrency为最小值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  |  | Yes | 设置read_concurrency/write_concurrency为最大值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  |  | Yes | 设置read_concurrency/write_concurrency为 CPU 核数 * 2 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  | 原始数据保存 | Yes | 验证原始数据可以正常保存
打开开关
设置保存天数为1天
使用默认保存路径 | 1. 在默认路径下可以查看到保存的数据文件
1. 过期后可删除 |  |  |  |  |
|  |  | Yes | 设置不存在的路径为文件保存路径 | 1. 数据同步正常
1. 目录自动创建 | Pass |  |  |  |
|  |  | Yes | 权限测试：将文件保存路径设置在一个只读的目录下 | 1. 数据同步正常
1. 日志中报错 | Pass |  |  |  |
|  |  | Yes | 磁盘满测试：将文件保存路径设置在一个较小的分区，运行一段时间将这个分区写满 | 1. 数据同步正常
1. 原始数据无法正常写入 |  |  |  |  |
| CSV | basic | No | 所有配置值使用默认值 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  | read_concurrency | No | read_concurrency设置最大值1000 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | read_concurrency设置最小值1 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  | batch_size | No | batch_size设置最大值100000 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |
|  |  | No | batch_size设置最大值1 | 1. 任务能够成功提交，并成功完成，前端提交任务时，传递的参数正确
1. 最终生效的参数正确 | Pass |  |  |  |

### 8.2 Usability

测试用例包括但不局限于：
- UI是否美观？
- 交互是否合理？
- 字体、字号是否合适？
- 是否存在错别字？

### 8.3 Reliability

n/a

### 8.4 Performance

n/a

### 8.5 Security

测试用例包括但不局限于：
- 日志中是否包含敏感信息？

### 8.6 Compatibility

测试用例包括但不局限于：
- 升级安装后，老版本（上一个版本）下创建的任务，能否继续执行？

### 8.7 Localization

测试用例包括但不局限于：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示？

## 9. Questions

这里用于记录需要讨论的问题：
- 如何验证配置是否生效？例如：batchSize 设置为 1000 后，测试过程中，如何验证 batchSize 确实为 1000？ 
            **Influxdb目前没法验证**
- 以 InfluxDB 数据源为例，read_concurrency/write_concurrency 的具体含义？如何观测(netstat)？  
         **Influxdb可以不用netstat**
  - Q: read_concurrency 是 InfluxDB 连接器与 InfluxDB 数据源之间，建立的连接个数？
  **A: 最大读取并发数最终的值：  取measurement个数 和 设置的最大读取并发数   两者的最小值，并且还要结合  每次读取的时间范围(readWindow)**
  比如如果最小值是5， 在influxdb.log中就会有  pool-2-thread-[1-5]
  - Q:   write_concurrency 是 InfluxDB 连接器与 agent/taosX 之间，建立的连接个数？    
  **A:  最大写入并发数 最终的值：  取measurement个数 和 设置的最大写入并发数   两者的最小值**

## 10. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: advanced:

## 11. Schedule

这里用于计划此feature测试的开始和结束时间。

## 12. Notes

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 13. Summary

## 14. Reference

[数据源统一参数 Advanced Options](https://taosdata.feishu.cn/wiki/M7DtwijKMinpPIkVxX0cYAE9n2K)
