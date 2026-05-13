# taosX 断点续传

## 1. 背景

目前 taosX 所支持数据源中，
1. TDengine 3.0 ： 采用数据订阅接口，进度信息由数据源端保存
2. Kafka：采用数据订阅接口，进度信息由数据源端保存
3. OPC UA/DA, MQTT：数据源并不存储数据，获取到的是即时数据，无需进度信息
4. TDengine 2.6：采用查询接口获取，没有进度信息，一旦传输中断只能从头开始
5. InfluxDB：同 TDengine 2.6
6. OpenTSDB：同 TDengine 2.6
7. Pi：Pi 连接器目前采用了另外一种方法，从 Data Sink （即 TDengine 3.0 ）一侧获取最新数据的 timestamp ，由此判定断点，暂时保留这种方案。
为了解决 TDengine 2.6，InfluxDB，OpenTSDB 到 TDengine 3.0 的数据传输在传输因为某种异常中断后能够从断点继续恢复，避免从头传输浪费资源和时间，产生了本文所描述的 taosX 断点续传功能。

## 2. 行为说明

### 2.1 范围

本功能所支持的数据源：TDengine 2.6, InfluxDB, OpenTSDB。
本功能所支持的断点续传机制为通用机制，在本 Feature 完成后新加的所有数据源的断点续传都必须与本Feature 适配来实现相应数据源的断点续传。本功能仅在 taosX 在服务态下运行时具备，但本功能的引入不能对 taosX 的命令行运行模式产生任何影响。

### 2.2 用户的正常操作

1. 创建传输任务：创建传输任务的用户操作不受影响，用户也感知不到任何行为变化
2. 暂停传输任务：对于原来不支持断点续传的三种数据源，是不支持暂停行为的，暂停等同于停止 。在支持断点续传后，可以支持真正的暂停，当任务暂停时，已经入库的数据的断点信息会被记录下来，连接器退出。
3. 恢复传输任务：对于原来不支持断点续传的三种数据源，因为不支持真正的暂停，所以也不支持真正的恢复，恢复等同于重新启动，即重新开始。
4. 删除/取消传输任务：删除传输任务即 将该任务从系统中去除，此行为不变。在本 Feature 完成后，在删除任务时，与该任务有关的断点数据也一并删除。
5. Clone 传输任务：克隆任务是为了快捷地创建新任务，此时内部断点数据不应被复制。用户感知到的行为无变化。

### 2.3 传输任务的异常中断和恢复

本节描述以下几种异常情况导致传输任务中断后的恢复行为，鉴于这些异常的恢复机制有很大共性，故进行统一说明。以下异常情况如无特别说明均指单一异常，即未考虑多种异常同时出现的情况。理论上多种异常有可能同时出现，但其处理机制并无特殊之处，对传输任务的影响由同时发生的多种异常中最严重的一种决定。
1. 与数据源的连接中断 （但连接器进程，Agent，及 taosX 里程均正常）
2. Agent 进程异常中止
3. 连接器进程异常中止
4. taosX 进程异常中止
5. 与 Data Sink （TDengine 3.0）的连接中断
6. 网络中断

当以上任意一种异常发生时，此时任务的传输状态应该由 “taosX 异常处理机制” 这个 Feature 来决定。传输任务必须是某种广义的“中断”的状态，比如 “Interrupted", "Suspended", Failed", 等，具体状态参考 "taosX 异常处理机制”。但在异常解决后，用户能够恢复该任务继续进行（此处仅指人工触发恢复，自动恢复机制不在此列），此时 taosX 应该重新建立与数据源的连接并从断点恢复数据拉取。
taosX 在发生异常后自动恢复传输任务的机制将由独立的 Feature 进行说明。

### 2.4 配置参数

配置参数暂定使用 TAOSX_DATA_DIR，参照：
[[设计文档][TD-26002]重构配置解析和内部存储方式](https://taosdata.feishu.cn/wiki/I6jGwyiW6i27C7kuAZZcXnDOnxb) 
该存储路径的默认值为 {TAOSX_DATA_DIR}/$task_id/breakpoints

## 3. 集成接口

因为 taosX  通过 taos-explorer 对外提供用户界面进行操作，所以 taosX 需要提供相应的接口供 taos-explorer 调用。涉及到断点续传的接口有：
1. 暂停 (Pause)
   - 此处需要定义接口细节，end point, 参数，所有参数的合法值域，以及默认值
   - 此处需要定义在哪些状态下的任务可以暂停，对于非这些状态的任务需要返回明确的错误码和错误信息：Tasks in xxx state can't be paused。
   - Error Code (和 Error Message ）列表 
  暂停接口对于发生错误的情况，没有 Error Code，仅有 Error Message。
  | message | 含义 |
| --- | --- |
| Agent not found | Agent 未找到 |
| Task state not allow stop | Task 不是可停止的状态 |
| trying to stop task via agent:{agent_id} but failed: {err:#} | 其他运行时错误 |

1. 恢复 （Resume）
   - 此处需要定义接口细节，end point, 参数，所有参数的合法值域，以及默认值
   - 此处需要定义在哪些状态下的任务可以暂停，对于非这些状态的任务需要返回明确的错误码和错误信息：Tasks in xxx state can't be paused
   - 断点元数据如果因为某种意外而丢失或损坏，则断点续传功能不可用。此时对于用户的恢复操作， taosX 需要能够返回明确的错误码和错误提示告知该情况。如果任务本身可以恢复，此时的行为是从新开始该任务。如果任务本身不可恢复（即使从零开始也不行），则应有明确的错误码和提示信息。这些错误码和提示信息要明确定义在本文档中。
   - Error Code (和 Error Message ）列表 
  | message | 含义 |
| --- | --- |
| Agent not found | Agent 未找到 |
| Agent not alive | Agent 不在线 |
| Task state not allow start | Task 不是可开始的状态 |
| Task already running | Task 已经在运行 |
| Connector {connector} expired, please contact the database administrator for license | 连接器授权过期 |
| {err:?}. A non-expired enterprise edition is required in most of steps. | TDengine 非企业版或授权过期 |
| Authentication failure | 网关鉴权失败 |
| WebSocket internal error | Websocket 链接错误 |
| run task {id} failed with: {err}, please check the task information | 其他运行时错误 |

1. 其它接口（我未想到）

## 4. 性能影响

每批数据写入成功后都会更新断点元数据，对性能必然会有影响，具体的量化影响需要测试验证。测试基准：100万子表的数据传输。对于 InfluxDB 和 OpenTSDB ，子表的概念不适用，但在各自概念下用同样数量级作测试基准。
此处对实现方案进行提醒 ：对于断点元数据的更新也应尽量聚合，最小化更新次数。（本行最终可删除）
