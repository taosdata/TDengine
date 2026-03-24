# taosX 测试进度跟踪-v3.3.0.0

以下表格阶段性更新各测试功能的最新状态：

|  | TDengine 社区版：支持 Explorer | TDengine 双活 | Data In: Mysql | Data In: PostgreSQL | TD3->TD3: 支持复合主键和压缩增强 | TD3->TD3: 配置和指标优化 | Transformer: 支持缺省值 | OPC: CSV 文件合法性校验 | Kafka: 支持 SSL & SASL | TDengine 2: Advanced Options | Installer: reduce taosx package size |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Owner | @王旭 | @王旭 | @贾晨阳 | @聂敏慧 | @贾晨阳 @秦冲 | @聂敏慧 | @贾晨阳 | @秦冲 | @聂敏慧 @王旭 | @贾晨阳 | @宋正勤 |
| Jira | [explorer-community](https://jira.taosdata.com:18080/issues/?jql=summary%20~%20%22explorer-community%22%20AND%20reporter%20in%20(xwang%2C%20cqin)) | [active-standby](https://jira.taosdata.com:18080/issues/?jql=summary%20~%20%22active-standby%22) | [datain mysql](https://jira.taosdata.com:18080/issues/?jql=labels%20%3D%20taosX%20AND%20labels%20%3D%20mysql) | [PostgreSQL](https://jira.taosdata.com:18080/issues/?jql=labels%20%3D%20postgresql%20) | [primary key and compression](https://jira.taosdata.com:18080/issues/?jql=labels%20%20%20%3D%20taosX%20and%20labels%20%20%3D%20legacy%20AND%20labels%20%3D%20tmq%20%20) | [TD3-TD3](https://jira.taosdata.com:18080/issues/?jql=text%20~%20%22taosx%20tmq%22%20AND%20(creator%20%3D%20Mia%20or%20Tester%20%3D%20Mia)%2520AND%2520createdDate%2520%253E%253D%25202024-03-27) | [transformer](https://jira.taosdata.com:18080/issues/?jql=labels%20%3D%20transformer%20and%20labels%20%3D%20%22default%22%20) | [OPC 模板校验](https://jira.taosdata.com:18080/issues/?jql=summary%20~%20%22taosx%20OPC%20%E6%A8%A1%E6%9D%BF%E6%A0%A1%E9%AA%8C%22) | [kafka](https://jira.taosdata.com:18080/issues/?jql=text%20~%20%22taosx%20kafka%22%20AND%20%20%22Epic%20Link%22%20%3DTD-28725) | [advanced options for TD2](https://jira.taosdata.com:18080/issues/?jql=labels%20%20%20%3D%20taosX%20and%20labels%20%20%3D%20legacy%20AND%20labels%20%3D%20advanced%20) | TD-29121 |
| 未解决/总数 | 0/15 | 0/10 | 0/10 | 1/11 | 1/6 | 4/13 | 1/2 | 0/18 | 6/9 | 1/2 |  |
| 测试完成ETA | 0426 | 0417 | 0426 | 0426 | 0426 | 0419 | 0412 | 0417 |  | 0412 | 0426 |
| 当前进度 | 100% | 100% | 95% | 90% | 100% | 80% | 100% | 100% |  | 100% | 100% |
| 风险和异常 | 无 | taosx replica 命令0408提测 |  | PG 的支持在 0412 尚未提测 | ~~超级表的压缩参数在tmq中没有同步，还在等待tmq侧的修复~~ | 无 | 无 | 提测时间推迟至0403 | 有风险 这个 feature 是临时加入的，有遗留问题，且还没有充分测试 | 无 |  |
| 0412 |  | 本周主要对 replica 子命令进行了测试，基本测试完成，还需要对 replica 场景下的数据同步功能进行测试 |  |  |  | 1. vgroup消费进度需要重构 1. 遗留bug待修复 | 已完成测试，除json字段问题外没有其他遗留问题 | 编写的测试用例已测试完，还有遗留 bug 待修复。 |  | UI修改放到下版本，本阶段测试完成 |  |
| 0417 |  | taosX replica 子命令的测试已完成，删除子表、超级表数据的操作 (delete from) 未同步，明明在跟进 | 由于前端开发还未完成，今日开始进行后端测试 |  |  | 无更新 |  | 还有数据积压以及性能较低的问题正在处理中。 |  |  | Linux 的测试已完成 |
| 0418 |  |  | ssl_mode = VERIFY_CA、VERIFY_IDENTITY两个访问模式该版本不支持，相关测试不开展 | ssl_mode = VERIFY_CA、VERIFY_IDENTITY两个访问模式该版本不支持，相关测试不开展 |  |  |  | 数据积压的问题在调整了参数之后得到解决。 |  |  |  |
| 0419 |  |  | UI 已经提测 binary类型本次不支持，不开展不测试 已经开始测试，基本测试用例已经跑通 | UI 已提测 基本测试用例可以跑通 问题： tdinsight中是否新增Mysql、postgreSQL，oracle面板还在协调中 |  | 无更新 1. vgroup消费进度需要重构 1. 遗留bug待修复 |  |  |  |  |  |
| 0422 | 0422 提测 但注册页面无法展示 |  |  |  |  |  |  |  |  |  |  |
| 0423 | 0423 中英文环境均可以测试了 新增了 4 个 Jira 系统消息的联系按钮、跳转官网文档的连接，尚未替换为有效的 安装包尚未测试 |  |  |  |  |  |  |  |  |  | Windows的测试已完成 |
| 0424 | 验证并关闭了昨天的 Jira, 新增一个 Jira 目前 UI 上还有两个链接尚未替换为正式的有效链接，将由顾香完成 与肖平沟通了安装包的改动 |  | 按日期分表时，如果某一天的表不存在会报错，随后会panic，目前还没有找到根本原因 还剩宽表列转行 |  | 订阅部分超级表的压缩参数不能同步 还剩备份恢复的版本兼容性验证 |  |  |  |  |  |  |
| 0425 | 在社区版 TDengine 上测试了 Explorer, 发现在配合使用社区版的 TDengine 时，Explorer 无法登录，已解决。 |  | panic问题已确认触发条件： TD-29802 通过agent启动的任务不能手动stop |  |  |  |  |  |  |  |  |
| 0428 | 对安装包进行了测试，新增 Jira 1 个待肖平修复 根据周五的讨论，云服务在 MySQL 中新增了 id_community_user 表，Explorer 的修改尚未完成 |  | 两个遗留问题还未转测，其他已经完成 |  |  |  |  |  |  |  |  |
|  |  |  |  | 剩余SSL模式验证问题： allow和prefer不符合预期 使用test_disable_only, ssl_mode选择prefer，连通性检查不通过。不符合预期 使用test_ssl_only, ssl_mode选择allow，连通性检查不通过。不符合预期 |  |  |  |  |  |  |  |
| 0430 | 待云服务的正式接口上线后，发现中英文的注册信息均保存在了海外环境，原因是由于前端调用时没有传 lang 参数导致的 | 基本功能验证通过，包括：replica 任务的创建、查看状态、停止、删除、数据同步、数据删除等 | 基本功能验证通过， 包括： 任务创建、查看状态、停止、删除、数据任务同步。 授权功能验证通过：包括连接数限制超过以后会有警告提示“超出连接器数量限制”从而无法创建新的任务、连接器授权过期以后也会有警告提示“过期了多少天多少小时”从而无法创建新的任务 | 基本功能验证通过，任务创建，停止。 实时数据同步，历史数据同步，按日期分表的基本用例验证通过。 授权功能验证通过：包括连接数限制超过以后会有警告提示“超出连接器数量限制”从而无法创建新的任务、连接器授权过期以后也会有警告提示“过期了多少天多少小时”从而无法创建新的任务 | 基本功能验证通过：包括 TD3 -> TD3 同步复合主键和压缩参数，备份恢复包含复合主键和压缩参数的表，数据同步复合主键和压缩参数。 | TD3-TD3任务同步基本功能正常。 测试包中查看表同步进度有两个问题： 1. 源与目标时间差 1. 查询按钮有时候没有响应。 这两个问题已经在3.0分支修改，测试包出来后才合并到main分支中。现在已经合并到main分支。 会在正式版本中再验证。 | 缺省值基本功能验证通过：字符串和数值、布尔类型 | 基本功能验证通过：包括：使用下载的模板创建写入任务，只使用必填列创建任务，使用选择数据点位创建任务。 基本的文件校验也已验证通过。 | SASL基本功能验证通过 | 基本功能验证通过，使用默认的 Advanced Options 参数，任务的启动、停止、数据同步正常。 | TD-29892 |
| 0507 | 使用社区版安装包，基本功能验证通过。 | 使用 3.3.0.0 的安装包，基本功能验证通过，包括：replica 任务的创建、查看状态、停止、删除、数据同步、数据删除等 | 使用3.3.0.0安装包，执行功能自动化测试用例通过 | 使用3.3.0.0安装包验证基本功能包括：实时数据同步，历史数据同步，按日期分表 |  | 使用3.3.0.0安装包，TD3 任务基本功能正常 | 使用3.3.0.0安装包，验证了数值和字符串类型的缺省值 | 使用3.3.0.0安装包，执行任务，验证通过 | 使用3.3.0.0安装包，执行任务，kafka基本功能正常 | 使用3.3.0.0安装包，执行任务，验证通过 | 打包环境有问题，最后的大小仍然是没有压缩的，和Linhe沟通过，这次就这样保持大小不变 |
|  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |

## 1. Notes:

### 1.1 20240417

- 大家要在测试报告中记录这个需求开发过程中的异常
- 敏慧负责测试的“Explorer 建表：支持复合主键和压缩增强”推迟到下个版本
- 敏慧负责 PG 连接器的测试，在 0418 以命令行的方式提测
- MySQL 的 UI 部分在 0419 早晨可以完成，在此基础上 PG 的 UI 部分可以很快提测
- Oracle 的支持预计在 0422 前后端一起提测
- 文档的改动需要开发在发版前完成

### 1.2 20240507

老功能的回归：
- InfluxDB: Pass
- OpenTSDB: Pass
