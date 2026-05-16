# 慢查询日志保存策略优化 Test Spec

## 1. 测试目标

慢查询日志

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.7.31 | 0.1 | 翟坤 | 初版 |
| 2024.8.5 | 0.2 | 翟坤 | 根据最新的设计，更新测试用例 |
| 2024.8.30 | 1.0 | 翟坤 | 更新测试结果 |

## 3. 测试结论

测试通过

## 4. 开发质量报告

结论：本特性/优化的开发质量是 良

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 （测试阻塞，无法进行） | 0 |
| 基础测试用例不通过 | 4（问题不严重且多为配置文件旧机制引起的问题） |
| Bug 总数 | 6 |
| 严重 Bug 总数 | 0 |

## 5. 测试策略

1. 参数配置功能复用之前的旧代码逻辑，发现新问题也会jira记录，但测试通过结论会依据此类问题的严重性做主观判断，若对本次开发功能影响较大，也视为阻塞问题
2. 慢查询同步和异步写对于黑盒测试很难进行场景验证，本次测试除参数验证以外的测试场景都是用默认开启异步日志

## 6. 已知问题和限制

- 
  TD-30783

- 
  TD-31697

- 多节点部署环境，taosc启动在哪个节点，慢查询日志会记录在对应节点机器的的logDir目录

## 7. 测试资源及环境

   测试平台：Linux x64
   测试资源：192.168.0.215

## 8. 测试用例

### 8.1 功能测试

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 参数logDir | logDir默认配置路径为/var/log/taos | 1. taos.cfg不配置logDIr
2.通过命令select * from information_schema.ins_dnode_variables where  name like 'logDir'; 查询logDir的值 | logDir的值为/var/log/taos | Y | Pass |  |
|  | logDir配置路径为非默认参数 | 1. taos.cfg配置为指定路径
2.通过命令select * from information_schema.ins_dnode_variables where name like 'logDir'; 查询logDir的值 | logDir的值为指定路径 | Y | Pass |  |
|  | logDir配置的路径不存在 | taos.cfg配置为不存在的非法路径 | 启动taosd后会自动创建log目录 | Y | Pass |  |
|  | logDir配置为空 | 1. taos.cfg配置文件将logDir配置为空
1. 通过命令将logDir配置为空 | 修改操作失败，返回明确信息提示改参数不允许通过命令进行修改 | Y | Fail | 已知问题：[配置文件行为优化](https://jira.taosdata.com:18080/browse/TD-30783) |
|  | 通过命令修改服务端logDIr | 通过一下命令修改logDir的值
1、alter dnode x "";
2、alter all dnodes ""; | 修改操作失败，返回Invalid config option | Y | Pass |  |
|  | 通过命令修改客户端logDIr | 1. 通过一下命令修改logDir的值
alter local 'logDir /data/taos'; | 修改操作失败，返回Invalid config option | Y | Pass | [通过命令修改客户端logDIr参数，不生效](https://jira.taosdata.com:18080/browse/TD-31772) |
| 参数minimalLogDirGB | minimalLogDirGB默认配置为1G | 1. taos.cfg不配置logDIr
2.通过命令select * from information_schema.ins_dnode_variables where dnode_id=1 and name like 'minimalLogDirGB';查询logDir的值 | logDir的值为1.000000 | Y | Pass |  |
|  | minimalLogDirGB配置为异常值 | minimalLogDirGB配置为异常值：
1、a1
2、0.0009
3、10000001
4、10G
5、-1 | 启动taosd报错 | Y | Pass | [参数minimalLogDirGB配置错误，启动taos或taosd时提示信息不明确，用户无法确认哪个配置错误](https://jira.taosdata.com:18080/browse/TD-31694)

[日志相关参数minimalLogDirGB配置数值超过范围，启动taos或taosd时提示信息中范围提示的数字显示为out of range[0.001000, 10000000.000000]](https://jira.taosdata.com:18080/browse/TD-31695) |
|  | minimalLogDirGB配置为正常值 | minimalLogDirGB配置为正常值：
1、0.001
2、10000000
3、20 | 1.taosd正常启动
2.minimalLogDirGB值正确 | Y | Pass | [日志相关参数minimalLogDirGB配置为0.001，启动taos或taosd时报错](https://jira.taosdata.com:18080/browse/TD-31696) |
|  | 通过命令修改服务端minimalLogDirGB | 通过一下命令修改minimalLogDirGB的值
1、alter dnode x "";
2、alter all dnodes ""; | 修改操作失败，返回Invalid config option | Y | Pass |  |
|  | 通过命令修改客户端minimalLogDirGB | 1. 通过一下命令修改logDir的值
alter local 'minimalLogDirGB 100';
1. 通过show local variables验证minimalLogDirGB的值 | 1. 修改操作成功，查询参数值修改正确
2. minimalLogDirGB新值生效 | Y | Pass |  |
| 参数asyncLog | asyncLog默认配置为1 | 1. taos.cfg不配置asyncLog
2.通过命令select * from ins_dnode_variables where dnode_id=1查询logDir的值 | asyncLog的值为1 | Y | Pass |  |
|  | 配置asyncLog为非true和1为关闭异步 | 配置asyncLog为：
1、0.1
2、falsee
3、trrue
4、-1
5、false
6、0 | asyncLog的值为0 | Y | Pass |  |
|  | 配置asyncLog为true和1为开启异步 | 配置asyncLog为：
1、1
2、true | asyncLog的值为1 | Y | Pass |  |
|  | 配置asyncLog的值超出范围[0, 1] | 配置asyncLog为：
1、2
2、-1
3、1.1
4、-0.5 | 报错Out of range | Y | Pass | 遗留问题：
[日志相关参数asyncLog配置超出范围[0, 1]，未报错](https://jira.taosdata.com:18080/browse/TD-31697) |
|  | 通过命令修改服务端asyncLog | 通过一下命令修改asyncLog的值
1、alter dnode x "";
2、alter all dnodes ""; | 操作成功，查询当前为修改后配置 | Y | Pass |  |
|  | 通过命令修改客户端asyncLog | 1. 通过一下命令修改logDir的值
alter local 'asyncLog 1';
1. 通过show local variables验证asyncLog的值 | 1. 修改操作成功，查询参数值修改正确
2. asyncLog新值生效 | Y | Pass |  |
| 参数numOfLogLines | numOfLogLines对慢查询日志不生效 | 1.配置numOfLogLines=1000
2.slowLogThresholdTest 0
3.slowLogExceptDb  log
4.通过taoBenchmark执行1000+次查询 | 慢查询日志中记录条数大于1000 | Y | Pass |  |
| 参数logKeepDays | logKeepDays对慢查询日志不生效 | 1.配置logKeepDays=1
2.生成慢查询日志文件
3.修改系统时间为后天，再次触发慢查询 | 已经超过logKeepDays的慢查询日志文件不会被删除 | Y | Pass |  |
| 慢查询日志内容格式验证 | 包括客户端PID、连接ID、查询ID、SQL语句、执行时间、起始时间 | 1.触发慢查询
2.查看慢查询日志文件 | 内容包含：客户端PID、链接ID、查询ID、SQL语句、执行时间、起始时间 | Y | Pass | 内容格式：
08/28 13:54:21.945934 02480755 E PID:2480712, Conn:4170314202, qid:0xb8ca488c96b8000a, Start:1724824458936576 us, Duration:3009328us, SQL:select my_sleep(n) from t1 where n=3; |
| 慢查询写入策略测试 | 删除日志文件 | 1.通过脚本持续触发慢查询（slowLogThreshold=1）
2.手动删除慢查询日志文件
3.再次执行慢查询sql | 1.日志文件被删除后不会自动重新创建
2.再次执行慢查询，会重新创建慢查询日志文件 | N | Pass |  |
|  | 删除日志所在目录目录 | 1.通过脚本持续触发慢查询（slowLogThresholdTest=0）
2.手动删除慢查询日志所在目录 | 1.taosd运行正常
2.taosc或taosd日志中出现慢查询日志写入错误的error信息 | N |  |  |
|  | 多客户端同时触发慢查询 | 1.配置slowLogThresholdTest=1
2.通过多个客户端同时执行超过1s的查询1分钟 | 统计各个客户端执行慢查询的数量，其与慢查询taos_slow_sql_detail中数据条数一致 | N | Pass | 多节点部署环境，taosc启动在哪个节点，默认慢查询日志会记录在那节点所在机器的的logDir目录 |
|  | taosd启动后，minimalLogDirGB配置超过日志文件夹所在磁盘可用空间大小，停止写日志 | 1.配置minimalLogDirGB小于磁盘可用空间大小
2.触发慢查询，日志记录正确
3.通过dd命令将磁盘可用空间占降低到低于minimalLogDirGB
4.再次触发慢查询，不会记录日志
5.恢复可用空间高于minimalLogDirGB
6.再次触发慢查询，会记录日志 | minimalLogDirGB配置超过日志文件夹所在磁盘可用空间大小，停止写日志 | N | Pass |  |
|  | taosd启动前，minimalLogDirGB配置超过日志文件夹所在磁盘可用空间大小 | 1.配置minimalLogDirGB大于磁盘可用空间大小
2.启动taosd服务 | 启动taosd成功，但慢查询日志不会被记录 | N | Pass |  |
|  | 每天生成一个慢查询日志文件，慢日志文件不自动删除，不进行压缩 | 1.触发慢查询生成一个日志文件
2.修改系统时间为明天日期再次触发慢查询生成一个新的日志文件
3.修改系统时间为后天日期再次触发慢查询生成一个新的日志文件 | 1.系统会生成3个慢查询日志文件，且文件名称时间对应日期正确
2.历史日志文件不会被压缩 | N | Pass |  |
|  | 重启taosd清除缓存后继续写慢查询日志 | 1.执行sql生成慢查询日志文件
2.重启taosd服务，清除缓存
3.重新执行sql触发生成慢查询 | 慢查询日志中正确记录该条数据 | N | Pass |  |

### 8.2 稳定性测试

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 稳定性测试 | 验证多客户端长时间持续写入慢查询日志的稳定性和数据正确性 | 1. 配置slowLogThreshold=1
1. 启动3个脚本（模拟3个客户端），通过rest api方式循环执行慢查询sql，执行过程记录各自执行成功的慢查询条数。该过程持续4小时 | 1.执行慢查询期间taosd运行正常，不会因慢查询日志记录而taosd运行异常
2.统计慢查询日志中数据条数，应该等于各自客户端执行的慢查询条数
3.查询慢查询表taos_slow_sql_detail，其数据数量应该等于3个客户端执行的慢查询条数之和 | N | Pass | 总计写入了500W条慢查询记录，数据库运行正常，数据记录条数正确，内容抽查正确 |

## 9. 引用文档

### 9.1 需求文档

TS-3718

### 9.2 设计文档

[【优化】慢查询日志文件的保存策略需和普通日志保持一致](https://taosdata.feishu.cn/wiki/AJj1w4muri7H8Fk7kRAcDgSGn6b)

### 9.3 其他文档

[慢查询日志 Test Spec](https://taosdata.feishu.cn/wiki/PGRDwAcdqiujl8kIhAac5nBmnmd)
