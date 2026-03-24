# 统一应用组件日志 - Test Spec

## 1. 测试目标

确认本次改动符合[统一应用组件日志](https://taosdata.feishu.cn/wiki/L78swkAJniw1w3ksDwvcR6eUn2g)

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-08-22 | v0.1 | 智勇 |  |
| 2014-09-26 | v0.2 | 智勇 |  |

## 3. 测试范围

taosAdapter、taosX、taosxAgent、Explorer、taosKeeper 的日志改造

## 4. 测试结论

taosAdapter、taosX、taosxAgent、Explorer、taosKeeper 支持对日志进行设置，并且可以通过 QID 进行跨组件请求链路追踪。

## 5. 开发质量报告

结论：本特性/优化的开发质量是一般

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 1 |
| Bug 总数 | 23 |
| 严重 Bug 总数 | 1 |

## 6. 已知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- taoskeeper 和 taosx-agent 的默认 instanceId 重复了，都是 64
- 
  TD-32328

- 
  TD-32319

## 7. 测试环境

- OS: Windows, Linux, macOS
- Browser: Chrome

## 8. 测试数据 (Optional)

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：
- field的数量、类型
- tag的数量、类型
- 数据量的大小

## 9. 测试用例

### 9.1 功能

#### 9.1.1 公共部分

| 分类 | 测试步骤 | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 日志配置文件 | 日志配置文件格式 | toml |  |  |
|  | 日志配置文件内容 | 示例： instanceId = 31 [log] path = "/var/log/taos" level = "WARN" compress = false rotationCount = 30 rotationSize = "1GB" reservedDiskSize = "2GB" |  |  |
| 日志文件 | 检查日志名称 | 第一个日志为component_instanceid_YYYYMMDD.log 模块名称、instanceid、日期均正确 | Explorer 每次启动生成一个文件 Adapter 仍然在当前文件里 |  |
|  | 检查日志生成时间 | 本地时间 0 时 |  |  |
|  | 检查切割后的日志名称 | component_instanceid_YYYYMMDD.log.index 模块名称、instanceid、日期、index均正确 |  |  |
|  | 配置instanceId 为空，检查日志名称 | component_YYYYMMDD.log？ |  |  |
|  | 启动两个应用，instanceId、log.path配置都一样 | 同时往一个文件里写日志？ 部分组件会有 QID 重复的风险 | 指定配置文件参数不一样 Explorer -C Adapter -c |  |
|  | OEM | 日志名称做相应替换 component.replace('taos', CUS_PROPMT) |  |  |
|  |  |  |  |  |
| 日志级别 | 检查日志级别默认值 | "INFO" 日志中包括的级别：ERROR、WARN、INFO 均为大写 |  |  |
|  | 调整日志级别为 "ERROR" 后启动 | 日志中包括的级别：ERROR 均为大写 |  |  |
|  | 调整日志级别为 "WARN" 后启动 | 日志中包括的级别：ERROR、WARN 均为大写 |  |  |
|  | 调整日志级别为 "INFO" 后启动 | 日志中包括的级别：ERROR、WARN、INFO 均为大写 |  |  |
|  | 调整日志级别为 "DEBUG" 后启动 | 日志中包括的级别：ERROR、WARN、INFO、DEBUG 均为大写 |  |  |
|  | 调整日志级别为 "TRACE" 后启动 | 日志中包括的级别：ERROR、WARN、INFO、DEBUG、TRACE 均为大写 |  |  |
|  | ~~运行时调整日志级别，至少验证一个升级场景、一个降级场景~~ | ~~参考上面的验证方式~~ |  |  |
|  | 调整日志级别为 "DEBUG1111" 后启动 | 配置文件解析失败，无法启动 explorer 启动失败 |  |  |
|  | 调整日志级别为 "debug" 后启动 （小写） | 是否识别？ |  |  |
|  | 调整日志级别为 'DEBUG' 后启动 （单引号） | 是否识别？ |  |  |
|  | 调整日志级别为 DEBUG 后启动 （不带引号） | 是否识别？ explorer 启动失败 |  |  |
|  |  |  |  |  |
| 日志目录 | 检查默认日志目录 | "/var/log/taos" 日志确实生成在该目录 |  |  |
|  | 修改为存在的目录后启动 | 日志生成在指定目录 |  |  |
|  | 修改为不存在的目录后启动 | 创建目录后，日志生成在指定目录 |  |  |
|  | 修改为没有权限的目录后启动 | 启动报错？ | Explorer Permission denied Adapter 启动成功 |  |
|  | 修改为 var/log/taos1 启动（不带引号） | 是否识别？ |  |  |
|  | 修改为本来就存在就日志文件的目录后启动 | 日志在原有基础上继续生成 |  |  |
|  | 验证 Windows 平台 |  |  |  |
|  | 验证 OEM 版本 | "/var/log/CUS_PROMT" |  |  |
|  |  |  |  |  |
| 日志滚动 | 检查 rotationCount 默认值 | 30 可以同时保持 30 个日志，超过时依次删除最早的 |  |  |
|  | 检查 0 点时是否生成新的日志 date -s "2024-09-10 23:59:30" | 生成且内容正确 |  |  |
|  | 修改 rotationCount = 2 后启动 | 生成第三个日志的时候，将最老的日志删除 |  |  |
|  | 修改 rotationCount = “2” 后启动 | 是否识别？ | explorer 启动失败 Adapter 正常处理 |  |
|  | 修改 rotationCount = 65535 后启动，配合修改rotationSize、level 使日志快速切割 | 可以同时保存65535个日志 |  |  |
|  | ~~修改 rotationCount = 0 后启动，配合修改rotationSize、level 使日志快速切割~~ | ~~可以同时保存 超过65535个日志~~ |  |  |
|  | 修改 rotationCount = -1 后启动 | ？ | explorer 启动失败 Adapter 正常启动 保存几十个文件 |  |
|  | 修改 rotationCount = 65536 后启动 | ？ | Explorer 正常启动 Adapter 正常启动 |  |
|  | 同时启动两个 taosx，日志目录配置一样 一个 instanceId=1 rotationCount=2 一个 instanceId=2 rotationCount=3 | taosx_1xxx 文件最多保存 2 个 taosx_2xxx 文件最多保存 3 个 |  |  |
|  | 同时启动两个 taosx，日志目录配置一样 一个 instanceId=1 rotationCount=2 一个 instanceId=1 rotationCount=3 | 是按照封顶 2 个还是 3 个删除？ |  |  |
|  |  |  |  |  |
| 保存天数 | 检查 keepDays 默认值 | 30 |  |  |
|  | 修改 keepDays = 2 后启动 | 只保存两天的日志，删除再早的日志 | Explorer 按照文件名字时间删除 Taosadapter 按照文件创建时间删除 |  |
|  | 修改 keepDays = 0 后启动 | 不限制日志保存天数 |  |  |
|  |  |  |  |  |
| 日志限制 | 检查 rotationSize 默认值 | "1GB" 日志文件每 1G 切割一次 |  |  |
|  | 修改 rotationSize = "1MB" 后启动 | 日志文件每 1M 切割一次 |  |  |
|  | 修改 rotationSize = "100KB" 后启动 | 日志文件每 100K 切割一次 |  |  |
|  | 修改 rotationSize = "100kb" 后启动 | 正常，日志文件每 100K 切割一次 |  |  |
|  | 修改 rotationSize = "100K" 后启动 | 单位不正确怎么办？ | explorer 启动失败 Adapter 启动成功 |  |
|  | 修改 rotationSize = "1M" 后启动 | ？ explorer 启动失败 |  |  |
|  | 修改 rotationSize = 1MB 后启动 （不带引号） | 是否识别？ | explorer 启动失败 Adapter 启动成功 |  |
|  | 检查 reservedDiskSize 默认值 | "2GB" 从日志盘空间降到 2G 后，日志级别降级到 ERROR |  |  |
|  | 修改 lelvel = "TRACE"，reservedDiskSize = 当前日志盘硬盘余量 后启动 | 日志级别直接降级到 ERROR，并有相应的降级日志 explorer =======level downgrade===== =======level upgrade===== |  |  |
|  | 修改 lelvel = ERROR，reservedDiskSize = 当前日志盘硬盘余量 后启动 | 日志降级 ERROR -> ERROR，并有相应的降级日志 |  |  |
|  | 修改 lelvel = "DEBUG"，reservedDiskSize = "500MB" 后启动 | 日志盘空间降到 500MB 后，日志级别直接降级到 ERROR |  |  |
|  | 修改 lelvel = "WARN"，reservedDiskSize = "500KB" 后启动 | 日志盘空间降到 500KB 后，日志级别直接降级到 ERROR |  |  |
|  | 修改 reservedDiskSize = "500M" 后启动 | 单位不正确怎么办？ explorer 启动失败 |  |  |
|  | 修改 reservedDiskSize = "500.5MB" 后启动 | explorer 启动失败 |  |  |
|  | 修改为单引号 | 启动成功 |  |  |
|  | 修改为不带引号 | 是否识别 explorer 启动失败 |  |  |
|  | 降级后等待完全停止写入 | 停止日志写入 停止前有明确的告警日志 |  |  |
|  | 日志在数据盘，构造系统盘容量缩小到 reservedDiskSize ，校验程序是否判断的是日志所在盘 | 日志写入不降级 |  |  |
|  |  |  |  |  |
| 日志压缩 | 检查 compress 默认值 | false 切割的日志不进行压缩 |  |  |
|  | ~~修改 compress = 0 后启动~~ | ~~切割的日志不进行压缩~~ |  |  |
|  | ~~修改 compress = 1 后启动~~ | ~~切割的日志进行压缩，压缩名为原文件名.gz~~ | ~~Adapter 启动成功，但不压缩~~ |  |
|  | 修改 compress = true 后启动 | 切割的日志进行压缩，压缩名为原文件名.gz |  |  |
|  | 解压压缩文件后检查内容 | 日志内容正确 |  |  |
|  | 解压因大小超标切割 的日志文件 | 解压后大小为 rotationSize |  |  |
|  | 检查压缩比 | 有明显的压缩 |  |  |
|  | 修改 compress = True 后启动 | 取值范围不正确？ | explorer 启动失败 |  |
|  | 修改 compress = False1 后启动 | ？ explorer 启动失败 |  |  |
|  | 修改为带引号 "true" | ? explorer 启动失败 |  |  |
|  | 修改 compress = true、rotationCount = 2 后启动 | 当有第二个日志压缩文件生成时，将最早的文件删除 |  |  |
|  | 修改 rotationSize = 10GB、compress = true、rotationCount = 2 后启动 | 主要验证能正确处理切割压缩大文件和删除多余文件 |  |  |
|  | 当已经有 5 个未压缩日志时，修改 compress = true、rotationCount = 3 后启动 | 启动后保留最新的3个日志文件， 且都进行压缩，其他的都删除 |  |  |
|  |  |  |  |  |
| 日志内容 | 检查每行日志的内容项 | 包括时间戳、进程或线程 ID、日志级别、QID、日志消息体、文件及行号 |  |  |
|  | 检查时间戳格式 | 精确到微秒，不含年份，格式统一为 mm/dd HH:MM:SS.000000 内容正确 |  |  |
|  | 检查进程或线程 ID | 进程或线程 ID正确记录 |  |  |
|  | 检查日志级别 | 日志级别正确记录 INFO、WARN 级别需要多一个空格，其他级别均为 5 个字符 |  |  |
|  | 检查 QID | `QID:0x1234567890abcdef` |  |  |
|  | 检查日志消息体 | key:value 形式，使用冒号分隔 key:value，使用逗号空格分割 key:value 对，逗号前无空格逗号后一个空格，消息体在 key:value 对之后显示 |  |  |
|  | 检查文件及行号 | 哪些模块有，哪些模块没有？ |  |  |
|  |  |  |  |  |
| QID | instanceId=1 | 正确生成 QID 0x01{14}d |  |  |
|  | instanceId=255 | 正确生成 QID 0xff{14}d |  |  |
|  | instanceId=256 | ? | explorer 启动失败 Adapter 启动失败 |  |
|  | instanceId=0 | ? | explorer 启动成功 Adapter 启动成功 |  |
|  | instanceId= | ? | explorer 启动失败 Adapter 启动失败 |  |
|  |  |  |  |  |
| 启动日志 | 检查启动日志是否有新日志 flag | 有 # New log file |  |  |
|  | 检查启动日志是否有 编译信息 | 正确记录 |  |  |
|  | 检查启动日志是否有 Commit ID | 正确记录 |  |  |
|  | 检查启动日志是否有 版本号 | 正确记录 |  |  |
|  | 检查启动日志是否有 配置信息 | 正确记录 |  |  |
|  |  |  |  |  |

#### 9.1.2 各个模块

| 分类 | 测试步骤 | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| taosexplorer | 检查 instanceId 默认值 | 1 |  |  |
|  | 在数据写入页面刷新数据源任务列表，检查 QID | 正确记录，包括/x/tasks、/x/tasks/2/activities、QID:` 0x{explorer组件ID}{01}XXX` |  |  |
|  | 在浏览器页面刷新数据库列表，检查 QID | 正确记录，包括 sql 内容、QID: `0x{explorer组件ID}{02}XXX` |  |  |
|  | 访问 taosc 接口？ |  |  |  |
|  | 检查 QID 2 ~ 6位 | 从零开始递增 |  |  |
|  | 重启 explorer 检查QID 2 ~ 6位 | 从零开始递增 |  |  |
|  | cluster = "http://localhost:6041"，执行 sql，检查日志 | 正确记录操作，包括QID |  |  |
|  | cluster_native = "taos://localhost:6030"，执行 sql，检查日志 | 正确记录操作，包括QID |  |  |
|  | 执行开源版注册操作 | 正确记录操作，包括QID |  |  |
|  | 云服务页面 | 正确记录操作，包括QID |  |  |
|  | 其他 case 参考 [公共部分](https://taosdata.feishu.cn/wiki/FUvpwkoOEiC5ddkbqtYcNEMMnne#share-KQk1dZiJyo1tF0xrrlOcrLIlnAg) |  |  |  |
|  |  |  |  |  |
| taosx | 检查 instanceId 默认值 | 16 |  |  |
|  | taosx配置内容 | 除了标准配置外，增加 [log] # 监听配置文件以实时更新日志配置 watching = true [log.loggers] # Tracing 框架支持的模块表达式定义，各依赖模块的自定义日志级别 rdkafka = "trace" taos_ws = "trace" |  |  |
|  | 检查 watching 默认值 root@u2-14 /var/log/taos/test $ grep "reload tracing" taosx_17_20240924.log.2 09/24 15:55:32.015986 139706533574400 INFO QID:0x1100000000000000 mod:taosx received config file change event, start to reload tracing filter 09/24 15:55:32.016577 139706533574400 INFO QID:0x1100000000000000 mod:taosx reload tracing filter successfully 09/24 16:00:03.736588 139706533574400 INFO QID:0x1100000000000000 mod:taosx received config file change event, start to reload tracing filter 09/24 16:00:03.736834 139706533574400 INFO QID:0x1100000000000000 mod:taosx reload tracing filter successfully | true |  |  |
|  | 检查 log.loggers 默认值 | rdkafka = "TRACE" taos_ws = "TRACE" |  |  |
|  | watching = true，调整 level= "DEBUG" | 日志内容实时增加 DEBUG 日志，重新加载配置文件是否打印相应的日志？ |  |  |
|  | watching = true，调整 level= " ERROR" | 日志内容实时减少到只有 ERROR 日志 |  |  |
|  | watching = false，调整 level= "DEBUG" | 当前日志级别不变化，重启服务后增加 DEBUG 日志 |  |  |
|  | watching = false，调整 rdkafka= "ERROR" | 当前 kafka 日志级别不变化，重启服务后减少到只有 ERROR 日志 |  |  |
|  | watching = true，调整 level= "DEBUG1111" | 调整失败？ |  |  |
|  | 分模块日志 level= "INFO" rdkafka = "TRACE" | Kafka 任务记录 trace 日志，其他日志为 INFO |  |  |
|  | 分模块日志 level= "INFO" rdkafka = "ERROR" | Kafka 任务记录 error 日志，其他日志为 INFO |  |  |
|  | 点击 explorer datain 导航栏 | 正确记录，QID 来自 explorer |  |  |
|  | 创建 datain 任务，检查日志QID | `QID：0x{taosx组件ID}{任务ID}{批次 ID}{子批次 ID}`，任务 ID 正确 |  |  |
|  | 任务的增删改查 |  |  |  |
|  | 检查同步过程中 QID 中的批次 ID 是否正确更新 | 3~6 正确更新 |  |  |
|  | 重启 taosx，确认QID 中的批次 ID 是否累加 | 累加 |  |  |
|  | 构造任务中断再恢复，查看 QID 是否连续 | 连续 |  |  |
|  | 构造批次 ID 用完，重新循环的场景 |  |  |  |
|  | 构造有子批次的任务，查看 QID | 第七个字节序显示为子批次 id |  |  |
|  | 其他 case 参考 [公共部分](https://taosdata.feishu.cn/wiki/FUvpwkoOEiC5ddkbqtYcNEMMnne#share-KQk1dZiJyo1tF0xrrlOcrLIlnAg) |  |  |  |
|  |  |  |  |  |
| taosxagent | 检查 instanceId 默认值 | 64 |  |  |
|  | 创建使用 agent 的任务，检查日志中的 QID | `QID：0x{Agent组件ID}{任务ID}{批次 ID}{子批次 ID}`，任务 ID 正确 taosx 沿用该 QID，不再重新生成 |  |  |
|  | 检查 QID 中批次 ID、子批次 ID | 正确记录 |  |  |
|  | 正常心跳日志 | TRACE，带有正确的 QID `0x{Agent组件ID}{请求ID}` |  |  |
|  | 失去心跳日志 | WARN，带有正确的 QID `0x{Agent组件ID}{请求ID}` |  |  |
|  | 与 taosx 断开连接 | ERROR，带有正确的 QID `0x{Agent组件ID}{请求ID}` |  |  |
|  | 数据源同步日志 agent-> GRPC | 元数据中有正确的 QID Taosx 接收并记录QID |  |  |
|  | 其他 case 参考 [公共部分](https://taosdata.feishu.cn/wiki/FUvpwkoOEiC5ddkbqtYcNEMMnne#share-KQk1dZiJyo1tF0xrrlOcrLIlnAg) |  |  |  |
|  |  |  |  |  |
| taosadapter | 检查 instanceId 默认值 | 32 |  |  |
|  | 在日志中检查上游传入的 QID 是否正确 | 正确记录，可追溯 |  |  |
|  | 1、动态调整日志级别为 TRACE ```powershell {wrap} curl --location --request PUT 'http://127.0.0.1:6041/config' \ -u root:taosdata \ --data '{"log.level": "trace"}' ``` 2、重启服务 | 1、日志中出现 trace 日志 2、日志级别调整为默认 |  |  |
|  | 1、动态调整日志级别为 ERROR ```powershell {wrap} curl --location --request PUT 'http://127.0.0.1:6041/config' \ -u root:taosdata \ --data '{"log.level": "error"}' ``` 2、重启服务 | 1、日志中只有 error 日志 2、日志级别调整为默认 |  |  |
|  | C 函数调用参数（Trace）日志中的 QID | 记录正确的`QID:0xXXX` |  |  |
|  | C 函数调用耗时（Debug） | 记录正确的`QID:0xXXX` |  |  |
|  | 执行 SQL （Debug） | 记录正确的`QID:0xXXX` |  |  |
|  | Websocket 请求（Debug） | 记录正确的`QID:0xXXX` |  |  |
|  | Websocket 返回 （Trace） | 记录正确的`QID:0xXXX` |  |  |
|  | 其他 case 参考 [公共部分](https://taosdata.feishu.cn/wiki/FUvpwkoOEiC5ddkbqtYcNEMMnne#share-KQk1dZiJyo1tF0xrrlOcrLIlnAg) |  |  |  |
|  |  |  |  |  |
| taoskeeper | 检查 instanceId 默认值 | 48 |  |  |
|  | 1、动态调整日志级别为 TRACE ```bash {wrap} curl --location --request PUT 'http://127.0.0.1:6043/config' \ -u root:taosdata \ --data '{"log.level": "trace"}' ``` 2、重启服务 | 1、日志中出现 trace 日志 2、日志级别调整为默认 |  |  |
|  | 1、动态调整日志级别为 ERROR ```bash {wrap} curl --location --request PUT 'http://127.0.0.1:6043/config' \ -u root:taosdata \ --data '{"log.level": "error"}' ``` 2、重启服务 | 1、日志中只有 error 日志 2、日志级别调整为默认 |  |  |
|  | taosd 上报数据 | 记录正确的`QID:0xXXX` |  |  |
|  | taosx 上报数据 | 记录正确的`QID:0xXXX` |  |  |
|  | taosAdapter 上报数据 | 记录正确的`QID:0xXXX` |  |  |
|  | Taoskeeper 拆分请求 | QID 最后一位更新，其他保持不变 |  |  |
|  | 其他 case 参考 [公共部分](https://taosdata.feishu.cn/wiki/FUvpwkoOEiC5ddkbqtYcNEMMnne#share-KQk1dZiJyo1tF0xrrlOcrLIlnAg) |  |  |  |
|  |  |  |  |  |

### 9.2 可用性

请求链路可追溯
测试方法是将所有组件日志生成在一个目录里，然后 grep $QID 查询出所有日志，检查是否能匹配完整流转节点，是否有环节缺失或者描述模糊的情况

| 场景分类 | 场景描述 | 预期结果 |  |  |
| --- | --- | --- | --- | --- |
| MQTT Data In 任务，没有 Agent | 跟踪一条没有使用 taosxagent 的 mqtt datain 日志，从生成该同步任务到入库，该任务应该包括提取、拆分、过滤等环节 | 通过 QID 能够追踪每一条数据的完整链路： taosx -> taosadapter -> taosd -> taoskeeper |  |  |
| Kafka Data In 任务，有 Agent | 跟踪一条使用 taosxagent 的 kafka datain 日志，从生成该同步任务到入库，该任务应该包括提取、拆分、过滤等环节 | 通过 QID 能够追踪每一条数据的完整链路：taosxagent -> taosx -> taosadapter -> taosd -> taoskeeper |  |  |
| explorer 执行 sql | 跟踪在 explorer 执行一条 sql 的日志，从页面触发到 taosd，最后返回到页面 | 通过 QID 能够追踪每一条数据的完整链路：explorer -> taosadapter -> taosd -> taoskeeper -> taosadapter -> explorer |  |  |
| explorer 审计 | 跟踪 【 explorer 系统管理 审计 】 操作的日志链路 | 通过 QID 能够追踪每一条数据的完整链路：explorer -> taosadapter -> taosd -> taoskeeper -> taosadapter -> explorer |  |  |
| 监控上报 | 跟踪 taosx 上报监控数据的日志 |  |  |  |
|  | 跟踪 taosAdapter 上报监控数据的日志 |  |  |  |
|  | 跟踪 taosxagent 上报监控数据的日志 |  |  |  |
| 分片请求 | 跟踪有分片请求的日志 | 请求拆分和汇总应该有明确的日志记录，且 QID 与子 ID 对应关系都正确记录 |  |  |
| 链路故障 | taosd 故障 | 日志有清晰的链路中断记录 |  |  |
|  | taosadapter 故障 | 日志有清晰的链路中断记录 |  |  |
|  | taoskeeper 故障 | 日志有清晰的链路中断记录 |  |  |
| 探索 | 在日志中任意找一个 QID，确认是否可以找到该 QID 的头和尾 |  |  |  |
|  | explorer 做典型操作 |  |  |  |
|  |  |  |  |  |

### 9.3 可靠性

日志没有串行

### 9.4 性能

高速同步任务下，Trace 级别日志打印性能下降不超过 15%

### 9.5 安全性

- 日志中是否包含敏感信息

### 9.6 兼容性

测试用例包括但不局限于：
- 升级安装后，老版本（上一个版本）下创建的任务，能否继续执行？
- 升级安装后，未写入任何数据（未创建任何新任务），是否能够降级并继续运行
- 升级安装后，写入新数据（或创建新的任务）， 是否能够降级并继续运行

### 9.7 本地化

无

## 10. 待讨论(Optional)

这里用于记录在测试或用例编写过程中想到的需要讨论的问题：
- aaa
- bbb

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: abc

## 12. 测试计划 (Optional)

这里用于计划此 feature 测试的开始和结束时间。

## 13. 风险评估

用户记录这个需求的潜在风险，例如：对于功能复杂，开发时间长的功能，是否需要分期提测？

## 14. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 15. 参考文档 (Optional)

- [统一应用组件日志](https://taosdata.feishu.cn/wiki/L78swkAJniw1w3ksDwvcR6eUn2g)
-
