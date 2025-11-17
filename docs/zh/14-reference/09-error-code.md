---
toc_max_heading_level: 4
sidebar_label: 错误码
title: TDengine 错误码
description: TDengine TSDB 客户端和服务端的错误码列表和详细说明
---

本文中详细列举了在使用 TDengine TSDB 时可能得到的来自客户端和服务端的错误码以及所要采取的相应动作。

## TSDB

TSDB 错误码包括 taosc 客户端和服务端，所有语言的连接器无论是原生连接还是 WebSocket 连接，都有可能将这些错误码返回给调用方。**WebSocket 连接返回错误码时会只保留后四位**。

### 错误码结构

错误码由 0x 开头的 8 位 16 进制数表示，格式如下:

错误码 = 分类前缀（前 4 位）+ 具体错误码（后 4 位）

#### 前缀分类

| 错误类型                      | 前缀    | 说明                                                  |
| ---------------------------- | ------- | ---------------------------------------------------- |
| TDengine TSDB 业务错误        | 0x8000  | TDengine TSDB 引擎自定义的业务逻辑错误码，详见下文各模块错误码说明 |
| Linux 系统调用错误            | 0x80FF  | 后 4 位对应 Linux 系统 API 返回的 errno，可参考 [Linux Error Codes](https://www.chromium.org/chromium-os/developer-library/reference/linux-constants/errnos/) |
| Windows API 系统错误          | 0x81FF  | 后 4 位对应 Windows API 返回的错误码，可参考 [Windows Error Codes](https://learn.microsoft.com/en-us/windows/win32/debug/system-error-codes#system-error-codes) |
| Windows Socket 系统错误       | 0x82FF  | 后 4 位对应 Windows Socket API 返回的错误码，可参考 [Windows Sockets 错误码](https://learn.microsoft.com/en-us/windows/win32/winsock/windows-sockets-error-codes-2) |

#### 示例说明

以错误码 `0x80000216` 为例：
- **前缀**: `0x8000` → TDengine 业务错误。
- **具体错误码**: `0x0216` → 对应 TSC 模块的 "Syntax error in SQL"。

以错误码 `0x80FF0002` 为例：
- **前缀**: `0x80FF` → Linux 系统错误。
- **具体错误码**: `0x0002` → 对应 Linux errno 2，即 "No such file or directory"。

### 业务错误

下面是各模块的业务错误码。

#### rpc

| 错误码     | 错误描述                                     | 可能的出错场景或者可能的原因                                                                       | 建议用户采取的措施                                                                                   |
| ---------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| 0x8000000B | Unable to establish connection               | 1.网络不通 2.多次重试、依然不能执行请求                                                            | 1.检查网络 2.分析日志，具体原因比较复杂                                                              |
| 0x80000013 | Client and server's time is not synchronized | 1.客户端和服务端不在同一个时区 2.客户端和服务端在同一个时区，但是两者的时间不同步，相差超过 900 秒 | 1.调整到同一个时区 2.校准客户端和服务端的时间                                                        |
| 0x80000015 | Unable to resolve FQDN                       | 设置了无效的 fqdn                                                                                  | 检查 fqdn 的设置                                                                                     |
| 0x80000017 | Port already in use                          | 端口已经被某个服务占用的情况下，新启的服务依然尝试绑定该端口                                       | 1.改动新服务的服务端口 2.杀死之前占用端口的服务                                                      |
| 0x80000018 | Conn is broken                               | 由于网络抖动或者请求时间过长（超过 900 秒），导致系统主动摘掉连接                                  | 1.设置系统的最大超时时长 2.检查请求时长                                                              |
| 0x80000019 | Conn read timeout                            | 1.请求是否处理时间过长 2.服务端处理不过来 3.服务端已经死锁                                         | 1.显式配置 readTimeout 参数，2.分析 taosd 上堆栈                                                     |
| 0x80000020 | some vnode/qnode/mnode(s) out of service     | 多次重试之后，仍然无法连接到集群，可能是所有的节点都宕机了，或者存活的节点不是 Leader 节点         | 1.查看 taosd 的状态、分析 taosd 宕机的原因 2.分析存活的 taosd 为什么无法选取 Leader                  |
| 0x80000021 | some vnode/qnode/mnode(s) conn is broken     | 多次重试之后，仍然无法连接到集群，可能是网络异常、请求时间太长、服务端死锁等问题                   | 1.检查网络 2.请求的执行时间                                                                          |
| 0x80000022 | rpc open too many session                    | 1.并发太高导致占用链接已经到达上限 2.服务端的 BUG，导致连接一直不释放                              | 1.调整配置参数 numOfRpcSessions 2.调整配置参数 timeToGetAvailableConn 3.分析服务端不释放的连接的原因 |
| 0x80000023 | rpc network error                            | 1.网络问题，可能是闪断，2.服务端 crash                                                             | 1.检查网络 2.检查服务端是否重启                                                                      |
| 0x80000024 | rpc network bus                              | 1.集群间互相拉数据的时候，没有拿到可用链接，或者链接数目已经到上限                                 | 1.是否并发太高 2.检查集群各个节点是否有异常，是否出现了死锁等情况                                    |
| 0x80000025 | http-report already quit                     | 1.http 上报出现的问题                                                                              | 内部问题，可以忽略                                                                                   |
| 0x80000026 | rpc module already quit                      | 1.客户端实例已经退出，依然用该实例做查询                                                           | 检查业务代码，是否用错                                                                               |
| 0x80000027 | rpc async module already quit                | 1.引擎错误，可以忽略，该错误码不会返回到用户侧                                                     | 如果返回到用户侧，需要引擎侧追查问题                                                                 |
| 0x80000028 | rpc async in process                         | 1.引擎错误，可以忽略，该错误码不会返回到用户侧                                                     | 如果返回到用户侧，需要引擎侧追查问题                                                                 |
| 0x80000029 | rpc no state                                 | 1.引擎错误，可以忽略，该错误码不会返回到用户侧                                                     | 如果返回到用户侧，需要引擎侧追查问题                                                                 |
| 0x8000002A | rpc state already dropped                    | 1.引擎错误，可以忽略，该错误码不会返回到用户侧                                                     | 如果返回到用户侧，需要引擎侧追查问题                                                                 |
| 0x8000002B | rpc msg exceed limit                         | 1.单个 rpc 消息超过上限，该错误码不会返回到用户侧                                                  | 如果返回到用户侧，需要引擎侧追查问题                                                                 |

#### common  

| 错误码     | 错误描述                                | 可能的出错场景或者可能的原因                                               | 建议用户采取的措施                                                                                                                                    |
| ---------- | --------------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| 0x80000100 | Operation not supported                 | 操作不被支持、不允许的场景                                                 | 检查操作是否有误，确认该功能是否被支持                                                                                                                |
| 0x80000102 | Out of Memory                           | 客户端或服务端内存分配失败的场景                                           | 检查客户端、服务端内存是否充足                                                                                                                        |
| 0x80000104 | Data file corrupted                     | 1.存储数据文件损坏 2.udf 文件无法创建                                      | 1.联系涛思客户支持 2.确认服务端对临时目录有读写创建文件权限                                                                                           |
| 0x80000106 | too many Ref Objs                       | 无可用 ref 资源                                                            | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000107 | Ref ID is removed                       | 引用的 ref 资源已经释放                                                    | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000108 | Invalid Ref ID                          | 无效 ref ID                                                                | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000010A | Ref is not there                        | ref 信息不存在                                                             | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000010B | Driver was not loaded                   | 未在系统路径中找到 libtaosnative.so 或 libtaosws.so                        | 重新安装客户端驱动                                                                                                                                    |
| 0x8000010C | Function was not loaded from the driver | 在 libtaos.so 中定义的一些函数在 libtaosnative.so 或 libtaosws.so 中未实现 | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000110 | Unexpected generic error                | 系统内部错误                                                               | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000111 | Action in progress                      | 操作进行中                                                                 | 1.等待操作完成 2.根据需要取消操作 3.当超出合理时间仍然未完成可保留现场和日志，或联系客户支持                                                          |
| 0x80000112 | Out of range                            | 配置参数超出允许值范围                                                     | 更改参数                                                                                                                                              |
| 0x80000115 | Invalid message                         | 消息错误                                                                   | 1.检查是否存在节点间版本不一致 2.保留现场和日志，github 上报 issue                                                                                    |
| 0x80000116 | Invalid message len                     | 消息长度错误                                                               | 1.检查是否存在节点间版本不一致 2.保留现场和日志，github 上报 issue                                                                                    |
| 0x80000117 | Invalid pointer                         | 无效指针                                                                   | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000118 | Invalid parameters                      | 无效参数                                                                   | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000119 | Invalid config option                   | 无效配置                                                                   | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000011A | Invalid option                          | 无效选项                                                                   | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000011B | Invalid json format                     | JSON 格式错误                                                              | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000011C | Invalid version number                  | 无效版本格式                                                               | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000011D | Invalid version string                  | 无效版本格式                                                               | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000011E | Version not compatible                  | 节点间版本不兼容                                                           | 检查各节点版本（包括服务端与客户端），确保节点间版本一致或兼容                                                                                        |
| 0x8000011F | Checksum error                          | 文件 checksum 校验失败                                                     | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000120 | Failed to compress msg                  | 压缩失败                                                                   | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000121 | Message not processed                   | 消息未被正确处理                                                           | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000122 | Config not found                        | 未找到配置项                                                               | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000123 | Repeat initialization                   | 重复初始化                                                                 | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000124 | Cannot add duplicate keys to hash       | 添加重复 key 数据到哈希表中                                                | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000125 | Retry needed                            | 需要应用进行重试                                                           | 应用按照 API 使用规范进行重试                                                                                                                         |
| 0x80000126 | Out of memory in rpc queue              | rpc 消息队列内存使用达到上限                                               | 1.检查确认系统负载是否过大 2.（如必要）通过配置 rpcQueueMemoryAllowed 增大 rpc 消息队列内存上限 3.如果问题还未解决，保留现场和日志，github 上报 issue |
| 0x80000127 | Invalid timestamp format                | 时间戳格式错误                                                             | 检查并确认输入的时间戳格式正确                                                                                                                        |
| 0x80000128 | Msg decode error                        | 消息解码错误                                                               | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000012A | Not found                               | 未找到内部缓存信息                                                         | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x8000012B | Out of disk space                       | 磁盘空间不足                                                               | 1.检查并确保数据目录、临时文件夹目录有足够磁盘空间 2.定期检查维护上述目录，确保空间足够                                                               |
| 0x80000130 | Database is starting up                 | 数据库启动中，暂无法提供服务                                               | 检查数据库状态，待系统完成启动后继续或重试                                                                                                            |
| 0x80000131 | Database is closing down                | 数据库正在或已经关闭，无法提供服务                                         | 检查数据库状态，确保系统工作在正常状态                                                                                                                |
| 0x80000132 | Invalid data format                     | 数据格式错误                                                               | 1.保留现场和日志，github 上报 issue 2.联系涛思客户支持                                                                                                |
| 0x80000133 | Invalid operation                       | 无效的或不支持的操作                                                       | 1.修改确认当前操作为合法有效支持的操作，检查参数有效性 2.如果问题还未解决，保留现场和日志，github 上报 issue                                          |
| 0x80000134 | Invalid value                           | 无效值                                                                     | 保留现场和日志，github 上报 issue                                                                                                                     |
| 0x80000135 | Invalid fqdn                            | 无效 FQDN                                                                  | 检查配置或输入的 FQDN 值是否正确                                                                                                                      |
| 0x8000013C | Invalid disk id                         | 不合法的 disk id                                                           | 建议用户检查挂载磁盘是否失效或者使用参数 diskIDCheckEnabled 来跳过磁盘检查                                                                            |
| 0x8000013D | Decimal value overflow                  | Decimal 计算溢出                                                           | 检查计算表达式和参数值是否计算结果导致类型溢出                                                                                                        |
| 0x8000013E | Division by zero error                  | Division by zero                                                           | 检查除法操作是否除以 0                                                                                                                                |

#### tsc

| 错误码     | 错误描述                          | 可能的出错场景或者可能的原因 | 建议用户采取的措施                                                               |
| ---------- | --------------------------------- | ---------------------------- | -------------------------------------------------------------------------------- |
| 0x80000207 | Invalid user name                 | 数据库用户名不合法           | 检查数据库用户名是否正确                                                         |
| 0x80000208 | Invalid password                  | 数据库密码不合法             | 检查数据库密码是否正确                                                           |
| 0x80000209 | Database name too long            | 数据库名称不合法             | 检查数据库名称是否正确                                                           |
| 0x8000020A | Table name too long               | 表名不合法                   | 检查表名是否正确                                                                 |
| 0x8000020F | Query terminated                  | 查询被中止                   | 检查是否有用户中止了查询                                                         |
| 0x80000213 | Disconnected from server          | 连接已中断                   | 检查连接是否被人为中断或客户端正在退出                                           |
| 0x80000216 | Syntax error in SQL               | SQL 语法错误                 | 检查 SQL 语句并修正错误                                                          |
| 0x80000219 | SQL statement too long            | SQL 长度超出限制             | 检查 SQL 语句并修正错误                                                          |
| 0x8000021A | File is empty                     | 文件内容为空                 | 检查输入文件内容                                                                 |
| 0x8000021F | Invalid column length             | 列长度错误                   | 保留现场和日志，github 上报 issue                                                |
| 0x80000222 | Invalid JSON data type            | JSON 数据类型错误            | 检查输入 JSON 内容                                                               |
| 0x80000224 | Value out of range                | 数据大小超过类型范围         | 检查输入的数据值                                                                 |
| 0x80000229 | Invalid tsc input                 | API 输入错误                 | 检查应用调用 API 时传递的参数                                                    |
| 0x8000022A | Stmt API usage error              | STMT/STMT2 API 使用错误      | 检查 STMT/STMT2 API 调用的顺序、适用场景、错误处理                               |
| 0x8000022B | Stmt table name not set correctly | STMT/STMT2 未正确设置表名    | 检查 STMT/STMT2 绑定的表名是否合法                                               |
| 0x8000022D | Query killed                      | 查询被中止                   | 检查是否有用户中止了查询                                                         |
| 0x8000022E | No available execution node       | 没有可用的查询执行节点       | 检查当前 query policy 配置，如果需要有 Qnode 参与确保系统中存在可用的 Qnode 节点 |
| 0x8000022F | Table is not a super table        | 当前语句中的表名不是超级表   | 检查当前语句中所用表名是否是超级表                                               |
| 0x80000230 | Stmt cache error                  | STMT/STMT2 内部缓存出错      | 保留现场和日志，github 上报 issue                                                |
| 0x80000231 | Tsc internal error                | TSC 内部错误                 | 保留现场和日志，github 上报 issue                                                |

#### mnode

| 错误码     | 错误描述                                                                                     | 可能的出错场景或者可能的原因                                                      | 建议用户采取的措施                                                                                   |
| ---------- | -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| 0x80000303 | Insufficient privilege for operation                                                         | 无权限                                                                            | 赋权                                                                                                 |
| 0x8000030B | Data expired                                                                                 | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000030C | Invalid query id                                                                             | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000030E | Invalid connection id                                                                        | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000315 | User is disabled                                                                             | 该用户不可用                                                                      | 赋权                                                                                                 |
| 0x80000318 | Mnode internal error                                                                         | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000320 | Object already there                                                                         | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000322 | Invalid table type                                                                           | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000323 | Object not there                                                                             | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000326 | Invalid action type                                                                          | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000328 | Invalid raw data version                                                                     | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000329 | Invalid raw data len                                                                         | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000032A | Invalid raw data content                                                                     | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000032C | Object is creating                                                                           | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000032D | Object is dropping                                                                           | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000330 | Dnode already exists                                                                         | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000331 | Dnode does not exist                                                                         | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000332 | Vgroup does not exist                                                                        | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000333 | Cannot drop mnode which is leader                                                            | 操作节点为 leader                                                                 | 确认操作是否正确                                                                                     |
| 0x80000334 | Out of dnodes                                                                                | dnode 节点数量不够                                                                | 增加 dnode 节点                                                                                      |
| 0x80000335 | Cluster cfg inconsistent                                                                     | 配置不一致                                                                        | 检查 dnode 节点与 mnode 节点配置是否一致。检查方式：1.节点启动时，在日志中输出 2.使用 show variables |
| 0x8000033B | Cluster id not match                                                                         | 节点配置数据不一致                                                                | 检查各节点 data/dnode/dnodes.json 文件中的 clusterid                                                 |
| 0x80000340 | Account already exists                                                                       | （仅企业版）内部错误                                                              | 上报 issue                                                                                           |
| 0x80000342 | Invalid account options                                                                      | （仅企业版）该操作不支持                                                          | 确认操作是否正确                                                                                     |
| 0x80000344 | Invalid account                                                                              | 账户不存在                                                                        | 确认账户是否正确                                                                                     |
| 0x80000350 | User already exists                                                                          | Create user, 重复创建                                                             | 确认操作是否正确                                                                                     |
| 0x80000351 | Invalid user                                                                                 | 用户不存在                                                                        | 确认操作是否正确                                                                                     |
| 0x80000352 | Invalid user format                                                                          | 格式不正确                                                                        | 确认操作是否正确                                                                                     |
| 0x80000353 | Invalid password format                                                                      | 密码长度必须为 8 到 16 位，并且至少包含大写字母、小写字母、数字、特殊字符中的三类 | 确认密码字符串的格式                                                                                 |
| 0x80000354 | Can not get user from conn                                                                   | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000355 | Too many users                                                                               | （仅企业版）用户数量超限                                                          | 调整配置                                                                                             |
| 0x80000357 | Authentication failure                                                                       | 密码不正确                                                                        | 确认操作是否正确                                                                                     |
| 0x80000358 | User not available                                                                           | 用户不存在                                                                        | 确认操作是否正确                                                                                     |
| 0x80000360 | STable already exists                                                                        | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000361 | STable not exist                                                                             | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000364 | Too many tags                                                                                | tag 数量太多                                                                      | 不能修改，代码级别限制                                                                               |
| 0x80000365 | Too many columns                                                                             | columns 数量太多                                                                  | 不能修改，代码级别限制                                                                               |
| 0x80000369 | Tag already exists                                                                           | tag 已存在                                                                        | 确认操作是否正确                                                                                     |
| 0x8000036A | Tag does not exist                                                                           | tag 不存在                                                                        | 确认操作是否正确                                                                                     |
| 0x8000036B | Column already exists                                                                        | Column 已存在                                                                     | 确认操作是否正确                                                                                     |
| 0x8000036C | Column does not exist                                                                        | Column 不存在                                                                     | 确认操作是否正确                                                                                     |
| 0x8000036E | Invalid stable options                                                                       | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000036F | Invalid row bytes                                                                            | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000370 | Invalid func name                                                                            | name 长度错误                                                                     | 确认操作是否正确                                                                                     |
| 0x80000372 | Invalid func code                                                                            | code 长度错误                                                                     | 确认操作是否正确                                                                                     |
| 0x80000373 | Func already exists                                                                          | Func 已存在                                                                       | 确认操作是否正确                                                                                     |
| 0x80000374 | Func not exists                                                                              | Func 不存在                                                                       | 确认操作是否正确                                                                                     |
| 0x80000375 | Invalid func bufSize                                                                         | bufSize 长度错误，或者超过限制                                                    | 确认操作是否正确                                                                                     |
| 0x80000378 | Invalid func comment                                                                         | 长度错误，或者超过限制                                                            | 确认操作是否正确                                                                                     |
| 0x80000379 | Invalid func retrieve msg                                                                    | 长度错误，或者超过限制                                                            | 确认操作是否正确                                                                                     |
| 0x80000380 | Database not specified or available                                                          | 未指定 database                                                                   | 使用 use database;                                                                                   |
| 0x80000381 | Database already exists                                                                      | Database 已存在                                                                   | 确认操作是否正确                                                                                     |
| 0x80000382 | Invalid database options                                                                     | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000383 | Invalid database name                                                                        | 长度错误                                                                          | 确认操作是否正确                                                                                     |
| 0x80000385 | Too many databases for account                                                               | 数量超限                                                                          | 调整配置                                                                                             |
| 0x80000386 | Database in dropping status                                                                  | 数据库正在被删除                                                                  | 重试，长时间保持该状态上报 issue                                                                     |
| 0x80000388 | Database not exist                                                                           | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x80000389 | Invalid database account                                                                     | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000038A | Database options not changed                                                                 | 操作无变化                                                                        | 确认操作是否正确                                                                                     |
| 0x8000038B | Index not exist                                                                              | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x80000396 | Database in creating status                                                                  | 数据库正在被创建                                                                  | 重试                                                                                                 |
| 0x8000039A | Invalid system table name                                                                    | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x8000039F | No VGroup's leader need to be balanced                                                       | 执行 balance vgroup leader 操作                                                   | 没有需要进行 balance leader 操作的 VGroup                                                            |
| 0x800003A0 | Mnode already exists                                                                         | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003A1 | Mnode not there                                                                              | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003A2 | Qnode already exists                                                                         | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003A3 | Qnode not there                                                                              | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003A4 | Snode already exists                                                                         | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003A5 | Snode not there                                                                              | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003A8 | The replica of mnode cannot less than 1                                                      | mnode 少于 1                                                                      | 操作不允许                                                                                           |
| 0x800003A9 | The replica of mnode cannot exceed 3                                                         | mnode 多于 1                                                                      | 操作不允许                                                                                           |
| 0x800003AE | VGroup is offline                                                                            | Vgroup 离线                                                                       | 检查 dnode 是否离线                                                                                  |
| 0x800003B1 | No enough memory in dnode                                                                    | 内存不足                                                                          | 调整配置                                                                                             |
| 0x800003B3 | Invalid dnode end point                                                                      | ep 配置不正确                                                                     | 确认操作是否正确                                                                                     |
| 0x800003B6 | Offline dnode exists                                                                         | Dnode offline                                                                     | 检查节点状态                                                                                         |
| 0x800003B7 | Invalid vgroup replica                                                                       | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003B8 | Dnode in creating status                                                                     | 正在创建                                                                          | 重试                                                                                                 |
| 0x800003B9 | Dnode in dropping status                                                                     | 正在删除                                                                          | 重试                                                                                                 |
| 0x800003C2 | Invalid stable alter options                                                                 | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003C3 | STable option unchanged                                                                      | 操作无变化                                                                        | 确认操作是否正确                                                                                     |
| 0x800003C4 | Field used by topic                                                                          | 被使用                                                                            | 确认操作是否正确                                                                                     |
| 0x800003C5 | Database is single stable mode                                                               | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003C6 | Invalid schema version while alter stb                                                       | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003C7 | Invalid stable uid while alter stb                                                           | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003C8 | Field used by tsma                                                                           | 被使用                                                                            | 确认操作是否正确                                                                                     |
| 0x800003D1 | Transaction not exists                                                                       | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003D2 | Invalid stage to kill                                                                        | 事务处在不能被 kill 的节点（比如 在 commit 阶段）                                 | 等待事务结束，如长时间不结束，上报 issue                                                             |
| 0x800003D3 | Conflict transaction not completed                                                           | 事务冲突，不能执行该操作                                                          | 使用 show transactions 命令查看冲突的事务，等待冲突事务结束，如长时间不结束，上报 issue              |
| 0x800003D4 | Transaction commitlog is null                                                                | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003D5 | Unable to establish connection While execute transaction and will continue in the background | 网络错误                                                                          | 检查网络是否正常                                                                                     |
| 0x800003D6 | Last Transaction not finished                                                                | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003D7 | Sync timeout While execute transaction and will continue in the background                   | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003DA | The transaction is not able to be killed                                                     | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003DF | Unknown transaction error                                                                    | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003E0 | Topic already exists                                                                         | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003E1 | Topic not exist                                                                              | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003E3 | Invalid topic                                                                                | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003E4 | Topic with invalid query                                                                     | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003E5 | Topic with invalid option                                                                    | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003E6 | Consumer not exist                                                                           | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003E7 | Topic unchanged                                                                              | 无变化                                                                            | 确认操作是否正确                                                                                     |
| 0x800003E8 | Subscribe not exist                                                                          | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003E9 | Offset not exist                                                                             | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003EA | Consumer not ready                                                                           | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003EB | Topic subscribed cannot be dropped                                                           | 被使用                                                                            | 确认操作是否正确                                                                                     |
| 0x800003EC | Consumer group being used by some consumer                                                   | 被使用                                                                            | 确认操作是否正确                                                                                     |
| 0x800003ED | Topic must be dropped first                                                                  | 被使用                                                                            | 确认操作是否正确                                                                                     |
| 0x800003EE | Invalid subscribe option                                                                     | 内部错误                                                                          | 确认操作是否正确                                                                                     |
| 0x800003EF | Topic being rebalanced                                                                       | 操作中                                                                            | 重试                                                                                                 |
| 0x800003F0 | Stream already exists                                                                        | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003F1 | Stream not exist                                                                             | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x800003F2 | Invalid stream option                                                                        | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x800003F3 | Stream must be dropped first                                                                 | 被使用                                                                            | 确认操作是否正确                                                                                     |
| 0x800003F5 | Stream temporarily does not support source db having replica > 1                             | 超过限制                                                                          | 操作不被允许                                                                                         |
| 0x800003F6 | Too many streams                                                                             | 超过限制                                                                          | 不能修改，代码级别限制                                                                               |
| 0x800003F7 | Cannot write the same stable as other stream                                                 | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000480 | index already exists                                                                         | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x80000481 | index not exist                                                                              | 不存在                                                                            | 确认操作是否正确                                                                                     |
| 0x80000482 | Invalid sma index option                                                                     | 内部错误                                                                          | 上报 issue                                                                                           |
| 0x80000483 | index already exists                                                                         | 已存在                                                                            | 确认操作是否正确                                                                                     |
| 0x80000484 | index not exist                                                                              | 不存在                                                                            | 确认操作是否正确                                                                                     |

#### Bnode

| 错误码     | 错误描述                 | 可能的出错场景或者可能的原因 | 建议用户采取的措施     |
| ---------- | ------------------------ | ---------------------------- | ---------------------- |
| 0x80000450 | Bnode already exists     | 已创建                       | 检查节点状态           |
| 0x80000451 | Bnode already deployed   | 已部署                       | 确认操作是否正确       |
| 0x80000452 | Bnode not deployed       | 内部错误                     | 上报 issue             |
| 0x80000453 | Bnode not there          | 不在线                       | 确认操作是否正确       |
| 0x80000454 | Bnode not found          | 内部错误                     | 上报 issue             |
| 0x80000455 | Bnode exec launch failed | 内部错误                     | 上报 issue             |
| 0x8000261C | Invalid Bnode option     | Bnode 选项值非法             | 检查并修正数据库选项值 |

#### dnode

| 错误码     | 错误描述                                | 可能的出错场景或者可能的原因            | 建议用户采取的措施 |
| ---------- | --------------------------------------- | --------------------------------------- | ------------------ |
| 0x80000408 | Dnode is offline                        | 不在线                                  | 检查节点状态       |
| 0x80000409 | Mnode already deployed                  | 已部署                                  | 确认操作是否正确   |
| 0x8000040A | Mnode not found                         | 内部错误                                | 上报 issue         |
| 0x8000040B | Mnode not deployed                      | 内部错误                                | 上报 issue         |
| 0x8000040C | Qnode already deployed                  | 已部署                                  | 确认操作是否正确   |
| 0x8000040D | Qnode not found                         | 内部错误                                | 上报 issue         |
| 0x8000040E | Qnode not deployed                      | 内部错误                                | 上报 issue         |
| 0x8000040F | Snode already deployed                  | 已部署                                  | 确认操作是否正确   |
| 0x80000410 | Snode not found                         | 内部错误                                | 上报 issue         |
| 0x80000411 | Snode not deployed                      | 已部署                                  | 确认操作是否正确   |
| 0x8000042D | Request is not matched with local dnode | taos.cfg 中的 FQDN 或者 port 被错误修改 | 取消修改           |

#### vnode

| 错误码     | 错误描述                                           | 可能的出错场景或者可能的原因   | 建议用户采取的措施 |
| ---------- | -------------------------------------------------- | ------------------------------ | ------------------ |
| 0x80000503 | Invalid vgroup ID                                  | 老客户端未更新 cache，内部错误 | 上报问题           |
| 0x80000512 | No writing privilege                               | 无写权限                       | 寻求授权           |
| 0x80000520 | Vnode does not exist                               | 内部错误                       | 上报问题           |
| 0x80000521 | Vnode already exists                               | 内部错误                       | 上报问题           |
| 0x80000522 | Hash value of table is not in the vnode hash range | 表不属于 vnode                 | 上报问题           |
| 0x80000524 | Invalid table operation                            | 表非法操作                     | 上报问题           |
| 0x80000525 | Column already exists                              | 修改表是列已存在               | 上报问题           |
| 0x80000526 | Column does not exists                             | 修改表时，表不存在             | 上报问题           |
| 0x80000527 | Column is subscribed                               | 列被订阅，不能操作             | 上报问题           |
| 0x80000529 | Vnode is stopped                                   | Vnode 已经关闭                 | 上报问题           |
| 0x80000530 | Duplicate write request                            | 重复写入请求，内部错误         | 上报问题           |
| 0x80000531 | Vnode query is busy                                | 查询忙碌                       | 上报问题           |
| 0x80000540 | Vnode already exist but Dbid not match             | 内部错误                       | 上报问题           |

#### tsdb

| 错误码     | 错误描述                                  | 可能的出错场景或者可能的原因               | 建议用户采取的措施             |
| ---------- | ----------------------------------------- | ------------------------------------------ | ------------------------------ |
| 0x80000600 | Invalid table ID to write                 | 写表不存在                                 | 重启客户端                     |
| 0x80000602 | Invalid table schema version              | 表的 schema 版本号过期，内部错误           | 无需处理，内部自动更新         |
| 0x80000603 | Table already exists                      | 表已存在                                   | 上报问题                       |
| 0x80000604 | Invalid configuration                     | 内部错误                                   | 上报问题                       |
| 0x80000605 | Init failed                               | 启动失败                                   | 上报问题                       |
| 0x8000060B | Timestamp is out of range                 | 写入时间范围越界                           | 上报问题，检查应用写入时间逻辑 |
| 0x8000060C | Submit message is messed up               | 消息错误，可能由于客户端和服务端不兼容导致 | 上报问题                       |
| 0x80000618 | Table does not exists                     | 表已经存在                                 | 上报问题                       |
| 0x80000619 | Super table already exists                | 超级表已经存在                             | 上报问题                       |
| 0x8000061A | Super table does not exist                | 超级表不存在                               | 上报问题                       |
| 0x8000061B | Invalid table schema version              | 同 TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION     | 上报问题                       |
| 0x8000061D | Table already exists in other super table | 表已存在，但属于其他超级表                 | 检查写入应用逻辑               |

#### query

| 错误码        | 错误描述                                                                        | 可能的出错场景或者可能的原因                                                                                       | 建议用户采取的措施                                                                                                               |
|------------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| 0x80000700 | Invalid query handle                                                        | 当前查询句柄不存在                                                                                            | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x80000709 | Multiple retrieval of this query                                            | 当前子查询已经正在进行中                                                                                         | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x8000070A | Too many groups/time window in query                                        | 当前查询结果中的分组或窗口个数超过限制个数                                                                                | 调整查询语句，确保查询条件中的分组和窗口个数不超过上限                                                                                             |
| 0x8000070D | System error                                                                | 底层系统 API 返回错误                                                                                        | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x80000720 | Scheduler not exist                                                         | 当前子查询对应的客户端信息不存在                                                                                     | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x80000721 | Task not exist                                                              | 子查询不存在                                                                                               | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x80000722 | Task already exist                                                          | 子查询已经存在                                                                                              | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x80000729 | Task message error                                                          | 查询消息错误                                                                                               | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x8000072B | Task status error                                                           | 子查询状态错误                                                                                              | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x8000072F | Job not exist                                                               | 查询 JOB 已经不存在                                                                                         | 保留现场和日志，github 上报 issue                                                                                                 |
| 0x80000739 | Query memory upper limit is reached                                         | 单个查询达到内存使用上限                                                                                         | 设置合理的内存上限或调整 SQL 语句                                                                                                     |
| 0x8000073A | Query memory exhausted                                                      | dnode 查询内存到达使用上限                                                                                     | 设置合理的内存上限或调整并发查询量或增大系统内存                                                                                                |
| 0x8000073B | Timeout for long time no fetch                                              | 查询被长时间中断未恢复                                                                                          | 调整应用实现尽快 fetch 数据                                                                                                       |
| 0x8000073C | Memory pool not initialized                                                 | 内存池没有初始化                                                                                             | 确认开关 queryUseMemoryPool 是否打开；如果 queryUseMemoryPool 已经打开，检查服务器是否达到了开启内存池的基本条件：1. 系统的可用内存总量不低于 5G；2. 扣除预留部分后系统的可用内存不低于 4G |
| 0x8000073D | Alter minReservedMemorySize failed since no enough system available memory  | 更新 minReservedMemorySize 失败                                                                          | 确认当前的系统内存：1. 系统的可用内存总量不低于 5G；2. 扣除预留部分后系统的可用内存不低于 4G                                                                    |
| 0x8000073E | Duplicate timestamp not allowed in count/event/state window                  | 窗口输入主键列有重复时间戳。对状态窗口、事件窗口、计数窗口做超级表查询时，所有子表数据会按照时间戳进行排序后合并为一条时间线进行计算，因此子表合并后的时间戳可能会出现重复，导致某些计算没有意义而报错。 | 如果需要对超级表查询并且使用这些窗口时，确保子表中不存在重复时间戳数据。                                                                                    |

#### grant

| 错误码     | 错误描述                            | 可能的出错场景或者可能的原因 | 建议用户采取的措施               |
| ---------- | ----------------------------------- | ---------------------------- | -------------------------------- |
| 0x80000800 | License expired                     | 授权时间过期                 | 检查授权信息，联系交付更新授权码 |
| 0x80000801 | DNode creation limited by license   | Dnode 数量超过授权限制       | 检查授权信息，联系交付更新授权码 |
| 0x80000802 | Account creation limited by license | 账号数量超过授权限制         | 检查授权信息，联系交付更新授权码 |
| 0x80000803 | Time series limited by license      | 测点数量超过授权限制         | 检查授权信息，联系交付更新授权码 |
| 0x80000804 | DB creation limited by license      | 数据库数量超过授权限制       | 检查授权信息，联系交付更新授权码 |
| 0x80000805 | User creation limited by license    | 用户数量超过授权限制         | 检查授权信息，联系交付更新授权码 |
| 0x80000806 | Conn creation limited by license    | 连接数量超过授权限制         | 暂未限制，联系交付进行检查       |
| 0x80000807 | Stream creation limited by license  | 流数量超过授权限制           | 暂未限制，联系交付进行检查       |
| 0x80000808 | Write speed limited by license      | 写入速度超过授权限制         | 暂未限制，联系交付进行检查       |
| 0x80000809 | Storage capacity limited by license | 存储空间超过授权限制         | 检查授权信息，联系交付更新授权码 |
| 0x8000080A | Query time limited by license       | 查询次数超过授权限制         | 暂未限制，联系交付进行检查       |
| 0x8000080B | CPU cores limited by license        | CPU 核数超过授权限制         | 暂未限制，联系交付进行检查       |
| 0x8000080C | STable creation limited by license  | 超级表数量超过授权限制       | 检查授权信息，联系交付更新授权码 |
| 0x8000080D | Table creation limited by license   | 子表/普通表数量超过授权限制  | 检查授权信息，联系交付更新授权码 |

#### sync

| 错误码     | 错误描述                     | 可能的出错场景或者可能的原因                                                                                | 建议用户采取的措施                                                                         |
| ---------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| 0x80000903 | Sync timeout                 | 场景 1：发生了切主 旧主节点上已经开始协商但尚未达成一致的请求将超时。场景 2：从节点响应超时，导致协商超时。 | 检查集群状态，例如：show vgroups 查看服务端日志，以及服务端节点之间的网络状况。            |
| 0x8000090C | Sync leader is unreachable   | 场景 1：选主过程中 场景 2：客户端请求路由到了从节点，且重定向失败 场景 3：客户端或服务端网络配置错误        | 检查集群状态、网络配置、应用程序访问状态等。查看服务端日志，以及服务端节点之间的网络状况。 |
| 0x8000090F | Sync new config error        | 成员变更配置错误                                                                                            | 内部错误，用户无法干预                                                                     |
| 0x80000911 | Sync not ready to propose    | 场景 1：恢复未完成                                                                                          | 检查集群状态，例如：show vgroups。查看服务端日志，以及服务端节点之间的网络状况。           |
| 0x80000914 | Sync leader is restoring     | 场景 1：发生了切主 选主后，日志重演中                                                                       | 检查集群状态，例如：show vgroups。查看服务端日志，观察恢复进度。                           |
| 0x80000915 | Sync invalid snapshot msg    | 快照复制消息错误                                                                                            | 服务端内部错误                                                                             |
| 0x80000916 | Sync buffer is full          | 场景 1：客户端请求并发数特别大，超过了服务端处理能力，或者因为网络和 CPU 资源严重不足，或者网络连接问题等。 | 检查集群状态，系统资源使用率（例如磁盘 IO、CPU、网络通信等），以及节点之间网络连接状况。   |
| 0x80000917 | Sync write stall             | 场景 1：状态机执行被阻塞，例如因系统繁忙，磁盘 IO 资源严重不足，或落盘失败等                                | 检查集群状态，系统资源使用率（例如磁盘 IO 和 CPU 等），以及是否发生了落盘失败等。          |
| 0x80000918 | Sync negotiation win is full | 场景 1：客户端请求并发数特别大，超过了服务端处理能力，或者因为网络和 CPU 资源严重不足，或者网络连接问题等。 | 检查集群状态，系统资源使用率（例如磁盘 IO、CPU、网络通信等），以及节点之间网络连接状况。   |
| 0x800009FF | Sync internal error          | 其它内部错误                                                                                                | 检查集群状态，例如：show vgroups                                                           |

#### tq

| 错误码     | 错误描述                  | 可能的出错场景或者可能的原因                                        | 建议用户采取的措施                        |
| ---------- | ------------------------- | ------------------------------------------------------------------- | ----------------------------------------- |
| 0x80000A0C | TQ table schema not found | 消费数据时表不存在                                                  | 内部错误，不透传给用户                    |
| 0x80000A0D | TQ no committed offset    | 消费时设置 offset reset = none，并且 server 端没有之前消费的 offset | 设置 offset reset 为 earliest 或者 latest |

#### wal

| 错误码     | 错误描述              | 可能的出错场景或者可能的原因     | 建议用户采取的措施 |
| ---------- | --------------------- | -------------------------------- | ------------------ |
| 0x80001001 | WAL file is corrupted | WAL 文件损坏                     | 服务端内部错误     |
| 0x80001003 | WAL invalid version   | 请求日志版本，超过了当前日志范围 | 服务端内部错误     |
| 0x80001005 | WAL log not exist     | 请求日志记录，不存在             | 服务端内部错误     |
| 0x80001006 | WAL checksum mismatch | 场景：发生了 WAL 文件损坏        | 服务端内部错误     |
| 0x80001007 | WAL log incomplete    | 日志文件发生了丢失或损坏         | 服务端内部错误     |

#### tfs

| 错误码     | 错误描述                         | 可能的出错场景或者可能的原因         | 建议用户采取的措施                       |
| ---------- | -------------------------------- | ------------------------------------ | ---------------------------------------- |
| 0x80002201 | TFS invalid configuration        | 多级存储配置错误                     | 检查配置是否正确                         |
| 0x80002202 | TFS too many disks on one level  | 多级存储配置错误                     | 检查一级硬盘上的配置个数是否超过最大限制 |
| 0x80002203 | TFS duplicate primary mount disk | 多级存储配置错误                     | 检查配置是否正确                         |
| 0x80002204 | TFS no primary mount disk        | 多级存储配置错误                     | 检查配置是否正确                         |
| 0x80002205 | TFS no disk mount on tire        | 多级存储配置错误                     | 检查配置是否正确                         |
| 0x80002208 | No disk available on a tier.     | TFS 内部错误，多发生在硬盘满的场景下 | 增加硬盘，扩充容量                       |

#### catalog

| 错误码     | 错误描述                         | 可能的出错场景或者可能的原因 | 建议用户采取的措施                |
| ---------- | -------------------------------- | ---------------------------- | --------------------------------- |
| 0x80002400 | catalog internal error           | catalog 内部错误             | 保留现场和日志，github 上报 issue |
| 0x80002401 | catalog invalid input parameters | catalog 输入参数错误         | 保留现场和日志，github 上报 issue |
| 0x80002402 | catalog is not ready             | catalog 未初始化完成         | 保留现场和日志，github 上报 issue |
| 0x80002403 | catalog system error             | catalog 系统错误             | 保留现场和日志，github 上报 issue |
| 0x80002404 | Database is dropped              | db 缓存被删除                | 保留现场和日志，github 上报 issue |
| 0x80002405 | catalog is out of service        | catalog 模块已经退出         | 保留现场和日志，github 上报 issue |
| 0x80002550 | Invalid msg order                | 消息顺序错误                 | 保留现场和日志，github 上报 issue |
| 0x80002501 | Job status error                 | 任务状态错误                 | 保留现场和日志，github 上报 issue |
| 0x80002502 | scheduler internal error         | scheduler 内部错误           | 保留现场和日志，github 上报 issue |
| 0x80002504 | Task timeout                     | 子任务超时                   | 保留现场和日志，github 上报 issue |
| 0x80002505 | Job is dropping                  | 任务正在或已经被取消         | 检查是否有手动或应用中断当前任务  |

#### parser

| 错误码     | 错误描述                                                                                               | 可能的出错场景或者可能的原因                            | 建议用户采取的措施                     |
| ---------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------- | -------------------------------------- |
| 0x80002600 | syntax error near                                                                                      | SQL 语法错误                                            | 检查并修正 SQL 语句                    |
| 0x80002601 | Incomplete SQL statement                                                                               | 不完整的 SQL 语句                                       | 检查并修正 SQL 语句                    |
| 0x80002602 | Invalid column name                                                                                    | 不合法或不存在的列名                                    | 检查并修正 SQL 语句                    |
| 0x80002603 | Table does not exist                                                                                   | 表不存在                                                | 检查并确认 SQL 语句中的表是否存在      |
| 0x80002604 | Column ambiguously defined                                                                             | 列名（别名）重复定义                                    | 检查并修正 SQL 语句                    |
| 0x80002605 | Invalid value type                                                                                     | 常量值非法                                              | 检查并修正 SQL 语句                    |
| 0x80002608 | There mustn't be aggregation                                                                           | 聚合函数出现在非法子句中                                | 检查并修正 SQL 语句                    |
| 0x80002609 | ORDER BY item must be the number of a SELECT-list expression                                           | Order by 指定的位置不合法                               | 检查并修正 SQL 语句                    |
| 0x8000260A | Not a GROUP BY expression                                                                              | 非法 group by 语句                                      | 检查并修正 SQL 语句                    |
| 0x8000260B | Not SELECTed expression                                                                                | 非法表达式                                              | 检查并修正 SQL 语句                    |
| 0x8000260C | Not a single-group group function                                                                      | 非法使用列与函数                                        | 检查并修正 SQL 语句                    |
| 0x8000260D | Tags number not matched                                                                                | tag 列个数不匹配                                        | 检查并修正 SQL 语句                    |
| 0x8000260E | Invalid tag name                                                                                       | 无效或不存在的 tag 名                                   | 检查并修正 SQL 语句                    |
| 0x80002610 | Value is too long                                                                                      | 值长度超出限制                                          | 检查并修正 SQL 语句或 API 参数         |
| 0x80002611 | Password too short or empty                                                                            | 密码为空或少于 8 个字符                                 | 使用合法的密码                         |
| 0x80002612 | Port should be an integer that is less than 65535 and greater than 0                                   | 端口号非法                                              | 检查并修正端口号                       |
| 0x80002613 | Endpoint should be in the format of 'fqdn:port'                                                        | 地址格式错误                                            | 检查并修正地址信息                     |
| 0x80002614 | This statement is no longer supported                                                                  | 功能已经废弃                                            | 参考功能文档说明                       |
| 0x80002615 | Interval too small                                                                                     | interval 值超过允许的最小值                             | 更改 INTERVAL 值                       |
| 0x80002616 | Database not specified                                                                                 | 未指定数据库                                            | 指定当前操作的数据库                   |
| 0x80002617 | Invalid identifier name                                                                                | ID 非法或长度不合法                                     | 检查语句中相关的库、表、列、TAG 等名称 |
| 0x80002618 | Corresponding super table not in this db                                                               | 超级表不存在                                            | 检查库中是否存在对应的超级表           |
| 0x80002619 | Invalid database option                                                                                | 数据库选项值非法                                        | 检查并修正数据库选项值                 |
| 0x8000261A | Invalid table option                                                                                   | 表选项值非法                                            | 检查并修正数据表选项值                 |
| 0x80002624 | GROUP BY and WINDOW-clause can't be used together                                                      | Group by 和窗口不能同时使用                             | 检查并修正 SQL 语句                    |
| 0x80002627 | Aggregate functions do not support nesting                                                             | 函数不支持嵌套使用                                      | 检查并修正 SQL 语句                    |
| 0x80002628 | Only support STATE_WINDOW on integer/bool/varchar column                                               | 不支持的 STATE_WINDOW 数据类型                          | 检查并修正 SQL 语句                    |
| 0x80002629 | Not support STATE_WINDOW on tag column                                                                 | 不支持 TAG 列的 STATE_WINDOW                            | 检查并修正 SQL 语句                    |
| 0x8000262A | STATE_WINDOW not support for super table query                                                         | 不支持超级表的 STATE_WINDOW                             | 检查并修正 SQL 语句                    |
| 0x8000262B | SESSION gap should be fixed time window, and greater than 0                                            | SESSION 窗口值非法                                      | 检查并修正 SQL 语句                    |
| 0x8000262C | Only support SESSION on primary timestamp column                                                       | SESSION 窗口列非法                                      | 检查并修正 SQL 语句                    |
| 0x8000262D | Interval offset cannot be negative                                                                     | INTERVAL offset 值非法                                  | 检查并修正 SQL 语句                    |
| 0x8000262E | Cannot use 'year' as offset when interval is 'month'                                                   | INTERVAL offset 单位非法                                | 检查并修正 SQL 语句                    |
| 0x8000262F | Interval offset should be shorter than interval                                                        | INTERVAL offset 值非法                                  | 检查并修正 SQL 语句                    |
| 0x80002630 | Does not support sliding when interval is natural month/year                                           | sliding 单位非法                                        | 检查并修正 SQL 语句                    |
| 0x80002631 | sliding value no larger than the interval value                                                        | sliding 值非法                                          | 检查并修正 SQL 语句                    |
| 0x80002632 | sliding value can not less than 1%% of interval value                                                  | sliding 值非法                                          | 检查并修正 SQL 语句                    |
| 0x80002633 | Only one tag if there is a json tag                                                                    | 只支持单个 JSON TAG 列                                  | 检查并修正 SQL 语句                    |
| 0x80002634 | Query block has incorrect number of result columns                                                     | 列个数不匹配                                            | 检查并修正 SQL 语句                    |
| 0x80002635 | Incorrect TIMESTAMP value                                                                              | 主键时间戳列值非法                                      | 检查并修正 SQL 语句                    |
| 0x80002637 | soffset/offset can not be less than 0                                                                  | soffset/offset 值非法                                   | 检查并修正 SQL 语句                    |
| 0x80002638 | slimit/soffset only available for PARTITION/GROUP BY query                                             | slimit/soffset 只支持 PARTITION BY/GROUP BY 语句        | 检查并修正 SQL 语句                    |
| 0x80002639 | Invalid topic query                                                                                    | 不支持的 TOPIC 查询语法                                 |
| 0x8000263A | Cannot drop super table in batch                                                                       | 不支持批量删除超级表                                    | 检查并修正 SQL 语句                    |
| 0x8000263B | Start(end) time of query range required or time range too large                                        | 窗口个数超出限制                                        | 检查并修正 SQL 语句                    |
| 0x8000263C | Duplicated column names                                                                                | 列名称重复                                              | 检查并修正 SQL 语句                    |
| 0x8000263D | Tags length exceeds max length                                                                         | TAG 值长度超出最大支持范围                              | 检查并修正 SQL 语句                    |
| 0x8000263E | Row length exceeds max length                                                                          | 行长度检查并修正 SQL 语句                               | 检查并修正 SQL 语句                    |
| 0x8000263F | Illegal number of columns                                                                              | 列个数错误                                              | 检查并修正 SQL 语句                    |
| 0x80002640 | Too many columns                                                                                       | 列个数超出上限                                          | 检查并修正 SQL 语句                    |
| 0x80002641 | First column must be timestamp                                                                         | 第一列必须是主键时间戳列                                | 检查并修正 SQL 语句                    |
| 0x80002642 | Invalid binary/nchar column/tag length                                                                 | binary/nchar 长度错误                                   | 检查并修正 SQL 语句                    |
| 0x80002643 | Invalid number of tag columns                                                                          | TAG 列个数错误                                          | 检查并修正 SQL 语句                    |
| 0x80002644 | Permission denied                                                                                      | 权限错误                                                | 检查确认用户是否有相应操作权限         |
| 0x80002645 | Invalid stream query                                                                                   | 非法流语句                                              | 检查并修正 SQL 语句                    |
| 0x80002646 | Invalid _c0 or_rowts expression                                                                        | _c0 或_rowts 非法使用                                   | 检查并修正 SQL 语句                    |
| 0x80002647 | Invalid timeline function                                                                              | 函数依赖的主键时间戳不存在                              | 检查并修正 SQL 语句                    |
| 0x80002648 | Invalid password                                                                                       | 密码不符合规范                                          | 检查并修改密码                         |
| 0x80002649 | Invalid alter table statement                                                                          | 修改表语句不合法                                        | 检查并修正 SQL 语句                    |
| 0x8000264A | Primary timestamp column cannot be dropped                                                             | 主键时间戳列不允许删除                                  | 检查并修正 SQL 语句                    |
| 0x8000264B | Only binary/nchar column length could be modified, and the length can only be increased, not decreased | 非法列修改                                              | 检查并修正 SQL 语句                    |
| 0x8000264C | Invalid tbname pseudo column                                                                           | 非法使用 tbname 列                                      | 检查并修正 SQL 语句                    |
| 0x8000264D | Invalid function name                                                                                  | 非法函数名                                              | 检查并修正函数名                       |
| 0x8000264E | Comment too long                                                                                       | 注释长度超限                                            | 检查并修正 SQL 语句                    |
| 0x8000264F | Function(s) only allowed in SELECT list, cannot mixed with non scalar functions or columns             | 非法的函数混用                                          | 检查并修正 SQL 语句                    |
| 0x80002650 | Window query not supported, since no valid timestamp column included in the result of subquery         | 窗口查询依赖的主键时间戳列不存在                        | 检查并修正 SQL 语句                    |
| 0x80002651 | No columns can be dropped                                                                              | 必须的列不能被删除                                      | 检查并修正 SQL 语句                    |
| 0x80002652 | Only tag can be json type                                                                              | 普通列不支持 JSON 类型                                  | 检查并修正 SQL 语句                    |
| 0x80002655 | The DELETE statement must have a definite time window range                                            | DELETE 语句中存在非法 WHERE 条件                        | 检查并修正 SQL 语句                    |
| 0x80002656 | The REDISTRIBUTE VGROUP statement only support 1 to 3 dnodes                                           | REDISTRIBUTE VGROUP 指定的 DNODE 个数非法               | 检查并修正 SQL 语句                    |
| 0x80002657 | Fill now allowed                                                                                       | 函数不允许 FILL 功能                                    | 检查并修正 SQL 语句                    |
| 0x80002658 | Invalid windows pc                                                                                     | 非法使用窗口伪列                                        | 检查并修正 SQL 语句                    |
| 0x80002659 | Window not allowed                                                                                     | 函数不能在窗口中使用                                    | 检查并修正 SQL 语句                    |
| 0x8000265A | Stream not allowed                                                                                     | 函数不能在流计算中使用                                  | 检查并修正 SQL 语句                    |
| 0x8000265B | Group by not allowed                                                                                   | 函数不能在分组中使用                                    | 检查并修正 SQL 语句                    |
| 0x8000265D | Invalid interp clause                                                                                  | 非法 INTERP 或相关语句                                  | 检查并修正 SQL 语句                    |
| 0x8000265E | Not valid function ion window                                                                          | 非法窗口语句                                            | 检查并修正 SQL 语句                    |
| 0x8000265F | Only support single table                                                                              | 函数只支持在单表查询中使用                              | 检查并修正 SQL 语句                    |
| 0x80002660 | Invalid sma index                                                                                      | 非法创建 SMA 语句                                       | 检查并修正 SQL 语句                    |
| 0x80002661 | Invalid SELECTed expression                                                                            | 无效查询语句                                            | 检查并修正 SQL 语句                    |
| 0x80002662 | Fail to get table info                                                                                 | 获取表元数据信息失败                                    | 保留现场和日志，github 上报 issue      |
| 0x80002663 | Not unique table/alias                                                                                 | 表名（别名）冲突                                        | 检查并修正 SQL 语句                    |
| 0x80002664 | Join requires valid time series input                                                                  | 不支持子查询不含主键时间戳列输出的 JOIN 查询            | 检查并修正 SQL 语句                    |
| 0x80002665 | The _TAGS pseudo column can only be used for subtable and supertable queries                           | 非法 TAG 列查询                                         | 检查并修正 SQL 语句                    |
| 0x80002666 | 子查询不含主键时间戳列输出                                                                             | 检查并修正 SQL 语句                                     |
| 0x80002667 | Invalid usage of expr: %s                                                                              | 非法表达式                                              | 检查并修正 SQL 语句                    |
| 0x80002687 | True_for duration cannot be negative                                                                   | true_for 的值不能是负数                                 | 检查并修正 SQL 语句                    |
| 0x80002688 | Cannot use 'year' or 'month' as true_for duration                                                      | 不能使用 n(月), y(年) 作为 true_for 的时间单位          | 检查并修正 SQL 语句                    |
| 0x80002689 | Invalid using cols function                                                                            | cols 函数使用错误                                       | 检查并修正 SQL 语句                    |
| 0x8000268A | Cols function's first param must be a select function that output a single row                         | cols 函数第一个参数应该为选择函数                       | 检查并修正 SQL 语句                    |
| 0x8000268B | Invalid using alias for cols function                                                                  | cols 函数输出列重命名错误                               | 检查并修正 SQL 语句                    |
| 0x8000268C | Join primary key col must be timestamp type                                                            | 关联查询主键列等值条件类型错误                          | 检查并修正 SQL 语句                    |
| 0x8000268D | Invalid virtual table's ref column                                                                     | 创建/更新虚拟表时数据源列不正确                         | 检查并修正 SQL 语句                    |
| 0x8000268E | Invalid table type                                                                                     | 表类型不正确                                            | 检查并修正 SQL 语句                    |
| 0x8000268F | Invalid ref column type                                                                                | 虚拟表列的数据类型与数据源的数据类型不同                | 检查并修正 SQL 语句                    |
| 0x80002690 | Create child table using virtual super table                                                           | 创建非虚拟子表 USING 了虚拟超级表                       | 检查并修正 SQL 语句                    |
| 0x80002696 | Invalid sliding offset                                                                                 | sliding 窗口偏移量非法                                  | 检查并修正 SQL 语句                    |
| 0x80002697 | Invalid interval offset                                                                                | interval 窗口偏移量非法                                 | 检查并修正 SQL 语句                    |
| 0x80002698 | Invalid extend value                                                                                   | extend 参数非法                                         | 检查并修正 SQL 语句                    |
| 0x800026FF | Parser internal error                                                                                  | 解析器内部错误                                          | 保留现场和日志，github 上报 issue      |
| 0x80002700 | Planner internal error                                                                                 | 计划期内部错误                                          | 保留现场和日志，github 上报 issue      |
| 0x80002701 | Expect ts equal                                                                                        | JOIN 条件校验失败                                       | 保留现场和日志，github 上报 issue      |
| 0x80002702 | Cross join not support                                                                                 | 不支持 CROSS JOIN                                       | 检查并修正 SQL 语句                    |
| 0x80002704 | Planner slot key not found                                                                             | 生成物理计划时查找不到 slotId                           | 保留现场和日志，github 上报 issue      |
| 0x80002705 | Planner invalid table type                                                                             | 计划器生成计划时得到了错误的表类型                      | 保留现场和日志，github 上报 issue      |
| 0x80002706 | Planner invalid query control plan type                                                                | 计划器生成 dynamic query control 计划时得到的类型不正确 | 保留现场和日志，github 上报 issue      |

#### function

| 错误码     | 错误描述                                     | 可能的出错场景或者可能的原因                                                                                                                                                                                                                                                          | 建议用户采取的措施                                                                                           |
| ---------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| 0x80002800 | Function internal error                      | 函数参数输入不合理造成的错误，随错误码会返回具体错误描述信息。比如 APERCENTILE 函数第三个参数指定算法时只能使用字符串"default"                                                                                                                                                        | "t-digest", 使用其他输入会报此类错误。或者 TO_ISO8601 函数第二个参数指定时区时，字符串不符合时区格式规范等。 | 根据具体错误描述信息，调整函数输入。 |
| 0x80002801 | Invalid function para number                 | 函数输入参数个数不正确。函数规定必须要使用 n 个参数，而用户给定参数个数不为 n。比如 COUNT(col1, col2)。                                                                                                                                                                               | 调整函数输入参数为正确个数。                                                                                 |
| 0x80002802 | Invalid function para type                   | 函数输入参数类型不正确。函数输入参数要求为数值类型，但是用户所给参数为字符串。比如 SUM("abc")。                                                                                                                                                                                       | 调整函数参数输入为正确类型                                                                                   |
| 0x80002803 | Invalid function para value                  | 函数输入参数取值不正确。函数输入参数范围不正确。比如 SAMPLE 函数第二个参数指定采样个数范围为 [1, 1000], 如果不在这个范围内会会报错。                                                                                                                                                  | 调整函数参数输入为正确取值。                                                                                 |
| 0x80002804 | Not builtin function                         | 函数非内置函数。内置函数不在的哈希表中会报错，用户应该很少遇见这个问题，否则是内部内置函数哈希初始化的时候出错或者写坏。                                                                                                                                                              | 客户应该不会遇到，如果遇到，说明程序有 bug，咨询开发人员。                                                   |
| 0x80002805 | Duplicate timestamps not allowed in function | 函数输入主键列有重复时间戳。对某些依赖时间线顺序函数做超级表查询时，所有子表数据会按照时间戳进行排序后合并为一条时间线进行计算，因此子表合并后的时间戳可能会出现重复，导致某些计算没有意义而报错。涉及到的函数有：CSUM、DERIVATIVE、DIFF、IRATE、MAVG、STATECOUNT、STATEDURATION、TWA | 如果需要对超级表查询并且使用这些依赖时间线顺序函数时，确保子表中不存在重复时间戳数据。                       |

#### udf

| 错误码     | 错误描述                           | 可能的出错场景或者可能的原因                                                           | 建议用户采取的措施                              |
| ---------- | ---------------------------------- | -------------------------------------------------------------------------------------- | ----------------------------------------------- |
| 0x80002901 | udf is stopping                    | dnode 退出时，收到 udf 调用                                                            | 停止执行 udf 查询                               |
| 0x80002902 | udf pipe read error                | taosd 读取 udfd pipe，发生错误                                                         | udfd 异常退出，1. c udf 崩溃 2. udfd 崩溃       |
| 0x80002903 | udf pipe connect error             | taosd 建立到 udfd 的管道连接时，发生错误                                               | 1. taosd 对应的 udfd 未启动。重启 taosd         |
| 0x80002904 | udf pip not exist                  | udf 建立，调用，拆除三个阶段，两个阶段中间发生连接错误，导致连接消失，后续阶段继续执行 | udfd 异常退出，1. c udf 崩溃 2. udfd 崩溃       |
| 0x80002905 | udf load failure                   | udfd 加载 udf 时错误                                                                   | 1.mnode 中 udf 不存在 2. udf 加载出错。查看日志 |
| 0x80002906 | udf invalid function input         | udf 检查输入                                                                           | udf 函数不接受输入，如输入列类型错误            |
| 0x80002907 | udf invalid bufsize                | udf 聚合函数中间结果大于创建 udf 中指定的 bufsize                                      | 增大 bufSize，或者降低中间结果大小              |
| 0x80002908 | udf invalid output type            | udf 输出的类型和创建 udf 中指定的类型                                                  | 修改 udf，或者创建 udf 的类型，使得结果相同     |
| 0x80002909 | udf program language not supported | udf 编程语言不支持                                                                     | 使用支持的语言，当前支持 c、python              |
| 0x8000290A | udf function execution failure     | udf 函数执行错误，如返回错误的行数                                                     | 具体查看错误日志                                |

#### sml

| 错误码     | 错误描述                         | 可能的出错场景或者可能的原因                    | 建议用户采取的措施                                               |
| ---------- | -------------------------------- | ----------------------------------------------- | ---------------------------------------------------------------- |
| 0x80003000 | Invalid line protocol type       | schemaless 接口传入的协议非法                   | 检查传入的协议是否为 taos.h 中定位的三种 TSDB_SML_PROTOCOL_TYPE  |
| 0x80003001 | Invalid timestamp precision type | schemaless 接口传入的时间精度非法               | 检查传入的协议是否为 taos.h 中定位的七种 TSDB_SML_TIMESTAMP_TYPE |
| 0x80003002 | Invalid data format              | schemaless 接口传入的数据格式非法               | 具体查看 client 端的错误日志提示                                 |
| 0x80003004 | Not the same type as before      | schemaless 数据一批的多行数据里相同列类型不一致 | 检测数据里每行相同列的数据类型是否一致                           |
| 0x80003005 | Internal error                   | schemaless 内部逻辑错误，一般不会出现           | 具体查看 client 端的错误日志提示                                 |

#### sma

| 错误码     | 错误描述                                                       | 可能的出错场景或者可能的原因                                 | 建议用户采取的措施                                 |
| ---------- | -------------------------------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| 0x80003100 | Tsma init failed                                               | TSMA 环境初始化失败                                          | 检查错误日志，联系开发处理                         |
| 0x80003101 | Tsma already exists                                            | TSMA 重复创建                                                | 避免重复创建                                       |
| 0x80003102 | Invalid tsma env                                               | TSMA 运行环境异常                                            | 检查错误日志，联系开发处理                         |
| 0x80003103 | Invalid tsma state                                             | 流计算下发结果的 vgroup 与创建 TSMA index 的 vgroup 不一致   | 检查错误日志，联系开发处理                         |
| 0x80003104 | Invalid tsma pointer                                           | 在处理写入流计算下发的结果，消息体为空指针。                 | 检查错误日志，联系开发处理                         |
| 0x80003105 | Invalid tsma parameters                                        | 在处理写入流计算下发的结果，结果数量为 0。                   | 检查错误日志，联系开发处理                         |
| 0x80003113 | Tsma optimization cannot be applied with INTERVAL AUTO offset. | 当前查询条件下使用 INTERVAL AUTO OFFSET 无法启用 tsma 优化。 | 使用 SKIP_TSMA Hint 或者手动指定 INTERVAL OFFSET。 |
| 0x80003150 | Invalid rsma env                                               | Rsma 执行环境异常。                                          | 检查错误日志，联系开发处理                         |
| 0x80003151 | Invalid rsma state                                             | Rsma 执行状态异常。                                          | 检查错误日志，联系开发处理                         |
| 0x80003152 | Rsma qtaskinfo creation error                                  | 创建流计算环境异常。                                         | 检查错误日志，联系开发处理                         |
| 0x80003153 | Rsma invalid schema                                            | 启动恢复时元数据信息错误                                     | 检查错误日志，联系开发处理                         |
| 0x80003154 | Rsma stream state open                                         | 打开流算子状态存储失败                                       | 检查错误日志，联系开发处理                         |
| 0x80003155 | Rsma stream state commit                                       | 提交流算子状态存储失败                                       | 检查错误日志，联系开发处理                         |
| 0x80003156 | Rsma fs ref error                                              | 算子文件引用计数错误                                         | 检查错误日志，联系开发处理                         |
| 0x80003157 | Rsma fs sync error                                             | 算子文件同步失败                                             | 检查错误日志，联系开发处理                         |
| 0x80003158 | Rsma fs update error                                           | 算子文件更新失败                                             | 检查错误日志，联系开发处理                         |

#### index

| 错误码     | 错误描述         | 可能的出错场景或者可能的原因                                         | 建议用户采取的措施         |
| ---------- | ---------------- | -------------------------------------------------------------------- | -------------------------- |
| 0x80003200 | INDEX 正在重建中 | 1.写入过快，导致 index 的合并线程处理不过来 2.索引文件损坏，正在重建 | 检查错误日志，联系开发处理 |
| 0x80003201 | 索引文件损坏     | 文件损坏                                                             | 检查错误日志，联系开发处理 |

#### tmq

| 错误码     | 错误描述                                     | 可能的出错场景或者可能的原因                                                         | 建议用户采取的措施                 |
| ---------- | -------------------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------- |
| 0x800003E6 | Consumer not exist                           | Consumer 超时下线                                                                    | 重新建 consumer 订阅数据           |
| 0x800003EA | Consumer not ready                           | Consumer 正在平衡中                                                                  | 等待 2 秒后重试                    |
| 0x80004000 | Invalid message                              | 订阅到的数据非法，一般不会出现                                                       | 具体查看 client 端的错误日志提示   |
| 0x80004001 | Consumer mismatch                            | 订阅请求的 vnode 和重新分配的 vnode 不一致，一般存在于有新消费者加入相同消费者组里时 | 内部错误                           |
| 0x80004002 | Consumer closed                              | 消费者已经不存在了                                                                   | 查看是否已经 close 掉了            |
| 0x80004017 | Invalid status, please subscribe topic first | 数据订阅状态不对                                                                     | 没有调用 subscribe，直接 poll 数据 |
| 0x80004100 | Stream task not exist                        | 流计算任务不存在                                                                     | 具体查看 server 端的错误日志       |

#### virtual table

| 错误码     | 错误描述                                                                    | 可能的出错场景或者可能的原因                                                   | 建议用户采取的措施                             |
| ---------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------- |
| 0x80006200 | Virtual table scan 算子内部错误                                             | virtual table scan 算子内部逻辑错误，一般不会出现                              | 具体查看 client 端的错误日志提示               |
| 0x80006201 | Virtual table scan invalid downstream operator type                         | 由于生成的执行计划不对，导致 virtual table scan 算子的下游算子类型不正确       | 保留 explain 执行计划，联系开发处理            |
| 0x80006202 | Virtual table prim timestamp column should not has ref                      | 虚拟表的时间戳主键列不应该有数据源，如果有，后续查询虚拟表的时候就会出现该错误 | 检查错误日志，联系开发处理                     |
| 0x80006203 | Create virtual child table must use virtual super table                     | 虚拟子表必须建在虚拟超级表下，否则就会出现该错误                               | 创建虚拟子表的时候，USING 虚拟超级表           |
| 0x80006204 | Virtual table not support decimal type                                      | 虚拟表不支持 decimal 类型                                                      | 创建虚拟表时不使用 decimal 类型的列/tag        |
| 0x80006205 | Virtual table not support in STMT query and STMT insert                     | 不支持在 stmt 写入和查询中使用虚拟表                                           | 不在 stmt 写入和查询中使用虚拟表               |
| 0x80006206 | Virtual table not support in Topic                                          | 不支持在订阅中使用虚拟表                                                       | 不在订阅中使用虚拟表                           |
| 0x80006207 | Virtual super table query not support origin table from different databases | 虚拟超级表不支持子表的数据源来自不同的数据库                                   | 确保虚拟超级表的子表的数据源都来自同一个数据库 |

#### TDgpt

| 错误码     | 错误描述                                            | 可能的出错场景或者可能的原因 | 建议用户采取的措施                           |
| ---------- | --------------------------------------------------- | ---------------------------- | -------------------------------------------- |
| 0x80000440 | Analysis service response is NULL                   | 分析服务返回错误             | 检查服务端日志确认返回信息是否正确           |
| 0x80000441 | Analysis service can't access                       | 分析服务无法使用             | 检查 anoded 服务是否可用                     |
| 0x80000442 | Analysis algorithm is missing                       | 未指定分析算法名称           | 增加算法名称                                 |
| 0x80000443 | Analysis algorithm not loaded                       | 指定算法未加载               | 指定算法未加载                               |
| 0x80000444 | Analysis invalid buffer type                        | 缓存数据格式不对             | 具体查看 server 端的错误日志                 |
| 0x80000445 | Analysis failed since anode return error            | anode 返回错误信息           | 请检查服务端日志确认问题原因                 |
| 0x80000446 | Analysis failed since too many input rows for anode | 输入数据太多                 | 减小分析数据输入规模                         |
| 0x80000447 | white-noise data not processed                      | 白噪声数据不分析             |                                              |
| 0x80000448 | Analysis internal error, not processed              | anode 出现内部错误           | 具体查看 server 端的日志 (taosanode.app.log) |
| 0x80000449 | Analysis failed since not enough rows               | 预测分析输入数据行数太少     | 增加输入数据规模（预测至少 10 行记录）       |
| 0x8000044A | Not support co-variate/multi-variate forecast       | 不支持协变量/多变量预测      | 更换使用的预测模型                           |


#### STREAM

| 错误码     | 错误描述                              | 可能的出错场景或者可能的原因             | 建议用户采取的措施                                     |
| ---------- | ------------------------------------- | ---------------------------------------- | ------------------------------------------------------ |
| 0x80007007 | Snode still in use with streams       | SNode 正在使用，无法删除                 | 检查 SNode 的流使用情况，确认是否继续删除              |
| 0x8000700E | Db used by stream                     | DB 正被某个流使用，不能删除              | 检查流对 DB 的使用情况                                 |
| 0x80007014 | Stream output table name too long     | 输出表名超长                             | 检查建流语句中输出表名规则是否正确，是否结果超长       |
| 0x80007016 | Stream output table name calc failed  | 输出表名计算失败                         | 检查建流语句中输出表名规则是否正确，是否有 NULL 值存在 |
| 0x80007017 | Stream vtable calculate need redeploy | 流计算语句中的虚拟表的原始表分布发生变更 | 流会自动处理该错误，无需处理                           |

## 连接器
下面是各语言连接器自身的错误码。连接器除了返回自身错误码外，也会返回上文中 TSDB 的错误码。

### C

在 C 接口的设计中，错误码采用整数类型表示，每个错误码都对应一个特定的错误状态。如未特别说明，当 API 的返回值是整数时，_0_ 代表成功，其他是代表失败原因的错误码，当返回值是指针时，_NULL_ 表示失败。  
C 连接器的错误码分为两种：  
- 通用错误码  
    所有的错误码以及对应的原因描述在 `taoserror.h` 文件中。
    详细的错误码说明参考：[TSDB 错误码](./#tsdb)
- WebSocket 连接错误码  
    除通用错误码外，WebSocket 连接还有以下错误码：

    | 错误码 | 错误描述   | 可能的出错场景或者可能的原因 | 建议用户采取的措施                      |
    | ------ | ---------- | ---------------------------- | --------------------------------------- |
    | 0xE000 | DSN 错误   | DSN 不符合规范               | 检查 DSN 字符串是否符合规范             |
    | 0xE001 | 内部错误   | 不确定                       | 保留现场和日志，在 GitHub 上报告 issue  |
    | 0xE002 | 连接关闭   | 网络断开                     | 请检查网络状况，查看 `taosadapter` 日志 |
    | 0xE003 | 发送超时   | 网络断开                     | 请检查网络状况                          |
    | 0xE004 | 接收超时   | 慢查询或网络断开             | 排查 `taosadapter` 日志                 |
    | 0xE005 | I/O 错误   | 网络 I/O 异常或磁盘错误      | 检查网络连接和磁盘状态                  |
    | 0xE006 | 认证失败   | 用户名密码错误或权限不足     | 检查用户名密码，确认用户权限            |
    | 0xE007 | 编解码错误 | 数据编解码异常               | 检查数据格式，排查 `taosadapter` 日志   |
    | 0xE008 | 连接断开   | WebSocket 连接断开           | 检查网络状况，重新建立连接              |
        
### Java

Java 连接器可能报错的错误码包括 4 种：

- JDBC driver 本身的报错（错误码在 0x2301 到 0x2350 之间）
- 原生连接方法的报错（错误码在 0x2351 到 0x2360 之间）
- 数据订阅的报错（错误码在 0x2371 到 0x2380 之间）
- TDengine TSDB 其他功能模块的报错，请参考本页面 TSDB 错误码。   

具体的错误码请参考：

| 错误码 | 错误描述                                                        | 可能的出错场景或者可能的原因                   | 建议用户采取的措施                                                                       |
| ------ | --------------------------------------------------------------- | ---------------------------------------------- | ---------------------------------------------------------------------------------------- |
| 0x2301 | connection already closed                                       | 连接已经关闭                                   | 检查连接情况，或重新创建连接去执行相关指令                                               |
| 0x2302 | this operation is NOT supported currently!                      | 调用了不支持的接口                             | 当前使用接口不支持，可以更换其他连接方式                                                 |
| 0x2303 | invalid variables                                               | 参数不合法                                     | 请检查相应接口规范，调整参数类型及大小                                                   |
| 0x2304 | statement is closed                                             | statement 已经关闭                             | 请检查 statement 是否关闭后再次使用，或是连接是否正常                                    |
| 0x2305 | resultSet is closed                                             | resultSet 结果集已经释放                       | 请检查 resultSet 是否释放后再次使用                                                      |
| 0x2306 | Batch is empty!                                                 | prepareStatement 没有绑定参数                  | prepareStatement 添加参数后再执行 executeBatch                                           |
| 0x2307 | Can not issue data manipulation statements with executeQuery()  | api 调用错误                                   | 更新操作应该使用 executeUpdate()，而不是 executeQuery()                                  |
| 0x2308 | Can not issue SELECT via executeUpdate()                        | api 调用错误                                   | 查询操作应该使用 executeQuery()，而不是 executeUpdate()                                  |
| 0x230D | parameter index out of range                                    | 参数越界                                       | 请检查参数的合理范围                                                                     |
| 0x230E | connection already closed                                       | 连接已经关闭                                   | 请检查 Connection 是否关闭后再次使用，或是连接是否正常                                   |
| 0x230F | unknown sql type in tdengine                                    | 不支持的数据类型                               | 请检查 TDengine TSDB 支持的 Data Type 类型                                               |
| 0x2310 | can't register JDBC-JNI driver                                  | 不能注册 JNI 驱动                              | 请检查 url 是否填写正确                                                                  |
| 0x2314 | numeric value out of range                                      | 数字类型超出范围                               | 请检查获取结果集中数值类型是否使用了正确的接口                                           |
| 0x2315 | unknown taos type in tdengine                                   | 参数传递错误                                   | 在 TDengine TSDB 数据类型与 JDBC 数据类型转换时，是否指定了正确的 TDengine TSDB 数据类型 |
| 0x2319 | user is required                                                | 参数缺少用户名                                 | 创建连接时缺少用户名信息                                                                 |
| 0x231A | password is required                                            | 参数缺少密码                                   | 创建连接时缺少密码信息                                                                   |
| 0x231D | can't create connection with server within                      | 连接失败                                       | 请检查与 taosAdapter 之间的连接情况                                                      |
| 0x231E | failed to complete the task within the specified time           | 请求处理超时                                   | 通过增加参数 messageWaitTimeout 增加执行耗时，或是请检查与 taosAdapter 之间的连接情况    |
| 0x2320 | type convert exception                                          | 类型转换错误                                   | 检查是否没有使用正确的类型                                                               |
| 0x2321 | TDengine TSDB version incompatible                              | 使用了原生连接，且客户端驱动版本与服务端不一致 | TDengine TSDB 版本不匹配，请升级至对应版本。 或者采用 WebSocket 连接方式                 |
| 0x2322 | resource has been freed                                         | 资源已经释放                                   | 资源已经释放，请确认操作正确                                                             |
| 0x2323 | BLOB is unsupported on the server                               | 服务端版本低                                   | 服务端不支持 BLOB 类型，需要升级                                                         |
| 0x2324 | line bind mode is unsupported on the server                     | 服务端版本低                                   | 服务端不支持行绑定模式，需要升级                                                         |
| 0x2350 | unknown error                                                   | 未知异常                                       | 未知异常，请在 github 反馈给开发人员                                                     |
| 0x2352 | Unsupported encoding                                            | 不支持的字符集                                 | 本地连接下指定了不支持的字符编码集                                                       |
| 0x2353 | internal error of database, please see taoslog for more details | 本地连接执行 prepareStatement 时出现错误       | 请检查 taos log 进行问题定位                   |
| 0x2354 | JNI connection is NULL                                          | 连接已经关闭                                   | 本地连接执行命令时，Connection 已经关闭。请检查与 TDengine TSDB 的连接情况               |
| 0x2355 | JNI result set is NULL                                          | 结果集已经关闭                                 | 本地连接获取结果集，结果集异常，请检查连接情况，并重试                                   |
| 0x2356 | invalid num of fields                                           | 结果集列不匹配                                 | 本地连接获取结果集的 meta 信息不匹配                                                     |
| 0x2357 | empty sql string                                                | sql 为空                                       | 填写正确的 SQL 进行执行                                                                  |
| 0x2359 | JNI alloc memory failed, please see taoslog for more details    | 内存不足                                       | 本地连接分配内存错误，请检查 taos log 进行问题定位                                       |
| 0x2371 | consumer properties must not be null!                           | 订阅参数为空                                   | 创建订阅时参数为空，请填写正确的参数                                                     |
| 0x2372 | configs contain empty key, failed to set consumer property      | 订阅参数 key 中包含空值                        | 参数 key 中包含空值，请填写正确的参数                                                    |
| 0x2373 | failed to set consumer property,                                | 订阅参数 value 中包含空值                      | 参数 value 中包含空值，请填写正确的参数                                                  |
| 0x2375 | topic reference has been destroyed                              | topic 引用失效                                 | 创建数据订阅过程中，topic 引用被释放，请检查与 TDengine TSDB 的连接情况                  |
| 0x2376 | failed to set consumer topic, topic name is empty               | topic 为空                                     | 创建数据订阅过程中，订阅 topic 名称为空，请检查指定的 topic 名称是否填写正确             |
| 0x2377 | consumer reference has been destroyed                           | 消费者引用已失效                               | 订阅数据传输通道已经关闭，请检查与 TDengine TSDB 的连接情况                              |
| 0x2378 | consumer create error                                           | 创建数据订阅失败                               | 请根据错误信息检查 taos log 进行问题定位                                                 |
| 0x2379 | seek offset must not be a negative number                       | 参数错误                                       | seek 接口参数不能为负值，请使用正确的参数                                                |
| 0x237A | vGroup not found in result set                                  | vGroup 没有分配给当前 consumer                 | 由于 Rebalance 机制导致 consumer 与 vGroup 不是绑定的关系                                |
| 0x2390 | background thread write error in Efficient Writing              | 高效写入后台线程写入错误                       | 可以停止写入，重建连接                                                                   |

- [TDengine TSDB Java Connector Error Code](https://github.com/taosdata/taos-connector-jdbc/blob/main/src/main/java/com/taosdata/jdbc/TSDBErrorNumbers.java)


### Rust

| 错误码 | 错误描述   | 可能的出错场景或者可能的原因 | 建议用户采取的措施                      |
| ------ | ---------- | ---------------------------- | --------------------------------------- |
| 0xE000 | DSN 错误   | DSN 不符合规范               | 检查 DSN 字符串是否符合规范             |
| 0xE001 | 内部错误   | 不确定                       | 保留现场和日志，在 GitHub 上报告 issue  |
| 0xE002 | 连接关闭   | 网络断开                     | 请检查网络状况，查看 `taosadapter` 日志 |
| 0xE003 | 发送超时   | 网络断开                     | 请检查网络状况                          |
| 0xE004 | 接收超时   | 慢查询或网络断开             | 排查 `taosadapter` 日志                 |
| 0xE005 | I/O 错误   | 网络 I/O 异常或磁盘错误      | 检查网络连接和磁盘状态                  |
| 0xE006 | 认证失败   | 用户名密码错误或权限不足     | 检查用户名密码，确认用户权限            |
| 0xE007 | 编解码错误 | 数据编解码异常               | 检查数据格式，排查 `taosadapter` 日志   |
| 0xE008 | 连接断开   | WebSocket 连接断开           | 检查网络状况，重新建立连接              |


### Node.js

在调用连接器 api 报错后，通过 try catch 可以获取到错误的信息和错误码。

错误说明：Node.js 连接器错误码在 100 到 120 之间，之外的错误为 TDengine TSDB 其他功能模块的报错。

具体的连接器错误码请参考：

| 错误码 | 错误描述                                                                | 可能的出错场景或者可能的原因                   | 建议用户采取的措施                                                               |
| ------ | ----------------------------------------------------------------------- | ---------------------------------------------- | -------------------------------------------------------------------------------- |
| 100    | invalid variables                                                       | 参数不合法                                     | 请检查相应接口规范，调整参数类型及大小。                                         |
| 101    | invalid url                                                             | url 错误                                       | 请检查 url 是否填写正确。                                                        |
| 102    | received server data but did not find a callback for processing         | 接收到服务端数据但没有找到上层回调             | 检查网络环境                                                                     |
| 103    | invalid message type                                                    | 接收到的消息类型无法识别                       | 请检查服务端是否正常。                                                           |
| 104    | connection creation failed                                              | 连接创建失败                                   | 请检查网络是否正常。                                                             |
| 105    | websocket request timeout                                               | 请求超时                                       | 请检查网络和 taosAdapter 服务是否正常。                                          |
| 106    | authentication fail                                                     | 认证参数不正确                                       | 请检查用户名，密码是否正确。                                                     |
| 107    | unknown sql type in tdengine                                            | 不支持的数据类型                               | 请检查 TDengine TSDB 支持的 Data Type 类型。                                     |
| 108    | connection has been closed                                              | 连接已经关闭                                   | 请检查 Connection 是否关闭后再次使用，或是连接是否正常。                         |
| 109    | fetch block data parse fail                                             | 数据解析异常                                   | 获取到的查询数据，解析失败，请确认连接器跟 TDengine 服务端版本匹配。             |
| 110    | websocket connection has reached its maximum limit                      | 连接数达到上限                                 | WebSocket 连接达到上限，请上报 github issue                                                         |
| 111    | topic partitions and positions are not equal in length                  | 获取给定分区当前的偏移量的数据与主题分区不匹配 |   重新订阅                                                                               |
| 112    | version mismatch. The minimum required TDengine TSDB version is 3.3.2.0 | 版本不匹配                                     | TDengine TSDB 的版本低于 3.3.2.0 连接器不支持，用户需要升级到 3.3.2.0 以上版本。 |
- [TDengine TSDB Node.js Connector Error Code](https://github.com/taosdata/taos-connector-node/blob/main/nodejs/src/common/wsError.ts)

### C#

| 错误码 | 错误描述                         | 可能的出错场景或者可能的原因              | 建议用户采取的措施                                           |
| ------ | -------------------------------- | ----------------------------------------- | ------------------------------------------------------------ |
| 0xF001 | WebSocket Reconnection Failed    | 连接的 taosAdapter 退出导致重连失败       | 检查 taosAdapter 状态，等待一段时间后重试                    |
| 0xF002 | WebSocket Message Mismatch       | 产生重复的请求 ID                         | 关闭当前连接等待一段时间后重连，不要传递重复的请求 ID        |
| 0xF003 | WebSocket Connection Closed      | 使用已关闭的 WebSocket 连接               | 关闭当前连接使用新连接进行重试                               |
| 0xF004 | WebSocket Write Timeout          | WebSocket 写请求超时                      | 检查网络问题，调大写超时参数，关闭当前连接使用新连接进行重试 |
| 0xF005 | WebSocket Connection Failed      | 连接的 taosAdapter 退出                   | 检查 taosAdapter 状态，等待一段时间后重试                    |
| 0xF006 | WebSocket Close Message Received | 网络原因导致心跳超时 taosAdapter 关闭连接 | 检查网络问题，等待一段时间后重试                             |

### taosAdapter

| 错误码    | 错误描述                                           | 可能的出错场景或者可能的原因        | 建议用户采取的措施    |
|--------|------------------------------------------------|-----------------------|--------------|
| 0xFFFF | taosAdapter request parameter or process error | taosAdapter 请求参数或流程错误 | 根据错误消息检查错误原因 |
| 0xFFFE | taosAdapter query request exceeded the limit   | taosAdapter 查询请求超过限制  | 降低查询请求并发     |
