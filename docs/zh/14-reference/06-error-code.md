---
sidebar_label: TDengine 错误码
title: TDengine 错误码
description: TDengine 服务端的错误码列表和详细说明
---

本文中详细列举了在使用 TDengine 客户端可能得到的服务端错误码以及所要采取的相应动作。所有语言的连接器在使用原生连接方式时也会将这些错误码返回给连接器的调用者。




## rpc

| 定义                                 | 错误码     | 错误描述                                                       | 可能的出错场景或者可能的原因                                                                  | 建议用户采取的措施                                                                             |
| ------------------------------------ | ---------- | -------------------------------------------------------------- | --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| TSDB_CODE_RPC_NETWORK_UNAVAIL        | 0x8000000B | 无法正常收发请求                                               | 1. 网络不通 2. 多次重试、依然不能执行REQ                                                      | 1. 检查网络 2. 分析日志，具体原因比较复杂                                                      |
| TSDB_CODE_TIME_UNSYNCED              | 0x80000013 | 客户端和服务端之间的时间时间相差太大, 默认900s                 | 1. 客户端和服务端不在同一个时区 2. 客户端和服务端在同一个时区，但是两者的时间不同步、相差太大 | 1. 调整到同一个时区 2. 校准客户端和服务端的时间                                                |
| TSDB_CODE_RPC_FQDN_ERROR             | 0x80000015 | 无法解析FQDN                                                   | 设置了无效的fqdn                                                                              | 检查fqdn 的设置                                                                                |
| TSDB_CODE_RPC_PORT_EADDRINUSE        | 0x80000017 | 当前端口被占用                                                 | 端口P已经被某个服务占用的情况下，新启的服务依然尝试绑定端口P                                  | 1. 改动新服务的服务端口 2. 杀死之前占用端口的服务                                              |
| TSDB_CODE_RPC_BROKEN_LINK            | 0x80000018 | 由于网络抖动/ REQ 请求时间过长导致系统主动摘掉REQ 所使用的conn | 1. 网络抖动 2. REQ 请求时间过长，大于900s                                                     | 1. 设置系统的最大超时时长 2. 检查REQ的请求时长                                                 |
| TSDB_CODE_RPC_TIMEOUT                | 0x80000019 | 暂时没有用到这个错误码                                         |                                                                                               |                                                                                                |
| TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED | 0x80000020 | 多次重试之后，所有dnode 依然都链接不上                         | 1. 所有的节点都挂了 2. 有节点挂了，但是存活的节点都不是master 节点                            | 1. 查看taosd 的状态、分析taosd 挂掉的原因或者分析存活的taosd 为什么不是主                      |
| TSDB_CODE_RPC_SOMENODE_BROKEN_LINK   | 0x80000021 | 多次重试之后，所有dnode 依然都链接不上                         | 1. 网络异常 2. req请求时间太长，服务端可能发生死锁等问题。系统自己断开了链接                  | 1. 检查网络 2. 检查req 的执行时间                                                              |
| TSDB_CODE_RPC_MAX_SESSIONS           | 0x80000022 | 达到了可用链接上线。                                           | 1. 并发太高、占用链接已经到达上线。 2. 服务端的BUG，导致conn 一直不释放，                     | 1. 提高tsNumOfRpcSessions这个值。 2. tsTimeToGetAvailableConn 3. 分析服务端不释放的conn 的原因 |


## common  

| 定义                              | 错误码     | 错误描述                          | 可能的出错场景或者可能的原因           | 建议用户采取的措施                                                                                                                                 |
| --------------------------------- | ---------- | --------------------------------- | -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| TSDB_CODE_OPS_NOT_SUPPORT         | 0x80000100 | Operation not supported           | 1. 操作不被支持、不允许的场景          | 1. 检查操作是否有误，确认该功能是否被支持                                                                                                          |
| TSDB_CODE_OUT_OF_MEMORY           | 0x80000102 | Out of Memory                     | 1. 客户端或服务端内存分配失败的场景    | 1. 检查客户端、服务端内存是否充足                                                                                                                  |
| TSDB_CODE_FILE_CORRUPTED          | 0x80000104 | Data file corrupted               | 1. 存储数据文件损坏 2. udf文件无法创建 | 1. 联系涛思客户支持 2. 确认服务端对临时目录有读写创建文件权限                                                                                      |
| TSDB_CODE_REF_FULL                | 0x80000106 | too many Ref Objs                 | 无可用ref资源                          | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_REF_ID_REMOVED          | 0x80000107 | Ref ID is removed                 | 引用的ref资源已经释放                  | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_REF_INVALID_ID          | 0x80000108 | Invalid Ref ID                    | 无效ref ID                             | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_REF_NOT_EXIST           | 0x8000010A | Ref is not there                  | ref信息不存在                          | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_APP_ERROR               | 0x80000110 |                                   |                                        |                                                                                                                                                    |
| TSDB_CODE_ACTION_IN_PROGRESS      | 0x80000111 | Action in progress                | 操作进行中                             | 1. 等待操作完成 2. 根据需要取消操作 3. 当超出合理时间仍然未完成可保留现场和日志，或联系客户支持                                                    |
| TSDB_CODE_OUT_OF_RANGE            | 0x80000112 | Out of range                      | 配置参数超出允许值范围                 | 更改参数                                                                                                                                           |
| TSDB_CODE_INVALID_MSG             | 0x80000115 | Invalid message                   | 消息错误                               | 1. 检查是否存在节点间版本不一致 2. 保留现场和日志，github上报issue                                                                                 |
| TSDB_CODE_INVALID_MSG_LEN         | 0x80000116 | Invalid message len               | 消息长度错误                           | 1. 检查是否存在节点间版本不一致 2. 保留现场和日志，github上报issue                                                                                 |
| TSDB_CODE_INVALID_PTR             | 0x80000117 | Invalid pointer                   | 无效指针                               | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_INVALID_PARA            | 0x80000118 | Invalid parameters                | 无效参数                               | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_INVALID_CFG             | 0x80000119 | Invalid config option             | 无效配置                               | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_INVALID_OPTION          | 0x8000011A | Invalid option                    | 无效选项                               | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_INVALID_JSON_FORMAT     | 0x8000011B | Invalid json format               | JSON格式错误                           | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_INVALID_VERSION_NUMBER  | 0x8000011C | Invalid version number            | 无效版本格式                           | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_INVALID_VERSION_STRING  | 0x8000011D | Invalid version string            | 无效版本格式                           | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_VERSION_NOT_COMPATIBLE  | 0x8000011E | Version not compatible            | 节点间版本不兼容                       | 检查各节点版本（包括服务端与客户端），确保节点间版本一致或兼容                                                                                     |
| TSDB_CODE_CHECKSUM_ERROR          | 0x8000011F | Checksum error                    | 文件checksum校验失败                   | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_COMPRESS_ERROR          | 0x80000120 | Failed to compress msg            | 压缩失败                               | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_MSG_NOT_PROCESSED       | 0x80000121 | Message not processed             | 消息未被正确处理                       | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_CFG_NOT_FOUND           | 0x80000122 | Config not found                  | 未找到配置项                           | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_REPEAT_INIT             | 0x80000123 | Repeat initialization             | 重复初始化                             | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_DUP_KEY                 | 0x80000124 | Cannot add duplicate keys to hash | 添加重复key数据到哈希表中              | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_NEED_RETRY              | 0x80000125 | Retry needed                      | 需要应用进行重试                       | 应用按照API使用规范进行重试                                                                                                                        |
| TSDB_CODE_OUT_OF_RPC_MEMORY_QUEUE | 0x80000126 | Out of memory in rpc queue        | rpc消息队列内存使用达到上限            | 1. 检查确认系统负载是否过大 2. （如必要）通过配置rpcQueueMemoryAllowed增大rpc消息队列内存上限 3. 如果问题还未解决，保留现场和日志，github上报issue |
| TSDB_CODE_INVALID_TIMESTAMP       | 0x80000127 | Invalid timestamp format          | 时间戳格式错误                         | 检查并确认输入的时间戳格式正确                                                                                                                     |
| TSDB_CODE_MSG_DECODE_ERROR        | 0x80000128 | Msg decode error                  | 消息解码错误                           | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_NOT_FOUND               | 0x8000012A | Not found                         | 未找到内部缓存信息                     | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_NO_DISKSPACE            | 0x8000012B | Out of disk space                 | 磁盘空间不足                           | 1. 检查并确保数据目录、临时文件夹目录有足够磁盘空间 2. 定期检查维护上述目录，确保空间足够                                                          |
| TSDB_CODE_APP_IS_STARTING         | 0x80000130 | Database is starting up           | 数据库启动中，暂无法提供服务           | 检查数据库状态，待系统完成启动后继续或重试                                                                                                         |
| TSDB_CODE_APP_IS_STOPPING         | 0x80000131 | Database is closing down          | 数据库正在或已经关闭，无法提供服务     | 检查数据库状态，确保系统工作在正常状态                                                                                                             |
| TSDB_CODE_INVALID_DATA_FMT        | 0x80000132 | Invalid data format               | 数据格式错误                           | 1. 保留现场和日志，github上报issue 2. 联系涛思客户支持                                                                                             |
| TSDB_CODE_INVALID_OPERATION       | 0x80000133 | Invalid operation                 | 无效的或不支持的操作                   | 1. 修改确认当前操作为合法有效支持的操作，检查参数有效性 2. 如果问题还未解决，保留现场和日志，github上报issue                                       |
| TSDB_CODE_INVALID_VALUE           | 0x80000134 | Invalid value                     | 无效值                                 | 保留现场和日志，github上报issue                                                                                                                    |
| TSDB_CODE_INVALID_FQDN            | 0x80000135 | Invalid fqdn                      | 无效FQDN                               | 检查配置或输入的FQDN值是否正确                                                                                                                     |



## tsc

| 定义                                  | 错误码     | 错误描述                    | 可能的出错场景或者可能的原因 | 建议用户采取的措施                                                         |
| ------------------------------------- | ---------- | --------------------------- | ---------------------------- | -------------------------------------------------------------------------- |
| TSDB_CODE_TSC_INVALID_USER_LENGTH     | 0x80000207 | Invalid user name           | 数据库用户名不合法           | 检查数据库用户名是否正确                                                   |
| TSDB_CODE_TSC_INVALID_PASS_LENGTH     | 0x80000208 | Invalid password            | 数据库密码不合法             | 检查数据库密码是否正确                                                     |
| TSDB_CODE_TSC_INVALID_DB_LENGTH       | 0x80000209 | Database name too long      | 数据库名称不合法             | 检查数据库名称是否正确                                                     |
| TSDB_CODE_TSC_INVALID_TABLE_ID_LENGTH | 0x8000020A | Table name too long         | 表名不合法                   | 检查表名是否正确                                                           |
| TSDB_CODE_TSC_QUERY_CANCELLED         | 0x8000020F | Query terminated            | 查询被中止                   | 检查是否有用户中止了查询                                                   |
| TSDB_CODE_TSC_DISCONNECTED            | 0x80000213 | Disconnected from server    | 连接已中断                   | 检查连接是否被人为中断或客户端正在退出                                     |
| TSDB_CODE_TSC_SQL_SYNTAX_ERROR        | 0x80000216 | Syntax error in SQL         | SQL语法错误                  | 检查SQL语句并修正错误                                                      |
| TSDB_CODE_TSC_EXCEED_SQL_LIMIT        | 0x80000219 | SQL statement too long      | SQL长度超出限制              | 检查SQL语句并修正错误                                                      |
| TSDB_CODE_TSC_FILE_EMPTY              | 0x8000021A | File is empty               | 文件内容为空                 | 检查输入文件内容                                                           |
| TSDB_CODE_TSC_INVALID_COLUMN_LENGTH   | 0x8000021F | Invalid column length       | 列长度错误                   | 保留现场和日志，github上报issue                                            |
| TSDB_CODE_TSC_INVALID_JSON_TYPE       | 0x80000222 | Invalid JSON data type      | JSON数据类型错误             | 检查输入JSON内容                                                           |
| TSDB_CODE_TSC_VALUE_OUT_OF_RANGE      | 0x80000224 | Value out of range          | 数据大小超过类型范围         | 检查输入的数据值                                                           |
| TSDB_CODE_TSC_INVALID_INPUT           | 0x80000229 | Invalid tsc input           | API输入错误                  | 检查应用调用API时传递的参数                                                |
| TSDB_CODE_TSC_STMT_API_ERROR          | 0x8000022A | Stmt API usage error        | STMT API使用错误             | 检查STMT API调用的顺序、适用场景、错误处理                                 |
| TSDB_CODE_TSC_STMT_TBNAME_ERROR       | 0x8000022B | Stmt table name not set     | STMT未正确设置table name     | 检查是否调用了设置table name接口                                           |
| TSDB_CODE_TSC_QUERY_KILLED            | 0x8000022D | Query killed                | 查询被中止                   | 检查是否有用户中止了查询                                                   |
| TSDB_CODE_TSC_NO_EXEC_NODE            | 0x8000022E | No available execution node | 没有可用的查询执行节点       | 检查当前query policy配置，如果需要有Qnode参与确保系统中存在可用的Qnode节点 |
| TSDB_CODE_TSC_NOT_STABLE_ERROR        | 0x8000022F | Table is not a super table  | 当前语句中的表名不是超级表   | 检查当前语句中所用表名是否是超级表                                         |
| TSDB_CODE_TSC_STMT_CACHE_ERROR        | 0x80000230 | Stmt cache error            | STMT内部缓存出错             | 保留现场和日志，github上报issue                                            |
| TSDB_CODE_TSC_INTERNAL_ERROR          | 0x80000231 | Tsc internal error          | TSC内部错误                  | 保留现场和日志，github上报issue                                            |



## mnode

| 定义                                    | 错误码 | 错误描述                                                                                     | 可能的出错场景或者可能的原因                  | 建议用户采取的措施                                                                              |
| --------------------------------------- | ------ | -------------------------------------------------------------------------------------------- | --------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| TSDB_CODE_MND_NO_RIGHTS                 | 0x0303 | Insufficient privilege for operation                                                         | 无权限                                        | 赋权                                                                                            |
| TSDB_CODE_MND_INVALID_SHOWOBJ           | 0x030B | Data expired                                                                                 | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_QUERY_ID          | 0x030C | Invalid query id                                                                             | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_CONN_ID           | 0x030E | Invalid connection id                                                                        | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_USER_DISABLED             | 0x0315 | User is disabled                                                                             | 该用户不可用                                  | 赋权                                                                                            |
| TSDB_CODE_SDB_OBJ_ALREADY_THERE         | 0x0320 | Object already there                                                                         | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_INVALID_TABLE_TYPE        | 0x0322 | Invalid table type                                                                           | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_OBJ_NOT_THERE             | 0x0323 | Object not there                                                                             | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_INVALID_ACTION_TYPE       | 0x0326 | Invalid action type                                                                          | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_INVALID_DATA_VER          | 0x0328 | Invalid raw data version                                                                     | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_INVALID_DATA_LEN          | 0x0329 | Invalid raw data len                                                                         | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_INVALID_DATA_CONTENT      | 0x032A | Invalid raw data content                                                                     | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_OBJ_CREATING              | 0x032C | Object is creating                                                                           | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_SDB_OBJ_DROPPING              | 0x032D | Object is dropping                                                                           | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_DNODE_ALREADY_EXIST       | 0x0330 | Dnode already exists                                                                         | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_DNODE_NOT_EXIST           | 0x0331 | Dnode does not exist                                                                         | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_VGROUP_NOT_EXIST          | 0x0332 | Vgroup does not exist                                                                        | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_CANT_DROP_LEADER          | 0x0333 | Cannot drop mnode which is leader                                                            | 操作节点为leader                              | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_NO_ENOUGH_DNODES          | 0x0334 | Out of dnodes                                                                                | dnode节点数量不够                             | 增加dnode节点                                                                                   |
| TSDB_CODE_MND_INVALID_CLUSTER_CFG       | 0x0335 | Cluster cfg inconsistent                                                                     | 配置不一致                                    | 检查dnode节点与mnode节点配置是否一致。检查方式：1.节点启动时，在日志中输出 2.使用show variables |
| TSDB_CODE_MND_INVALID_CLUSTER_ID        | 0x033B | Cluster id not match                                                                         | 节点配置数据不一致                            | 检查各节点data/dnode/dnodes.json文件中的clusterid                                               |
| TSDB_CODE_MND_ACCT_ALREADY_EXIST        | 0x0340 | Account already exists                                                                       | （仅企业版）内部错误                          | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_ACCT_OPTION       | 0x0342 | Invalid account options                                                                      | （仅企业版）操作不zh                          | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_ACCT_NOT_EXIST            | 0x0344 | Invalid account                                                                              | 账户不存在                                    | 确认账户是否正确                                                                                |
| TSDB_CODE_MND_USER_ALREADY_EXIST        | 0x0350 | User already exists                                                                          | Create user, 重复创建                         | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_USER_NOT_EXIST            | 0x0351 | Invalid user                                                                                 | 用户不存在                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_USER_FORMAT       | 0x0352 | Invalid user format                                                                          | 格式不正确                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_PASS_FORMAT       | 0x0353 | Invalid password format                                                                      | 格式不正确                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_NO_USER_FROM_CONN         | 0x0354 | Can not get user from conn                                                                   | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TOO_MANY_USERS            | 0x0355 | Too many users                                                                               | （仅企业版）用户数量超限                      | 调整配置                                                                                        |
| TSDB_CODE_MND_AUTH_FAILURE              | 0x0357 | Authentication failure                                                                       | 密码不正确                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_USER_NOT_AVAILABLE        | 0x0358 | User not available                                                                           | 用户不存在                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_STB_ALREADY_EXIST         | 0x0360 | STable already exists                                                                        | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_STB_NOT_EXIST             | 0x0361 | STable not exist                                                                             | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TOO_MANY_TAGS             | 0x0364 | Too many tags                                                                                | tag数量太多                                   | 不能修改，代码级别限制                                                                          |
| TSDB_CODE_MND_TOO_MANY_COLUMNS          | 0x0365 | Too many columns                                                                             | columns数量太多                               | 不能修改，代码级别限制                                                                          |
| TSDB_CODE_MND_TAG_ALREADY_EXIST         | 0x0369 | Tag already exists                                                                           | tag已存在                                     | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TAG_NOT_EXIST             | 0x036A | Tag does not exist                                                                           | tag不存在                                     | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_COLUMN_ALREADY_EXIST      | 0x036B | Column already exists                                                                        | Column 已存在                                 | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_COLUMN_NOT_EXIST          | 0x036C | Column does not exist                                                                        | Column 不存在                                 | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_STB_OPTION        | 0x036E | Invalid stable options                                                                       | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_ROW_BYTES         | 0x036F | Invalid row bytes                                                                            | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_FUNC_NAME         | 0x0370 | Invalid func name                                                                            | name长度错误                                  | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_FUNC_CODE         | 0x0372 | Invalid func code                                                                            | code长度错误                                  | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_FUNC_ALREADY_EXIST        | 0x0373 | Func already exists                                                                          | Func已存在                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_FUNC_NOT_EXIST            | 0x0374 | Func not exists                                                                              | Func不存在                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_FUNC_BUFSIZE      | 0x0375 | Invalid func bufSize                                                                         | bufSize长度错误，或者超过限制                 | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_FUNC_COMMENT      | 0x0378 | Invalid func comment                                                                         | 长度错误，或者超过限制                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_FUNC_RETRIEVE     | 0x0379 | Invalid func retrieve msg                                                                    | 长度错误，或者超过限制                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_DB_NOT_SELECTED           | 0x0380 | Database not specified or available                                                          | 未指定database                                | 使用 use database;                                                                              |
| TSDB_CODE_MND_DB_ALREADY_EXIST          | 0x0381 | Database already exists                                                                      | Database已存在                                | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_DB_OPTION         | 0x0382 | Invalid database options                                                                     | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_DB                | 0x0383 | Invalid database name                                                                        | 长度错误                                      | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TOO_MANY_DATABASES        | 0x0385 | Too many databases for account                                                               | 数量超限                                      | 调整配置                                                                                        |
| TSDB_CODE_MND_DB_IN_DROPPING            | 0x0386 | Database in dropping status                                                                  | 数据库正在被删除                              | 重试，长时间保持该状态上报issue                                                                 |
| TSDB_CODE_MND_DB_NOT_EXIST              | 0x0388 | Database not exist                                                                           | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_DB_ACCT           | 0x0389 | Invalid database account                                                                     | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_DB_OPTION_UNCHANGED       | 0x038A | Database options not changed                                                                 | 操作无变化                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_DB_INDEX_NOT_EXIST        | 0x038B | Index not exist                                                                              | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_DB_IN_CREATING            | 0x0396 | Database in creating status                                                                  | 数据库正在被创建                              | 重试                                                                                            |
| TSDB_CODE_MND_INVALID_SYS_TABLENAME     | 0x039A | Invalid system table name                                                                    | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_MNODE_ALREADY_EXIST       | 0x03A0 | Mnode already exists                                                                         | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_MNODE_NOT_EXIST           | 0x03A1 | Mnode not there                                                                              | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_QNODE_ALREADY_EXIST       | 0x03A2 | Qnode already exists                                                                         | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_QNODE_NOT_EXIST           | 0x03A3 | Qnode not there                                                                              | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_SNODE_ALREADY_EXIST       | 0x03A4 | Snode already exists                                                                         | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_SNODE_NOT_EXIST           | 0x03A5 | Snode not there                                                                              | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TOO_FEW_MNODES            | 0x03A8 | The replica of mnode cannot less than 1                                                      | mnode少于1                                    | 操作不允许                                                                                      |
| TSDB_CODE_MND_TOO_MANY_MNODES           | 0x03A9 | The replica of mnode cannot exceed 3                                                         | mnode多于1                                    | 操作不允许                                                                                      |
| TSDB_CODE_MND_NO_ENOUGH_MEM_IN_DNODE    | 0x03B1 | No enough memory in dnode                                                                    | 内存不足                                      | 调整配置                                                                                        |
| TSDB_CODE_MND_INVALID_DNODE_EP          | 0x03B3 | Invalid dnode end point                                                                      | ep配置不正确                                  | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_HAS_OFFLINE_DNODE         | 0x03B6 | Offline dnode exists                                                                         | Dnode offline                                 | 检查节点状态                                                                                    |
| TSDB_CODE_MND_INVALID_REPLICA           | 0x03B7 | Invalid vgroup replica                                                                       | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_DNODE_IN_CREATING         | 0x03B8 | Dnode in creating status                                                                     | 正在创建                                      | 重试                                                                                            |
| TSDB_CODE_MND_DNODE_IN_DROPPING         | 0x03B9 | Dnode in dropping status                                                                     | 正在删除                                      | 重试                                                                                            |
| TSDB_CODE_MND_INVALID_STB_ALTER_OPTION  | 0x03C2 | Invalid stable alter options                                                                 | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_STB_OPTION_UNCHNAGED      | 0x03C3 | STable option unchanged                                                                      | 操作无变化                                    | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_FIELD_CONFLICT_WITH_TOPIC | 0x03C4 | Field used by topic                                                                          | 被使用                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_SINGLE_STB_MODE_DB        | 0x03C5 | Database is single stable mode                                                               | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_SCHEMA_VER        | 0x03C6 | Invalid schema version while alter stb                                                       | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_STABLE_UID_NOT_MATCH      | 0x03C7 | Invalid stable uid while alter stb                                                           | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_FIELD_CONFLICT_WITH_TSMA  | 0x03C8 | Field used by tsma                                                                           | 被使用                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TRANS_NOT_EXIST           | 0x03D1 | Transaction not exists                                                                       | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TRANS_INVALID_STAGE       | 0x03D2 | Invalid stage to kill                                                                        | 事务处在不能被kill的节点（比如 在commit阶段） | 等待事务结束，如长时间不结束，上报issue                                                         |
| TSDB_CODE_MND_TRANS_CONFLICT            | 0x03D3 | Conflict transaction not completed                                                           | 事务冲突，不能执行该操作                      | 使用show transactions命令查看冲突的事务，等待冲突事务结束，如长时间不结束，上报issue            |
| TSDB_CODE_MND_TRANS_CLOG_IS_NULL        | 0x03D4 | Transaction commitlog is null                                                                | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TRANS_NETWORK_UNAVAILL    | 0x03D5 | Unable to establish connection While execute transaction and will continue in the background | 网络错误                                      | 检查网络是否正常                                                                                |
| TSDB_CODE_MND_LAST_TRANS_NOT_FINISHED   | 0x03D6 | Last Transaction not finished                                                                | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TRNAS_SYNC_TIMEOUT        | 0x03D7 | Sync timeout While execute transaction and will continue in the background                   | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TRANS_UNKNOW_ERROR        | 0x03DF | Unknown transaction error                                                                    | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TOPIC_ALREADY_EXIST       | 0x03E0 | Topic already exists                                                                         | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TOPIC_NOT_EXIST           | 0x03E1 | Topic not exist                                                                              | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_TOPIC             | 0x03E3 | Invalid topic                                                                                | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_TOPIC_QUERY       | 0x03E4 | Topic with invalid query                                                                     | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_INVALID_TOPIC_OPTION      | 0x03E5 | Topic with invalid option                                                                    | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_CONSUMER_NOT_EXIST        | 0x03E6 | Consumer not exist                                                                           | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TOPIC_OPTION_UNCHNAGED    | 0x03E7 | Topic unchanged                                                                              | 无变化                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST       | 0x03E8 | Subcribe not exist                                                                           | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_OFFSET_NOT_EXIST          | 0x03E9 | Offset not exist                                                                             | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_CONSUMER_NOT_READY        | 0x03EA | Consumer not ready                                                                           | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TOPIC_SUBSCRIBED          | 0x03EB | Topic subscribed cannot be dropped                                                           | 被使用                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_CGROUP_USED               | 0x03EC | Consumer group being used by some consumer                                                   | 被使用                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TOPIC_MUST_BE_DELETED     | 0x03ED | Topic must be dropped first                                                                  | 被使用                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_SUB_OPTION        | 0x03EE | Invalid subscribe option                                                                     | 内部错误                                      | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_IN_REBALANCE              | 0x03EF | Topic being rebalanced                                                                       | 操作中                                        | 重试                                                                                            |
| TSDB_CODE_MND_STREAM_ALREADY_EXIST      | 0x03F0 | Stream already exists                                                                        | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_STREAM_NOT_EXIST          | 0x03F1 | Stream not exist                                                                             | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_STREAM_OPTION     | 0x03F2 | Invalid stream option                                                                        | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_STREAM_MUST_BE_DELETED    | 0x03F3 | Stream must be dropped first                                                                 | 被使用                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_MULTI_REPLICA_SOURCE_DB   | 0x03F5 | Stream temporarily does not support source db having replica > 1                             | 超过限制                                      | 操作不被允许                                                                                    |
| TSDB_CODE_MND_TOO_MANY_STREAMS          | 0x03F6 | Too many streams                                                                             | 超过限制                                      | 不能修改，代码级别限制                                                                          |
| TSDB_CODE_MND_INVALID_TARGET_TABLE      | 0x03F7 | Cannot write the same stable as other stream                                                 | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_SMA_ALREADY_EXIST         | 0x0480 | index already exists                                                                         | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_SMA_NOT_EXIST             | 0x0481 | index not exist                                                                              | 不存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_INVALID_SMA_OPTION        | 0x0482 | Invalid sma index option                                                                     | 内部错误                                      | 上报issue                                                                                       |
| TSDB_CODE_MND_TAG_INDEX_ALREADY_EXIST   | 0x0483 | index already exists                                                                         | 已存在                                        | 确认操作是否正确                                                                                |
| TSDB_CODE_MND_TAG_INDEX_NOT_EXIST       | 0x0484 | index not exist                                                                              | 不存在                                        | 确认操作是否正确                                                                                |


## dnode

| 错误码                           | 错误描述 | 可能的出错场景或者可能的原因 | 建议用户采取的措施 |
| -------------------------------- | -------- | ---------------------------- | ------------------ |
| TSDB_CODE_DNODE_OFFLINE          | 0x0408   | Dnode is offline             | 检查节点状态       |
| TSDB_CODE_MNODE_ALREADY_DEPLOYED | 0x0409   | Mnode already deployed       | 确认操作是否正确   |
| TSDB_CODE_MNODE_NOT_FOUND        | 0x040A   | Mnode not found              | 上报issue          |
| TSDB_CODE_MNODE_NOT_DEPLOYED     | 0x040B   | Mnode not deployed           | 上报issue          |
| TSDB_CODE_QNODE_ALREADY_DEPLOYED | 0x040C   | Qnode already deployed       | 确认操作是否正确   |
| TSDB_CODE_QNODE_NOT_FOUND        | 0x040D   | Qnode not found              | 上报issue          |
| TSDB_CODE_QNODE_NOT_DEPLOYED     | 0x040E   | Qnode not deployed           | 上报issue          |
| TSDB_CODE_SNODE_ALREADY_DEPLOYED | 0x040F   | Snode already deployed       | 确认操作是否正确   |
| TSDB_CODE_SNODE_NOT_FOUND        | 0x0410   | Snode not found              | 上报issue          |
| TSDB_CODE_SNODE_NOT_DEPLOYED     | 0x0411   | Snode not deployed           | 确认操作是否正确   |


## vnode

| 错误码                             | 错误描述 | 可能的出错场景或者可能的原因                       | 建议用户采取的措施 |
| ---------------------------------- | -------- | -------------------------------------------------- | ------------------ |
| TSDB_CODE_VND_INVALID_VGROUP_ID    | 0x0503   | Invalid vgroup ID                                  | 上报问题           |
| TSDB_CODE_VND_NO_WRITE_AUTH        | 0x0512   | No writing previlege                               | 寻求授权           |
| TSDB_CODE_VND_NOT_EXIST            | 0x0520   | Vnode does not exist                               | 上报问题           |
| TSDB_CODE_VND_ALREADY_EXIST        | 0x0521   | Vnode already exists                               | 上报问题           |
| TSDB_CODE_VND_HASH_MISMATCH        | 0x0522   | Hash value of table is not in the vnode hash range | 上报问题           |
| TSDB_CODE_VND_INVALID_TABLE_ACTION | 0x0524   | Invalid table operation                            | 上报问题           |
| TSDB_CODE_VND_COL_ALREADY_EXISTS   | 0x0525   | Column already exists                              | 上报问题           |
| TSDB_CODE_VND_COL_NOT_EXISTS       | 0x0526   | Column does not exists                             | 上报问题           |
| TSDB_CODE_VND_COL_SUBSCRIBED       | 0x0527   | Column is subscribed                               | 上报问题           |
| TSDB_CODE_VND_STOPPED              | 0x0529   | Vnode is stopped                                   | 上报问题           |
| TSDB_CODE_VND_DUP_REQUEST          | 0x0530   | Duplicate write request                            | 上报问题           |
| TSDB_CODE_VND_QUERY_BUSY           | 0x0531   | Vnode query is busy                                | 上报问题           |


## tsdb

| 错误码                                 | 错误描述 | 可能的出错场景或者可能的原因              | 建议用户采取的措施             |
| -------------------------------------- | -------- | ----------------------------------------- | ------------------------------ |
| TSDB_CODE_TDB_INVALID_TABLE_ID         | 0x0600   | Invalid table ID to write                 | 重启客户端                     |
| TSDB_CODE_TDB_IVD_TB_SCHEMA_VERSION    | 0x0602   | Invalid table schema version              | 无需处理，内部自动更新         |
| TSDB_CODE_TDB_TABLE_ALREADY_EXIST      | 0x0603   | Table already exists                      | 上报问题                       |
| TSDB_CODE_TDB_INVALID_CONFIG           | 0x0604   | Invalid configuration                     | 上报问题                       |
| TSDB_CODE_TDB_INIT_FAILED              | 0x0605   | Init failed                               | 上报问题                       |
| TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE   | 0x060B   | Timestamp is out of range                 | 上报问题，检查应用写入时间逻辑 |
| TSDB_CODE_TDB_SUBMIT_MSG_MSSED_UP      | 0x060C   | Submit message is messed up               | 上报问题                       |
| TSDB_CODE_TDB_TABLE_NOT_EXIST          | 0x0618   | Table does not exists                     | 上报问题                       |
| TSDB_CODE_TDB_STB_ALREADY_EXIST        | 0x0619   | Super table already exists                | 上报问题                       |
| TSDB_CODE_TDB_STB_NOT_EXIST            | 0x061A   | Super table does not exist                | 上报问题                       |
| TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER | 0x061B   | Invalid table schema version              | 上报问题                       |
| TSDB_CODE_TDB_TABLE_IN_OTHER_STABLE    | 0x061D   | Table already exists in other super table | 检查写入应用逻辑               |

## query

| 定义                              | 错误码     | 错误描述                             | 可能的出错场景或者可能的原因               | 建议用户采取的措施                                     |
| --------------------------------- | ---------- | ------------------------------------ | ------------------------------------------ | ------------------------------------------------------ |
| TSDB_CODE_QRY_INVALID_QHANDLE     | 0x80000700 | Invalid query handle                 | 当前查询句柄不存在                         | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_IN_EXEC             | 0x80000709 | Multiple retrieval of this query     | 当前子查询已经正在进行中                   | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_TOO_MANY_TIMEWINDOW | 0x8000070A | Too many groups/time window in query | 当前查询结果中的分组或窗口个数超过限制个数 | 调整查询语句，确保查询条件中的分组和窗口个数不超过上限 |
| TSDB_CODE_QRY_SYS_ERROR           | 0x8000070D | System error                         | 底层系统API返回错误                        | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_SCH_NOT_EXIST       | 0x80000720 | Scheduler not exist                  | 当前子查询对应的客户端信息不存在           | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_TASK_NOT_EXIST      | 0x80000721 | Task not exist                       | 子查询不存在                               | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_TASK_ALREADY_EXIST  | 0x80000722 | Task already exist                   | 子查询已经存在                             | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_TASK_MSG_ERROR      | 0x80000729 | Task message error                   | 查询消息错误                               | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_TASK_STATUS_ERROR   | 0x8000072B | Task status error                    | 子查询状态错误                             | 保留现场和日志，github上报issue                        |
| TSDB_CODE_QRY_JOB_NOT_EXIST       | 0x8000072F | Job not exist                        | 查询JOB已经不存在                          | 保留现场和日志，github上报issue                        |

## grant

| 定义                               | 错误码     | 错误描述                            | 可能的出错场景或者可能的原因 | 建议用户采取的措施               |
| ---------------------------------- | ---------- | ----------------------------------- | ---------------------------- | -------------------------------- |
| TSDB_CODE_GRANT_EXPIRED            | 0x80000800 | License expired                     | 授权时间过期                 | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_DNODE_LIMITED      | 0x80000801 | DNode creation limited by license   | Dnode 数量超过授权限制       | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_ACCT_LIMITED       | 0x80000802 | Account creation limited by license | 账号数量超过授权限制         | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_TIMESERIES_LIMITED | 0x80000803 | Time series limited by license      | 测点数量超过授权限制         | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_DB_LIMITED         | 0x80000804 | DB creation limited by license      | 数据库数量超过授权限制       | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_USER_LIMITED       | 0x80000805 | User creation limited by license    | 用户数量超过授权限制         | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_CONN_LIMITED       | 0x80000806 | Conn creation limited by license    | 连接数量超过授权限制         | 暂未限制，联系交付进行检查       |
| TSDB_CODE_GRANT_STREAM_LIMITED     | 0x80000807 | Stream creation limited by license  | 流数量超过授权限制           | 暂未限制，联系交付进行检查       |
| TSDB_CODE_GRANT_SPEED_LIMITED      | 0x80000808 | Write speed limited by license      | 写入速度超过授权限制         | 暂未限制，联系交付进行检查       |
| TSDB_CODE_GRANT_STORAGE_LIMITED    | 0x80000809 | Storage capacity limited by license | 存储空间超过授权限制         | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_QUERYTIME_LIMITED  | 0x8000080A | Query time limited by license       | 查询次数超过授权限制         | 暂未限制，联系交付进行检查       |
| TSDB_CODE_GRANT_CPU_LIMITED        | 0x8000080B | CPU cores limited by license        | CPU 核数超过授权限制         | 暂未限制，联系交付进行检查       |
| TSDB_CODE_GRANT_STABLE_LIMITED     | 0x8000080C | STable creation limited by license  | 超级表数量超过授权限制       | 检查授权信息，联系交付更新授权码 |
| TSDB_CODE_GRANT_TABLE_LIMITED      | 0x8000080D | Table creation limited by license   | 子表/普通表数量超过授权限制  | 检查授权信息，联系交付更新授权码 |

## sync

| 定义                               | 错误码     | 错误描述                     | 可能的出错场景或者可能的原因                                                                                | 建议用户采取的措施                                                                         |
| ---------------------------------- | ---------- | ---------------------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| TSDB_CODE_SYN_TIMEOUT              | 0x80000903 | Sync timeout                 | 场景1：发生了切主；旧主节点上已经开始协商但尚未达成一致的请求将超时。 场景2：从节点响应超时，导致协商超时。 | 检查集群状态，例如：show vgroups；查看服务端日志，以及服务端节点之间的网络状况。           |
| TSDB_CODE_SYN_NOT_LEADER           | 0x8000090C | Sync leader is unreachable   | 场景1：选主过程中 场景2：客户端请求路由到了从节点，且重定向失败 场景3：客户端或服务端网络配置错误           | 检查集群状态、网络配置、应用程序访问状态等。查看服务端日志，以及服务端节点之间的网络状况。 |
| TSDB_CODE_SYN_NEW_CONFIG_ERROR     | 0x8000090F | Sync new config error        | 成员变更新配置错误                                                                                          | 预留                                                                                       |
| TSDB_CODE_SYN_PROPOSE_NOT_READY    | 0x80000911 | Sync not ready to propose    | 场景1：恢复未完成                                                                                           | 检查集群状态，例如：show vgroups。查看服务端日志，以及服务端节点之间的网络状况。           |
| TSDB_CODE_SYN_RESTORING            | 0x80000914 | Sync leader is restoring     | 场景1：发生了切主；选主后，日志重演中                                                                       | 检查集群状态，例如：show vgroups。查看服务端日志，观察恢复进度。                           |
| TSDB_CODE_SYN_INVALID_SNAPSHOT_MSG | 0x80000915 | Sync invalid snapshot msg    | 快照复制消息错误                                                                                            | 服务端内部错误                                                                             |
| TSDB_CODE_SYN_BUFFER_FULL          | 0x80000916 | Sync buffer is full          | 场景1：客户端请求并发数特别大，超过了服务端处理能力，或者因为网络和CPU资源严重不足，或者网络连接问题等。    | 检查集群状态，系统资源使用率（例如磁盘IO、CPU、网络通信等），以及节点之间网络连接状况。    |
| TSDB_CODE_SYN_WRITE_STALL          | 0x80000917 | Sync write stall             | 场景1：状态机执行被阻塞，例如因系统繁忙，磁盘IO资源严重不足，或落盘失败等                                   | 检查集群状态，系统资源使用率（例如磁盘IO和CPU等），以及是否发生了落盘失败等。              |
| TSDB_CODE_SYN_NEGOTIATION_WIN_FULL | 0x80000918 | Sync negotiation win is full | 场景1：客户端请求并发数特别大，超过了服务端处理能力，或者因为网络和CPU资源严重不足，或者网络连接问题等。    | 检查集群状态，系统资源使用率（例如磁盘IO、CPU、网络通信等），以及节点之间网络连接状况。    |
| TSDB_CODE_SYN_INTERNAL_ERROR       | 0x800009FF | Sync internal error          | 其它内部错误                                                                                                | 检查集群状态，例如：show vgroups                                                           |



## tq

| 定义                                | 错误码     | 错误描述                  | 可能的出错场景或者可能的原因                                    | 建议用户采取的措施                     |
| ----------------------------------- | ---------- | ------------------------- | --------------------------------------------------------------- | -------------------------------------- |
| TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND | 0x80000A0C | TQ table schema not found | 消费数据时表不存在                                              | 内部错误，不透传给用户                 |
| TSDB_CODE_TQ_NO_COMMITTED_OFFSET    | 0x80000A0D | TQ no committed offset    | 消费时设置offset reset = none，并且server端没有之前消费的offset | 设置offset reset为earliest 或者 latest |


## wal

| 定义                          | 错误码     | 错误描述              | 可能的出错场景或者可能的原因     | 建议用户采取的措施 |
| ----------------------------- | ---------- | --------------------- | -------------------------------- | ------------------ |
| TSDB_CODE_WAL_FILE_CORRUPTED  | 0x80001001 | WAL file is corrupted | WAL文件损坏                      | 服务端内部错误     |
| TSDB_CODE_WAL_INVALID_VER     | 0x80001003 | WAL invalid version   | 请求日志版本，超过了当前日志范围 | 服务端内部错误     |
| TSDB_CODE_WAL_LOG_NOT_EXIST   | 0x80001005 | WAL log not exist     | 请求日志记录，不存在             | 服务端内部错误     |
| TSDB_CODE_WAL_CHKSUM_MISMATCH | 0x80001006 | WAL checksum mismatch | 场景：发生了WAL文件损坏          | 服务端内部错误     |
| TSDB_CODE_WAL_LOG_INCOMPLETE  | 0x80001007 | WAL log incomplete    | 日志文件发生了丢失或损坏         | 服务端内部错误     |

## tfs

| 定义                          | 错误码     | 错误描述                         | 可能的出错场景或者可能的原因        | 建议用户采取的措施                       |
| ----------------------------- | ---------- | -------------------------------- | ----------------------------------- | ---------------------------------------- |
| TSDB_CODE_FS_INVLD_CFG        | 0x80002201 | TFS invalid configuration        | 多级存储配置错误                    | 检查配置是否正确                         |
| TSDB_CODE_FS_TOO_MANY_MOUNT   | 0x80002202 | TFS too many disks on one level  | 多级存储配置错误                    | 检查一级硬盘上的配置个数是否超过最大限制 |
| TSDB_CODE_FS_DUP_PRIMARY      | 0x80002203 | TFS duplicate primary mount disk | 多级存储配置错误                    | 检查配置是否正确                         |
| TSDB_CODE_FS_NO_PRIMARY_DISK  | 0x80002204 | TFS no primary mount disk        | 多级存储配置错误                    | 检查配置是否正确                         |
| TSDB_CODE_FS_NO_MOUNT_AT_TIER | 0x80002205 | TFS no disk mount on tire        | 多级存储配置错误                    | 检查配置是否正确                         |
| TSDB_CODE_FS_NO_VALID_DISK    | 0x80002208 | No disk available on a tier.     | TFS内部错误，多发生在硬盘满的场景下 | 增加硬盘，扩充容量                       |



## catalog

| 定义                          | 错误码     | 错误描述                         | 可能的出错场景或者可能的原因 | 建议用户采取的措施               |
| ----------------------------- | ---------- | -------------------------------- | ---------------------------- | -------------------------------- |
| TSDB_CODE_CTG_INTERNAL_ERROR  | 0x80002400 | catalog internal error           | catalog内部错误              | 保留现场和日志，github上报issue  |
| TSDB_CODE_CTG_INVALID_INPUT   | 0x80002401 | catalog invalid input parameters | catalog输入参数错误          | 保留现场和日志，github上报issue  |
| TSDB_CODE_CTG_NOT_READY       | 0x80002402 | catalog is not ready             | catalog未初始化完成          | 保留现场和日志，github上报issue  |
| TSDB_CODE_CTG_SYS_ERROR       | 0x80002403 | catalog system error             | catalog系统错误              | 保留现场和日志，github上报issue  |
| TSDB_CODE_CTG_DB_DROPPED      | 0x80002404 | Database is dropped              | db缓存被删除                 | 保留现场和日志，github上报issue  |
| TSDB_CODE_CTG_OUT_OF_SERVICE  | 0x80002405 | catalog is out of service        | catalog模块已经退出          | 保留现场和日志，github上报issue  |
| TSDB_CODE_QW_MSG_ERROR        | 0x80002550 | Invalid msg order                | 消息顺序错误                 | 保留现场和日志，github上报issue  |
| TSDB_CODE_SCH_STATUS_ERROR    | 0x80002501 | Job status error                 | 任务状态错误                 | 保留现场和日志，github上报issue  |
| TSDB_CODE_SCH_INTERNAL_ERROR  | 0x80002502 | scheduler internal error         | scheduler内部错误            | 保留现场和日志，github上报issue  |
| TSDB_CODE_SCH_TIMEOUT_ERROR   | 0x80002504 | Task timeout                     | 子任务超时                   | 保留现场和日志，github上报issue  |
| TSDB_CODE_SCH_JOB_IS_DROPPING | 0x80002505 | Job is dropping                  | 任务正在或已经被取消         | 检查是否有手动或应用中断当前任务 |



## parser

| 定义                                         | 错误码     | 错误描述                                                                                               | 可能的出错场景或者可能的原因                  | 建议用户采取的措施                    |
| -------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------ | --------------------------------------------- | ------------------------------------- |
| TSDB_CODE_PAR_SYNTAX_ERROR                   | 0x80002600 | syntax error near                                                                                      | SQL语法错误                                   | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INCOMPLETE_SQL                 | 0x80002601 | Incomplete SQL statement                                                                               | 不完整的SQL语句                               | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_COLUMN                 | 0x80002602 | Invalid column name                                                                                    | 不合法或不存在的列名                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_TABLE_NOT_EXIST                | 0x80002603 | Table does not exist                                                                                   | 表不存在                                      | 检查并确认SQL语句中的表是否存在       |
| TSDB_CODE_PAR_AMBIGUOUS_COLUMN               | 0x80002604 | Column ambiguously defined                                                                             | 列名（别名）重复定义                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_WRONG_VALUE_TYPE               | 0x80002605 | Invalid value type                                                                                     | 常量值非法                                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION       | 0x80002608 | There mustn't be aggregation                                                                           | 聚合函数出现在非法子句中                      | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT         | 0x80002609 | ORDER BY item must be the number of a SELECT-list expression                                           | Order by指定的位置不合法                      | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION        | 0x8000260A | Not a GROUP BY expression                                                                              | 非法group by语句                              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION        | 0x8000260B | Not SELECTed expression                                                                                | 非法表达式                                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_NOT_SINGLE_GROUP               | 0x8000260C | Not a single-group group function                                                                      | 非法使用列与函数                              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_TAGS_NOT_MATCHED               | 0x8000260D | Tags number not matched                                                                                | tag列个数不匹配                               | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TAG_NAME               | 0x8000260E | Invalid tag name                                                                                       | 无效或不存在的tag名                           | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_VALUE_TOO_LONG                 | 0x80002610 | Value is too long                                                                                      | 值长度超出限制                                | 检查并修正SQL语句或API参数            |
| TSDB_CODE_PAR_PASSWD_EMPTY                   | 0x80002611 | Password can not be empty                                                                              | 密码为空                                      | 使用合法的密码                        |
| TSDB_CODE_PAR_INVALID_PORT                   | 0x80002612 | Port should be an integer that is less than 65535 and greater than 0                                   | 端口号非法                                    | 检查并修正端口号                      |
| TSDB_CODE_PAR_INVALID_ENDPOINT               | 0x80002613 | Endpoint should be in the format of 'fqdn:port'                                                        | 地址格式错误                                  | 检查并修正地址信息                    |
| TSDB_CODE_PAR_EXPRIE_STATEMENT               | 0x80002614 | This statement is no longer supported                                                                  | 功能已经废弃                                  | 参考功能文档说明                      |
| TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL          | 0x80002615 | Interval too small                                                                                     | interval值超过允许的最小值                    | 更改INTERVAL值                        |
| TSDB_CODE_PAR_DB_NOT_SPECIFIED               | 0x80002616 | Database not specified                                                                                 | 未指定数据库                                  | 指定当前操作的数据库                  |
| TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME        | 0x80002617 | Invalid identifier name                                                                                | ID非法或长度不合法                            | 检查语句中相关的库、表、列、TAG等名称 |
| TSDB_CODE_PAR_CORRESPONDING_STABLE_ERR       | 0x80002618 | Corresponding super table not in this db                                                               | 超级表不存在                                  | 检查库中是否存在对应的超级表          |
| TSDB_CODE_PAR_INVALID_DB_OPTION              | 0x80002619 | Invalid database option                                                                                | 数据库选项值非法                              | 检查并修正数据库选项值                |
| TSDB_CODE_PAR_INVALID_TABLE_OPTION           | 0x8000261A | Invalid table option                                                                                   | 表选项值非法                                  | 检查并修正数据表选项值                |
| TSDB_CODE_PAR_GROUPBY_WINDOW_COEXIST         | 0x80002624 | GROUP BY and WINDOW-clause can't be used together                                                      | Group by和窗口不能同时使用                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_AGG_FUNC_NESTING               | 0x80002627 | Aggregate functions do not support nesting                                                             | 函数不支持嵌套使用                            | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE         | 0x80002628 | Only support STATE_WINDOW on integer/bool/varchar column                                               | 不支持的STATE_WINDOW数据类型                  | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_STATE_WIN_COL          | 0x80002629 | Not support STATE_WINDOW on tag column                                                                 | 不支持TAG列的STATE_WINDOW                     | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_STATE_WIN_TABLE        | 0x8000262A | STATE_WINDOW not support for super table query                                                         | 不支持超级表的STATE_WINDOW                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_SESSION_GAP              | 0x8000262B | SESSION gap should be fixed time window, and greater than 0                                            | SESSION窗口值非法                             | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_SESSION_COL              | 0x8000262C | Only support SESSION on primary timestamp column                                                       | SESSION窗口列非法                             | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_OFFSET_NEGATIVE          | 0x8000262D | Interval offset cannot be negative                                                                     | INTERVAL offset值非法                         | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_OFFSET_UNIT              | 0x8000262E | Cannot use 'year' as offset when interval is 'month'                                                   | INTERVAL offset单位非法                       | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG           | 0x8000262F | Interval offset should be shorter than interval                                                        | INTERVAL offset值非法                         | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_SLIDING_UNIT             | 0x80002630 | Does not support sliding when interval is natural month/year                                           | sliding单位非法                               | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_SLIDING_TOO_BIG          | 0x80002631 | sliding value no larger than the interval value                                                        | sliding值非法                                 | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTER_SLIDING_TOO_SMALL        | 0x80002632 | sliding value can not less than 1%% of interval value                                                  | sliding值非法                                 | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_ONLY_ONE_JSON_TAG              | 0x80002633 | Only one tag if there is a json tag                                                                    | 只支持单个JSON TAG列                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INCORRECT_NUM_OF_COL           | 0x80002634 | Query block has incorrect number of result columns                                                     | 列个数不匹配                                  | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INCORRECT_TIMESTAMP_VAL        | 0x80002635 | Incorrect TIMESTAMP value                                                                              | 主键时间戳列值非法                            | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_OFFSET_LESS_ZERO               | 0x80002637 | soffset/offset can not be less than 0                                                                  | soffset/offset值非法                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_SLIMIT_LEAK_PARTITION_GROUP_BY | 0x80002638 | slimit/soffset only available for PARTITION/GROUP BY query                                             | slimit/soffset只支持PARTITION BY/GROUP BY语句 | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TOPIC_QUERY            | 0x80002639 | Invalid topic query                                                                                    | 不支持的TOPIC查询语                           |
| TSDB_CODE_PAR_INVALID_DROP_STABLE            | 0x8000263A | Cannot drop super table in batch                                                                       | 不支持批量删除超级表                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE        | 0x8000263B | Start(end) time of query range required or time range too large                                        | 窗口个数超出限制                              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_DUPLICATED_COLUMN              | 0x8000263C | Duplicated column names                                                                                | 列名称重复                                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TAGS_LENGTH            | 0x8000263D | Tags length exceeds max length                                                                         | TAG值长度超出最大支持范围                     | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_ROW_LENGTH             | 0x8000263E | Row length exceeds max length                                                                          | 行长度检查并修正SQL语句                       | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_COLUMNS_NUM            | 0x8000263F | Illegal number of columns                                                                              | 列个数错误                                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_TOO_MANY_COLUMNS               | 0x80002640 | Too many columns                                                                                       | 列个数超出上限                                | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_FIRST_COLUMN           | 0x80002641 | First column must be timestamp                                                                         | 第一列必须是主键时间戳列                      | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_VAR_COLUMN_LEN         | 0x80002642 | Invalid binary/nchar column/tag length                                                                 | binary/nchar长度错误                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TAGS_NUM               | 0x80002643 | Invalid number of tag columns                                                                          | TAG列个数错误                                 | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_PERMISSION_DENIED              | 0x80002644 | Permission denied                                                                                      | 权限错误                                      | 检查确认用户是否有相应操作权限        |
| TSDB_CODE_PAR_INVALID_STREAM_QUERY           | 0x80002645 | Invalid stream query                                                                                   | 非法流语句                                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_INTERNAL_PK            | 0x80002646 | Invalid _c0 or _rowts expression                                                                       | _c0或_rowts非法使用                           | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TIMELINE_FUNC          | 0x80002647 | Invalid timeline function                                                                              | 函数依赖的主键时间戳不存在                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_PASSWD                 | 0x80002648 | Invalid password                                                                                       | 密码不符合规范                                | 检查并修改密码                        |
| TSDB_CODE_PAR_INVALID_ALTER_TABLE            | 0x80002649 | Invalid alter table statement                                                                          | 修改表语句不合法                              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_CANNOT_DROP_PRIMARY_KEY        | 0x8000264A | Primary timestamp column cannot be dropped                                                             | 主键时间戳列不允许删除                        | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_MODIFY_COL             | 0x8000264B | Only binary/nchar column length could be modified, and the length can only be increased, not decreased | 非法列修改                                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TBNAME                 | 0x8000264C | Invalid tbname pseudo column                                                                           | 非法使用tbname列                              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_FUNCTION_NAME          | 0x8000264D | Invalid function name                                                                                  | 非法函数名                                    | 检查并修正函数名                      |
| TSDB_CODE_PAR_COMMENT_TOO_LONG               | 0x8000264E | Comment too long                                                                                       | 注释长度超限                                  | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_NOT_ALLOWED_FUNC               | 0x8000264F | Function(s) only allowed in SELECT list, cannot mixed with non scalar functions or columns             | 非法的函数混用                                | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_NOT_ALLOWED_WIN_QUERY          | 0x80002650 | Window query not supported, since no valid timestamp column included in the result of subquery         | 窗口查询依赖的主键时间戳列不存在              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_DROP_COL               | 0x80002651 | No columns can be dropped                                                                              | 必须的列不能被删除                            | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_COL_JSON               | 0x80002652 | Only tag can be json type                                                                              | 普通列不支持JSON类型                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_DELETE_WHERE           | 0x80002655 | The DELETE statement must have a definite time window range                                            | DELETE语句中存在非法WHERE条件                 | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_REDISTRIBUTE_VG        | 0x80002656 | The REDISTRIBUTE VGROUP statement only support 1 to 3 dnodes                                           | REDISTRIBUTE VGROUP指定的DNODE个数非法        | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC          | 0x80002657 | Fill now allowed                                                                                       | 函数不允许FILL功能                            | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_WINDOW_PC              | 0x80002658 | Invalid windows pc                                                                                     | 非法使用窗口伪列                              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_WINDOW_NOT_ALLOWED_FUNC        | 0x80002659 | Window not allowed                                                                                     | 函数不能在窗口中使用                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_STREAM_NOT_ALLOWED_FUNC        | 0x8000265A | Stream not allowed                                                                                     | 函数不能在流计算中使用                        | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_GROUP_BY_NOT_ALLOWED_FUNC      | 0x8000265B | Group by not allowd                                                                                    | 函数不能在分组中使用                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_INTERP_CLAUSE          | 0x8000265D | Invalid interp clause                                                                                  | 非法INTERP或相关语句                          | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_NO_VALID_FUNC_IN_WIN           | 0x8000265E | Not valid function ion window                                                                          | 非法窗口语句                                  | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_ONLY_SUPPORT_SINGLE_TABLE      | 0x8000265F | Only support single table                                                                              | 函数只支持在单表查询中使用                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_SMA_INDEX              | 0x80002660 | Invalid sma index                                                                                      | 非法创建SMA语句                               | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_SELECTED_EXPR          | 0x80002661 | Invalid SELECTed expression                                                                            | 无效查询语句                                  | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_GET_META_ERROR                 | 0x80002662 | Fail to get table info                                                                                 | 获取表元数据信息失败                          | 保留现场和日志，github上报issue       |
| TSDB_CODE_PAR_NOT_UNIQUE_TABLE_ALIAS         | 0x80002663 | Not unique table/alias                                                                                 | 表名（别名）冲突                              | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_NOT_SUPPORT_JOIN               | 0x80002664 | Join requires valid time series input                                                                  | 不支持子查询不含主键时间戳列输出的JOIN查询    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TAGS_PC                | 0x80002665 | The _TAGS pseudo column can only be used for subtable and supertable queries                           | 非法TAG列查询                                 | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INVALID_TIMELINE_QUERY         | 0x80002666 | 子查询不含主键时间戳列输出                                                                             | 检查并修正SQL语句                             |
| TSDB_CODE_PAR_INVALID_OPTR_USAGE             | 0x80002667 | Invalid usage of expr: %s                                                                              | 非法表达式                                    | 检查并修正SQL语句                     |
| TSDB_CODE_PAR_INTERNAL_ERROR                 | 0x800026FF | Parser internal error                                                                                  | 解析器内部错误                                | 保留现场和日志，github上报issue       |
| TSDB_CODE_PLAN_INTERNAL_ERROR                | 0x80002700 | Planner internal error                                                                                 | 计划期内部错误                                | 保留现场和日志，github上报issue       |
| TSDB_CODE_PLAN_EXPECTED_TS_EQUAL             | 0x80002701 | Expect ts equal                                                                                        | JOIN条件校验失败                              | 保留现场和日志，github上报issue       |
| TSDB_CODE_PLAN_NOT_SUPPORT_CROSS_JOIN        | 0x80002702 | Cross join not support                                                                                 | 不支持CROSS JOIN                              | 检查并修正SQL语句                     |


## function

| 定义                               | 错误码     | 错误描述                                     | 可能的出错场景或者可能的原因                                                                                                                                                                                                                                                          | 建议用户采取的措施                                                                                         |
| ---------------------------------- | ---------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| TSDB_CODE_FUNC_FUNTION_ERROR       | 0x80002800 | Function internal error                      | 函数参数输入不合理造成的错误，随错误码会返回具体错误描述信息。比如APERCENTILE函数第三个参数指定算法时只能使用字符串"default"                                                                                                                                                          | "t-digest", 使用其他输入会报此类错误。或者TO_ISO8601函数第二个参数指定时区时，字符串不符合时区格式规范等。 | 根据具体错误描述信息，调整函数输入。 |
| TSDB_CODE_FUNC_FUNTION_PARA_NUM    | 0x80002801 | Invalid function para number                 | 函数输入参数个数不正确。函数规定必须要使用n个参数，而用户给定参数个数不为n。比如COUNT(col1, col2)。                                                                                                                                                                                   | 调整函数输入参数为正确个数。                                                                               |
| TSDB_CODE_FUNC_FUNTION_PARA_TYPE   | 0x80002802 | Invalid function para type                   | 函数输入参数类型不正确。函数输入参数要求为数值类型，但是用户所给参数为字符串。比如SUM("abc")。                                                                                                                                                                                        | 调整函数参数输入为正确类型                                                                                 |
| TSDB_CODE_FUNC_FUNTION_PARA_VALUE  | 0x80002803 | Invalid function para value                  | 函数输入参数取值不正确。函数输入参数范围不正确。比如SAMPLE函数第二个参数指定采样个数范围为[1, 1000], 如果不在这个范围内会会报错。                                                                                                                                                     | 调整函数参数输入为正确取值。                                                                               |
| TSDB_CODE_FUNC_NOT_BUILTIN_FUNTION | 0x80002804 | Not builtin function                         | 函数非内置函数。内置函数不在的哈希表中会报错，用户应该很少遇见这个问题，否则是内部内置函数哈希初始化的时候出错或者写坏。                                                                                                                                                              | 客户应该不会遇到，如果遇到，说明程序有bug，咨询开发人员。                                                  |
| TSDB_CODE_FUNC_DUP_TIMESTAMP       | 0x80002805 | Duplicate timestamps not allowed in function | 函数输入主键列有重复时间戳。对某些依赖时间线顺序函数做超级表查询时，所有子表数据会按照时间戳进行排序后合并为一条时间线进行计算，因此子表合并后的时间戳可能会出现重复，导致某些计算没有意义而报错。涉及到的函数有：CSUM，DERIVATIVE，DIFF，IRATE，MAVG，STATECOUNT，STATEDURATION，TWA | 如果需要对超级表查询并且使用这些依赖时间线顺序函数时，确保子表中不存在重复时间戳数据。                     |


## udf
| 定义                               | 错误码     | 错误描述                           | 可能的出错场景或者可能的原因                                                          | 建议用户采取的措施                            |
| ---------------------------------- | ---------- | ---------------------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------- |
| TSDB_CODE_UDF_STOPPING             | 0x80002901 | udf is stopping                    | dnode退出时，收到udf调用                                                              | 停止执行udf查询                               |
| TSDB_CODE_UDF_PIPE_READ_ERR        | 0x80002902 | udf pipe read error                | taosd读取udfd pipe，发生错误                                                          | udfd异常退出，1）c udf崩溃 2）udfd崩溃        |
| TSDB_CODE_UDF_PIPE_CONNECT_ERR     | 0x80002903 | udf pipe connect error             | taosd建立到udfd的管道连接时，发生错误                                                 | 1)taosd对应的udfd未启动。重启taosd            |
| TSDB_CODE_UDF_PIPE_NOT_EXIST       | 0x80002904 | udf pip not exist                  | udf建立，调用，拆除三个阶段，两个阶段中间发生连接错误，导致连接消失，后续阶段继续执行 | udfd异常退出，1）c udf崩溃 2）udfd崩溃        |
| TSDB_CODE_UDF_LOAD_UDF_FAILURE     | 0x80002905 | udf load failure                   | udfd加载udf时错误                                                                     | 1）mnode中udf不存在 2）udf 加载出错。查看日志 |
| TSDB_CODE_UDF_INVALID_INPUT        | 0x80002906 | udf invalid function input         | udf检查输入                                                                           | udf函数不接受输入，如输入列类型错误           |
| TSDB_CODE_UDF_INVALID_BUFSIZE      | 0x80002907 | udf invalid bufsize                | udf聚合函数中间结果大于创建udf中指定的bufsize                                         | 增大bufSize，或者降低中间结果大小             |
| TSDB_CODE_UDF_INVALID_OUTPUT_TYPE  | 0x80002908 | udf invalid output type            | udf输出的类型和创建udf中指定的类型                                                    | 修改udf，或者创建udf的类型，使得结果相同      |
| TSDB_CODE_UDF_SCRIPT_NOT_SUPPORTED | 0x80002909 | udf program language not supported | udf编程语言不支持                                                                     | 使用支持的语言,当前支持c，python              |
| TSDB_CODE_UDF_FUNC_EXEC_FAILURE    | 0x8000290A | udf function execution failure     | udf函数执行错误，如返回错误的行数                                                     | 具体查看错误日志                              |


## sml
| 定义                                 | 错误码     | 错误描述                         | 可能的出错场景或者可能的原因                    | 建议用户采取的措施                                              |
| ------------------------------------ | ---------- | -------------------------------- | ----------------------------------------------- | --------------------------------------------------------------- |
| TSDB_CODE_SML_INVALID_PROTOCOL_TYPE  | 0x80003000 | Invalid line protocol type       | schemaless接口传入的协议非法                    | 检查传入的协议是否为taos.h 中定位的三种 TSDB_SML_PROTOCOL_TYPE  |
| TSDB_CODE_SML_INVALID_PRECISION_TYPE | 0x80003001 | Invalid timestamp precision type | schemaless接口传入的时间精度非法                | 检查传入的协议是否为taos.h 中定位的七种 TSDB_SML_TIMESTAMP_TYPE |
| TSDB_CODE_SML_INVALID_DATA           | 0x80003002 | Invalid data format              | schemaless接口传入的数据格式非法                | 具体查看client端的错误日志提示                                  |
| TSDB_CODE_SML_NOT_SAME_TYPE          | 0x80003004 | Not the same type as before      | schemaless 数据一批的多行数据里相同列类型不一致 | 检测数据里每行相同列的数据类型是否一致                          |
| TSDB_CODE_SML_INTERNAL_ERROR         | 0x80003005 | Internal error                   | schemaless 内部逻辑错误，一般不会出现           | 具体查看client端的错误日志提示                                  |


## sma

| 定义                               | 错误码     | 错误描述                      | 可能的出错场景或者可能的原因                               | 建议用户采取的措施         |
| ---------------------------------- | ---------- | ----------------------------- | ---------------------------------------------------------- | -------------------------- |
| TSDB_CODE_TSMA_INIT_FAILED         | 0x80003100 | Tsma init failed              | TSMA 环境初始化失败                                        | 检查错误日志，联系开发处理 |
| TSDB_CODE_TSMA_ALREADY_EXIST       | 0x80003101 | Tsma already exists           | TSMA 重复创建                                              | 避免重复创建               |
| TSDB_CODE_TSMA_INVALID_ENV         | 0x80003102 | Invalid tsma env              | TSMA 运行环境异常                                          | 检查错误日志，联系开发处理 |
| TSDB_CODE_TSMA_INVALID_STAT        | 0x80003103 | Invalid tsma state            | 流计算下发结果的 vgroup 与创建 TSMA index 的 vgroup 不一致 | 检查错误日志，联系开发处理 |
| TSDB_CODE_TSMA_INVALID_PTR         | 0x80003104 | Invalid tsma pointer          | 在处理写入流计算下发的结果，消息体为空指针。               | 检查错误日志，联系开发处理 |
| TSDB_CODE_TSMA_INVALID_PARA        | 0x80003105 | Invalid tsma parameters       | 在处理写入流计算下发的结果，结果数量为0。                  | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_INVALID_ENV         | 0x80003150 | Invalid rsma env              | Rsma 执行环境异常。                                        | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_INVALID_STAT        | 0x80003151 | Invalid rsma state            | Rsma 执行状态异常。                                        | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_QTASKINFO_CREATE    | 0x80003152 | Rsma qtaskinfo creation error | 创建流计算环境异常。                                       | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_INVALID_SCHEMA      | 0x80003153 | Rsma invalid schema           | 启动恢复时元数据信息错误                                   | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_STREAM_STATE_OPEN   | 0x80003154 | Rsma stream state open        | 打开流算子状态存储失败                                     | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_STREAM_STATE_COMMIT | 0x80003155 | Rsma stream state commit      | 提交流算子状态存储失败                                     | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_FS_REF              | 0x80003156 | Rsma fs ref error             | 算子文件引用计数错误                                       | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_FS_SYNC             | 0x80003157 | Rsma fs sync error            | 算子文件同步失败                                           | 检查错误日志，联系开发处理 |
| TSDB_CODE_RSMA_FS_UPDATE           | 0x80003158 | Rsma fs update error          | 算子文件更新失败                                           | 检查错误日志，联系开发处理 |


## index
| 定义                         | 错误码     | 错误描述         | 可能的出错场景或者可能的原因                                          | 建议用户采取的措施         |
| ---------------------------- | ---------- | ---------------- | --------------------------------------------------------------------- | -------------------------- |
| TSDB_CODE_INDEX_REBUILDING   | 0x80003200 | INDEX 正在重建中 | 1. 写入过快，导致index 的合并线程处理不过来 2. 索引文件损坏，正在重建 | 检查错误日志，联系开发处理 |
| TSDB_CODE_INDEX_INVALID_FILE | 0x80003201 | 索引文件损坏     | 文件损坏                                                              | 检查错误日志，联系开发处理 |


## tmq

| 定义                            | 错误码     | 错误描述              | 可能的出错场景或者可能的原因                                                     | 建议用户采取的措施             |
| ------------------------------- | ---------- | --------------------- | -------------------------------------------------------------------------------- | ------------------------------ |
| TSDB_CODE_TMQ_INVALID_MSG       | 0x80004000 | Invalid message       | 订阅到的数据非法，一般不会出现                                                   | 具体查看client端的错误日志提示 |
| TSDB_CODE_TMQ_CONSUMER_MISMATCH | 0x80004001 | Consumer mismatch     | 订阅请求的vnode和重新分配的vnode不一致，一般存在于有新消费者加入相同消费者组里时 | 内部错误，不暴露给用户         |
| TSDB_CODE_TMQ_CONSUMER_CLOSED   | 0x80004002 | Consumer closed       | 消费者已经不存在了                                                               | 查看是否已经close掉了          |
| TSDB_CODE_STREAM_TASK_NOT_EXIST | 0x80004100 | Stream task not exist | 流计算任务不存在                                                                 | 具体查看server端的错误日志     |

