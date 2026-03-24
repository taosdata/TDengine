# taosAdapter 添加SQL查询请求并发限制 FS

## 1. 背景

https://jira.taosdata.com:18080/browse/TS-6856
https://jira.taosdata.com:18080/browse/TD-36925
目标是希望控制 taosd 的资源使用。三峡新能源云化集控并发请求引起 taosd cpu 飙升导致频繁切主等异常情况时服务不可用。
taosAdapter通过设计合理的连接管理，限制请求并发数来达到限制发送到 taosd 请求并发数的目的。支持区分写入、查询、订阅业务的并发数，或合并处理。
09-10 晚与肖波和邓怡豪交流后采用限制 taosAdapter 总请求并发数来控制发送到 taosd 请求并发数。
09-15 与霍琳贺、佘彦杰讨论后改为限制查询语句的全生命周期。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/09/11 | 0.1 | 谭雪峰 | 编写文档 |
| 2025/09/15 | 0.2 | 谭雪峰 | 限制改为 select 语句的全生命周期 |
| 2025/10/09 | 1.0 | 谭雪峰 | 添加总开关配置项 添加正则表达式排除 移除 SQL 匹配长度 |
|  |  |  |  |

## 3. 定义

**洪水攻击：**在网络安全领域，通常指的是拒绝服务攻击的一种形式，其核心思想非常直接粗暴：用海量的数据流淹没目标服务器、网络或服务，耗尽其关键资源。

## 4. 行为说明

### 4.1 限制查询 SQL 并发度

#### 4.1.1 配置文件

1. taosAdapter 添加默认参数，支持配置文件、环境变量和命令行参数
   - request.queryLimitEnable 全局开关，表示是否启用并发查询 SQL 限制
   - request.default.queryLimit 限制所有来自 HTTP 和 WebSocket 并发查询 SQL 请求数，默认 0 表示不限制
   - request.default.queryWaitTimeout 限制并发请求超过限制后的等待时间（单位：秒），请求等待执行超时后将直接返回错误，默认 900。request.default.queryLimit为 0 时此项无效
   - request.default.queryMaxWait 限制并发请求超过限制后的等待并发最大数，超过此数量的等待请求将直接返回错误。默认值为 0，表示不限制。request.default.queryLimit为 0 时此项无效
  ```toml
  [request]
  queryLimitEnable = false
  [request.default]
  queryLimit = 0
  queryWaitTimeout = 900
  queryMaxWait = 0
  ```

1. 针对每个用户可单独设置 queryLimit、queryWaitTimeout、queryMaxWait 参数，仅支持配置文件
  ```toml
  [request.users]
  root = {queryLimit = 0, queryWaitTimeout = 60, queryMaxWait = 0}
  user1 = {queryLimit = 5, queryWaitTimeout = 30, queryMaxWait = 10}
  ```

1. 如果等待超时或超过等待数
   - HTTP 请求返回 HTTP Code 503
   - WebSocket 请求返回错误码 code:0xfffe
2. request.excludeQueryLimitSql 不进行限制 的查询 SQL 列表，必须以 select 开头忽略大小写，不限制字符数。支持配置文件、环境变量和命令行参数
```toml
[request]
excludeQueryLimitSql = [
    "select 1",
    "select server_version()",
]
```

1. request.excludeQueryLimitSqlRegex 不进行限制的查询 SQL 正则表达式列表。支持配置文件、环境变量和命令行参数
```toml
[request]
excludeQueryLimitSqlRegex = [
    '(?i)^select\s+.*from\s+information_schema.*',
    '(?i)^select\s+.*from\s+performance_schema.*',
]
```

#### 4.1.2 限制范围

1. HTTP 和 WebSocket 请求的 SQL 查询请求（去除开头的空格和换行后以 select 开头的 SQL）
2. 限制从 Query 开始到释放结果为止
3. request.excludeQueryLimitSql 列表内的 SQL 将被排除
   - request.excludeQueryLimitSql 列表内的 SQL 去除开头空格后变成小写字符，将转变为小写的字符串去除中间全部空格
   - 收到 sql 后按照同样的方式进行处理
   - 将处理后的 sql 与处理后的排除列表进行匹配
  例如：排除 `select server_version()`，收到 sql `select  server_version();`
   - 处理排除配置 SQL
  `select server_version()` => `selectserver_version()`
   - 处理接收字符串
  `select  server_version();` => `selectserver_version();`
   - 前缀匹配
  `selectserver_version();` 以 `selectserver_version()`开头，验证通过
1. 符合 request.excludeQueryLimitSqlRegex 列表内正则表达式的 SQL 将被排除
   - 收到 sql 判断是否为 select 开头
   - 按照 excludeQueryLimitSqlRegex 顺序进行正则匹配，只要有一个符合验证通过

### 4.2 单个查询结果获取数据超时

#### 4.2.1 配置文件

1. taosAdapter 添加配置项 result.maxIdle 单位秒，默认 0 表示不超时，如果一个查询结果超过这个配置时间没有进行操作将被释放，收到获取数据请求后将刷新超时时间

## 5. 性能

对于收到 SQL 需要做处理和匹配，每个查询结果增加超时释放机制，查询总时间会增加
由于限制并发度可能导致业务高峰时等待导致时间变长

## 6. 兼容性

无。

## 7. 运维

如果设置参数后产生频繁地超时或因为积压造成内存增长需和用户解释此行为。

## 8. 使用场景

希望针对 taosAdapter 的查询 SQL 请求进行并发控制的情况。

## 9. 约束和限制

限制：
1. 控制并发之后会产生长尾以及超时，需要根据情况判断设置具体并发数
2. 如果客户端对限制并发导致的失败进行重试将产生类似洪水攻击的现象请求量越来越大
3. 限制 taosAdapter 并发数后 taosd 资源仍被占用光
   - 单个请求可能对多个 vnode 或 mnode 进行写入或查询，本身需要大量资源，限制 taosAdapter 请求并发度并不能完全解决 taosd 资源使用
4. 限制 taosAdapter 并发数后性能下降，内存增高，请求延迟变大，taosd 资源利用率不高
   - 限制 taosAdapter 并发数可能将请求积压在 taosAdapter 内，如果正在执行的请求导致阻塞后续请求无法执行，比如某个 vnode 写入或查询阻塞将导致对该 vnode 读写操作长时间无法完成，如果当前执行的请求与该 vnode 有关那么会在 c 接口阻塞。即使后续 taosAdapter 请求不对该 vnode 进行操作也由于之前请求未完成而无法执行
   - 由于请求积压在 taosAdapter 内会导致内存升高和延迟增大
   - 由于限制请求无法将请求都发送到 taosd 去执行会导致 taosd 利用率不高

## 10. 常见错误和排查

1. 内存升高：通过 taosAdapter 监控排查 `adapter_status` 表的 go_heap_sys + go_stack_sys = go 内存占用
   - 如果此占用变大，观察 request_query_inflight 和 request_query_wait_count 请求等待和执行数比较多，此时可能是由于请求积压导致的。
   - 如果 go 内存占用不高排查 ws_ws_sql_result_count 是否增大，如果增大可能是客户端未释放资源或未达到释放时间
   - 内存泄漏：使用 jemalloc 或 tcmalloc 排查内存增长
2. 资源利用率下降：排查 request_query_inflight 是否达到上限，request_query_wait_count 是否有积压
3. 延迟增大：排查 request_query_wait_count 是否有请求积压，是否存在慢查询
4. 大量请求由于并发限制失败：排查 request_query_wait_count 是否有请求积压，是否存在慢查询

## 11. 可观测性

向 taoskeeper 上报指标到 `adapter_request_limit ` tag 为 endpoint 、user
1. query_limit：当前配置的最高查询 SQL 并发度
2. query_max_wait：当前配置查询 SQL 最大等待数
3. query_inflight：当前正在执行的查询 SQL 请求数
4. query_wait_count：正在等待的查询 SQL 请求数
5. query_count：采集周期内收到的查询 SQL 请求数，不包含已排除的 SQL
6. query_wait_fail_count：采集周期内等待超时或超过等待数而失败的查询 SQL 请求数,，不包含已排除的 SQL

## 12. 安装和卸载

无

## 13. 文档

需要修改文档

## 14. 参考文档

无

## 15. 附录

无
