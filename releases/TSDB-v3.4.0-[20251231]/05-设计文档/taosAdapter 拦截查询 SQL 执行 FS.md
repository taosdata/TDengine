# taosAdapter 拦截查询 SQL 执行 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-11-17 | 2025-11-18 | 0.1 | 谭雪峰 | 编写文档 |
| 2025-11-19 | 2025-11-19 | 1.0 | 谭雪峰 | 根据 review 意见修改 |

## 2. 背景

TS-7422

taosAdapter 提供异常查询 SQL 拦截功能，根据正则表达式匹配 SQL 并拒绝执行返回错误

## 3. 定义

无

## 4. 行为说明

### 4.1 配置参数

新增参数 `rejectQuerySqlRegex` 值为字符串数组，用于表示拒绝执行的查询 SQL，为保证写入性能，只有非 insert 开头（不区分大小写）的 sql 会被匹配。正则解析使用 Goole RE2 语法[https://github.com/google/re2/wiki/Syntax](https://github.com/google/re2/wiki/Syntax)
此样例配置表示拒绝所有 test_db 数据库的查询请求。
```toml {wrap}
rejectQuerySqlRegex = [
    '(?i)^select\s+.*from\s+test_db.*',
]
```

### 4.2 响应

1. HTTP 请求返回 HTTP Code 403
2. WebSocket 请求返回错误码 code:0xfffd

### 4.3 动态更新

 taosAdapter 将监控配置文件的 create 和 write 事件
1. 修改配置文件后 taosAdapter 会自动读取并启用最新的拒绝执行的查询 SQL 列表，如果配置错误将打印错误信息，并沿用之前成功的配置。
2. 如果 taosAdapter 启动没有配置文件，启动后不要直接创建配置文件，如有必要在其他位置创建修改好后复制过来
3. taosAdapter 监控配置文件变化后会读取 rejectQuerySqlRegex 和 log.level 这两个配置项的变化，其他配置修改不生效
4. 当配置文件修改时将忽略环境变量、命令行参数以及通过 http 接口修改的配置，以修改后的配置文件为准。例如通过 http 接口修改了 log.level 为 debug，配置文件中 log.level 为 info，如果只修改配置文件中的 rejectQuerySqlRegex 配置，日志级别也会被改为 info
5. 修改文件使用 vim 等创建临时文件再复制的工具不要直接修改文件内容，有可能读取到错误的文件格式

## 5. 性能

启用该功能时查询语句将进行正则表达式匹配，执行时间将变长

## 6. 安全

不提供接口进行配置修改，修改配置文件方式依托于系统安全和系统权限

## 7. 兼容性

无

## 8. 运维

无。

## 9. 使用场景

拒绝查询 SQL 的场景

## 10. 约束和限制

约束：
1. 为降低对写入的影响，只限制非 insert 开头的 sql（insert 不区分大小写）
2. 如果动态更新配置错误将继续沿用之前的配置
3. 如果 taosAdapter 启动没有配置文件，如果启动后再创建 taosAdapter 将会读到空配置文件
限制：只对 `/rest/sql` 和 `/ws` 这两个端点的 sql 执行，`/rest/ws` 端点不进行此特性开发

## 11. 常见错误和排查

1. 更新配置后不生效：排查修改格式是否符合要求，taosAdapter 日志排查是否存在配置文件解析错误或正则表达式解析错误

## 12. 可观测性

对于拒绝的 SQL 将在日志中记录该 SQL 被拒绝的溯源信息，包括client_ip，client_port，req_id，user，app_name（如果存在），匹配的正则表达式

## 13. 安装和卸载

无

## 14. 文档

需要修改文档

## 15. 参考文档

## 16. 附录
