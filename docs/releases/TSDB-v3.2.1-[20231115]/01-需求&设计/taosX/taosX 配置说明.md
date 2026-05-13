# taosX 配置说明

## 1. 背景

目前 taosx 项目中存在常量、配置文件、环境变量及命令行参数四种配置方式，由于配置方式种类多但配置项所支持的配置方式不尽相同，所以编写此文档进行一个简单的说明，供使用者进行参考。

## 2. 行为说明

当同一个配置项使用不同配置方式存在多个不同值时，配置的优先级遵循：命令行参数 > 环境变量 > 配置文件 > 常量（默认值）。

## 3. 配置列表

| **使用范围** | **配置项** | **默认值** | **配置文件** | **环境变量** | **命令行参数** |
| --- | --- | --- | --- | --- | --- |
| Vendor 名称 | "TDengine" | - | CUS_NAME | - |
| Vendor 缩写 | "taos" | - | CUS_PROMPT | - |
| 插件路径 | windows: "C:\TDengine\plugins" else: "/usr/local/taos/plugins" | plugins_home | TAOSX_PLUGINS_HOME | --plugins-home |
| 数据目录 | windows: "C:\TDengine\data\taosx" else: "/var/lib/taos/taosx" | data_dir | TAOSX_DATA_DIR | --data-dir |
| 日志文件目录 | windows: "C:\TDengine\log" else: "/var/log/taos" | logs_home | TAOSX_LOGS_HOME | --logs-home |
| taosx 默认日志等级 | info | log_level | TAOSX_LOG_LEVEL | - |
| taosx 调试开关 | false | debug | - | -d --debug |
| taosx 日志保存天数 | 30 | log_keep_days | LOG_KEEP_DAYS | --log-keep-days |
| Taosx TMQ 任务数量 | 0 | jobs | - | -j --jobs |
| taosx 链路追踪开关 | false | otel | ENABLE_OTEL | --otel |
| taosx 配置文件 | windows: "C:\TDengine\cfg\taosx.toml" else: "/etc/taos/taosx.toml" | - | - | -c --config |
| taosx 强制执行 | false | - | - | -y --yes-i-really-mean-it |
| taosx run 链路追踪事件 | [] | - | TRACING_EVENTS | --tracing-events |
| ~~CSV 文件最大有效行数~~ | ~~10~~ | ~~csv_max_validate_lines~~ | ~~-~~ | ~~-~~ |
| ~~legacy SQL 语句最大长度~~ | ~~1000 * 1000~~ | ~~max_sql_len~~ | ~~-~~ | ~~-~~ |
| ~~legacy 超级表最大显示数量~~ | ~~5~~ | ~~max_display_stables~~ | ~~-~~ | ~~-~~ |
| ~~legacy WebSocket最大重试次数~~ | ~~5~~ | ~~max_ws_retries~~ | ~~-~~ | ~~-~~ |
| ~~legacy TD 客户端参数集合~~ | ~~[&str; 53]~~ | ~~td_client_options~~ | ~~-~~ | ~~-~~ |
| taosx run 数据源 DSN | - | - | - | -f --from |
| taosx run 目标库 DSN | - | - | - | -t --to |
| taosx run 解析配置 | - | - | - | -p --parser |
| taosx run 转换配置 | - | - | - | -T --transform |
| taosx run 持续运行开关 | false | - | - | -e --endless |
| taosx run 开启 websocket | false | - | - | -w --websocket |
| taosx 不恢复已存在任务 | false | - | - | --do-not-resume |
| taosx serve 监听地址 | 0.0.0.0:6050 | listen | LISTEN | -l --listen |
| taosx serve 数据库连接地址 | sqlite:taosx.db | database_url | DATABASE_URL | -D --database-url |
| taosx serve 密钥前缀 | "XaNeGt" | secret_prefix | - | - |
| 插件路径 | windows: "C:\TDengine\plugins" else: "/usr/local/taos/plugins" | plugins_home | PLUGINS_HOME | --plugins-home |
| 数据目录 | windows: "C:\TDengine\data\taosx" else: "/var/lib/taos/taosx" | data_dir | TAOSX_DATA_DIR | --data-dir |
| agent 配置文件 | windows: "C:\TDengine\cfg\agent.toml" else: "/etc/taos/agent.toml" | - | - | -c --config |
| 日志文件目录 | windows: "C:\TDengine\log" else: "/var/log/taos" | logs_home | LOGS_HOME | --logs-home |
| agent 地址 | - | agent单独配置文件 endpoint | - | -e --endpoint |
| agent 令牌 | - | agent单独配置文件 token | - | -t --token |
| 日志等级 | info | agent单独配置文件 log_level | LOG_LEVEL | - |
| 日志保存天数 | 30 | agent单独配置文件 log_keep_days | LOG_KEEP_DAYS | --log-keep-days |

## 4. 配置文件示例

### 4.1 taosx.toml

```json

## 5. 插件路径

plugins_home = "/usr/local/taos/plugins"

## 6. 数据目录

data_dir = "/var/lib/taos/taosx"

## 7. 日志文件目录

logs_home = "/var/log/taos"

## 8. 日志等级

log_level = "info"

## 9. 日志调试开关

debug = false

## 10. 日志保存天数

log_keep_days = 30

## 11. TMQ 任务数量

jobs = 0

## 12. 链路追踪开关

otel = false

### 12.1 serve

[serve]

## 13. 监听地址

listen = "0.0.0.0:6050"

## 14. 数据库连接地址

database_url = "sqlite:taosx.db"

## 15. 密钥前缀

secret_prefix = "XaNeGt"
```

### 15.1 agent.toml

```json

## 16. 插件路径

plugins_home = "/usr/local/taos/plugins"

## 17. 数据目录

data_dir = "/var/lib/taos/taosx"

## 18. 日志文件目录

logs_home = "/var/log/taos"

## 19. agent 地址

endpoint = "http://localhost:6055"

## 20. agent 令牌

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjMsImlhdCI6MTY5Mjk0NjUzN30.UqLjhcsN2F7KOo9sTRSEKzzviajar4sOpTOr9bNDCwU"

## 21. 日志等级

log_level = "info"

## 22. 日志保存天数

log_keep_days = 30
```
