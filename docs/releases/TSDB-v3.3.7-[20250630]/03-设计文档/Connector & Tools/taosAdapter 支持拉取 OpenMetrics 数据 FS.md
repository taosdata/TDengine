# taosAdapter 支持拉取 OpenMetrics 数据 FS

## 1. 背景

OpenMetrics 是云原生监控领域的新兴标准（CNCF 项目），扩展并规范了 Prometheus 的指标格式，已成为现代监控工具的事实标准。从 Prometheus 或其他监控系统迁移的用户通常依赖 OpenMetrics 格式，原生支持将简化其数据接入流程。taosAdapter 支持该格式将显著提升 TDengine 的生态兼容性。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/21 | 0.1 | 谭雪峰 |  |
| 2025/06/20 | 0.2 | 谭雪峰 | 支持 prometheus 0.0.4 |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

1. OpenMetrics：一种基于文本的指标暴露格式，是 Prometheus 监控体系的标准化扩展，由 CNCF 维护。

## 4. 行为说明

### 4.1 数据写入

taosAdapter 通过拉取方式获取 OpenMetrics 数据。配置应用的指标接口、权限信息和采集间隔，taosAdapter 按照配置进行数据拉取，获取到的数据转换为无模式格式写入TDengine。同时支持 OpenMetrics 1.0.0 和 Prometheus 0.0.4，判断 response header 进行区分。

### 4.2 配置项

1. open_metrics.enable
  - 类型：bool
  - 默认值：false
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_ENABLE
  - 说明：是否启用 OpenMetrics 采集
1. open_metrics.user
  - 类型：string
  - 默认值：root
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_USER
  - 说明：连接到 TDengine 的用户名
1. open_metrics.password
  - 类型：string
  - 默认值：taosdata
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_PASSWORD
  - 说明：连接到 TDengine 的密码
1. open_metrics.urls
  - 类型：string 数组
  - 默认值：["http://localhost:9100"]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_URLS
  - 说明：采集地址，如果没有指定路由将默认添加`/metrics`
1. open_metrics.dbs
  - 类型：string 数组
  - 默认值：["open_metrics"]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_DBS
  - 说明：写入 TDengine 的数据库，数量与采集地址数量相同，与采集地址一一对应。
1. open_metrics.responseTimeoutSeconds
  - 类型：int 数组
  - 默认值：[5]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_RESPONSE_TIMEOUT_SECONDS
  - 说明：采集超时秒数，必须与采集地址数量相同，与采集地址一一对应。
1. open_metrics.httpUsernames
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_HTTP_USERNAMES
  - 说明：采集使用的 Basic 验证用户名，如果有值，需满足与采集地址数量相同。
1. open_metrics.httpPasswords
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_HTTP_PASSWORDS
  - 说明：采集使用的 Basic 验证密码，如果有值，需满足与采集地址数量相同。
1. open_metrics.httpBearerTokenStrings
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_HTTP_BEARER_TOKEN_STRINGS
  - 说明：采集使用的 Bearer 验证，如果有值，需满足与采集地址数量相同。
1. open_metrics.caCertFiles
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_CA_CERT_FILES
  - 说明：采集使用的根证书路径，如果有值，需满足与采集地址数量相同。
1. open_metrics.certFiles
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_CERT_FILES
  - 说明：采集使用的客户端证书路径，如果有值，需满足与采集地址数量相同。
1. open_metrics.keyFiles
  - 类型：string 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_KEY_FILES
  - 说明：采集使用的客户端证书密钥路径，如果有值，需满足与采集地址数量相同。
1. open_metrics.insecureSkipVerify
  - 类型：bool
  - 默认值：true
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_INSECURE_SKIP_VERIFY
  - 说明：采集是否跳过证书验证。
1. open_metrics.gatherDurationSeconds
  - 类型：int 数组
  - 默认值：[5]
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_GATHER_DURATION_SECONDS
  - 说明：采集间隔秒数，必须与采集地址数量相同，与采集地址一一对应。
1. open_metrics.ignoreTimestamp
  - 类型：bool
  - 默认值：false
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_IGNORE_TIMESTAMP
  - 说明：是否忽略采集到的时间戳，如果忽略将使用采集时刻的时间戳。
1. open_metrics.ttl
  - 类型：int 数组
  - 默认值：空
  - 环境变量：TAOS_ADAPTER_OPEN_METRICS_TTL
  - 说明：数据表超时时间（0 代表不超时），如果有值，需满足与采集地址数量相同。

## 5. 性能

无，使用无模式写入。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

对符合 OpenMetrics 1.0.0 的数据进行拉取写入到 TDengine

## 9. 约束和限制

无

## 10. 常见错误和排查

1. open_metrics.dbs and open_metrics.urls must have the same length
  原因：数据库配置项数量与采集地址配置项数量不一致
1. open_metrics.responseTimeoutSeconds and open_metrics.urls must have the same length
  原因：超时秒数配置项数量与采集地址配置项数量不一致
1. open_metrics.httpUsernames and open_metrics.urls must have the same length
  原因：配置了 Basic 验证用户名但是数量与采集地址不一致
1. open_metrics.httpPasswords and open_metrics.urls must have the same length
原因：配置了 Basic 验证密码但是数量与采集地址不一致
1. open_metrics.httpUsernames and open_metrics.httpPasswords must have the same length
原因：配置了Basic 验证用户名但是没有配置密码
1. open_metrics.httpBearerTokenStrings and open_metrics.urls must have the same length
原因：配置了Bearer 验证但是数量与采集地址不一致
1. open_metrics.caCertFiles and open_metrics.urls must have the same length
  原因：配置了根证书但是数量与采集地址不一致
1. open_metrics.certFiles and open_metrics.urls must have the same length
  原因：配置了客户端证书但是数量与采集地址不一致
1. open_metrics.keyFiles and open_metrics.urls must have the same length
  原因：配置了客户端证书密钥但是数量与采集地址不一致
1. open_metrics.certFiles and open_metrics.keyFiles must have the same length
  原因：配置了客户端证书但是没有配置证书密钥
1. open_metrics.gatherDurationSeconds and open_metrics.urls must have the same length
  原因：采集间隔配置数量与采集地址不一致
1. open_metrics.ttl and open_metrics.urls must have the same length
  配置了表超时但是数量与采集地址不一致

## 11. 可观测性

通过日志文件可以查看配置项与采集过程信息

## 12. 安装和卸载

无

## 13. 文档

需要修改官网文档

## 14. 参考文档

1. https://github.com/prometheus/OpenMetrics/blob/main/specification/OpenMetrics.md
2. https://prometheus.io/docs/specs/om/open_metrics_spec/

## 15. 附录

无
