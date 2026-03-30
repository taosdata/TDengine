# taosAdapter 阻塞时拒绝新请求Test Spec

## 1. 测试目标

这里用于描述本需求主要的测试目标
- taosAdapter在无法从连接池获取连接处理新请求时返回503错误

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.08.28 | 1.0 | 霍宏 | Initial Draft |
|  |  |  |  |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- 两个配置参数有效值生效
- 命令行、环境变量、配置文件优先级
- 高并发场景taosAdapter返回503错误

## 4. 测试结论

测试结果符合设计，测试通过

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 1 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- 

## 7. 测试环境

- OS: Linux

## 8. 测试用例

### 8.1 功能

在提测时，开发应保证基础用例全部通过。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
|  | 最大等待时间 | 命令行参数pool.waitTimeout=-1 | http请求等待60s返回503错误 |  | Pass | 所有用例都是在连接池中无可用连接下进行，下同 |
|  |  | 命令行参数pool.waitTimeout=0 | http请求等待超过60s仍旧等待不返回 |  | Pass |  |
|  |  | 命令行参数pool.waitTimeout=10 | http请求等待10s返回503错误； | Y | Pass |  |
|  |  | 命令行参数pool.waitTimeout=3600 | 大量请求taosAdapter不崩溃 |  | Pass |  |
|  |  | 配置文件参数pool.waitTimeout=10 | http请求等待10s返回503错误 |  | Pass |  |
|  |  | 环境变量TAOS_ADAPTER_POOL_WAIT_TIMEOUT=10 | http请求等待10s返回503错误 |  | Pass |  |
|  |  | 不配置 | http请求等待60s返回503错误 |  | Pass |  |
|  | 最大等待时间配置优先级 | 命令行参数pool.waitTimeout=5，环境变量TAOS_ADAPTER_POOL_WAIT_TIMEOUT=10 | http请求等待5s返回503错误 |  | Pass |  |
|  |  | 环境变量TAOS_ADAPTER_POOL_WAIT_TIMEOUT=5，配置文件参数pool.waitTimeout=10 | http请求等待5s返回503错误 |  | Pass | 环境变量生效需要用命令行启动，用systemctl start无效 |
|  | 最大等待请求数量 | 命令行参数pool.maxWait=-1 | 按0处理，http请求等待不返回 |  | Pass |  |
|  |  | 命令行参数pool.maxWait=0 | http请求等待不返回 |  | Pass |  |
|  |  | 命令行参数pool.maxWait=10 | 新增10个ws请求不返回，第11个请求返回503错误 | Y | Pass |  |
|  |  | 配置文件参数pool.maxWait=10 | http请求等待10s返回503错误 |  | Pass |  |
|  |  | 环境变量TAOS_ADAPTER_POOL_MAX_WAIT=10 | 新增10个ws请求不返回，第11个请求返回503错误 |  | Pass |  |
|  |  | 不配置 | 按0处理，http请求等待不返回 |  | Pass |  |
|  | 最大等待请求数量配置优先级 | 命令行参数pool.maxWait=5，环境变量TAOS_ADAPTER_POOL_MAX_WAIT=10 | 新增5个ws请求不返回，第6个请求返回503错误 |  | Pass |  |
|  |  | 环境变量TAOS_ADAPTER_POOL_MAX_WAIT=5，配置文件参数pool.maxWait=10 | 新增5个ws请求不返回，第6个请求返回503错误 |  | Pass |  |
|  | 同时配置最大等待时间和最大等待请求 | 命令行参数pool.waitTimeout=10 pool.maxWait=10 | 10s内前10个请求等待，第11个请求开始直接返回503,10s后前10个请求返回503，新的10个请求等待，以此循环 |  | Pass |  |
|  | 日志 | 命令行参数pool.waitTimeout=10
日志级别ERROR | 日志中报get connection timeout错误 |  | Pass | DEBUG级别也会报 |
|  |  | 命令行参数pool.maxWait=10
日志级别ERROR | 日志中报exceeded connection pool max wait错误 |  | Pass | DEBUG级别也会报 |
|  | 上游组件 | Java连接器请求 | 正常处理503错误 |  | Pass |  |
|  |  | Explorer请求 | 正常处理503错误 |  | Pass | Explorer使用ws连接，不占用连接池 |
|  | 文档 | 参考手册-产品组件-taosAdapter-taosAdapter 参数列表 | 新增pool.waitTimeout、pool.maxWait参数及说明 |  | Pass |  |

### 8.2 可用性

无

### 8.3 可靠性

无

### 8.4 性能

无

### 8.5 安全性

无

### 8.6 兼容性

无

### 8.7 本地化

无

## 9. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: [taosAdapter_ret_503]

## 10. 风险评估

无

## 11. 测试备忘 (Optional)

 pool.maxConnect 配置连接池最大连接数，测试中可以调小

## 12. 参考文档 (Optional)

这里用于添加对该需求测试有帮助的文档链接：
- [taosAdapter 阻塞时拒绝新请求](https://taosdata.feishu.cn/wiki/O2bgwPREniTqlqkBrdLcLcQynzg)
- 
  TD-31170
