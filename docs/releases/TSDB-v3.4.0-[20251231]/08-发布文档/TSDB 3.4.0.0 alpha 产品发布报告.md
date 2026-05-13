# TSDB 3.4.0.0 alpha 产品发布报告

## 1. 发布概述

1. 发布名称和版本号​：TSDB 3.4.0.0 alpha
2. 发布类型​：季度版本发布
3. 发布日期：2026-01-03
4. 发布摘要：
   - 安全能力提升[企业版]：覆盖身份鉴别、访问控制、存储/传输安全、安全函数、安全审计及加密算法全链路。
   - 虚拟表性能优化：针对投影查询、聚合函数、选择函数、窗口查询等核心场景优化性能。
   - 流计算能力增强：优化事件窗口触发机制，显著降低资源消耗与计算延迟。
   - taosx 高可用[企业版]：新增多节点 XNODE 管理，taosx 任务支持高可用，支持 Kafka 数据写入负载均衡。
   - taosExplorer 支持 OAuth 2.0 和 OIDC( OpenID Connection) 1.0 标准单点登录。
   - 存在 5 个已知问题，不属于P0、P1级别，计划将在下个版本解决。

## 2. 安装包和镜像

1. 下载中心
   - https://www.taosdata.com/download-center?product=TDengine+TSDB-OSS
   - https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise
2. NAS 服务器
   - http://192.168.1.252:5000/
   - 目录：`/Release/TDengine/3.``4``/v``3.4.0.0`
3. Docker Hub
   - tdengine/tsdb:3.4.0.0
   - tdengine/tsdb-ee:3.4.0.0
   - tdengine/tdgpt:3.4.0.0
   - tdengine/tdgpt-full:3.4.0.0

## 3. 发布说明 （Release Notes）

- 官网：https://docs.taosdata.com/releases/notes/3.4.0.0/
- Github: https://github.com/taosdata/TDengine/releases/tag/ver-3.4.0.0
- 飞书文档：[TSDB 3.4.0.0 中英文 Release Notes](https://taosdata.feishu.cn/wiki/Zgx0w59XLij6y6ksGB5cfa6qntf)

## 4. 相关材料

无。

## 5. 行为变更

1. **BREAKING**: taosx 高可用架构变动，不兼容之前的版本，需要重新创建任务，双活也受此影响，建议升级前评估变更或联系研发部确认。
2. **BREAKING**: taosx-agent 受架构变动影响，同样不兼容旧版本，建议暂缓升级。
3. 禁止社区版与企业版互连。
4. `SHOW TABLE DISTRIBUTE` 不支持虚拟超级表。
5. 虚拟表支持的最大列数提升至 `32767` 列。
6. 禁止在超级表的 `state_window`、`count_window` 与 `event_window` 中使用重复时间戳。

## 6. 已知问题

1. 
1. 
1. 
1. 
1. 

## 7. 测试结果

### 7.1 冒烟测试

1. 结论：冒烟测试用例共运行 109 个，失败 1 个，通过率 99%
2. 测试结果： http://192.168.0.176/smoke_test 
3. 备注：失败用例与巡检工具相关，已创建飞书项目缺陷，详见已知[问题 3](https://project.feishu.cn/taosdata_td/job/detail/6651825653).

### 7.2 CI 测试

1. 结论：CI 测试共运行 2266 个用例，测试通过  2266 个，通过率 100%。
2. 测试结果：
  | 组件 | 用例数 | 通过用例数 | 通过率 | 用例详情 |
| --- | --- | --- | --- | --- |
| taosd | 806 | 806 | 1 | [taosd 用例文档](https://taosdata.github.io/TDengine/main/) |
| taosx | 1460 | 1460 | 1 | [taosx 用例文档](https://taosdata.github.io/taosx/) |

1. 测试脚本：
   - taosd: [https://github.com/taosdata/TDengine/blob/main/test/ci/cases.task](https://github.com/taosdata/TDengine/blob/main/test/ci/cases.task)
   - taosx: [https://github.com/taosdata/taosx/tree/main/tests/](https://github.com/taosdata/taosx/tree/main/tests/e2e)
2. 备注：taosx 用例文档的部分内容和格式仍需调整

### 7.3 长时间用例测试

1. 结论：长时间用例测试共进行 1 轮，每轮测试用例 579 个，通过率 99.65%。
2. 测试结果：
  | 组件 | 用例数 | 通过用例数 | 通过率 |
| --- | --- | --- | --- |
| taosd | 199+195+106+79 | 199+193+106+79 | C2/B2 |

1. 测试脚本：
   - taosd: https://github.com/taosdata/TestNG/tree/master/scripts   
   - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest-main-query-replica3/detail/FullTest-main-query-replica3/1281/pipeline
   - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest_main/detail/FullTest_main/3817/pipeline
2. 备注：遗留两个失败用例，已经建立工作项跟踪。

### 7.4 稳定性测试

1. 结论：稳定性测试共测试 6 个场景，持续时间超过 48 小时，未发现问题。
2. 测试结果：
  | 组件 | 测试场景 | 发现问题 | 已修复问题 |
| --- | --- | --- | --- |
| taosd | 查询 | 0个 | 无 |
|  | 写入 | 0个 | 无 |
| taosx | 3.0 -> 3.0 | 0个 | 无 |
|  | kafka datain | 0个 | 无 |
|  | mqtt datain | 0个 | 无 |
|  | opc ua | 0个 | 无 |

1. 测试脚本：
   - taosd: https://github.com/taosdata/TestNG/tree/master/cases/stability 
   - taosx: https://github.com/taosdata/TestNG_taosX/blob/stability/test_stability/stability_test.py
2. 备注：无

### 7.5 性能测试

1. 结论：性能测试共测试 24 个场景，对比版本 3.3.8.0，各版本间测试时 avg、qps 均有小幅波动，未出现明显的性能下降。
2. 测试结果：http://192.168.0.204:3000/d/f39f4b6c-7243-44ee-817f-a8a52b5fe516/baseline-all?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.8.0&var-target_type=release&var-target_label=3.4.0.0
3. 测试脚本：[https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py](https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py)
4. 备注：本次性能测试有两个场景极限波动超过 10%，初步结论是目前测试手段存在抖动，已经建立工作项跟踪。

### 7.6 用户场景的稳定性测试

#### 7.6.1 场景 1：晶澳太阳能

详见：[晶澳太阳能客户场景测试](https://taosdata.feishu.cn/wiki/GYg6wSRbiihaw1kskBHcAxG9nfh)
