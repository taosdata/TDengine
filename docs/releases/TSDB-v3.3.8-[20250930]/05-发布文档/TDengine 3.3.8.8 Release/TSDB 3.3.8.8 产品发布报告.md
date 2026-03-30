# TSDB 3.3.8.8 产品发布报告  

## 1. 发布概述

1. 发布名称/版本号​：TSDB 3.3.8.8。
2. 发布类型​：`常规功能发布`。
3. 发布日期：2025-12-02
4. 关键数据​：本次发布共新增 2 项功能，优化 24 项功能，修复 40 个缺陷。

## 2. 产品安装包

- 下载中心
  - https://www.taosdata.com/download-center?product=TDengine+TSDB-OSS
  - https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise
- Docker Image
  - tdengine/tsdb:3.3.8.8
  - tdengine/tsdb-ee:3.3.8.8
  - tdengine/tdgpt:3.3.8.8
  - tdengine/tdgpt-full:3.3.8.8
- NAS 服务器： http://192.168.1.252:5000/
  - 目录：`/Release/TDengine/3.3/v3.3.8.8`

## 3. 产品相关材料

无。

## 4. 产品发布说明

- 官网：https://docs.taosdata.com/releases/notes/3.3.8.8/
- Github: https://github.com/taosdata/TDengine/releases/tag/ver-3.3.8.8
- 飞书文档：[3.3.8.8 中英文 Release Notes](https://taosdata.feishu.cn/wiki/Ew5hwQuNIisNyGkBKKzcLxLanXg)

## 5. 行为变更

- 关键字变更
  - [TS-7325](https://jira.taosdata.com:18080/browse/TS-7325) 新增：maxSQLLength

## 6. 已知问题

研发确认不阻塞发版，下个版本解决：
- 

## 7. 测试结果

### 7.1 冒烟测试

<quote-container>
结论：冒烟测试用例共运行 151 个，通过率 100%。
</quote-container>

测试结果： http://192.168.0.176/smoke_test 
备注：本次未运行 taosx 的用例。

### 7.2 CI 测试

<quote-container>
结论：CI 测试共运行 971 个用例，测试通过  971  个，通过率 100%。
</quote-container>

详见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 | 用例详情 |
| --- | --- | --- | --- | --- |
| taosd | 867 | 867 | 1 | [taosd 用例文档](https://taosdata.github.io/TDengine/main/) |
| taosx | 104 | 104 | 1 | [taosx 用例文档](https://taosdata.github.io/taosx/) |

测试脚本：
- taosd
  - [https://github.com/taosdata/TDengine/blob/main/test/ci/cases.task](https://github.com/taosdata/TDengine/blob/main/test/ci/cases.task)
- taosx: [https://github.com/taosdata/taosx/tree/main/tests/e2e](https://github.com/taosdata/taosx/tree/main/tests/e2e)
说明：
- taosx 用例文档的部分内容和格式仍需调整

### 7.3 长时间用例测试

<quote-container>
结论：长时间用例测试共进行 1 轮，每轮测试用例 683 个，通过率 99%。
</quote-container>

测试结果见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 |
| --- | --- | --- | --- |
| taosd | 199+195+106+79 | 199+195+106+79-6 | C2/B2 |
| taosx | 104 | 104 | C3/B3 |

测试脚本：
- taosd: https://github.com/taosdata/TestNG/tree/master/scripts   
  - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest_main/detail/FullTest_main/3771/pipeline
  - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest-main-query-replica3/detail/FullTest-main-query-replica3/1261/pipeline
- taosx: [https://github.com/taosdata/taosx/tree/main/tests/e2e](https://github.com/taosdata/taosx/tree/main/tests/e2e)
失败用例：
共有 6 个，均为同一个 schemaless bug，研发确认不阻塞发版，下个版本周期解决。

### 7.4 稳定性测试

<quote-container>
结论：稳定性测试共测试 6 个场景，持续时间超过 48 小时，未发现问题。
</quote-container>

测试结果见下表：
| 组件 | 测试场景 | 发现问题 | 已修复问题 |
| --- | --- | --- | --- |
| taosd | 查询 | 0个 | 无 |
|  | 写入 | 0个 | 无 |
| taosx | 3.0 -> 3.0 | 0个 | 无 |
|  | kafka datain | 0个 | 无 |
|  | mqtt datain | 0个 | 无 |
|  | opc ua | 0个 | 无 |

测试脚本：
- taosd: https://github.com/taosdata/TestNG/tree/master/cases/stability 
- taosx: https://github.com/taosdata/TestNG_taosX/blob/stability/test_stability/stability_test.py

### 7.5 性能测试

<quote-container>
结论：性能测试共测试 24 个场景，对比版本 3.3.8.0，各版本间测试时 avg、qps 均有小幅波动，未出现明显的性能下降。
</quote-container>

测试脚本：[https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py](https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py)
测试结果：http://192.168.0.204:3000/d/f39f4b6c-7243-44ee-817f-a8a52b5fe516/baseline-all?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.8.0&var-target_type=release&var-target_label=3.3.8.8

### 7.6 用户场景的稳定性测试

#### 7.6.1 场景 1：晶澳太阳能

详见：[晶澳太阳能客户场景测试](https://taosdata.feishu.cn/wiki/GYg6wSRbiihaw1kskBHcAxG9nfh)
