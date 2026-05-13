# 产品发布报告 - TDengine TSDB 3.4.0.2 

## 1. 发布概述

1. 发布名称和版本号​：TSDB 3.4.0.2
2. 发布类型​：月度版本发布
3. 发布日期：2026-02-02

## 2. 产品安装包

1. 下载中心
  - https://www.taosdata.com/download-center?product=TDengine+TSDB-OSS
  - https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise
1. NAS 服务器
  - http://192.168.1.252:5000/
  - 目录：`/Release/TDengine/3.4/v3.4.0.2`
1. Docker Hub
  - tdengine/tsdb:3.4.0.2
  - tdengine/tsdb-ee:3.4.0.2
  - tdengine/tdgpt:3.4.0.2
  - tdengine/tdgpt-full:3.4.0.2

## 3. 发布说明 （Release Notes）

- 官网：[https://docs.taosdata.com/releases/notes/3.4.0.2/](https://docs.taosdata.com/releases/notes/3.4.0.2/)
- Github: [https://github.com/taosdata/TDengine/releases/tag/ver-3.4.0.2](https://github.com/taosdata/TDengine/releases/tag/ver-3.4.0.2)
- 飞书文档：[TSDB 3.4.0.2 中英文 Release Notes](https://taosdata.feishu.cn/wiki/LHz0wCsk2iebbEkBjjXc2Fppnae)

## 4. 相关材料

无

## 5. 行为变更

### 5.1 产品行为变更

- 6728731962 重命名 lag 函数为 fill_forward, 参数和行为不变

### 5.2 配置变更

无

## 6. 兼容性说明

无

## 7. 已知问题

无

## 8. 测试结果

### 8.1 冒烟测试

1. 结论：冒烟测试用例共运行 109 个，失败 0 个，通过率 100%
2. 测试结果： http://192.168.0.176/smoke_test 

### 8.2 CI 测试

1. 结论：CI 测试共运行 2288 个用例，测试通过  2288 个，通过率 100%。
2. 测试结果：
  | 组件 | 用例数 | 通过用例数 | 通过率 | 用例详情 |
| --- | --- | --- | --- | --- |
| taosd | 828 | 828 | 1 | [taosd 用例文档](https://taosdata.github.io/TDengine/main/) |
| taosx | 1460 | 1460 | 1 | [taosx 用例文档](https://taosdata.github.io/taosx/) |

1. 测试脚本：
   - taosd: [https://github.com/taosdata/TDengine/blob/main/test/ci/cases.task](https://github.com/taosdata/TDengine/blob/main/test/ci/cases.task)
   - taosx: [https://github.com/taosdata/taosx/tree/main/tests/](https://github.com/taosdata/taosx/tree/main/tests/e2e)
2. 备注：taosx 用例文档的部分内容和格式仍需调整

### 8.3 长时间用例测试

1. 结论：长时间用例测试共进行 1 轮，每轮测试用例 579 个，通过率 100%。
2. 测试结果：
  | 组件 | 用例数 | 通过用例数 | 通过率 |
| --- | --- | --- | --- |
| taosd | 199+195+106+79 | 199+195+106+79 | C2/B2 |

1. 测试脚本：
   - taosd: https://github.com/taosdata/TestNG/tree/master/scripts   
   - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest-main-query-replica3/detail/FullTest-main-query-replica3/1294/pipeline
   - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest_main/detail/FullTest_main/3846/pipeline

### 8.4 稳定性测试

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

### 8.5 性能测试

1. 结论：性能测试共测试 24 个场景，对比版本 3.3.8.0，各版本间测试时 avg、qps 均有小幅波动，未出现明显的性能下降。
2. 测试结果：http://192.168.0.204:3000/d/f39f4b6c-7243-44ee-817f-a8a52b5fe516/baseline-all?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.8.0&var-target_type=release&var-target_label=3.4.0.2
3. 测试脚本：[https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py](https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py)

### 8.6 用户场景的稳定性测试

#### 8.6.1 场景 1：晶澳太阳能

详见：[晶澳太阳能客户场景测试](https://taosdata.feishu.cn/wiki/GYg6wSRbiihaw1kskBHcAxG9nfh)
