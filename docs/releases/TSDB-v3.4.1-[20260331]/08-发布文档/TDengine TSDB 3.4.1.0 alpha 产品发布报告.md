# 产品发布报告 - TDengine TSDB 3.4.1.0

## 发布概述

1. 发布名称和版本号：TSDB 3.4.1.0 alpha
2. 发布类型：月度版本发布
3. 发布日期：2026-04-02
4. 发布摘要：
   1. TDengine TSDB MCP 服务
   2. taosd 新增数据修复模式支持
   3. taosX 新增力控 pSpace 数据源
   4. taosgen 支持 Windows

## 产品安装包

- 下载中心（OSS）：<https://www.taosdata.com/download-center?product=TDengine+TSDB-OSS>
- 下载中心（Enterprise）：<https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise>
- Docker Image
  - tdengine/tsdb:3.4.1.0
  - tdengine/tsdb-ee:3.4.1.0
  - tdengine/tdgpt:3.4.1.0
  - tdengine/tdgpt:3.4.1.0
  - tdengine/tdgpt-full:3.4.1.0
- NAS 服务器：<https://nas.tdengine.net/>
  - 目录：`/Release/TDengine/3.4/v3.4.1.0`

## 版本发布说明（Release Notes）

- 官网：<https://docs.taosdata.com/releases/notes/3.4.1.0/>
- Github: <https://github.com/taosdata/TDengine/releases/tag/ver-3.4.1.0>
- 飞书文档：3.4.1.0 中英文 Release Notes

## 相关材料

无

## 行为变更

参考：3.4.1.0 中英文 Release Notes

### 产品行为变更

无

### 配置变更

无

## 兼容性说明

无

## 已知问题

无

## 测试结果

### 冒烟测试

1. 结论：冒烟测试用例共运行 150 个，失败 0 个，通过率 100%
2. 测试结果：<http://192.168.0.176/smoke_test>
3. 备注：无

### CI 测试

1. 结论：CI 测试共运行 2365 个用例，测试通过 2365 个，通过率 100%。
2. 测试结果：飞书表格（token: XfaeswcHohTcQmtVY5PcbYmUnLb_gUv3He）
3. 测试脚本：
   1. taosd: <https://github.com/taosdata/TDengine/blob/main/test/ci/cases.task>
   2. taosx: <https://github.com/taosdata/taosx/tree/main/tests/e2e>
4. 测试记录：
   1. <https://github.com/taosdata/TDengine/actions/runs/23848667324>
5. 备注：taosx 用例文档的部分内容和格式仍需调整

### 长时间用例测试

1. 结论：长时间用例测试共进行 1 轮，每轮测试用例 565 个，通过率 100%。
2. 测试结果：飞书表格（token: XfaeswcHohTcQmtVY5PcbYmUnLb_L4fqEU）
3. 测试脚本：
   1. taosd: <https://github.com/taosdata/TestNG/tree/master/scripts>
4. 测试记录：
   1. <http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest-3.0-query-replica3/detail/FullTest-3.0-query-replica3/246/pipeline>
   2. <http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest_main/detail/FullTest_main/3913/pipeline/72>
5. 备注：无

### 稳定性测试

1. 结论：稳定性测试共测试 6 个场景，持续时间超过 48 小时，未发现问题。
2. 测试结果：飞书表格（token: XfaeswcHohTcQmtVY5PcbYmUnLb_P9vh6k）
3. 测试脚本：
   1. taosd: <https://github.com/taosdata/TestNG/tree/master/cases/stability>
   2. taosx: <https://github.com/taosdata/TestNG_taosX/blob/stability/test_stability/stability_test.py>
4. 备注：无

### 性能测试

1. 结论：性能测试共测试 24 个场景，对比版本 3.3.8.0，各版本间测试时 avg、qps 均有小幅波动，未出现明显的性能下降。
2. 测试结果：<http://192.168.0.204:3000/d/f39f4b6c-7243-44ee-817f-a8a52b5fe516/baseline-all?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.8.0&var-target_type=release&var-target_label=3.4.1.0>
3. 测试脚本：<https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py>

### 用户场景的稳定性测试

#### 场景 1：晶澳太阳能

详见：V3.3.5.0 客户场景测试
