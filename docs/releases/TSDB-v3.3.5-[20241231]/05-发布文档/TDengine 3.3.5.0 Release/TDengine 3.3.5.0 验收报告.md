# TDengine 3.3.5.0 验收报告

## 1. 验收结论

平台部验收通过，并由研发部确认，可以发版。

## 2. 安装包地址

NAS 服务器： http://192.168.1.252:5000/
目录：`/Release/TDengine/3.3/v3.3.5.0`

## 3. 版本发布说明 （Release Notes）

[3.3.5.0 中英文 Release Notes](https://taosdata.feishu.cn/wiki/CNXNwfTgHiDcxSkxj8GcmEw3nce)

## 4. 已知问题

无

## 5. 验收测试详情

### 5.1 冒烟测试

<quote-container>
结论：冒烟测试用例共运行129 个，通过率 100%。
</quote-container>

测试结果： http://192.168.0.176/smoke_test 

### 5.2 CI 测试

<quote-container>
结论：CI 测试共运行 1716 个用例，测试通过 1716 个，通过率 100%。
</quote-container>

详见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 | 用例详情 |
| --- | --- | --- | --- | --- |
| taosd | 1613 | 1613 | 1 | n/a |
| taosx | 104 | 104 | 1 | [用例文档](https://taosdata.github.io/taosx/) |

测试脚本：
- taosd
  - [https://github.com/taosdata/TDengine/blob/main/tests/parallel_test/cases.task](https://github.com/taosdata/TDengine/blob/main/tests/parallel_test/cases.task)
  - http://ci.bl.taosdata.com:8080/view/fulltest/job/FullTest3.0/3370/execution/node/116/log/?consoleFull
- taosx: [https://github.com/taosdata/taosx/tree/main/tests/e2e](https://github.com/taosdata/taosx/tree/main/tests/e2e)
说明：
- taosx 用例文档的第一版已生成，但部分内容和格式仍需调整
- taosd 的用例文档待生成

### 5.3 长时间用例测试

<quote-container>
结论：长时间用例测试共进行 1 轮，每轮测试用例 683 个，通过率 100%。
</quote-container>

测试结果见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 |
| --- | --- | --- | --- |
| taosd | 199+195+106+79 | 199+195+106+79 | C2/B2 |
| taosx | 104 | 104 | C3/B3 |

测试脚本：
- taosd: https://github.com/taosdata/TestNG/tree/master/scripts   
  - http://ci.bl.taosdata.com:8080/view/fulltest/job/FullTest3.0/3372/execution/node/65/log/
  - http://ci.bl.taosdata.com:8080/view/fulltest/job/FullTest3.0/3370/execution/node/88/log/
  - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest-main-query-replica3/detail/FullTest-main-query-replica3/962/pipeline
- taosx: [https://github.com/taosdata/taosx/tree/main/tests/e2e](https://github.com/taosdata/taosx/tree/main/tests/e2e)

### 5.4 稳定性测试

<quote-container>
结论：稳定性测试共测试 6 个场景，持续时间超过 48 小时，捕获异常 7 个（捕获异常 7 个，已修复 7个）。
</quote-container>

测试结果见下表：
| 组件 | 测试场景 | 发现问题 | 已修复问题 |
| --- | --- | --- | --- |
| taosd | 查询 | 2个 | 2个 |
|  | 写入 | 2个 | 2个 |
| taosx | 3.0 -> 3.0 | 3个 | 3个 |
|  | opcua(直连方式) |  |  |
|  | kafka(直连方式) |  |  |
|  | mqtt(直连方式) |  |  |

测试脚本：
- taosd: https://github.com/taosdata/TestNG/tree/master/cases/stability 
- taosx: https://github.com/taosdata/TestNG_taosX/blob/stability/test_stability/stability_test.py

### 5.5 性能测试

<quote-container>
结论：性能测试共测试 24 个场景，对比版本 3.3.4.3，3.3.4.8，各版本间测试时avg、qps均有小幅波动，未出现明显的性能下降。
</quote-container>

测试脚本：[https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py](https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py)
测试结果：[V3.3.5.0 性能测试结果](https://taosdata.feishu.cn/wiki/Yhrrwb5pEikC6DkRiy6cym8mn6g)

### 5.6 外部性能基准测试

详见：[TS-5713 -信通院V3.3.5.0版本测试结果](https://taosdata.feishu.cn/wiki/IZ30wQErAihcJPkABSJcrUu7nzb)

### 5.7 用户场景的稳定性测试

#### 5.7.1 场景1：晶澳太阳能

详见：[V3.3.5.0客户场景测试](https://taosdata.feishu.cn/wiki/GYg6wSRbiihaw1kskBHcAxG9nfh)
