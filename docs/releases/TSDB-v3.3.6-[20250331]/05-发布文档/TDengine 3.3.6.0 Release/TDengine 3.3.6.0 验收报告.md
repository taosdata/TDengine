# TDengine 3.3.6.0 验收报告

## 1. 验收结论

## 2. 安装包地址

NAS 服务器： http://192.168.1.252:5000/
目录：`/Release/TDengine/3.3/v``3.3.6.0`

## 3. 版本发布说明 （Release Notes）

[3.3.6.0 中英文 Release Notes](https://taosdata.feishu.cn/docx/M46VdVRzhoZXrFxbFLGc10uwnbb)

## 4. 已知问题

## 5. 验收测试详情

### 5.1 冒烟测试

<quote-container>
结论：冒烟测试用例共运行129 个，通过率 100%。
</quote-container>

测试结果： http://192.168.0.176/smoke_test 

### 5.2 CI 测试

<quote-container>
结论：CI 测试共运行 1873 个用例，测试通过 1873 个，通过率 100%。
</quote-container>

详见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 | 用例详情 |
| --- | --- | --- | --- | --- |
| taosd | 1768 | 1768 | 1 | n/a |
| taosx | 105 | 105 | 1 | [用例文档](https://taosdata.github.io/taosx/) |

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
  - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest-3.0-query-replica3/detail/FullTest-3.0-query-replica3/49/pipeline
  - http://ci.bl.taosdata.com:8080/blue/organizations/jenkins/FullTest3.0/detail/FullTest3.0/3493/pipeline
  - http://ci.bl.taosdata.com:8080/job/FullTest3.0/3496/execution/node/73/log/
- taosx: [https://github.com/taosdata/taosx/tree/main/tests/e2e](https://github.com/taosdata/taosx/tree/main/tests/e2e)

### 5.4 稳定性测试

<quote-container>
结论：稳定性测试共测试 6 个场景，持续时间超过 48 小时，捕获异常 0 个（捕获异常 0 个，已修复 0个）。
</quote-container>

测试结果见下表：
| 组件 | 测试场景 | 发现问题 | 已修复问题 |
| --- | --- | --- | --- |
| taosd | 查询 | 0个 | 无 |
|  | 写入 | 0个 | 无 |
| taosx | 3.0 -> 3.0 | 0个 | 无 |
|  | kafka | 0个 | 无 |
|  | mqtt | 0个 | 无 |
|  | OPCUA | 0个 | 无 |

测试脚本：
- taosd: https://github.com/taosdata/TestNG/tree/master/cases/stability 
- taosx: https://github.com/taosdata/TestNG_taosX/blob/stability/test_stability/stability_test.py

### 5.5 性能测试

<quote-container>
结论：性能测试共测试 24 个场景，对比版本 3.3.5.0，各版本间测试时avg、qps均有小幅波动，未出现明显的性能下降，有个查询经过TS-5766优化后提升。
</quote-container>

测试脚本：[https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py](https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py)测试结果：http://192.168.0.204:3000/d/f001dc11-7c37-4b1f-a82c-dd4fbf4bdc4c/baseline-all-version?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.5.0&var-target_type=release&var-target_label=3.3.6.0

### 5.6 用户场景的稳定性测试

#### 5.6.1 场景1：晶澳太阳能

详见：[晶澳太阳能客户场景测试](https://taosdata.feishu.cn/wiki/GWf3wF8SLicjlVkSa9Sc2lABnpg)
