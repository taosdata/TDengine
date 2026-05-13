# TDengine 3.3.4.8 验收报告

## 1. 验收结论

已知问题，其他均验收通过，准予发布
TD-33066

## 2. 安装包地址

NAS 服务器： http://192.168.1.252:5000/
目录：`/Release/TDengine/3.3/v3.3.4.8`

## 3. 版本发布说明 （Release Notes）

[3.3.4.8 中英文 Release Notes](https://taosdata.feishu.cn/docx/TPa0dO6HzoHtkZxoWkQcNYpbnKf)

## 4. 已知问题

1. 
  TD-33066

## 5. 验收测试详情

### 5.1 CI 测试

<quote-container>
结论：CI 测试共运行 1654 个用例，测试通过 1654 个，通过率 100%。
</quote-container>

详见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 | 用例详情 |
| --- | --- | --- | --- | --- |
| taosd | 1550 | 1550 | 1 | n/a |
| taosx | 104 | 104 | 1 | [用例文档](https://taosdata.github.io/taosx/) |

测试脚本：
- taosd: [https://github.com/taosdata/TDengine/blob/main/tests/parallel_test/cases.task](https://github.com/taosdata/TDengine/blob/main/tests/parallel_test/cases.task)
- taosx: [https://github.com/taosdata/taosx/tree/main/tests/e2e](https://github.com/taosdata/taosx/tree/main/tests/e2e)
说明：
- taosx 用例文档的第一版已生成，但部分内容和格式仍需调整
- taosd 的用例文档待生成

### 5.2 长时间用例测试

<quote-container>
结论：长时间用例测试共进行 2 轮，每轮测试用例 693 个，通过率 100%。
</quote-container>

测试结果见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 |
| --- | --- | --- | --- |
| taosd | 199+195+106+79 | 199+195+106+79 | C2/B2 |
| taosx | 104 | 104 | C3/B3 |

测试脚本：
- taosd: https://github.com/taosdata/TestNG/tree/master/scripts
- taosx: [https://github.com/taosdata/taosx/tree/main/tests/e2e](https://github.com/taosdata/taosx/tree/main/tests/e2e)

### 5.3 稳定性测试

<quote-container>
结论：稳定性测试共测试 2 个场景，持续时间超过 48 小时，捕获异常 0 个（捕获异常 0 个，已修复 0 个）。
</quote-container>

测试结果见下表：
| 组件 | 测试场景 | 发现问题 | 已修复问题 |
| --- | --- | --- | --- |
| taosd | 查询 | 0个 | 无 |
|  | 写入 | 0个 | 无 |
| taosx | 2.6 -> 3.0 | n/a | n/a |
|  | data replication | n/a | n/a |
|  | PI | n/a | n/a |

测试脚本：
- taosd: https://github.com/taosdata/TestNG/tree/master/cases/stability
- taosx: https://github.com/taosdata/taosx/blob/main/tests/e2e/test_function/stability_test.py
说明：
- taosx 的稳定性测试在发版时尚未执行

### 5.4 性能测试

<quote-container>
结论：性能测试共测试 24 个场景，对比版本 3.3.4.3，只有 3 个场景有 1.1%以内的下降，其余场景均有小幅提升。另外 percentile 提升明显达到 369%，是因为上个版本有 bug，此版本已修复。
</quote-container>

测试脚本：[https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py](https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py)
测试结果：http://192.168.0.204:3000/d/e4dca7c7-ae86-44b3-a61c-a16aefe7483b/baseline?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.4.3&var-target_type=release&var-target_label=3.3.4.8
