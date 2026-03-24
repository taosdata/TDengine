# TDengine 企业版 3.3.4.3 验收报告

## 1. 验收结论

TDengine 企业版 3.3.4.3 版本验收通过，准予发布。

## 2. 安装包地址

NAS 服务器： http://192.168.1.252:5000/
目录：`/``Release/TDengine/3.3/v3.3.4.3`

## 3. 版本发布说明 （Release Notes）

[3.3.4.3 中英文 Release Notes ](https://taosdata.feishu.cn/wiki/WgoNwHAEIiV8QtkFuRUcTPywnbf)

## 4. 已知问题

1. 
  TD-32855

## 5. 验收测试详情

### 5.1 CI 测试

<quote-container>
结论：CI 测试共运行 1624 个用例，测试通过 1624个，通过率 100%.
</quote-container>

详见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 |
| --- | --- | --- | --- |
| TDengine | 1527 | 1527 | 1 |
| taosX | 97 | 97 | 1 |

### 5.2 全量测试

<quote-container>
结论：全量测试共进行 xx 轮，每轮测试用例 676 个，通过率 100%.
</quote-container>

详情见下表：
| 组件 | 用例数 | 通过用例数 | 通过率 |
| --- | --- | --- | --- |
| TDEngine | 199+195+106+79 | 199+195+106+79 | C2/B2 |
| TaosX | 97 | 97 | C3/B3 |

### 5.3 稳定性测试

<quote-container>
结论：稳定性测试共测试 2 个场景，因磁盘空间问题，只持续时间超过 36 小时，捕获异常 0 个（捕获异常 0 个，已修复 0 个）。
</quote-container>

详情见下表：
| 组件 | 测试场景 | 运行时间所执行的操作 | 发现问题 | 已修复问题 |
| --- | --- | --- | --- | --- |
| TDEngine | 查询 | 完成101544988次查询 | 0个 | 无 |
|  | 写入 | 完成9033610次写入 | 0个 | 无 |

### 5.4 性能测试

<quote-container>
结论：性能测试共测试 24 个场景，对比版本 3.3.2.0，除 Q30001, W10002 两个场景以外，其余场景整体性能无明显差异。
测试脚本：[https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py](https://github.com/taosdata/TestNG/blob/perf_tool_v3/perf-test/cliconsole.py)
测试结果：[http://192.168.0.204:3000/d/e4dca7c7-ae86-44b3-a61c-a16aefe7483b/baseline?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.2.0&var-target_type=release&var-target_label=3.3.4.3](http://192.168.0.204:3000/d/e4dca7c7-ae86-44b3-a61c-a16aefe7483b/baseline?orgId=1&refresh=1m&var-base_type=release&var-base_label=3.3.2.0&var-target_type=release&var-target_label=3.3.4.3)
</quote-container>

详情如下：
1. Q30001 percentile QPS 下降了 70% 左右，已提单待修复：，内部评估，percentile() 使用并不广泛，不影响发版。
  TD-32855

1. W10002 cachemodel=last insert QPS下降约11.85%，此问题已修复，测试结论和发开自测结论一致，后续以此数据为新的基准
  TD-32338
