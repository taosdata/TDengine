# fill(prev/next/near/linear)支持填充前/后non-NULL值 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-25 | 2025-12-25 | 1.0 | @张天毅 | 创建文档 |

## 2. 测试目标

- `interp`算子的参数`ignore_null_values`值为1时，`fill(prev/next/near/linear)`在批查询中的行为正确
- `interp`算子的参数`ignore_null_values`值为1时，`fill(prev/next/near/linear)`在流计算中的行为正确

## 3. 参考文档

[需求报告：fill(prev)行为调整](https://taosdata.feishu.cn/wiki/Mjcjw7pBviBnFEkwIeOcA5rCnrh)
[fill(prev/next/near/linear)支持填充前/后non-NULL值 FS](https://taosdata.feishu.cn/wiki/WIDJwUD99ivhojkMCzQcRtYRnkf)

## 4. 测试结论

测试结果正确

## 5. 测试环境

- OS: Linux

## 6. 功能测试

### 6.1 批查询

#### 6.1.1 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_interp_fill_ignore_null | 批查询中检查结果正确，通过fill(prev/next/near/linear)对单点和时间范围进行插值，源表包括普通表、超级表、超级表按tbname分组、超级表按tag分组 | 通过 |
| 2 | test_interp_fill_ignore_null_scan | 检查Prev/Next scan在扫描到有效数据后能够**及时停止**扫描，queryPolicy=1时通过日志检查是否只扫描了有限的数据而非全量数据 | 通过 |
| 3 | test_notify_table_merge_scan | 检查底层为table merge scan时功能正常、无内存泄漏，源表包括超级表、超级表按tbname分组、超级表按tag分组 |  |

### 6.2 流计算

#### 6.2.1 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_interp_fill_ignore_null_stream_basic | 使用少量数据，流计算的计算子句中通过fill(prev/next/near/linear)，对_twstart单点和[_twstart, _twend]时间范围进行插值，源表为普通表 | 通过 |
| 2 | test_interp_fill_ignore_null_stream_advance | 使用较多数据（数据量大于datablock大小）测试，同时测试超级表、%%trows等功能正常，源表包括普通表、超级表、%%trows占位符 | 通过 |

## 7. 性能测试

无

## 8. 安全测试

无

## 9. 兼容性测试

兼容老版本

## 10. 已知问题和限制

限制：`ignore_null_values`参数值为1时，性能受数据中NULL数据的密度影响，最坏情况是需要扫描全表数据才能完成range时间段内的插值。使用时建议通过`where`条件限定扫描数据的时间范围，非必要情况使用缺省值即可。
