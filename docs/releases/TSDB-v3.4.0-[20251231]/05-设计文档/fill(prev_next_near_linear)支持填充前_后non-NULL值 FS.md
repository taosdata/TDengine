# fill(prev/next/near/linear)支持填充前/后non-NULL值 FS

## 1. 背景

客户期望在插值时，能够有方法使prev/next插值方法不断向前/后探索有效（non-NULL）数据进行插值。
RS：[需求报告：fill(prev)行为调整](https://taosdata.feishu.cn/wiki/Mjcjw7pBviBnFEkwIeOcA5rCnrh)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/12/25 | 1.0 | @张天毅 | 初始版本 |

## 3. 定义

Range：指interp子句中`RANGE`字段指定的输出时间范围
Main scan：指针对range闭区间内部的正序数据扫描
Prev/Next scan：指针对range区间之前/后数据的倒序/正序扫描

## 4. 行为说明

`interp(column, ignore_null_values) where ... range(ts1, ts2) fill(prev/next/near/linear)`语句，除了会扫描时间范围在range闭区间`[ts1, ts2]`内的数据，还会扫描这区间外满足`where`条件的数据，例如未给定`where`条件时，包括倒序扫描`[MIN_INT64, ts1)`和正序扫描`(ts2, MAX_INT64]`。
在3.3.8.x及之前版本，Prev scan和Next scan均只扫描一行数据，无论插值目标列的数据是否为NULL均用其进行插值。
从3.4.0.0版本开始，该行为修改为：Prev scan和Next scan不断扫描直到出现有效数据为止，有效数据的判断取决于`ignore_null_values`参数，即参数为0（缺省值）时，NULL和non-NULL数据均为有效数据；参数为1时，NULL数据均被忽略，故只有non-NULL数据为有效数据。

## 5. 性能

`ignore_null_values`参数值为0时，性能应无明显变化；`ignore_null_values`参数值为1时，性能受数据中NULL数据的密度影响，最坏情况是需要扫描全表数据才能完成range时间段内的插值。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

与interp类似。

## 9. 约束和限制

约束：无
限制：`ignore_null_values`参数值为1时，性能受数据中NULL数据的密度影响，最坏情况是需要扫描全表数据才能完成range时间段内的插值。使用时建议通过`where`条件限定扫描数据的时间范围，非必要情况使用缺省值即可。

## 10. 常见错误和排查

`ignore_null_values`参数值为1不代表一定能找到non-NULL值进行填充，若Prev/Next scan扫描到的数据全部为NULL，仍将导致部分时间截面无法插值，从而不会输出这部分时间截面的插值结果数据。

## 11. 文档

文档内容已修改

## 12. 参考文档

[fill(prev/next) 目前行为](https://taosdata.feishu.cn/wiki/LlliwtW75icEeVkQq1EcVmGvn9I)
[需求报告：fill(prev)行为调整](https://taosdata.feishu.cn/wiki/Mjcjw7pBviBnFEkwIeOcA5rCnrh)
