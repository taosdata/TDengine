# 状态窗口跨 datablock TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-30 | YYYY-MM-DD | 0.1 | @张天毅 | 创建 |
| 2025-11-03 | 2025-11-03 | 1.0 | @张天毅 | 明确测试目标，记录测试结果 |
| 2025-11-25 | 2025-11-25 | 1.1 | @张天毅 | 增加测试项7 |

## 2. 测试目标

对于单个datablock内部的状态窗口，目前的处理是正确的。
但是对于跨datablock，尤其是datablock边界含有连续null状态数据的情况，状态窗口会错误计算窗口内的数据。
目标是测试“datablock边界有null状态数据”和“datablock中所有数据状态列为null”这两种情况分别发生和一起发生时状态窗口的聚合计算结果是否正确。

## 3. 参考文档

JIRA: [TD-38431](https://jira.taosdata.com:18080/browse/TD-38431)
JIRA: [TD-38673](https://jira.taosdata.com:18080/browse/TD-38673)
[状态窗口起止点配置功能 - FS](https://taosdata.feishu.cn/wiki/EqrJw4xTuiqP8EkoXWPcawq4ngh)

## 4. 测试结论

修复前
1. Extend = 0/1时，若每个datablock中均有non-null数据则可跨越多个datablock；丢弃数据全为null的datablock
2. Extend = 2时，任意datablock以null结尾时出错，向前扩展的null仅限于当前datablock
修复后
上述场景均处理正确

## 5. 功能测试

#### 5.0.1 测试要点

1. Data size > datablock size(4096)
2. Datablock contains null, sometimes only null

#### 5.0.2 用例列表

| 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- |
| 1. 三个datablock全为non-null数据，Extend = 0/1/2 | PASS |
| 2. null数据出现在datablcok内部（非边界），Extend = 0/1/2 | PASS |
| 3. null数据出现在首尾两个datablock内部，第二个datablock首尾为null，Extend = 0/1/2 | PASS |
| 4. null数据出现在三个datablcok内部和边界，Extend = 0/1/2 | PASS |
| 5. null数据出现在首尾两个datablock内部，第二个datablock全为null，Extend = 0/1/2 | PASS |
| 6. null数据出现在首尾两个datablock内部和边界，第二个datablock全为null，Extend = 0/1/2 | PASS |
| 7. 全为null的datablock出现在数据末尾，Extend = 0/1/2 | PASS |
