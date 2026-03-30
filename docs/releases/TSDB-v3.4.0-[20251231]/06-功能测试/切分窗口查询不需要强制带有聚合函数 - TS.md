# 切分窗口查询不需要强制带有聚合函数 - TS 

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2024-11-25 | - | 0.1 | 任新胜 | 新建 |
| 2025-12-18 | 2025-12-18 | 1.0 | 任新胜 | 发布 |

## 2. 测试目标

不同类型切分窗口在不使用聚合函数时的逻辑正确性
1. 时间窗口、状态窗口、会话窗口、事件窗口和计数窗口不使用聚合函数时结果测试
2. create stream 时，使用切分窗口查询，select 不使用聚合函数，验证结果正确性

## 3. 参考文档

**JIRA**：[TD-32977](https://jira.taosdata.com:18080/browse/TD-32977)    
FS: [切分窗口查询不需要强制带有聚合函数 FS](https://taosdata.feishu.cn/wiki/OGIRwuYGki2QU1kHeYPcclK5nff)

## 4. 测试结论

相关用例 CI 测试通过

## 5. 测试环境

CI 环境，linux 上执行

## 6. 功能测试

### 6.1 测试方法

对于要测试的场景，首先用带有聚合函数的 sql 测试，保存；然后使用不带聚合函数的 sql 进行查询，两次查询只有是否有聚合函数的差别；比较两次查询的结果，使用排序后，对应列查询结果一致，表示结果符合预期。

### 6.2 一般场景

对五种切分窗口（时间、状态、会话、事件、计数）分别对以下场景进行测试：
1. select _wstart,  _wend + partition by tag/tbname + split_windows
2. select _wstart,  _wend, tbname + partition by tbname + split_windows
3. select  _wstart, const_value + partition by tag/tbname + split_windows
4. select const_value + partition by tag/tbname + split_windows
5. select tbname + const_value + partition by tbname + split_windows
6. select tag + partition by tag/tbname + split_windows
7. select tag, const_value + partition by tag/tbname + split_windows
8. 嵌套查询
对select 允许的列各种组合测试，包括：
1. 仅 select 常量
2. Select 常量和时间伪列/tbname 测试
3. 有 partition by 时，select 常量和 partition 列
4. 对普通表/子表，select 常量和 tbname 组合

### 6.3 stream 场景测试

在流的查询语句中使用窗口查询时，select 不包含聚合函数进行测试
1. 仅 select 常量
2. Select 常量和时间伪列/tbname 测试
3. Select 普通列需报错

## 7. 易用性测试（可选）

不涉及

## 8. 长期稳定性测试（可选）

不涉及

## 9. 性能测试

不涉及

## 10. 安全测试

不涉及

## 11. 兼容性测试

不涉及

## 12. 已知问题和限制

无
