# Compact冲突检测 - Test Spec

## 1. 测试目标

<quote-container>
Compact冲突检测功能，对于data变动的操作建议不能同时执行，限制只能顺序执行。确保数据和集群安全。
</quote-container>

## 2. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 202411 | 0.1 | @陈东明 |  |
|  |  |  |  |

## 3. 测试范围

<quote-container>
执行副本变更、balance vgroup、redistribute vgroup、split vgroup的同时，不能执行compact db
反之，执行compact db的同时，不能执行副本变更、balance vgroup、redistribute vgroup、split vgroup。
</quote-container>

## 4. 测试结论

<quote-container>
所有功能在测试场景覆盖的范围内都已测试通过，符合预期；
</quote-container>

## 5. 已知问题和限制

## 6. 测试环境

- OS: Linux

## 7. 测试数据（可选）

## 8. 测试用例

### 8.1 功能

| 分类 | 测试场景 | 编号 | 测试用例 | 预期行为 | 测试结果 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| 基本功能 | 冲突检测 | 1 | compact db, 同时执行ALTER DATABASE db REPLICA 3 | 报错：Transaction not completed due to conflict with compact | 通过 |  |
|  |  | 2 | compact db, 同时执行REDISTRIBUTE VGROUP 5 DNODE 1; | 报错：Transaction not completed due to conflict with compact | 通过 |  |
|  |  | 3 | compact db, 同时执行BALANCE VGROUP; | 报错：Transaction not completed due to conflict with compact | 通过 |  |
|  |  | 4 | compact db, 同时执行split vgroup; | 报错：Transaction not completed due to conflict with compact | 通过 |  |
|  |  | 5 | ALTER DATABASE db REPLICA 3, 同时执行compact db | 报错：Conflict transaction not completed | 通过 |  |
|  |  | 6 | REDISTRIBUTE VGROUP 5 DNODE 1;, 同时执行compact db | 报错：Conflict transaction not completed | 通过 |  |
|  |  | 7 | BALANCE VGROUP,同时执行compact db | 报错：Conflict transaction not completed | 通过 |  |
|  |  | 8 | split vgroup, 同时执行compact db; | 报错：Conflict transaction not completed | 通过 |  |

### 8.2 可用性

### 8.3 可靠性


### 8.4 性能


### 8.5 安全性

### 8.6 兼容性

### 8.7 本地化

## 9. 待讨论（可选）

## 10. Jira（可选）


## 11. 测试计划（可选）


## 12. 风险评估

## 13. 测试备忘（可选）


## 14. 参考文档（可选）
