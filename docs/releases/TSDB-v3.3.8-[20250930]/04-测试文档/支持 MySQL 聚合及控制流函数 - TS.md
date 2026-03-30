# 支持 MySQL 聚合及控制流函数 - TS

## 1. 测试目标

支持 MySQL 聚合及控制流函数的功能测试，包括控制流函数，比较算子，聚合函数。

## 2. 相关资料

### 2.1 相关文档

1. [TS-6111 [产品] 支持 MySQL 的条件函数](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTS-6111)
2. [TS-6112 [产品] 支持 MySQL 的聚合函数](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTS-6112)
3. [支持 MySQL 聚合及控制流函数](https://taosdata.feishu.cn/wiki/Da2lwKXd2inREskELaZcLTxDnJc)

### 2.2 用新测试框架测试

1. 用户手册：TDinternal/community/test/README.md
2. 样例文件：
   - TDinternal/community/test/cases/21-Operator/06-Logical/test_if_smoking.py
   - TDinternal/community/test/cases/22-Functions/02-Aggregate/test_agg_smoking.py
3. 运行方法：
  ```bash
  cd /root/TDinternal/community/test
  ../tests/script/sh/stop_dnodes.sh
  rm -rf ~/TDinternal/sim/*
  pytest cases/21-Operator/06-Logical/test_if_smoking.py
  pytest cases/22-Functions/02-Aggregate/test_agg_smoking.py
  ```

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-9-12 | 0.1 | 金明磊 |  |

## 4. 测试结论

通过，功能测试符合预期。

## 5. 测试环境

1. 功能测试: 开发机、Linux 系统
2. 稳定性测试：物理机: 192.168.1.61

## 6. 功能测试

### 6.1 控制流函数

#### 6.1.1 测试要点

1. 基础功能
   - if, ifnull/nvl, nullif, nvl2
   - 语法校验、边界
2. 订阅任务
   - 删除有订阅任务的`bnode`
   - 同一个节点上反复创建`bnode`，检查订阅任务
   - `bnode`所在`dnode`启停对订阅任务的影响

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_if.py | 1. if 1. ifnull 1. nvl 1. nullif 1. nvl2 1. 语法错误情况 | 通过 |

### 6.2 比较算子

#### 6.2.1 测试要点

1. 基础功能
   - isnull, isnotnull, coalesce
   - 语法校验、边界

#### 6.2.2 用例列表

| # | 测试用例 | 测试描 | 测试结果 |
| --- | --- | --- | --- |
| 1 | cmp_opr_isnull | 1. 各数据类型 1. 语法错误情况 | 通过 |
| 3 | cmp_opr_isnotnull | 1. 各数据类型 1. 语法错误情况 | 通过 |
| 2 | cmp_opr_coalesce | 1. 特殊情况，如所有操作数均为空值 1. 各数据类型 1. 语法错误情况 | 通过 |

### 6.3 聚合函数

#### 6.3.1 测试要点

1. 基础功能
   - std, stddev_samp, variance, var_samp, group_concat
   - 语法校验、边界

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_agg_gconcat.py | 1. 边界检查 1. 各数据类型 1. 语法错误情况 1. 子表 1. 超级表，多 vnode | 通过 |

## 7. 特殊场景测试

无

## 8. 用户场景测试

无

## 9. 长期稳定性测试

无

## 10. 性能测试

无

## 11. 安全测试

在`6.1`和`6.2`的场景中，相关场景已得到覆盖，因此本节无需再进行额外的安全测试。

## 12. 兼容性测试

1. 无问题

## 13. 参考文档

标差，方差重新实现：
[TS-7115 修复标准差函数](https://jira.taosdata.com:18080/browse/TS-7115)
函数行为可参考：
https://dev.mysql.com/doc/refman/9.0/en/aggregate-functions.html#function_std
https://dev.mysql.com/doc/refman/9.0/en/comparison-operators.html#function_isnull
https://dev.mysql.com/doc/refman/9.0/en/flow-control-functions.html#function_if
