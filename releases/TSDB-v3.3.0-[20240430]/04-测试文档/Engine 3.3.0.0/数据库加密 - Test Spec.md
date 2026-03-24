# 数据库加密 - Test Spec

## 1. 测试目标

需求文档：[需求说明：数据库加密](https://taosdata.feishu.cn/wiki/MgEGwaJWCiXT2dkPxZIcZZ9undg)
主要测试目标：TDengine 支持数据库加密的功能，数据加密后能够防止攻击者直接从文件系统读取敏感数据

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-03-29 | 0.1 | Ping Xiao | Draft |
| 2024-04-02 | 0.2 | Ping Xiao | Fix review comments |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- 数据库加密基本功能
  - 秘钥长度限制
  - 加密后的数据在没有秘钥的情况下无法被解密
  - 每个节点秘钥强一致性
  - 加密，解密的数据一致性
- 性能
  - 数据库加密后有一定的性能下降预期，给出对比数据
- 异常
  - 密钥不一致报错
- 正确性：
  - 所有 CI 用例在加密后都能通过
- 安全性：
  - 加密数据迁移到另外一台服务器后无法被破解

## 4. 测试结论

所有功能在测试场景覆盖的范围内都已测试通过，符合预期；
写入性能：在 interlace_row = 0 的场景下下降 100%，在 interlace_rows = 1 的场景下下降 20% ；查询性能：select * 排序加 limit 组合场景下，加密后性能下降 3 倍左右，其他查询加密前后性能对比没有数量级变化；与开发前预测的结论一致，符合预期；
整体结论：测试通过

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 4 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

## 7. 测试环境

- OS: Linux, Windows

## 8. 测试数据 (Optional)

## 9. 测试用例

### 9.1 功能

| 分类 | 测试场景 | 编号 | 测试用例 | 预期行为 | 测试结果 | 说明 |
| --- | --- | --- | --- | --- | --- | --- |
| 基本功能 | 离线节点配置秘钥 | 1 | taosd -y {value} value 使用特殊字符、大小写、数字组合并且满足长度要求 | 可正确配置秘钥 |  |  |
|  |  | 2 | taosd -y {value} value length < 8 | 不符合长度要求报错 | Pass |  |
|  |  | 3 | taosd -y {value} value length > 16 | 不符合长度要求报错 | Pass |  |
|  |  | 4 | 同一个离线节点 使用 taosd -y {value} 第二次配置不同秘钥 | 无法重复配置秘钥 | Pass |  |
|  | 在线节点通过 sql 语句配置秘钥 | 1 | create encrypt_key 'value'  value 使用特殊字符、大小写、数字组合并且满足长度要求 | 可正确配置秘钥 | Pass |  |
|  |  | 2 | create encrypt_key 'value'  value length < 8 | 不符合长度要求报错 | Pass |  |
|  |  | 3 | create encrypt_key 'value' value length > 16 | 不符合长度要求报错 | Pass |  |
|  |  | 4 | 已通过 sql 语句正确配置秘钥的节点，使用 sql 语句配置不同的秘钥 | 无法配置 | Pass |  |
|  | 秘钥一致性检查 
（包括异常场景） | 1 | 已配置秘钥的集群新增一个未配置秘钥的节点 | 无法加入 | Pass |  |
|  |  | 2 | 已配置秘钥的集群新增一个离线方式配置不同秘钥的节点 | 新加入的节点显示为 offline 报错：dnode is offline since invalid encryption key | TD-29629 |  |
|  |  | 3 | 已配置秘钥的集群新增一个离线方式配置相同秘钥的节点 | 可以加入 | Pass |  |
|  |  | 4 | 未配置秘钥的集群新增一个未配置秘钥的节点 | 可以加入 | Pass |  |
|  |  | 5 | 未配置秘钥的集群新增一个离线方式配置秘钥的节点 | 无法加入 | Pass |  |
|  |  | 6 | 3个离线节点，1个节点未配置秘钥，2个配置相同秘钥 | 没有秘钥的节点显示为 offline 报错：dnode is offline since no encryption key exists | TD-29629 |  |
|  |  | 7 | 3个离线节点，每个都配置秘钥，但有一个节点秘钥不一致 | 无法搭建集群 | Pass |  |
|  |  | 8 | 3个离线节点，每个节点均配置相同秘钥 | 可以搭建集群 | Pass |  |
|  |  | 9 | 3个离线节点，每个节点均配置相同秘钥，搭建集群后使用 sql 语句更新秘钥 | 无法更新 | Pass |  |
|  | 创建加密数据库 | 1 | 在所有节点已配置秘钥的情况下创建数据库，不添加任何数据库选项 | 可以正常创建，加密算法显示为 none | Pass |  |
|  |  | 2 | 在所有节点已配置秘钥的情况下创建 sm4 算法加密数据库 | 可以正常创建，加密算法显示为 sm4 | Pass |  |
|  |  | 3 | 在只有部分节点配置秘钥的情况下创建其他加密算法的加密数据库 | 报错： Invalid option encrypt_algorithm: ${inputValue} | Pass |  |
|  |  | 4 | 在只有部分节点配置秘钥的情况下创建加密数据库 | 可以创建数据库 | Pass |  |
|  | 更新数据库加密类型 | 1 | 使用 alter database 语句将加密类型为 none 的数据库修改为 sm4 类型 | 无法修改，报错：encryption is not allowed to be changed after database is created | Pass |  |
|  |  | 2 | 使用 alter database 语句将加密类型为 sm4 的数据库修改为 none 类型 | 无法修改，报错：encryption is not allowed to be changed after database is created | Pass |  |
|  | 查看数据库加密配置 | 1 | select name, encrypt_algorithm from ins_databases; | 未加密显示 none, 加密显示 sm4 | Pass |  |
|  | 查看节点秘钥状态
select key_status from information_schema.ins_encryptions; | 1 | 未配置秘钥 | 显示 unset | Pass |  |
|  |  | 2 | 通过在线 sql 语句创建的秘钥，离线 | 显示 loaded | Pass |  |
|  |  | 3 | 通过离线方式配置秘钥并加入到已配置秘钥的集群，只针对离线方式 | 显示 unknown | Pass |  |
|  | 更新秘钥配置 | 1 | 机器码变更，不更新秘钥配置 | vnode 节点无法工作 | Pass |  |
|  |  | 2 | 机器码变更，更新秘钥配置，使用之前不同的秘钥 | 无法更新，节点无法工作 | Pass |  |
|  |  | 3 | 机器码变更，更新秘钥配置，使用之前相同的秘钥 | 更新成功，节点正常工作 | Pass |  |
|  | 通过 hexdump 验证加密效果 | 1 | vnode wal body | 加密后使用 hexdump 无法读取 | Pass |  |
|  |  | 2 | tsdb | 加密后使用 hexdump 无法读取 | Pass |  |
|  |  | 3 | tdb | 加密后使用 hexdump 无法读取 | Pass |  |
| 正确性验证 |  | 1 | 现有所有 CI 用例在加密后保证都能通过 | 现有所有 CI 用例在加密后保证都能通过 |  |  |


### 9.2 性能

1. Insert:  taosBenchmark 创建 1 万子表，每个子表写入 1 万条记录
|  | interlace_rows = 0 |  | interlace_rows = 1 |  |
| --- | --- | --- | --- | --- |
|  | 不加密 | 加密 | 不加密 | 加密 |
| speed | 3913332.83 | 2004190.52 | 498510.01 | 410420.83 |
| time | 25.55 | 49.89 | 200.59 | 243.65 |

1. query：使用 taosBenchmark 执行一下 sql 语句，每个 sql 语句执行 100 次取平均值
| case | 不加密 | 加密 |
| --- | --- | --- |
| select last_row(*) from meters | 0.002777 | 0.003283 |
| select count(*) from meters | 0.166002 | 0.173088 |
| select count(*) from d0 | 0.006477 | 0.003635 |
| select avg(current), max(voltage), min(phase) from meters | 0.285503 | 0.326105 |
| select avg(current), max(voltage), min(phase) from meters interval(10s) | 0.376186 | 0.891109 |
| select count(*) from meters where location = 'San Francisco' | 0.001727 | 0.001689 |
| select avg(current), max(voltage), min(phase) from meters where groupid = 1 | 0.039898 | 0.080463 |
| select * from meters limit 10000 | 0.037864 | 0.047912 |
| select spread(phase) from meters | 0.232507 | 0.269447 |
| select * from meters order by ts | 248.42 | 253.91 |
| select * from meters order by ts desc | 248.43 | 253.51 |
| select * from meters order by ts limit 1000 | 0.23984 | 0.734071 |
| select * from meters order by ts desc limit 1000 | 0.432519 | 1.128818 |
| select last(*) from meters | 0.003274 | 0.003286 |

### 9.3 安全性

1. 加密数据迁移到新的节点

### 9.4 兼容性

1. 有数据加密功能的版本无法回退到没有数据加密功能的版本
2. 有数据加密功能的高版本可以回退到有数据加密功能的低版本？

## 10. 问题(Optional)

## 11. Jira

TD-29597


TD-29604


TD-29612


TD-29629


TD-29640

## 12. 测试计划 (Optional)

4.15 ~ 4.26

## 13. 测试备忘 (Optional)

## 14. 参考文档 (Optional)
