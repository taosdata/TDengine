# Restore Dnode - Test Spec (draft)

## 1. 测试目标

 测试的需求文档：[当前客户成功面临的挑战和举措](https://taosdata.feishu.cn/wiki/Eit4wdGLciwMzikhkoScvJXtnng) 加强内部测试

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 5/30/2024 | 1.0 | Ping Xiao | draft |
|  |  |  |  |

## 3. 测试范围

3 节点 3 副本，3 节点双副本，环境测试下面各个场景：
restore dnode <dnode_id>；
restore mnode on dnode <dnode_id>；
restore vnode on dnode <dnode_id> ；
restore qnode on dnode <dnode_id>；

## 4. 测试结论

## 5. 已知问题和限制

## 6. 测试环境

- OS: Linux x64

## 7. 测试数据 

主要测试数据能否恢复，对数据类型要求不高，直接使用 taosBenchmark 默认配置写入一定量数据即可

## 8. 测试用例

### 8.1 功能，正确性以及大数据量验证

被删除节点非 mnode
被删除节点是 mnode 且无 DB
被删除节点是 mnode 且有 DB

#### 8.1.1 三节点三副本场景

|  | ID | 测试用例 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| mnode 单副本 | 1 | 非 mnode 节点，清空数据后 restore dnode <dnode_id> | 可以恢复并数据未丢失 |  |
|  | 2 | 非 mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 报错 |  |
|  | 3 | 非 mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 4 | 非 mnode 节点， 非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 报错 |  |
|  | 5 | 非 mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 6 | 非 mnode 节点，清空数据后，同时执行 1, 3, 5 | 可以恢复并数据未丢失 |  |
|  | 7 | 非 mnode 节点，数据写入过程中 offline, restore dnode <dnode_id> | 无法恢复 |  |
|  | 8 | 非 mnode 节点，数据写入过程中 offline,  restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 9 | 非 mnode 节点，数据写入过程中 offline,  restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 10 | 非 mnode 节点，数据写入过程中 offline,  restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 11 | mnode 节点，清空数据后 restore dnode <dnode_id> | 无法恢复 |  |
|  | 12 | mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 13 | mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 14 | mnode 节点，非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 15 | mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 16 | mnode 节点，清空数据后，同时执行 1, 3, 5 | 无法恢复 |  |
| mnode 双副本 | 17 | 非 mnode 节点，清空数据后 restore dnode <dnode_id> | 可以恢复并数据未丢失 |  |
|  | 18 | 非 mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 报错 |  |
|  | 19 | 非 mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 20 | 非 mnode 节点， 非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 报错 |  |
|  | 21 | 非 mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 22 | 非 mnode 节点，清空数据后，同时执行 16, 18, 20 | 可以恢复并数据未丢失 |  |
|  | 23 | 非 mnode 节点，数据写入过程中 offline, restore dnode <dnode_id> | 无法恢复 |  |
|  | 24 | 非 mnode 节点，数据写入过程中 offline,  restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 25 | 非 mnode 节点，数据写入过程中 offline,  restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 26 | 非 mnode 节点，数据写入过程中 offline,  restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 27 | mnode 节点，清空数据后 restore dnode <dnode_id> | 无法恢复 |  |
|  | 28 | mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 29 | mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 30 | mnode 节点，非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 31 | mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 32 | 非 mnode 节点，清空数据后，同时执行 16, 18, 20 | 无法恢复 |  |
| mnode 3 副本 | 33 | 清空数据后 restore dnode <dnode_id> | 可以恢复并数据未丢失 |  |
|  | 34 | 清空数据后 restore mnode on dnode <dnode_id>； | 可以恢复并数据未丢失 |  |
|  | 35 | 清空数据后 restore vnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 36 | 非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 报错 |  |
|  | 37 | 清空数据后 restore qnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 38 | 清空数据后，同时执行 33，35，37 | 可以恢复并数据未丢失 |  |
|  | 39 | 数据写入过程中 offline, restore dnode <dnode_id> | 无法恢复 |  |
|  | 40 | 数据写入过程中 offline,  restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 41 | 数据写入过程中 offline,  restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 42 | 数据写入过程中 offline,  restore qnode on dnode <dnode_id> ； | 无法恢复 |  |

#### 8.1.2 三节点双副本场景

|  | ID | 测试用例 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| mnode 单副本 | 1 | 非 mnode 节点，清空数据后 restore dnode <dnode_id> | 可以恢复并数据未丢失 |  |
|  | 2 | 非 mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 报错 |  |
|  | 3 | 非 mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 4 | 非 mnode 节点， 非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 报错 |  |
|  | 5 | 非 mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 6 | 非 mnode 节点，清空数据后，同时执行 1, 3, 5 | 可以恢复并数据未丢失 |  |
|  | 7 | 非 mnode 节点，数据写入过程中 offline, restore dnode <dnode_id> | 无法恢复 |  |
|  | 8 | 非 mnode 节点，数据写入过程中 offline,  restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 9 | 非 mnode 节点，数据写入过程中 offline,  restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 10 | 非 mnode 节点，数据写入过程中 offline,  restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 11 | mnode 节点，清空数据后 restore dnode <dnode_id> | 无法恢复 |  |
|  | 12 | mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 13 | mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 14 | mnode 节点，非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 15 | mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 16 | mnode 节点，清空数据后，同时执行 1, 3, 5 | 无法恢复 |  |
| mnode 双副本 | 17 | 非 mnode 节点，清空数据后 restore dnode <dnode_id> | 可以恢复并数据未丢失 |  |
|  | 18 | 非 mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 报错 |  |
|  | 19 | 非 mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 20 | 非 mnode 节点， 非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 报错 |  |
|  | 21 | 非 mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 22 | 非 mnode 节点，清空数据后，同时执行 16, 18, 20 | 可以恢复并数据未丢失 |  |
|  | 23 | 非 mnode 节点，数据写入过程中 offline, restore dnode <dnode_id> | 无法恢复 |  |
|  | 24 | 非 mnode 节点，数据写入过程中 offline,  restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 25 | 非 mnode 节点，数据写入过程中 offline,  restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 26 | 非 mnode 节点，数据写入过程中 offline,  restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 27 | mnode 节点，清空数据后 restore dnode <dnode_id> | 无法恢复 |  |
|  | 28 | mnode 节点，清空数据后 restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 29 | mnode 节点，清空数据后 restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 30 | mnode 节点，非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 31 | mnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 32 | 非 mnode 节点，清空数据后，同时执行 16, 18, 20 | 无法恢复 |  |
| mnode 3 副本 | 33 | 清空数据后 restore dnode <dnode_id> | 可以恢复并数据未丢失 |  |
|  | 34 | 清空数据后 restore mnode on dnode <dnode_id>； | 可以恢复并数据未丢失 |  |
|  | 35 | 清空数据后 restore vnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 36 | 非 qnode 节点，清空数据后 restore qnode on dnode <dnode_id> ； | 报错 |  |
|  | 37 | 清空数据后 restore qnode on dnode <dnode_id> ； | 可以恢复并数据未丢失 |  |
|  | 38 | 清空数据后，同时执行 33，35，37 | 可以恢复并数据未丢失 |  |
|  | 39 | 数据写入过程中 offline, restore dnode <dnode_id> | 无法恢复 |  |
|  | 40 | 数据写入过程中 offline,  restore mnode on dnode <dnode_id>； | 无法恢复 |  |
|  | 41 | 数据写入过程中 offline,  restore vnode on dnode <dnode_id> ； | 无法恢复 |  |
|  | 42 | 数据写入过程中 offline,  restore qnode on dnode <dnode_id> ； | 无法恢复 |  |

#### 8.1.3 异常场景

| ID | 测试用例 | 预期结果 | 测试结果 |
| --- | --- | --- | --- |
| 1 | restore dnode 过程中，让 dnode offline | 重新启动 dnode 后，restore 过程恢复 |  |
| 2 | restore dnode 过程中，让 mnode offline | 重新启动 mnode 后，restore 过程恢复 |  |
| 3 | restore dnode 过程中，执行 redistribute vgroup 命令 | 运维命令无法执行，transaction 冲突 |  |
| 4 | restore dnode 过程中，执行 drop dnode 命令 | 运维命令无法执行，transaction 冲突 |  |
| 5 | restore dnode 过程中，执行 split vgroup 命令 | 运维命令无法执行，transaction 冲突 |  |

#### 8.1.4 大数据量场景

1. 写入 100 亿条记录 （100 w tables, 100 w recoreds/table）保证 restore dnode 可以恢复所有数据

#### 8.1.5 数据正确性验证

思路：对比恢复前后 count(*), avg(*), sum, diff 的值

### 8.2 性能测试

| ID | 数据量 | vgroups 数量 | Restore dnode 耗时 |
| --- | --- | --- | --- |
| 1 | 10 亿 | 1 |  |
| 2 | 10 亿 | 2 |  |
| 3 | 10 亿 | 4 |  |
| 4 | 20 亿 | 4 |  |
| 5 | 40 亿 | 4 |  |

## 9. Jira

## 10. 测试计划 (Optional)

5.30 ~ 6.30

## 11. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 12. 参考文档 (Optional)

- [Restore dnode用户手册 （企业版）](https://taosdata.feishu.cn/wiki/wikcnFHgxQ2YpxwdzCWfpb8en4i)
