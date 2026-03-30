# PI 数据源同步增删改操作和提前建表 Test Spec

## 1. 测试目标

- 多列模式下， PI system 动态中新增/删除元素，在TDengine中能够同步进行子表的新增/删除
- 多列模式下， PI system 中修改静态 Attribute 值的操作，在 TDengine 修改对应子表的标签值
- 多列模式下，PI system 中修改/删除某 element 下某动态 Attribute 下 PI point 的历史值 ，在 TDengine 中对应修改/删除对应子表的数据

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-7-17 | v0.0 | @贾晨阳 |  |
| 2024-7-27 | v1.0 | @贾晨阳 |  |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- 动态新增/删除元素
- 动态修改静态 attribute 的值
- 动态修改/删除 point 的历史值

## 4. 测试结论

本次测试完成了测试目标中规定的测试内容，所有测试用例均通过，多列模式下PI system中动态进行元素新增、删除以及PI point历史值进行动态修改和删除操作，均能够正确同步到TDengine中。
遗留问题  为 AF SDK本身问题，在taosx侧无修改方案，故不做处理。
TD-31116

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 2 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- 当子表名中包含静态属性映射时，在PI中修改该静态属性值时，会导致taosx映射子表规则变化，若重启任务，则原子表会失效，新数据会写入新计算映射的子表中
- 不支持静态属性的data reference 类型为String Builder 、Table Lookup、URI Builder时的动态变更同步
- 本次修改及新增功能只在多列模式下生效，单列模式 UI 中不提供该功能

## 7. 测试环境

- OS: Windows, Linux, macOS
- Browser: Chrome

## 8. 测试数据 (Optional)

无

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证基础用例全部通过。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 修改element | 验证PI系统中对元素新增element，能够同步在TDengine中新增对应子表 | 1. 启动PI 实时任务后，在PI系统中新增一个element，且带PIPoint属性 | 新增的element能够同步在TDengine中创建子表并写入数据 | Y | Pass | 新增元素，如果没有绑定 pi point，不会有数据。且现在不支持动态绑定 pi point 后，同步数据 |
|  |  | 1. 启动PI 实时任务后，在PI系统中新增一个element但不绑定PIPoint | 新增的element能够同步在TDengine中创建子表 |  | Pass |  |
|  |  | 1. 启动PI 实时任务后，在PI系统中新增一个element但不带PIPoint属性
1. 在element中添加新的属性，并将其data reference配置为PIPoint | 新增的element能够同步在TDengine中创建子表，但无数据写入 |  | Pass |  |
|  |  | 1. 启动PI 实时任务后，在PI系统中新增一个child element | 新增的child element能够同步在TDengine中创建子表并写入数据 | Y | Pass | [https://jira.taosdata.com:18080/browse/TD-31112](https://jira.taosdata.com:18080/browse/TD-31112) |
|  | 验证PI系统中元素删除element，在TDengine中能够删除对应子表 | 1. 启动PI 实时任务后，在PI系统中删除一个element（其下没有child element） | 删除的element在TDengine中也会删除对应子表 | Y | Pass |  |
|  |  | 1. 启动PI 实时任务后，在PI系统中删除一个element下的child element | 删除的child element在TDengine中也会删除对应子表 | Y | Pass |  |
|  |  | 1. 启动PI 实时任务后，在PI系统中删除一个element，其包含child element | 删除的element及其child element在TDengine中也会删除对应子表 | N | Pass |  |
|  | 验证动态修改静态属性的值，TDengine中对应tag被修改 | 1. 启动PI 实时任务后，在PI系统中修改一个静态属性的值 | TDengine中对应子表的tag值被修改 | Y | Pass | 需要在表建好之后开始测试 |
|  | 验证动态修改PIPoint 的历史值，TDengine中对应attribute的历史值被修改 | 1. 启动PI 实时任务后，在PI系统中修改一个PIPoint的历史值 | TDengine中链接该PIPoint的attribute的列的指定历史值被修改 | Y | Pass | 建议任务启动一段时间后，已经有数据写入再修改。 |
|  | 验证动态批量删除PIPoint 的历史值，TDengine中对应attribute的历史值被修改 | 1. 启动PI 实时任务后，在PI系统中删除一个PIPoint的部分历史值 | TDengine中链接该PIPoint的attribute的列的对应的列的相应时间的值置为null | Y | Fail | [https://jira.taosdata.com:18080/browse/TD-31116](https://jira.taosdata.com:18080/browse/TD-31116) |
|  | 验证创建任务后，element在TDengine中会提前创建对应子表 | 1. 停止PIPoint模拟器，不再生成实时数据
1. 启动PI实时任务，回溯时间为0m | TDengine中会创建element对应的空子表 | Y | Pass |  |
| 单列模式 | 验证单列模式下没有对应操作开关 | 1.配置PI 实时任务
2.任务模式选择单列模式 | 单列模式下没有相应开关可选择 | Y | Pass | 需要等：[TD-30466](https://jira.taosdata.com:18080/browse/TD-30466) 提测 |
| 修改template | 验证修改template中某一静态属性的值 | 1. 启动PI 实时任务
1. 在PI中修改某一template中某一静态属性的值 | TDengine中所有相关联的子表的对应tag值均被修改 | N | Pass |  |
|  | 验证删除某一template | 1. 启动PI 实时任务
1. 在PI中删除一个template | TDengine中所有相关联的子表均被删除 | N |  |  |
| 开关有效性 | 验证开关打开和关闭是否生效 | 1.创建任务时将功能开关开启 | 对应功能在执行任务时生效 |  | Pass |  |
|  |  | 1.创建任务时将功能开关关闭 | 对应功能在执行任务时不生效 |  | Pass |  |

### 9.2 可用性

无

### 9.3 可靠性

无

### 9.4 性能

无

### 9.5 安全性

无

### 9.6 兼容性

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
|  | 验证新版本taosx和旧版本agent+connector的兼容 | 1. 在3.3.2.x上启动PI 实时任务后，将taosx和agent均动态升级到3.3.3.0 | 任务正常继续执行，数据继续写入 | N | Pass |  |
|  | 验证新版本taosx和旧版本agent+connector的兼容 | 1. 在3.3.2.x上启动PI 实时任务后，只将taosx动态升级到3.3.3.0 | 任务正常继续执行，数据继续写入 | N | Pass |  |

### 9.7 本地化

无

## 10. 待讨论(Optional)

无

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: abc

TD-30364


TD-30466


TD-30439


TD-30927


TD-30931

## 12. 背景

## 13. 测试计划 (Optional)

无

## 14. 风险评估

无

## 15. 测试备忘 (Optional)

无

## 16. 参考文档 (Optional)

- [PI 数据源同步增删改操作和提前建表](https://taosdata.feishu.cn/wiki/DztewsT1pi3R6Sk9EcDcqn40nDh)
