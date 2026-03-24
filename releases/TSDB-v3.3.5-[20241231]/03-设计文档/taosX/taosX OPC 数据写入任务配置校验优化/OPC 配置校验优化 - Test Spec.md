# OPC 配置校验优化 - Test Spec

## 1. 测试目标

<quote-container>
这里用于描述本需求主要的测试目标
</quote-container>

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
|  |  |  |  |
|  |  |  |  |

## 3. 测试范围

<quote-container>
这里用于描述本需求的覆盖范围：
- aaa
- bbb
</quote-container>

## 4. 测试结论

<quote-container>
测试结论中包含结论和关键数据，但不需罗列过多细节，此处需要把把握信息的详细程度，原则上是外部 Reviewer 能够获得清晰的测试结论且尽量没有冗余信息为标准（这个标准是一句正确的废话，具体实行中需要大家 case by case 来处理）
</quote-container>

## 5. 开发质量报告

结论：本特性/优化的开发质量是良

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 |  |
| 严重 Bug 总数 |  |

## 6. 知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- aaa
- bbb

## 7. 测试环境

- OS: Windows, Linux, macOS
- Browser: Chrome

## 8. 测试数据 (Optional)

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：
- field的数量、类型
- tag的数量、类型
- 数据量的大小

## 9. 测试用例

### 9.1 功能

#### 9.1.1 tag 校验：类型和值

| **测试用例** | **测试步骤** | **预期结果** | **测试结果** | **用例类型** | **备注** |
| --- | --- | --- | --- | --- | --- |
| 上传 CSV 文件，所有 tag value 和 type 都匹配 | 校验通过 | 通过 | 接口测试 | 正常用例 |
| 上传 CSV 文件，有 tag value 和 type 不匹配 | 校验失败，前端报错 | 通过 | 接口测试 | 异常用例 |
| 任务运行中，通过“新增点位” 添加，tag value 和 type 匹配 | 校验通过 | 通过 | 接口测试 | 正常用例 |
| 任务运行中，通过“新增点位” 添加，tag value 和 type 不匹配 | 校验失败，前端报错 | 通过 | 接口测试 | 异常用例 |
| bool 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| bool 类型的 tag，配置非法值 | 校验失败 | 通过 | 单元测试 | 异常用例 |
| tinyint 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| tinyint 类型的 tag，配置非法值 | 校验失败 | 通过 | 单元测试 | 异常用例 |
| smallint 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| smallint 类型的 tag，配置非法值 | 校验失败 | 通过 | 单元测试 | 异常用例 |
| int 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| int 类型的 tag，配置非法值 | 校验失败 | 通过 | 单元测试 | 异常用例 |
| bigint 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| bigint 类型的 tag，配置非法值 | 校验失败 | 通过 | 单元测试 | 异常用例 |
| float 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| float 类型的 tag，配置非法值 | 校验失败 | 通过 | 单元测试 | 异常用例 |
| double 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| double 类型的 tag，配置非法值 | 校验失败 | 通过 | 单元测试 | 异常用例 |
| varchar(10) 类型的 tag，配置合法值 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| varchar(10) 类型的 tag，配置非法值，tag value 长度超过 10 | 校验失败 | 通过 | 单元测试 | 异常用例 |

#### 9.1.2 value 校验：和数据库表的 schema 是否冲突

| **测试用例** | **测试步骤** | **预期结果** | **测试结果** | **用例类型** | **备注** |
| --- | --- | --- | --- | --- | --- |
| 1. Explorer 上创建 OPC UA 任务 1. 选择单列模型 1. 上传合法的 CSV 文件 | 校验通过 | 通过 | 接口测试 | 正常用例 |
| 1. Explorer 上创建 OPC UA 任务 1. 选择单列模型 1. 上传不合法的 CSV 文件 | 校验失败，前端报错 | 通过 | 接口测试 | 异常用例 |
| 1. Explorer 上创建 OPC UA 任务 1. 选择多列模型 1. 上传合法的 CSV 文件 | 校验通过 | 通过 | 接口测试 | 正常用例 |
| 1. Explorer 上创建 OPC UA 任务 1. 选择多列模型 1. 上传不合法的 CSV 文件 | 校验失败，前端报错 | 通过 | 接口测试 | 异常用例 |
| 单列模型，stable = `opc_{type}`，tbname = `t_{ns}_{id}` | stable 有参数，tbname 有参数 | 不校验，通过 | 通过 | 单元测试 | 正常用例 |
| 单列模型，stable = `opc_{type}`，tbname = `t_3_1001` | stable 有参数，tbname 无参数 | 不校验，通过 | 通过 | 单元测试 | 正常用例 |
| stable 不存在 | 不校验，通过 | 通过 | 单元测试 | 正常用例 |
| stable 存在，database tags 不能包含 csv tags | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，database tags 包含 csv tags，val_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，database tags 包含 csv tags，ts_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，database tags 包含 csv tags，received_ts_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，database tags 包含 csv tags，quality_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，database tags 包含 csv tags，value_col、ts_col、received_col、quality_col 值在 database 都存在 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| stable 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 存在，tbname 不属于 stable | 校验不通过 | 通过 | 单元测试 |  |
| stable 存在，tbname 存在，tbname 属于 stable，database tags 不能包含 csv tags | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，val_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，ts_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，received_ts_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，quality_col 值在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，val_col、ts_col、received_ts_col、quality_col 值在 database 都存在 | 校验通过 | 通过 | 单元测试 | 正常用例 |
| 多列模型，stable = opc_{type} | Stable 有参数 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| 多列模型，stable = metrics，tbname = t_{ns}_{id} | Stable 无参数，tbname 有参数 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 存在，tbname 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| Stable 存在，tbname 存在，tbname 不属于 stable | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| Stable 存在，tbname 存在，tbname 属于 stable，database tags 不能包含 csv tags | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| Stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，val_col 在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| Stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，ts_col 在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| Stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，received_ts_col 在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| Stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，quality_col 在 database 不存在 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| Stable 存在，tbname 存在，tbname 属于 stable，database tags 包含 csv tags，val_col、ts_col、received_ts_col、quality_col 在 database 都存在 | 校验通过 | 通过 | 单元测试 | 正常用例 |

#### 9.1.3 pattern 支持负向前馈正则

| **测试用例** | **测试步骤** | **预期结果** | **测试结果** | **用例类型** | **备注** |
| --- | --- | --- | --- | --- | --- |
| Explorer 使用 pattern 过滤 NodeID 或 BrowseName | 1. 使用旧版本的 taosx 创建 OPC UA 任务； 1. 升级 taosX + explorer； 1. 编辑打开 OPC UA 任务，“选择数据点位”； 1. 正则匹配：`^(?!._Error.$)`； 1. 点击“查看点位列表” | Explorer 返回的点位列表中，nodeId 或 BrowseName 符合正则`^(?!._Error.$)` | 通过 | 接口测试 | 正常用例 |
| Explorer 使用 pattern 过滤 NodeID | 1. 使用新版本 taosX 创建 OPC UA 任务； 1. 在“节点名称正则匹配”中，填`^(?!._Error.$)` 1. 点击“查看点位列表” | 1. 在“选择数据点位”中，只显示“节点名称正则匹配”和“节点 ID 正则匹配” 1. Explorer 返回的点位列表中，BrowseName 符合正则`^(?!._Error.$)` | 通过 | 接口测试 | 正常用例 |
| Explorer 处理 pattern 过滤 BrowseName | 1. 使用新版本 taosX 创建 OPC UA 任务； 1. 在“节点 ID 正则匹配”中，填`^(?!._Error.$)` 1. 点击“查看点位列表” | 1. 在“选择数据点位”中，只显示“节点名称正则匹配”和“节点 ID 正则匹配” 1. Explorer 返回的点位列表中，NodeId 符合正则`^(?!._Error.$)` | 通过 | 接口测试 | 正常用例 |
| Explorer 使用 pattern 过滤 NodeID | 1. 使用新版本 taosX 创建 OPC DA 任务； 1. 在“节点 ID”中，填`^(?!._Error.$)` 1. 点击“查看点位列表” | 1. 在“选择数据点位”中，只显示“节点名称正则匹配”和“节点 ID 正则匹配” 1. Explorer 返回的点位列表中，BrowseName 符合正则`^(?!._Error.$)` |  | 接口测试 | 正常用例 |
| Explorer 处理 pattern 过滤 BrowseName | 1. 使用新版本 taosX 创建 OPC DA 任务； 1. 在“节点名称”中，填`^(?!._Error.$)` 1. 点击“查看点位列表” | 1. 在“选择数据点位”中，只显示“节点名称正则匹配”和“节点 ID 正则匹配” 1. Explorer 返回的点位列表中，NodeId 符合正则`^(?!._Error.$)` |  | 接口测试 | 正常用例 |
| Pattern 是合法的正则表达式 | Pattern = `^(?!._Error.$)` | 前端校验通过 |  | 前端测试 | 正常用例 |
| pattern 是非法的正则表达式 | Pattern = `(abc`，未闭合的括号 | 前端校验不合法，提示用户 |  | 前端测试 | 异常用例 |
| pattern 不以 _Error 结尾 | Dsn = `opcua://?pattern=%5E%28%3F%21%2e_Error%2e%24%29` | regex = `^(?!._Error.$)` | 通过 | 单元测试 | 正常用例 |
| BrowseName 不以 _Error 结尾 | Dsn = `opcua://?browse_name_pattern=%5E%28%3F%21%2e_Error%2e%24%29` | regex_name = `^(?!._Error.$)` | 通过 | 单元测试 | 正常用例 |
| NodeId 不以 _Error 结尾 | Dsn = `opcua://?node_id_pattern=%5E%28%3F%21%2e_Error%2e%24%29` | regex_id = `^(?!._Error.$)` | 通过 | 单元测试 | 正常用例 |

#### 9.1.4 stable 校验：只能配置 {type} 参数

| **测试用例** | **测试步骤** | **预期结果** | **测试结果** | **用例类型** | **备注** |
| --- | --- | --- | --- | --- | --- |
| stable = `opc_{type}` | 校验通过 | 通过 | 单元测试 | 正常用例 |
| stable = `opc_abc` | 校验通过 | 通过 | 单元测试 | 正常用例 |
| stable 为空 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable 为空字符串 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| stable = `opc_{abc}` | 校验不通过 | 通过 | 单元测试 | 异常用例 |

#### 9.1.5 tbname 校验：只能配置 {ns}/ {id}/ {tag_name} 参数

| **测试用例** | **测试步骤** | **预期结果** | **测试结果** | **用例类型** | **备注** |
| --- | --- | --- | --- | --- | --- |
| OPC UA 且 tbname = `t_{ns}_{id}` | 校验通过 | 通过 | 单元测试 | 正常用例 |
| OPC DA 且 tbname = `t_{tag_name}` | 校验通过 | 通过 | 单元测试 | 正常用例 |
| OPC DA 且 tbname = `t_{TagName}` | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| OPC UA 且 tbname = `t_abc` | 校验通过 | 通过 | 单元测试 | 正常用例 |
| OPC UA 且 tbname = `t_{tag_name}` | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| OPC UA 且 tbname 为空 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| OPC UA 且 tbname 为空字符串 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| OPC DA 且 tbname 为空 | 校验不通过 | 通过 | 单元测试 | 异常用例 |
| OPC DA 且 tbname 为空字符串 | 校验不通过 | 通过 | 单元测试 | 异常用例 |

### 9.2 可用性

测试用例包括但不局限于：
- UI是否美观？
- 交互是否合理？
- 字体、字号是否合适？
- 是否存在错别字？

### 9.3 可靠性

这里用于描述稳定性测试相关的内容。

### 9.4 性能

这里用于描述性能测试相关的内容。

### 9.5 安全性

测试用例包括但不局限于：
- 日志中是否包含敏感信息？

### 9.6 兼容性

测试用例包括但不局限于：
- 升级安装后，老版本（上一个版本）下创建的任务，能否继续执行？
- 升级安装后，未写入任何数据（未创建任何新任务），是否能够降级并继续运行
- 升级安装后，写入新数据（或创建新的任务）， 是否能够降级并继续运行

### 9.7 本地化

测试用例包括但不局限于：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示？

## 10. 待讨论(Optional)

这里用于记录在测试或用例编写过程中想到的需要讨论的问题：
- aaa
- bbb

## 11. Jira

TD-31908

## 12. 测试计划 (Optional)

这里用于计划此 feature 测试的开始和结束时间。

## 13. 风险评估

用户记录这个需求的潜在风险，例如：对于功能复杂，开发时间长的功能，是否需要分期提测？

## 14. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 15. 参考文档 (Optional)

这里用于添加对该需求测试有帮助的文档链接：
- [link to functional spec]
- aaa
- bbb
