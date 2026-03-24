# WebSocket 支持 stmt2 接口 Test Spec

## 1. 测试目标

- WebSocket stmt2接口参数合法性校验
- WebSocket stmt2接口返回字段完整、正确

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.10.08 | 1.0 | 霍宏 | 初稿 |
|  |  |  |  |

## 3. 测试范围

- WebSocket stmt2接口参数合法性校验
- WebSocket stmt2接口返回字段完整、正确

## 4. 测试结论

## 5. 开发质量报告

结论：本特性/优化的开发质量是：

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 |  |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

- aaa
- bbb

## 7. 测试环境

- OS: Linux

## 8. 测试用例

### 8.1 功能

在提测时，开发应保证基础用例全部通过。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| stmt2_init | req_id | 指定数字reqid： | 返回正常 |  | Pass |  |
|  |  | 不指定reqid： | 返回正常，生成req_id |  | Pass |  |
|  |  | 指定字符串reqid： | 返回错误 |  | Pass |  |
|  | single_stb_insert | 指定false | 返回正常 |  | Pass |  |
|  |  | 不指定single_stb_insert： | 返回正常 |  | Pass |  |
|  | single_table_bind_once | 指定false | 返回正常 |  | Pass |  |
|  |  | 不指定single_table_bind_once： | 返回正常 |  | Pass |  |
| stmt2_prepare | req_id | 不指定req_id | 返回正常 |  | Pass |  |
|  | stmt_id | 有效stmt_id | 返回正常 |  | Pass |  |
|  |  | 不指定stmt_id | 返回错误 |  | Pass |  |
|  |  | 指定不存在stmt_id | 返回错误 |  | Pass |  |
|  | sql | 有效insert sql语句 | 返回正常 |  | Pass |  |
|  |  | 有效select sql语句 | is_insert返回false |  | Pass |  |
|  |  | 有效create sql语句 | 返回错误？ |  |  | 不支持？ |
|  |  | 不指定 | 返回正常 |  | Pass | [TD-32482](https://jira.taosdata.com:18080/browse/TD-32482)taosc不做sql为空校验，所以taosAdapter返回成功
get_fields=true时返回错误 |
|  | get_fields | 指定true | fields字段返回json结构 |  | Pass |  |
|  |  | 指定false | fields字段返回null |  | Pass |  |
|  |  | 不指定 | fields字段返回null |  | Pass |  |
|  |  | true，insert语句 | fields字段返回json结构 |  | Pass |  |
|  |  | true，select语句 | fields字段返回空，fields_count返回非0 |  | Pass |  |
|  |  | true，sql包括所有数据类型 | fields字段返回json结构包含所有数据类型 |  | Pass |  |
| stmt2_get_fields | req_id | 不指定req_id |  |  | Pass |  |
|  | stmt_id | 有效stmt_id | 返回正常 |  | Pass |  |
|  |  | 未执行prepare的stmt_id | 返回错误 |  | Pass | code=554 |
|  | field_types | 有效值[1,2,3,4] | 返回正常 |  | Pass |  |
|  |  | 无效值[5] | 返回错误 |  | Fail |  |
|  |  | 无效值2 | 返回错误 |  | Pass |  |
|  |  | select语句有效值[3] | 返回正常 |  | Pass |  |
| stmt2_exec | stmt_id | 有效stmt_id | 返回正常 |  | Pass |  |
|  |  | 未执行prepare的stmt_id | 返回错误 |  | Pass |  |
|  |  | 未执行bind_params的stmt_id | 返回错误 |  | Pass |  |
|  |  | 绑定insert语句 | 返回正常 |  | Pass |  |
|  |  | 绑定select语句 | 返回正常 |  | Pass |  |
| stmt2_result | stmt_id | 有效stmt_id，insert语句 | 返回结果 |  | Pass |  |
|  |  | 有效stmt_id，select语句，表包括所有数据类型 | 返回result_id |  | Pass |  |
|  |  | 未执行exec的stmt_id | 返回错误 |  | Pass |  |
| stmt2_close | stmt_id | 有效stmt_id | 返回正常 |  | Pass |  |
|  |  | 执行过close的同一req_id的stmt_id | 返回正常 |  | Pass |  |
| stmt2_bind_param | stmt_id | 有效stmt_id | 返回正常 |  | Pass |  |
|  |  | 未执行prepare的stmt_id | 返回错误 |  | Pass |  |
|  |  | 绑定insert语句 | 返回正常 |  | Pass |  |
|  |  | 绑定select语句 | 返回正常 |  | Pass |  |
|  | 数据类型覆盖 | 所有TDengine支持的数据类型写入 | 返回正常 |  | Pass |  |
|  | 编码错误 | tableCount错误 | 返回错误 |  | Pass | 65535 |
|  |  | ColsOffset错误 | 返回错误 |  | Pass | 65535 |
|  |  | ColDataLength错误 | 返回错误 |  | Pass | 65535 |
|  |  | BufferLength错误 | 返回错误 |  | Pass | 65535 |

### 8.2 可用性

无

### 8.3 可靠性

这里用于描述稳定性测试相关的内容。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 稳定性 | 内存无泄漏 | 持续进行插入操作 | 内存无泄漏 |  |  |  |
|  |  | 持续进行查询操作 | 内存无泄漏 |  |  |  |

### 8.4 性能

这里用于描述性能测试相关的内容。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
|  | 写入性能 | 单线程
每批次1000条
1亿条数据写入操作 | 与taosc写入性能差异在5%以内 |  |  |  |

### 8.5 安全性

无

### 8.6 兼容性

无

### 8.7 本地化

无

## 9. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: [websocket_stmt2]

## 10. 风险评估

无

## 11. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 12. 参考文档 (Optional)

- [WebSocket 支持 stmt2 接口](https://taosdata.feishu.cn/wiki/NjklwNxfTiieYfkyeCAc6x6pnQh)
- [stmt2 功能规格](https://taosdata.feishu.cn/wiki/OHn7wgE38i6LiCkKFQec3wyKnOf)
