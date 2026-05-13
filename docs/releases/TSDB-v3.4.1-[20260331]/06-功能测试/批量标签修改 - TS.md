# 批量标签修改 - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 46098 | 46098 | 1.0 | 张博民 | 创建 |

## 2. 测试目标

- 验证批量修改普通子表标签值的正确性。
- 验证通过超级表批量修改普通子表标签值的正确性。
- 验证批量修改虚拟子表标签值的正确性。
- 验证通过超级表批量修改虚拟子表标签值的正确性。

## 3. 参考文档

- [批量标签修改 FS](https://taosdata.feishu.cn/wiki/KbZHwBvpuiHExtkVt6Sc9MaInMd)
- [虚拟表标签批量操作](https://taosdata.feishu.cn/wiki/SBftwJxjUisxPhkqtJmcCzxan4f)
- [登录 - Confluence](https://jira.taosdata.com:18090/pages/viewpage.action?pageId=158206215)

## 4. 测试结论

所有测试用例全部通过。

## 5. 测试环境

### 5.1 **硬件环境**

操作系统：Linux
- CPU：x86_64
- 内存：≥ 8GB

### 5.2 **软件环境**

TDengine 版本：v3.4.1.0（企业版）
- Python 版本：3.x
- 测试框架：new_test_framework

### 5.3 测试脚本

- test_subtable_batch_set_tag_vals.py
- test_vtable_batch_set_tag_vals.py
- [test_tmq_batch_alter_tag.py](https://github.com/taosdata/TDengine/pull/34809/changes#diff-43cf9537dfbe266513412c16f778a2d627836bf2d7b9694e7a64c60ee5780def)
- [stream_schema.py](https://github.com/taosdata/TDengine/pull/34809/changes#diff-34f7d09dffc114589808b85d0a756f99d25bfc7866442140850ce2e3d7712894)

## 6. 功能测试

### 6.1 批量修改普通子表标签值

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-SUBTABLE-001 | 修改一个子表的一个标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-002 | 修改一个子表的多个标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-003 | 修改多个子表的多个标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-004 | 将标签值设置为 NULL | 修改成功 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-005 | 同时修改不同数据库中子表的标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-006 | 修改不同类型的标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-007 | 修改超级表的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-008 | 标签重复 | 修改失败 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-009 | 修改不存在的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-010 | 修改普通表的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-011 | 修改不存在的表的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-012 | 错误的标签值类型 | 修改失败 | 符合预期 | ✅ PASS |
| TC-SUBTABLE-013 | 标签值超过长度限制 | 修改失败 | 符合预期 | ✅ PASS |

### 6.2 通过超级表批量修改子表标签值

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-CHILD_TABLE-001 | 通过 WHERE 条件修改子表标签 | 匹配的子表标签被修改，其他保持不变 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-002 | 将标签设置为 NULL | 修改成功 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-003 | 不使用 WHERE 条件 | 所有子表被修改 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-004 | WHERE 条件不匹配任何子表 | 成功，但无标签被修改 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-005 | 正则表达式替换 | 修改成功，且新标签值正确 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-006 | 正则表达式替换后标签值过长 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-007 | 超级表不存在 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-008 | 标签不存在 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-009 | 对非字符串类型使用正则表达式替换 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-010 | 标签重复 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-011 | 正则表达式不合法 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-012 | 通过普通表修改子表标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-013 | WHERE 条件中使用聚合函数 | 修改失败 | 符合预期 | ✅ PASS |
| TC-CHILD_TABLE-014 | WHERE 条件中使用非标签列 | 修改失败 | 符合预期 | ✅ PASS |

### 6.3 批量修改虚拟子表标签值

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-VSUBTABLE-001 | 修改一个子表的一个标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-002 | 修改一个子表的多个标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-003 | 修改多个子表的多个标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-004 | 将标签值设置为 NULL | 修改成功 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-005 | 同时修改不同数据库中子表的标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-006 | 修改不同类型的标签 | 修改成功 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-007 | 修改超级表的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-008 | 标签重复 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-009 | 修改不存在的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-010 | 修改普通表的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-011 | 修改不存在的表的标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-012 | 错误的标签值类型 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VSUBTABLE-013 | 标签值超过长度限制 | 修改失败 | 符合预期 | ✅ PASS |

### 6.4 通过超级表批量修改虚拟子表标签值

| 测试项 | 测试内容 | 预期结果 | 实际结果 | 状态 |
| --- | --- | --- | --- | --- |
| TC-VCHILD_TABLE-001 | 通过 WHERE 条件修改子表标签 | 匹配的子表标签被修改，其他保持不变 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-002 | 将标签设置为 NULL | 修改成功 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-003 | 不使用 WHERE 条件 | 所有子表被修改 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-004 | WHERE 条件不匹配任何子表 | 成功，但无标签被修改 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-005 | 正则表达式替换 | 修改成功，且新标签值正确 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-006 | 正则表达式替换后标签值过长 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-007 | 超级表不存在 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-008 | 标签不存在 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-009 | 对非字符串类型使用正则表达式替换 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-010 | 标签重复 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-011 | 正则表达式不合法 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-012 | 通过普通表修改子表标签 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-013 | WHERE 条件中使用聚合函数 | 修改失败 | 符合预期 | ✅ PASS |
| TC-VCHILD_TABLE-014 | WHERE 条件中使用非标签列 | 修改失败 | 符合预期 | ✅ PASS |

### 6.5 测试数据订阅处理 alter 消息的正确性

| case1 | 数据订阅处理各种类型alter消息同步数据 | 同步数据正确 | 符合预期 | ✅ PASS |
| --- | --- | --- | --- | --- |

### 6.6 测试流计算获取 alter 消息的正确性

| insertCheckData3 | 流计算读取alter 的消息 | 流计算结果正确 | 符合预期 | ✅ PASS |
| --- | --- | --- | --- | --- |

## 7. 性能测试

无。

## 8. 安全测试

不涉及。

## 9. 兼容性测试

不涉及。
