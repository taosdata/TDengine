# 功能测试报告（Test Spec）- TMQ 元数据扩展

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-07 | 2026-04-07 | 3.4.2 | sheyanjie | 新增 TMQ 元数据扩展功能测试 |

# 测试目标

本需求扩展了 TDengine JDBC Connector 的 TMQ（数据订阅）元数据处理能力，主要目标包括：

- 支持 9 种新的 ALTER 操作类型（ALTER_MULTI_TABLE_TAG、ALTER_STABLE_TAG_WITH_FILTER 等）
- 支持虚拟表（Virtual Table）的元数据解析
- 支持列引用（Column Reference）机制
- 支持批量修改多表标签功能
- 提高元数据解析的代码覆盖率

# 参考文档

- https://jira.taosdata.com:18090/pages/viewpage.action?pageId=158206215

# 测试结论

本次扩展功能已完成单元测试和集成测试，新增的元数据字段和枚举值均能正确解析和处理。

**关键数据：**
- 新增 AlterType 枚举值：9 种（类型 11, 13-20）
- 新增实体类：3 个（ColRef、ChildColRef、AlterTableTagsInfo）
- 单元测试覆盖率：目标类达到 100% 覆盖率
- 集成测试：通过 7 个新的 DDL 场景测试

# 测试环境

- OS: macOS, Linux
- JDK: Java 8+
- TDengine: 3.0+
- 测试框架: JUnit 4, Mockito

# 功能测试

## 新增 AlterType 枚举值

### 测试要点

验证新增的 9 种 AlterType 枚举值能够正确序列化和反序列化：
- ADD_TAG_INDEX(11) - 添加标签索引
- UPDATE_COLUMN_COMPRESS(13) - 更新列压缩属性
- ADD_COLUMN_WITH_COMPRESS(14) - 添加带压缩属性的列
- SET_MULTI_TAG(15) - 设置多标签值
- ALTER_COLUMN_REF(16) - 修改列引用
- SET_REF_NULL(17) - 设置引用列为空
- ADD_COLUMN_WITH_REF(18) - 添加带引用的列
- ALTER_MULTI_TABLE_TAG(19) - 批量修改多表标签
- ALTER_STABLE_TAG_WITH_FILTER(20) - 使用正则表达式过滤修改超级表标签

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | AlterType 反序列化 - 类型 11 | JSON 值 "11" 应正确反序列化为 ADD_TAG_INDEX | Pass |
| 2 | AlterType 反序列化 - 类型 13 | JSON 值 "13" 应正确反序列化为 UPDATE_COLUMN_COMPRESS | Pass |
| 3 | AlterType 反序列化 - 类型 14 | JSON 值 "14" 应正确反序列化为 ADD_COLUMN_WITH_COMPRESS | Pass |
| 4 | AlterType 反序列化 - 类型 15 | JSON 值 "15" 应正确反序列化为 SET_MULTI_TAG | Pass |
| 5 | AlterType 反序列化 - 类型 16 | JSON 值 "16" 应正确反序列化为 ALTER_COLUMN_REF | Pass |
| 6 | AlterType 反序列化 - 类型 17 | JSON 值 "17" 应正确反序列化为 SET_REF_NULL | Pass |
| 7 | AlterType 反序列化 - 类型 18 | JSON 值 "18" 应正确反序列化为 ADD_COLUMN_WITH_REF | Pass |
| 8 | AlterType 反序列化 - 类型 19 | JSON 值 "19" 应正确反序列化为 ALTER_MULTI_TABLE_TAG | Pass |
| 9 | AlterType 反序列化 - 类型 20 | JSON 值 "20" 应正确反序列化为 ALTER_STABLE_TAG_WITH_FILTER | Pass |

## 虚拟表支持

### 测试要点

验证虚拟表的元数据解析：
- Meta 类新增 `isVirtual` 字段
- Column 类新增 `ref` 字段（ColRef 类型）
- 虚拟普通表和虚拟超级表的正确识别

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 解析虚拟超级表 | JSON 中 `tableType: 3, isVirtual: true` 应正确解析为虚拟超级表 | Pass |
| 2 | 解析虚拟普通表 | JSON 中 `tableType: 4, isVirtual: true` 应正确解析为虚拟普通表 | Pass |
| 3 | 解析列引用 | Column 的 `ref` 字段应包含 refDbName、refTbName、refColName | Pass |
| 4 | 虚拟表 DDL - 创建 | 测试 CREATE VIRTUAL TABLE 语句的元数据解析 | Pass |

## 列引用（ColRef）

### 测试要点

验证列引用机制的正确性：
- ColRef 类包含 refDbName、refTbName、refColName
- ChildColRef 类包含 colName、refDbName、refTableName、refColName
- equals() 和 hashCode() 方法实现
- JSON 反序列化时支持 `refTbName` 别名

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | ColRef equals 方法 | 验证相同字段值的 ColRef 对象相等 | Pass |
| 2 | ColRef hashCode 一致性 | 相等对象的 hashCode 必须一致 | Pass |
| 3 | ColRef null 值处理 | null 字段不应导致 equals/hashCode 异常 | Pass |
| 4 | ChildColRef 解析 | JSON 中的 `refTbName` 别名应正确映射到 `refTableName` | Pass |
| 5 | ChildColRef getter/setter | 所有字段的 getter/setter 应正常工作 | Pass |

## 批量修改多表标签

### 测试要点

验证 ALTER_MULTI_TABLE_TAG（类型 19）功能：
- MetaAlterTable 新增 `tables` 字段（List<AlterTableTagsInfo>）
- AlterTableTagsInfo 包含 tableName 和 tags（List<TagAlter>）
- 支持一次性修改多个子表的标签值

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 解析批量修改标签 JSON | tables 字段应正确解析为 List<AlterTableTagsInfo> | Pass |
| 2 | 多表标签修改场景 | 测试 ALTER 多张子表标签的 DDL 解析 | Pass |
| 3 | AlterTableTagsInfo equals | 相同 tableName 和 tags 的对象应相等 | Pass |

## 正则表达式过滤修改标签

### 测试要点

验证 ALTER_STABLE_TAG_WITH_FILTER（类型 20）功能：
- TagAlter 新增 `regexp` 和 `replacement` 字段
- MetaAlterTable 新增 `where` 字段
- 支持使用正则表达式批量修改匹配子表的标签

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 解析正则表达式字段 | regexp 和 replacement 应正确解析 | Pass |
| 2 | 解析 where 条件 | MetaAlterTable 的 where 字段应正确解析 | Pass |
| 3 | 正则标签修改场景 | 测试 ALTER ... TAG ... WHERE REGEXP 语句解析 | Pass |
| 4 | TagAlter 完整字段 | TagAlter 所有字段（name、type、value、regexp、replacement）正确解析 | Pass |

## 列压缩属性

### 测试要点

验证 UPDATE_COLUMN_COMPRESS（13）和 ADD_COLUMN_WITH_COMPRESS（14）功能：
- MetaAlterTable 新增 `encode`、`compress`、`level` 字段
- 支持设置列的压缩算法和压缩级别

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 解压缩字段解析 | encode、compress、level 字段应正确解析 | Pass |
| 2 | 更新列压缩场景 | 测试 ALTER COLUMN ... COMPRESS 语句解析 | Pass |
| 3 | 添加压缩列场景 | 测试 ADD COLUMN ... COMPRESS 语句解析 | Pass |

## 代码覆盖率

### 测试要点

验证单元测试覆盖率达到目标：
- AlterTableTagsInfo: 35% → 100%
- ChildColRef: 21% → 100%
- ColRef: 43% → 100%
- 其他 meta 类覆盖率提升

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | AlterTableTagsInfo 覆盖率 | equals、hashCode、getter/setter 全覆盖 | Pass |
| 2 | ChildColRef 覆盖率 | 新增测试类，全覆盖 | Pass |
| 3 | ColRef 覆盖率 | 新增测试类，全覆盖 | Pass |
| 4 | 整体覆盖率目标 | 新增功能相关类达到 100% 覆盖率 | Pass |

## 集成测试

### 测试要点

验证新功能在真实 TDengine 环境中的集成：
- 创建虚拟普通表（testCreateVirtualNormalTable）
- 创建虚拟子表（testCreateVirtualChildTable）
- 修改虚拟表添加带引用列（testAlterVirtualTableAddColumnWithRef）
- 批量修改多表标签（testAlterMultiTableTags）
- 使用正则表达式修改标签（testAlterStableTagWithRegexp）
- 使用过滤器修改标签（testAlterStableTagWithFilter）

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 虚拟普通表创建 | 验证虚拟普通表的元数据正确解析 | Pass |
| 2 | 虚拟子表创建 | 验证虚拟子表的 refs 字段正确解析 | Pass |
| 3 | 虚拟表 ALTER 操作 | 验证 ALTER_TYPE 18 的元数据正确解析 | Pass |
| 4 | 批量标签修改 | 验证 ALTER_TYPE 19 的 tables 字段正确解析 | Pass |
| 5 | 正则标签修改 | 验证 ALTER_TYPE 20 的 regexp 和 where 字段正确解析 | Pass |
| 6 | 过滤器标签修改 | 验证 WHERE 条件的正确解析 | Pass |

## 7. 易用性测试

不涉及。

## 8. 长期稳定性测试

无。

## 9. 性能测试

无。

## 10. 安全性测试

无。


# 兼容性测试

测试用例包括但不局限于：

- 老版本 TDengine 发送的元数据能否被新驱动正确解析？
- 新驱动能否兼容老版本的元数据格式？
- 向后兼容性：新增字段为 null 时的处理

# 已知问题和限制

- 目前虚拟表功能仅在 TDengine 3.0+ 版本支持
- ALTER_TYPE 12 未在此版本实现
- 某些集成测试依赖 TDengine 服务器环境，仅在主 CI 流程中运行
