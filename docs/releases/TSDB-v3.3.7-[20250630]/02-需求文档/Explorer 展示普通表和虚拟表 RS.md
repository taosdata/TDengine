# Explorer 展示普通表和虚拟表 RS

## 1. 引言

### 1.1 术语与缩写名词

1. 普通表：普通表建表语句不包含标签，仅包含时间戳和数据列。
2. 虚拟表：虚拟表是一种动态数据结构，根据多表的列和时间戳组合规则生成逻辑表，见  https://docs.taosdata.com/reference/taos-sql/virtualtable/ 。
  根据表模板的不同，分为虚拟超级表和虚拟普通表：
   - 虚拟超级表：通过 `CREATE STABLE stable_name (timestamp_col, value_col [, value_col]) tags(tag_col[, tag col] VIRTUAL 1` 创建。
   - 虚拟子表：通过虚拟超级表模板 `USING virtal_stable_name TAGS(const_tag_value)`创建。
   - 虚拟普通表：通过子表或普通表列组合创建。

### 1.2 相关文档资料

JIRA ：https://jira.taosdata.com:18080/browse/TS-6405

### 1.3 优先级要求

低

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/27 | 1.0 | 霍琳贺 | 新建 |

## 3. 需求目标

在 Explorer 数据库树形结构列表，支持展示普通表、虚拟普通表、虚拟超级表和虚拟子表。

## 4. 功能需求

1. 普通表和虚拟普通表分开展示；
2. 虚拟超级表可正常展示；

## 5. 性能需求

不影响现有超级表展示。

## 6. 其他需求

无
