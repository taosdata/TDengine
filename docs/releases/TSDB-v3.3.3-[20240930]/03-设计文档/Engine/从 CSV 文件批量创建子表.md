# 从 CSV 文件批量创建子表

## 1. 背景

在从一些数据源（比如关系型数据库）批量导入数据时，我们有可能需要批量创建子表。这些子表的表名、标签值可以从数据源导出，然后通过 CSV 文件提供给 TDengine。
- 已支持 使用 `INSERT INTO` 语句从 **包含普通列 **的 CSV 文件批量创建子表及导入数据。
- 需支持 从 **不包含普通列** 的 CSV 文件批量创建子表。考虑到该场景中 CSV 文件不包含表内数据，仅需建表，决定使用 `CREATE TABLE` 语句实现该功能。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/6/7 | 0.1 | 李顺纲 | 初稿 |
| 2024/6/14 | 0.2 | 李顺纲 | 使用 `CREATE TABLE` 建表 |
| 2024/6/18 | 0.3 | 李顺纲 | 根据 Wade review 意见修改 |
| 2024/6/18 | 0.4 | Wade | 明确约束规则，优化文档格式，补全缺失内容 |

## 3. 定义

无特殊定义

## 4. 行为说明

### 4.1 行为总览

| 场景 | 数据行含 普通列 | 数据行含 子表名称列 | 数据行含 标签列 | 说明 | 状态 | 示例 |
| --- | --- | --- | --- | --- | --- | --- |
| 场景一 | 是 | 1. 导入时序数据 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，不更新标签值 1. 如果子表不存在，创建子表，标签值取子表的第一行数据 | INSERT INTO 已经支持 CREATE TABLE 不支持 | [需求说明：从 CSV 批量建表](https://taosdata.feishu.cn/wiki/W8yOwD4W7ilHkPkqLYtcx01anrg) |
| 场景二 | 否 | 1. 导入时序数据 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，不更新标签值 1. 如果子表不存在，创建子表，标签值设置为 NULL | INSERT INTO 已经支持 CREATE TABLE 不支持 | [需求说明：从 CSV 批量建表](https://taosdata.feishu.cn/wiki/W8yOwD4W7ilHkPkqLYtcx01anrg) |
| 场景三 | 是 | - | 不支持 | [需求说明：从 CSV 批量建表](https://taosdata.feishu.cn/wiki/W8yOwD4W7ilHkPkqLYtcx01anrg) |
| 场景四 | 否 | 1. 导入时序数据 1. 通过 SQL 语句指定子表名 1. 如果子表存在，不更新标签值 1. 如果子表不存在，创建子表，标签值设置为 NULL | INSERT INTO 已经支持 CREATE TABLE 不支持 | [需求说明：从 CSV 批量建表](https://taosdata.feishu.cn/wiki/W8yOwD4W7ilHkPkqLYtcx01anrg) |
| 场景五 | 是 | 1. 不导入时序数据，仅建表 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，不做任何处理 1. 如果子表不存在，创建子表，标签值取子表的第一行数据 | INSERT INTO 不支持 CREATE TABLE 本次功能 | [从 CSV 文件批量创建子表](https://taosdata.feishu.cn/wiki/QU74w0DqDiMw6UkO5HbccZ4pnTe) |
| 场景六 | 否 | 1. 不导入时序数据，仅建表 1. 通过 SQL 语句指定超级表名 1. 如果子表存在，不做任何处理 1. 如果子表不存在，创建子表，标签值设置为 NULL | INSERT INTO 不支持 CREATE TABLE 本次功能 | [从 CSV 文件批量创建子表](https://taosdata.feishu.cn/wiki/QU74w0DqDiMw6UkO5HbccZ4pnTe) |
| 场景七 | 是 | - | 不支持 |  |
| 场景八 | 否 | - | 不支持 |  |

### 4.2 **SQL 语句说明**

使用 `create table` 语句实现依据 CSV 文件中的元数据批量建表，语法如下。
```sql {wrap}
CREATE TABLE
    [IF NOT EXISTS]
    USING stb_name (field1_name [, field2_name] ...) 
        FILE csv_file_path;
```

1. `if not exists`：
   - 如不指定，如果尝试建立已经存在的表会导致该命令报错并退出
   - 若指定，如果尝试建立已经存在的表将忽略错误并继续执行
   - 默认不指定
2. `stb_name`：引用的超级表名称
   - 依据该超级表的 schema 创建子表
   - 该超级表必须已经建立
3. `field_name`：表名和一个或多个标签名的列表
   - 列表顺序与 CSV 文件各列内容顺序一致
   - 列表中 **不允许存在重复项**，否则报错并退出
   - 列表中 **必须包含 **`**tbname**`，否则报错并退出
   - 列表中可包含零个或多个标签列，但必须是所引用的超级表中已经存在的标签列
   - 未包含在该列表中的标签，其值将被设置为 NULL
4. `csv_file_path`：csv 文件路径

### 4.3 **CSV 文件****格式和约束规则**

1. CSV 文件中每行必须包含与 SQL 语名中的字段列表中所指定的数量相同的列，由 ',' 分隔，如果列数不匹配则报错退出
2. CSV 文件中的注释行会被自动忽略，不做处理
3. CSV 文件各字段的值与所对应的标签的类型必须匹配，否则报错并退出。
   - 字符串类型值 需使用 单引号 ' 或 双引号 " 引用
   - 布尔类型值 可匹配 0/1、true/false、以及 ‘true’/'false' 等常见值
4. CSV 文件中对应 `tbname` 的值必须符合 TDengine 表名命名规则 

## 5. 性能

建表性能由测试结果确定，暂无预期

## 6. 兼容性

无兼容性问题，这是一个新的SQL命令，增加了一种建表入口

## 7. 运维

无运维要求

## 8. 使用场景

### 8.1 场景五

数据行不含普通列，含子表名称列，含标签列。

#### 8.1.1 准备数据

第一列对应 `location` 标签，第二列对应 `groupId` 标签，第三列是子表名
```bash {wrap}
#/users/lsg/downloads/auto5.csv
'California.SanFrancisco',2,'d1001'
'California.SanFrancisco',3,'d1002'
'California.SanFrancisco',2,'d1001'
'California.SanFrancisco',3,'d1002'
'California.LosAngeles',2,'d1003'
'California.LosAngeles',3,'d1004'
'California.LosAngeles',2,'d1003'
'California.LosAngeles',3,'d1004'
'California.LosAngeles',2,'d1003'
```

#### 8.1.2 导入数据

SQL 命令中指定的标签列表必须与上面的文件格式一致，且对应子表名的关键字 `tbname` 也要与第三列的子表名对应。换句话说文件内容决定了列表中标签和表名的顺序。
```sql {wrap}
create table using meters (location, groupId, tbname) file '/users/lsg/downloads/auto5.csv';
```

### 8.2 场景六

数据行不含普通列，含子表名称列，不含标签列。

#### 8.2.1 准备数据

数据中只有子表名这一列。
```bash {wrap}
#/users/lsg/downloads/auto6.csv
'd1001'
'd1002'
'd1001'
'd1002'
'd1003'
'd1004'
'd1003'
'd1004'
'd1003'
```

#### 8.2.2 导入数据

```sql {wrap}
create table using meters (tbname) file '/users/lsg/downloads/auto6.csv';
```

## 9. 约束和限制

配置文件中的`maxInsertBatchRows` ，可控制每批次从 csv 文件中解析的行数，同时影响本功能向 server 端发送的数据包的大小。
若导入过程中遭遇 `DB error: Invalid message len`，需减小该参数大小。

| 名称 | 功能 | 默认值 | 参考值 |
| --- | --- | --- | --- |
| `maxInsertBatchRows` | 每批次从 csv 文件中解析的行数 | 1 000 000 | `maxInsertBatchRows` * 平均行数据量 / `nVgroup` < 1M |

## 10. 常见错误和排查

详见行为说明中的 CSV 文件格式和约束规则

## 11. 可观测性

无观测性

## 12. 安装和卸载

无特殊要求

## 13. 文档

需要修改官网文档，在以下页面添加 File 相关语法描述：
https://docs.taosdata.com/taos-sql/table/#%E5%88%9B%E5%BB%BA%E8%A1%A8

## 14. 参考文档

TS-4917

[需求说明：从 CSV 批量建表](https://taosdata.feishu.cn/wiki/W8yOwD4W7ilHkPkqLYtcx01anrg)
