---
sidebar_label: 自定义函数
title: 自定义函数
description: 创建、管理与调用用户自定义函数（UDF）
---

除内置函数外，还可以编写自定义函数逻辑并注册到 TDengine 中使用。

## 创建 UDF

可通过 SQL 在系统中加载客户端所在主机上的 UDF 函数库（不能通过 REST 接口或 HTTP 管理界面完成）。创建成功后，当前集群的所有用户均可在 SQL 中使用这些函数。UDF 保存在 mnode 上，即使重启系统，已创建的 UDF 仍然可用。

创建时需要区分标量函数与聚合函数。若声明的函数类别有误，调用时可能出错。还需保证输入数据类型与 UDF 程序匹配，输出数据类型与 `OUTPUTTYPE` 匹配。

### 创建标量函数

```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type [LANGUAGE 'C|Python'];
```

**参数说明**

- `OR REPLACE`：若函数已存在，则修改其属性。
- `function_name`：在 SQL 中调用时的函数名。
- `LANGUAGE 'C|Python'`：编程语言，目前支持 C 与 Python；省略时默认为 C。
- `library_path`：C 语言时为动态链接库的绝对路径（客户端主机上的路径，通常为 `.so` 文件）；Python 时为 UDF 实现文件路径。路径需用英文单引号或双引号括起。
- `output_type`：函数返回值的数据类型名称。

将 `libbitand.so` 注册为可用 UDF：

```sql
CREATE FUNCTION bit_and AS "/home/taos/udf_example/libbitand.so" OUTPUTTYPE INT;
```

修改已定义的 `bit_and`：输出类型改为 `BIGINT`，并用 Python 实现：

```sql
CREATE OR REPLACE FUNCTION bit_and AS "/home/taos/udf_example/bit_and.py" OUTPUTTYPE BIGINT LANGUAGE 'Python';
```

### 创建聚合函数

```sql
CREATE [OR REPLACE] AGGREGATE FUNCTION function_name AS library_path OUTPUTTYPE output_type [BUFSIZE buffer_size] [LANGUAGE 'C|Python'];
```

**参数说明**

- `OR REPLACE`：若函数已存在，则修改其属性。
- `function_name`：在 SQL 中调用时的函数名，须与函数实现中的实际名称一致。
- `LANGUAGE 'C|Python'`：编程语言，目前支持 C 与 Python（Python 3.7 及以上）；省略时默认为 C。
- `library_path`：C 语言时为动态链接库的绝对路径（客户端主机上的路径，通常为 `.so` 文件）；Python 时为 UDF 实现文件路径。路径需用英文单引号或双引号括起。
- `output_type`：函数返回值的数据类型名称。
- `BUFSIZE`：中间结果缓冲区大小，单位为字节；不需要时可省略。

将 `libl2norm.so` 注册为可用 UDF：

```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE BUFSIZE 8;
```

将已定义 `l2norm` 的缓冲区大小修改为 64：

```sql
CREATE OR REPLACE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE BUFSIZE 64;
```

如何开发自定义函数，参见 [UDF 编程接口](../../10-developer-guide/06-udf.md)。

## 管理 UDF

删除指定名称的用户定义函数：

```sql
DROP FUNCTION function_name;
```

- `function_name`：要删除的函数名，含义与 `CREATE` 中的 `function_name` 一致，例如 `bit_and`、`l2norm`。

```sql
DROP FUNCTION bit_and;
```

显示系统中当前可用的全部 UDF：

```sql
SHOW FUNCTIONS;
```

## 调用 UDF

在 SQL 中可直接使用创建时指定的函数名调用 UDF：

```sql
SELECT bit_and(c1, c2) FROM table;
```

上例对表 `table` 的 `c1`、`c2` 列调用 `bit_and`。UDF 也可与 `WHERE` 等查询子句一起使用。
