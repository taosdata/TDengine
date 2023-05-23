---
sidebar_label: 自定义函数
title: 自定义函数
description: 使用 UDF 的详细指南
---

除了 TDengine 的内置函数以外，用户还可以编写自己的函数逻辑并加入TDengine系统中。
## 创建 UDF

用户可以通过 SQL 指令在系统中加载客户端所在主机上的 UDF 函数库（不能通过 RESTful 接口或 HTTP 管理界面来进行这一过程）。一旦创建成功，则当前 TDengine 集群的所有用户都可以在 SQL 指令中使用这些函数。UDF 存储在系统的 MNode 节点上，因此即使重启 TDengine 系统，已经创建的 UDF 也仍然可用。

在创建 UDF 时，需要区分标量函数和聚合函数。如果创建时声明了错误的函数类别，则可能导致通过 SQL 指令调用函数时出错。此外，用户需要保证输入数据类型与 UDF 程序匹配，UDF 输出数据类型与 OUTPUTTYPE 匹配。

- 创建标量函数
```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type [LANGUAGE 'C|Python'];
```
  - OR REPLACE: 如果函数已经存在，会修改已有的函数属性。
  - function_name：标量函数未来在 SQL 中被调用时的函数名；
  - LANGUAGE 'C|Python'：函数编程语言，目前支持C语言和Python语言。 如果这个从句忽略，编程语言是C语言 
  - library_path：如果编程语言是C，路径是包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件）。如果编程语言是Python，路径是包含 UDF 函数实现的Python文件路径。这个路径需要用英文单引号或英文双引号括起来；
  - output_type：此函数计算结果的数据类型名称；

例如，如下语句可以把 libbitand.so 创建为系统中可用的 UDF：

  ```sql
  CREATE FUNCTION bit_and AS "/home/taos/udf_example/libbitand.so" OUTPUTTYPE INT;
  ```

例如，使用以下语句可以修改已经定义的 bit_and 函数，输出类型是 BIGINT，使用Python语言实现。

  ```sql
  CREATE OR REPLACE FUNCTION bit_and AS "/home/taos/udf_example/bit_and.py" OUTPUTTYPE BIGINT LANGUAGE 'Python';
  ```
- 创建聚合函数：
```sql
CREATE [OR REPLACE] AGGREGATE FUNCTION function_name AS library_path OUTPUTTYPE output_type [ BUFSIZE buffer_size ] [LANGUAGE 'C|Python'];
```
  - OR REPLACE: 如果函数已经存在，会修改已有的函数属性。
  - function_name：聚合函数未来在 SQL 中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  - LANGUAGE 'C|Python'：函数编程语言，目前支持C语言和Python语言（v3.7+）。
  - library_path：如果编程语言是C，路径是包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件）。如果编程语言是Python，路径是包含 UDF 函数实现的Python文件路径。这个路径需要用英文单引号或英文双引号括起来；；
  - output_type：此函数计算结果的数据类型名称；
  - buffer_size：中间计算结果的缓冲区大小，单位是字节。如果不使用可以不设置。

  例如，如下语句可以把 libl2norm.so 创建为系统中可用的 UDF：

  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
  ```
  例如，使用以下语句可以修改已经定义的 l2norm 函数的缓冲区大小为64。
  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 64;
  ```  

关于如何开发自定义函数，请参考 [UDF使用说明](/develop/udf)。

## 管理 UDF

- 删除指定名称的用户定义函数：
```
DROP FUNCTION function_name;
```

- function_name：此参数的含义与 CREATE 指令中的 function_name 参数一致，也即要删除的函数的名字，例如bit_and, l2norm 
```sql
DROP FUNCTION bit_and;
```
- 显示系统中当前可用的所有 UDF：
```sql
SHOW FUNCTIONS;
```

## 调用 UDF

在 SQL 指令中，可以直接以在系统中创建 UDF 时赋予的函数名来调用用户定义函数。例如：
```sql
SELECT bit_and(c1,c2) FROM table;
```

表示对表 table 上名为 c1, c2 的数据列调用名为 bit_and 的用户定义函数。SQL 指令中用户定义函数可以配合 WHERE 等查询特性来使用。
