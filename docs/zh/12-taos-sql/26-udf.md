---
sidebar_label: 自定义函数
title: 用户自定义函数
description: 使用 UDF 的详细指南
---

除了 TDengine 的内置函数以外，用户还可以编写自己的函数逻辑并加入TDengine系统中。
## 创建 UDF

用户可以通过 SQL 指令在系统中加载客户端所在主机上的 UDF 函数库（不能通过 RESTful 接口或 HTTP 管理界面来进行这一过程）。一旦创建成功，则当前 TDengine 集群的所有用户都可以在 SQL 指令中使用这些函数。UDF 存储在系统的 MNode 节点上，因此即使重启 TDengine 系统，已经创建的 UDF 也仍然可用。

在创建 UDF 时，需要区分标量函数和聚合函数。如果创建时声明了错误的函数类别，则可能导致通过 SQL 指令调用函数时出错。此外，用户需要保证输入数据类型与 UDF 程序匹配，UDF 输出数据类型与 OUTPUTTYPE 匹配。

- 创建标量函数
```sql
CREATE FUNCTION function_name AS library_path OUTPUTTYPE output_type;
```

  - function_name：标量函数未来在 SQL 中被调用时的函数名，必须与函数实现中 udf 的实际名称一致；
  - library_path：包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  - output_type：此函数计算结果的数据类型名称；

  例如，如下语句可以把 libbitand.so 创建为系统中可用的 UDF：

  ```sql
  CREATE FUNCTION bit_and AS "/home/taos/udf_example/libbitand.so" OUTPUTTYPE INT;
  ```

- 创建聚合函数：
```sql
CREATE AGGREGATE FUNCTION function_name AS library_path OUTPUTTYPE output_type [ BUFSIZE buffer_size ];
```

  - function_name：聚合函数未来在 SQL 中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  - library_path：包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  - output_type：此函数计算结果的数据类型，与上文中 udfNormalFunc 的 itype 参数不同，这里不是使用数字表示法，而是直接写类型名称即可；
  - buffer_size：中间计算结果的缓冲区大小，单位是字节。如果不使用可以不设置。

  例如，如下语句可以把 libl2norm.so 创建为系统中可用的 UDF：

  ```sql
  CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
  ```
关于如何开发自定义函数，请参考 [UDF使用说明](../../develop/udf)。

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
SELECT X(c1,c2) FROM table/stable;
```

表示对名为 c1, c2 的数据列调用名为 X 的用户定义函数。SQL 指令中用户定义函数可以配合 WHERE 等查询特性来使用。
