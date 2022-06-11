---
sidebar_label: 用户定义函数
title: UDF（用户定义函数）
description: "支持用户编码的聚合函数和标量函数，在查询中嵌入并使用用户定义函数，拓展查询的能力和功能。"
---

在有些应用场景中，应用逻辑需要的查询无法直接使用系统内置的函数来表示。利用 UDF 功能，TDengine 可以插入用户编写的处理代码并在查询中使用它们，就能够很方便地解决特殊应用场景中的使用需求。 UDF 通常以数据表中的一列数据做为输入，同时支持以嵌套子查询的结果作为输入。

从 2.2.0.0 版本开始，TDengine 支持通过 C/C++ 语言进行 UDF 定义。接下来结合示例讲解 UDF 的使用方法。

用户可以通过 UDF 实现两类函数： 标量函数 和 聚合函数。

## 用 C/C++ 语言来定义 UDF

### 标量函数

用户可以按照下列函数模板定义自己的标量计算函数

 `void udfNormalFunc(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBuf, char* tsOutput, int* numOfOutput, short otype, short obytes, SUdfInit* buf)` 
 
 其中 udfNormalFunc 是函数名的占位符，以上述模板实现的函数对行数据块进行标量计算，其参数项是固定的，用于按照约束完成与引擎之间的数据交换。

- udfNormalFunc 中各参数的具体含义是：
  - data：输入数据。
  - itype：输入数据的类型。这里采用的是短整型表示法，与各种数据类型对应的值可以参见 [column_meta 中的列类型说明](/reference/rest-api/)。例如 4 用于表示 INT 型。
  - iBytes：输入数据中每个值会占用的字节数。
  - numOfRows：输入数据的总行数。
  - ts：主键时间戳在输入中的列数据(只读)。
  - dataOutput：输出数据的缓冲区，缓冲区大小为用户指定的输出类型大小 \* numOfRows。
  - interBuf：中间计算结果的缓冲区，大小为用户在创建 UDF 时指定的 BUFSIZE 大小。通常用于计算中间结果与最终结果不一致时使用，由引擎负责分配与释放。
  - tsOutput：主键时间戳在输出时的列数据，如果非空可用于输出结果对应的时间戳。
  - numOfOutput：输出结果的个数（行数）。
  - oType：输出数据的类型。取值含义与 itype 参数一致。
  - oBytes：输出数据中每个值占用的字节数。
  - buf：用于在 UDF 与引擎间的状态控制信息传递块。

  [add_one.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c) 是结构最简单的 UDF 实现，也即上面定义的 udfNormalFunc 函数的一个具体实现。其功能为：对传入的一个数据列（可能因 WHERE 子句进行了筛选）中的每一项，都输出 +1 之后的值，并且要求输入的列数据类型为 INT。

### 聚合函数

用户可以按照如下函数模板定义自己的聚合函数。

`void abs_max_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf)`

其中 udfMergeFunc 是函数名的占位符，以上述模板实现的函数用于对计算中间结果进行聚合，只有针对超级表的聚合查询才需要调用该函数。其中各参数的具体含义是：

  - data：udfNormalFunc 的输出数据数组，如果使用了 interBuf 那么 data 就是 interBuf 的数组。
  - numOfRows：data 中数据的行数。
  - dataOutput：输出数据的缓冲区，大小等于一条最终结果的大小。如果此时输出还不是最终结果，可以选择输出到 interBuf 中即 data 中。
  - numOfOutput：输出结果的个数（行数）。
  - buf：用于在 UDF 与引擎间的状态控制信息传递块。

[abs_max.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c) 实现的是一个聚合函数，功能是对一组数据按绝对值取最大值。

其计算过程为：与所在查询语句相关的数据会被分为多个行数据块，对每个行数据块调用 udfNormalFunc（在本例的实现代码中，实际函数名是 `abs_max`)来生成每个子表的中间结果，再将子表的中间结果调用 udfMergeFunc（本例中，其实际的函数名是 `abs_max_merge`）进行聚合，生成超级表的最终聚合结果或中间结果。聚合查询最后还会通过 udfFinalizeFunc（本例中，其实际的函数名是 `abs_max_finalize`）再把超级表的中间结果处理为最终结果，最终结果只能含 0 或 1 条结果数据。

其他典型场景，如协方差的计算，也可通过定义聚合 UDF 的方式实现。

### 最终计算

用户可以按下面的函数模板实现自己的函数对计算结果进行最终计算，通常用于有 interBuf 使用的场景。

`void abs_max_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf)`

其中 udfFinalizeFunc 是函数名的占位符 ，其中各参数的具体含义是：
  - dataOutput：输出数据的缓冲区。
  - interBuf：中间结算结果缓冲区，可作为输入。
  - numOfOutput：输出数据的个数，对聚合函数来说只能是 0 或者 1。
  - buf：用于在 UDF 与引擎间的状态控制信息传递块。

## UDF 实现方式的规则总结

三类 UDF 函数： udfNormalFunc、udfMergeFunc、udfFinalizeFunc ，其函数名约定使用相同的前缀，此前缀即 udfNormalFunc 的实际函数名，也即 udfNormalFunc 函数不需要在实际函数名后添加后缀；而udfMergeFunc 的函数名要加上后缀 `_merge`、udfFinalizeFunc 的函数名要加上后缀 `_finalize`，这是 UDF 实现规则的一部分，系统会按照这些函数名后缀来调用相应功能。

根据 UDF 函数类型的不同，用户所要实现的功能函数也不同：

- 标量函数：UDF 中需实现 udfNormalFunc。
- 聚合函数：UDF 中需实现 udfNormalFunc、udfMergeFunc（对超级表查询）、udfFinalizeFunc。

:::note
如果对应的函数不需要具体的功能，也需要实现一个空函数。

:::

## 编译 UDF

用户定义函数的 C 语言源代码无法直接被 TDengine 系统使用，而是需要先编译为 动态链接库，之后才能载入 TDengine 系统。

例如，按照上一章节描述的规则准备好了用户定义函数的源代码 add_one.c，以 Linux 为例可以执行如下指令编译得到动态链接库文件：

```bash
gcc -g -O0 -fPIC -shared add_one.c -o add_one.so
```

这样就准备好了动态链接库 add_one.so 文件，可以供后文创建 UDF 时使用了。为了保证可靠的系统运行，编译器 GCC 推荐使用 7.5 及以上版本。

## 在系统中管理和使用 UDF

### 创建 UDF

用户可以通过 SQL 指令在系统中加载客户端所在主机上的 UDF 函数库（不能通过 RESTful 接口或 HTTP 管理界面来进行这一过程）。一旦创建成功，则当前 TDengine 集群的所有用户都可以在 SQL 指令中使用这些函数。UDF 存储在系统的 MNode 节点上，因此即使重启 TDengine 系统，已经创建的 UDF 也仍然可用。

在创建 UDF 时，需要区分标量函数和聚合函数。如果创建时声明了错误的函数类别，则可能导致通过 SQL 指令调用函数时出错。此外， UDF 支持输入与输出类型不一致，用户需要保证输入数据类型与 UDF 程序匹配，UDF 输出数据类型与 OUTPUTTYPE 匹配。

- 创建标量函数
```sql
CREATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) [ BUFSIZE B ];
```

  - ids(X)：标量函数未来在 SQL 指令中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  - ids(Y)：包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  - typename(Z)：此函数计算结果的数据类型，与上文中 udfNormalFunc 的 itype 参数不同，这里不是使用数字表示法，而是直接写类型名称即可；
  - B：中间计算结果的缓冲区大小，单位是字节，最小 0，最大 512，如果不使用可以不设置。

  例如，如下语句可以把 add_one.so 创建为系统中可用的 UDF：

  ```sql
  CREATE FUNCTION add_one AS "/home/taos/udf_example/add_one.so" OUTPUTTYPE INT;
  ```

- 创建聚合函数：
```sql
CREATE AGGREGATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) [ BUFSIZE B ];
```

  - ids(X)：聚合函数未来在 SQL 指令中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  - ids(Y)：包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  - typename(Z)：此函数计算结果的数据类型，与上文中 udfNormalFunc 的 itype 参数不同，这里不是使用数字表示法，而是直接写类型名称即可；
  - B：中间计算结果的缓冲区大小，单位是字节，最小 0，最大 512，如果不使用可以不设置。

  关于中间计算结果的使用，可以参考示例程序[demo.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/demo.c)

  例如，如下语句可以把 demo.so 创建为系统中可用的 UDF：

  ```sql
  CREATE AGGREGATE FUNCTION demo AS "/home/taos/udf_example/demo.so" OUTPUTTYPE DOUBLE bufsize 14;
  ```

### 管理 UDF

- 删除指定名称的用户定义函数：
```
DROP FUNCTION ids(X);
```

- ids(X)：此参数的含义与 CREATE 指令中的 ids(X) 参数一致，也即要删除的函数的名字，例如 
```sql
DROP FUNCTION add_one;
```
- 显示系统中当前可用的所有 UDF：
```sql
SHOW FUNCTIONS;
```

### 调用 UDF

在 SQL 指令中，可以直接以在系统中创建 UDF 时赋予的函数名来调用用户定义函数。例如：
```sql
SELECT X(c) FROM table/stable;
```

表示对名为 c 的数据列调用名为 X 的用户定义函数。SQL 指令中用户定义函数可以配合 WHERE 等查询特性来使用。

## UDF 的一些使用限制

在当前版本下，使用 UDF 存在如下这些限制：

1. 在创建和调用 UDF 时，服务端和客户端都只支持 Linux 操作系统；
2. UDF 不能与系统内建的 SQL 函数混合使用，暂不支持在一条 SQL 语句中使用多个不同名的 UDF ；
3. UDF 只支持以单个数据列作为输入；
4. UDF 只要创建成功，就会被持久化存储到 MNode 节点中；
5. 无法通过 RESTful 接口来创建 UDF；
6. UDF 在 SQL 中定义的函数名，必须与 .so 库文件实现中的接口函数名前缀保持一致，也即必须是 udfNormalFunc 的名称，而且不可与 TDengine 中已有的内建 SQL 函数重名。

## 示例代码

### 标量函数示例 [add_one](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c)

<details>
<summary>add_one.c</summary>

```c
{{#include tests/script/sh/add_one.c}}
```

</details>

### 向量函数示例 [abs_max](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c)

<details>
<summary>abs_max.c</summary>

```c
{{#include tests/script/sh/abs_max.c}}
```

</details>

### 使用中间计算结果示例 [demo](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/demo.c)

<details>
<summary>demo.c</summary>

```c
{{#include tests/script/sh/demo.c}}
```

</details>
