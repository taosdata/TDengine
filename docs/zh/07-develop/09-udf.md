---
sidebar_label: 用户定义函数
title: UDF（用户定义函数）
description: "支持用户编码的聚合函数和标量函数，在查询中嵌入并使用用户定义函数，拓展查询的能力和功能。"
---

在有些应用场景中，应用逻辑需要的查询无法直接使用系统内置的函数来表示。利用 UDF 功能，TDengine 可以插入用户编写的处理代码并在查询中使用它们，就能够很方便地解决特殊应用场景中的使用需求。 UDF 通常以数据表中的一列数据做为输入，同时支持以嵌套子查询的结果作为输入。

从 2.2.0.0 版本开始，TDengine 支持通过 C/C++ 语言进行 UDF 定义。接下来结合示例讲解 UDF 的使用方法。

用户可以通过 UDF 实现两类函数： 标量函数和聚合函数。

## 接口函数

### 标量函数

用户可以按照下列函数模板定义自己的标量计算函数

 `int32_t udf(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn)` 
 
 其中 udf 是函数名的占位符，以上述模板实现的函数对行数据块进行标量计算。

- 其中各参数的具体含义是：
  - inputDataBlock: 输入的数据块
  - resultColumn: 输出列 

### 聚合函数

用户可以按照如下函数模板定义自己的聚合函数。

`int32_t udaf_start(SUdfInterBuf *interBuf)`

`int32_t udaf(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf)`

`int32_t udaf_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result)`
其中 udaf 是函数名的占位符。其中各参数的具体含义是：

  - interBuf：中间结果 buffer。
  - inputBlock：输入的数据块。
  - newInterBuf：新的中间结果buffer。
  - result：最终结果。


其计算过程为：首先调用udf_start生成结果buffer，然后相关的数据会被分为多个行数据块，对每个行数据块调用 udf 用数据块更新中间结果，最后再调用 udf_finish 从中间结果产生最终结果，最终结果只能含 0 或 1 条结果数据。

### UDF 初始化和销毁
`int32_t udf_init()`

`int32_t udf_destroy()`

其中 udf 是函数名的占位符，可以替换成自己的函数名。udf_init 完成初始化工作。 udf_destroy 完成清理工作。如果没有初始化工作，无需定义udf_init函数。如果没有清理工作，无需定义udf_destroy函数。


### UDF 数据结构
```c
typedef struct SUdfColumnMeta {
  int16_t type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
} SUdfColumnMeta;

typedef struct SUdfColumnData {
  int32_t numOfRows;
  int32_t rowsAlloc;
  union {
    struct {
      int32_t nullBitmapLen;
      char   *nullBitmap;
      int32_t dataLen;
      char   *data;
    } fixLenCol;

    struct {
      int32_t varOffsetsLen;
      int32_t   *varOffsets;
      int32_t payloadLen;
      char   *payload;
      int32_t payloadAllocLen;
    } varLenCol;
  };
} SUdfColumnData;

typedef struct SUdfColumn {
  SUdfColumnMeta colMeta;
  bool           hasNull;
  SUdfColumnData colData;
} SUdfColumn;

typedef struct SUdfDataBlock {
  int32_t numOfRows;
  int32_t numOfCols;
  SUdfColumn **udfCols;
} SUdfDataBlock;

typedef struct SUdfInterBuf {
  int32_t bufLen;
  char* buf;
  int8_t numOfResult; //zero or one
} SUdfInterBuf;
```

为了更好的操作以上数据结构，提供了一些便利函数，定义在 taosudf.h。

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

  例如，如下语句可以把 libsqrsum.so 创建为系统中可用的 UDF：

  ```sql
  CREATE AGGREGATE FUNCTION sqr_sum AS "/home/taos/udf_example/libsqrsum.so" OUTPUTTYPE DOUBLE bufsize 8;
  ```

### 管理 UDF

- 删除指定名称的用户定义函数：
```
DROP FUNCTION function_name;
```

- function_name：此参数的含义与 CREATE 指令中的 function_name 参数一致，也即要删除的函数的名字，例如 
```sql
DROP FUNCTION bit_and;
```
- 显示系统中当前可用的所有 UDF：
```sql
SHOW FUNCTIONS;
```

### 调用 UDF

在 SQL 指令中，可以直接以在系统中创建 UDF 时赋予的函数名来调用用户定义函数。例如：
```sql
SELECT X(c1,c2) FROM table/stable;
```

表示对名为 c1, c2 的数据列调用名为 X 的用户定义函数。SQL 指令中用户定义函数可以配合 WHERE 等查询特性来使用。


## 示例代码

### 标量函数示例 [bit_and](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/bit_and.c)

bit_add 实现多列的按位与功能。如果只有一列，返回这一列。bit_add 忽略空值。

<details>
<summary>bit_and.c</summary>

```c
{{#include tests/script/sh/bit_and.c}}
```

</details>

### 聚合函数示例 [sqr_sum](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/sqr_sum.c)

sqr_sum 实现了输入列的所有数据的二阶范数，即对每个数据先平方，再累加求和，最后开方。

<details>
<summary>sqr_sum.c</summary>

```c
{{#include tests/script/sh/sqr_sum.c}}
```

</details>
