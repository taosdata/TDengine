---
sidebar_label: UDF
title: 用户自定义函数
toc_max_heading_level: 4
---

## UDF 简介

在某些应用场景中，应用逻辑需要的查询功能无法直接使用内置函数来实现，TDengine 允许编写用户自定义函数（UDF），以便解决特殊应用场景中的使用需求。UDF 在集群中注册成功后，可以像系统内置函数一样在 SQL 中调用，就使用角度而言没有任何区别。UDF 分为标量函数和聚合函数。标量函数对每行数据输出一个值，如求绝对值（abs）、正弦函数（sin）、字符串拼接函数（concat）等。聚合函数对多行数据输出一个值，如求平均数（avg）、取最大值（max）等。

TDengine 支持用 C 和 Python 两种编程语言编写 UDF。C 语言编写的 UDF 与内置函数的性能几乎相同，Python 语言编写的 UDF 可以利用丰富的 Python 运算库。为了避免 UDF 执行中发生异常影响数据库服务，TDengine 使用了进程分离技术，把 UDF 的执行放到另一个进程中完成，即使用户编写的 UDF 崩溃，也不会影响 TDengine 的正常运行。

## 用 C 语言开发 UDF

使用 C 语言实现 UDF 时，需要实现规定的接口函数
- 标量函数需要实现标量接口函数 scalarfn 。
- 聚合函数需要实现聚合接口函数 aggfn_start、aggfn、aggfn_finish。
- 如果需要初始化，实现 udf_init。
- 如果需要清理工作，实现 udf_destroy。

### 接口定义

接口函数的名称是 UDF 名称，或者是 UDF 名称和特定后缀（_start、_finish、_init、_destroy）的连接。后面内容中描述的函数名称，例如 scalarfn、aggfn，需要替换成 UDF 名称。

#### 标量函数接口

标量函数是一种将输入数据转换为输出数据的函数，通常用于对单个数据值进行计算和转换。标量函数的接口函数原型如下。

```c
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn);
```
主要参数说明如下。
- inputDataBlock：输入的数据块。
- resultColumn：输出列。

#### 聚合函数接口

聚合函数是一种特殊的函数，用于对数据进行分组和计算，从而生成汇总信息。聚合函数的工作原理如下。
- 初始化结果缓冲区：首先调用 aggfn_start 函数，生成一个结果缓冲区（result buffer），用于存储中间结果。
- 分组数据：相关数据会被分为多个行数据块（row data block），每个行数据块包含一组具有相同分组键（grouping key）的数据。
- 更新中间结果：对于每个数据块，调用 aggfn 函数更新中间结果。aggfn 函数会根据聚合函数的类型（如 sum、avg、count 等）对数据进行相应的计算，并将计算结
果存储在结果缓冲区中。
- 生成最终结果：在所有数据块的中间结果更新完成后，调用 aggfn_ﬁnish 函数从结果缓冲区中提取最终结果。最终结果只包含 0 条或 1 条数据，具体取决于聚
合函数的类型和输入数据。

聚合函数的接口函数原型如下。

```c
int32_t aggfn_start(SUdfInterBuf *interBuf);
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf);
int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result);
```

其中 aggfn 是函数名的占位符。首先调用 aggfn_start 生成结果 buffer，然后相关的数据会被分为多个行数据块，对每个数据块调用 aggfn 用数据块更新中间结果，最后再调用 aggfn_finish 从中间结果产生最终结果，最终结果只能含 0 或 1 条结果数据。

主要参数说明如下。
- interBuf：中间结果缓存区。
- inputBlock：输入的数据块。
- newInterBuf：新的中间结果缓冲区。
- result：最终结果。

#### 初始化和销毁接口

初始化和销毁接口是标量函数和聚合函数共同使用的接口，相关 API 如下。

```c
int32_t udf_init()
int32_t udf_destroy()
```

其中，udf_init 函数完成初始化工作，udf_destroy 函数完成清理工作。如果没有初始化工作，无须定义 udf_init 函数；如果没有清理工作，无须定义 udf_destroy 函数。

### 标量函数模板

用 C 语言开发标量函数的模板如下。
```c
#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

// Initialization function. 
// If no initialization, we can skip definition of it. 
// The initialization function shall be concatenation of the udf name and _init suffix.
// @return error number defined in taoserror.h
int32_t scalarfn_init() {
    // initialization.
    return TSDB_CODE_SUCCESS;
}

// Scalar function main computation function.
// @param inputDataBlock, input data block composed of multiple columns with each column defined by SUdfColumn
// @param resultColumn, output column
// @return error number defined in taoserror.h
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn* resultColumn) {
    // read data from inputDataBlock and process, then output to resultColumn.
    return TSDB_CODE_SUCCESS;
}

// Cleanup function.
// If no cleanup related processing, we can skip definition of it.
// The destroy function shall be concatenation of the udf name and _destroy suffix.
// @return error number defined in taoserror.h
int32_t scalarfn_destroy() {
    // clean up
    return TSDB_CODE_SUCCESS;
}
```
### 聚合函数模板

用C语言开发聚合函数的模板如下。
```c
#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

// Initialization function.
// If no initialization, we can skip definition of it. 
// The initialization function shall be concatenation of the udf name and _init suffix.
// @return error number defined in taoserror.h
int32_t aggfn_init() {
    // initialization.
    return TSDB_CODE_SUCCESS;
}

// Aggregate start function.
// The intermediate value or the state(@interBuf) is initialized in this function. 
// The function name shall be concatenation of udf name and _start suffix.
// @param interbuf intermediate value to initialize
// @return error number defined in taoserror.h
int32_t aggfn_start(SUdfInterBuf* interBuf) {
    // initialize intermediate value in interBuf
    return TSDB_CODE_SUCCESS;
}

// Aggregate reduce function.
// This function aggregate old state(@interbuf) and one data bock(inputBlock) and output a new state(@newInterBuf).
// @param inputBlock input data block
// @param interBuf old state
// @param newInterBuf new state
// @return error number defined in taoserror.h
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
    // read from inputBlock and interBuf and output to newInterBuf
    return TSDB_CODE_SUCCESS;
}

// Aggregate function finish function.
// This function transforms the intermediate value(@interBuf) into the final output(@result).
// The function name must be concatenation of aggfn and _finish suffix.
// @interBuf : intermediate value
// @result: final result
// @return error number defined in taoserror.h
int32_t int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result) {
    // read data from inputDataBlock and process, then output to result
    return TSDB_CODE_SUCCESS;
}

// Cleanup function.
// If no cleanup related processing, we can skip definition of it. 
// The destroy function shall be concatenation of the udf name and _destroy suffix.
// @return error number defined in taoserror.h
int32_t aggfn_destroy() {
    // clean up
    return TSDB_CODE_SUCCESS;
}
```

### 编译

在 TDengine 中，为了实现 UDF，需要编写 C 语言源代码，并按照 TDengine 的规范编译为动态链接库文件。
按照前面描述的规则，准备 UDF 的源代码 bit_and.c。以 Linux 操作系统为例，执行如下指令，编译得到动态链接库文件。
```shell
gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
```

为了保证可靠运行，推荐使用 7.5 及以上版本的 GCC。

### C UDF 数据结构
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
      int32_t  varOffsetsLen;
      int32_t *varOffsets;
      int32_t  payloadLen;
      char    *payload;
      int32_t  payloadAllocLen;
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
  char   *buf;
  int8_t  numOfResult; //zero or one
} SUdfInterBuf;
```
数据结构说明如下：

- SUdfDataBlock 数据块包含行数 numOfRows 和列数 numCols。udfCols[i] (0 \<= i \<= numCols-1)表示每一列数据，类型为SUdfColumn*。
- SUdfColumn 包含列的数据类型定义 colMeta 和列的数据 colData。
- SUdfColumnMeta 成员定义同 taos.h 数据类型定义。
- SUdfColumnData 数据可以变长，varLenCol 定义变长数据，fixLenCol 定义定长数据。 
- SUdfInterBuf 定义中间结构 buffer，以及 buffer 中结果个数 numOfResult

为了更好的操作以上数据结构，提供了一些便利函数，定义在 taosudf.h。


### C UDF 示例代码

#### 标量函数示例 [bit_and](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/bit_and.c)

bit_add 实现多列的按位与功能。如果只有一列，返回这一列。bit_add 忽略空值。

<details>
<summary>bit_and.c</summary>

```c
{{#include tests/script/sh/bit_and.c}}
```

</details>

#### 聚合函数示例1 返回值为数值类型 [l2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/l2norm.c)

l2norm 实现了输入列的所有数据的二阶范数，即对每个数据先平方，再累加求和，最后开方。

<details>
<summary>l2norm.c</summary>

```c
{{#include tests/script/sh/l2norm.c}}
```

</details>

#### 聚合函数示例2 返回值为字符串类型 [max_vol](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/max_vol.c)

max_vol 实现了从多个输入的电压列中找到最大电压，返回由设备 ID + 最大电压所在（行，列）+ 最大电压值 组成的组合字符串值

创建表：
```bash
create table battery(ts timestamp, vol1 float, vol2 float, vol3 float, deviceId varchar(16));
```
创建自定义函数：
```bash
create aggregate function max_vol as '/root/udf/libmaxvol.so' outputtype binary(64) bufsize 10240 language 'C'; 
```
使用自定义函数：
```bash
select max_vol(vol1, vol2, vol3, deviceid) from battery;
```

<details>
<summary>max_vol.c</summary>

```c
{{#include tests/script/sh/max_vol.c}}
```

</details>

## 用 Python 语言开发 UDF

### 准备环境
  
准备环境的具体步骤如下：
- 第1步，准备好 Python 运行环境。
- 第2步，安装 Python 包 taospyudf。命令如下。
    ```shell
    pip3 install taospyudf
    ```
- 第3步，执行命令 ldconfig。
- 第4步，启动 taosd 服务。

安装过程中会编译 C++ 源码，因此系统上要有 cmake 和 gcc。编译生成的 libtaospyudf.so 文件自动会被复制到 /usr/local/lib/ 目录，因此如果是非 root 用户，安装时需加 sudo。安装完可以检查这个目录是否有了这个文件:

```shell
root@slave11 ~/udf $ ls -l /usr/local/lib/libtaos*
-rw-r--r-- 1 root root 671344 May 24 22:54 /usr/local/lib/libtaospyudf.so
```

### 接口定义

当使用 Python 语言开发 UDF 时，需要实现规定的接口函数。具体要求如下。
- 标量函数需要实现标量接口函数 process。
- 聚合函数需要实现聚合接口函数 start、reduce、finish。
- 如果需要初始化，则应实现函数 init。
- 如果需要清理工作，则实现函数 destroy。

#### 标量函数接口

标量函数的接口如下。
```Python
def process(input: datablock) -> tuple[output_type]:
```

主要参数说明如下：
- input:datablock 类似二维矩阵，通过成员方法 data(row, col) 读取位于 row 行、col 列的 python 对象
- 返回值是一个 Python 对象元组，每个元素类型为输出类型。

#### 聚合函数接口

聚合函数的接口如下。
```Python
def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
```

上述代码定义了 3 个函数，分别用于实现一个自定义的聚合函数。具体过程如下。

首先，调用 start 函数生成最初的结果缓冲区。这个结果缓冲区用于存储聚合函数的内部状态，随着输入数据的处理而不断更新。

然后，输入数据会被分为多个行数据块。对于每个行数据块，调用 reduce 函数，并将当前行数据块（inputs）和当前的中间结果（buf）作为参数传递。reduce 函数会根据输入数据和当前状态来更新聚合函数的内部状态，并返回新的中间结果。

最后，当所有行数据块都处理完毕后，调用 finish 函数。这个函数接收最终的中间结果（buf）作为参数，并从中生成最终的输出。由于聚合函数的特性，最终输出只能包含 0 条或 1 条数据。这个输出结果将作为聚合函数的计算结果返回给调用者。

#### 初始化和销毁接口

初始化和销毁的接口如下。
```Python
def init()
def destroy()
```

参数说明：
- init 完成初始化工作
- destroy 完成清理工作

**注意** 用 Python 开发 UDF 时必须定义 init 函数和 destroy 函数

### 标量函数模板

用Python语言开发标量函数的模板如下。
```Python
def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:  
```
### 聚合函数模板

用Python语言开发聚合函数的模板如下。
```Python
def init():
    #initialization
def destroy():
    #destroy
def start() -> bytes:
    #return serialize(init_state)
def reduce(inputs: datablock, buf: bytes) -> bytes
    # deserialize buf to state
    # reduce the inputs and state into new_state. 
    # use inputs.data(i, j) to access python object of location(i, j)
    # serialize new_state into new_state_bytes
    return new_state_bytes   
def finish(buf: bytes) -> output_type:
    #return obj of type outputtype   
```

### 数据类型映射

下表描述了TDengine SQL 数据类型和 Python 数据类型的映射。任何类型的 NULL 值都映射成 Python 的 None 值。

|  **TDengine SQL数据类型**   | **Python数据类型** |
| :-----------------------: | ------------ |
| TINYINT / SMALLINT / INT / BIGINT | int |
| TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | int |
| FLOAT / DOUBLE | float |
| BOOL | bool |
| BINARY / VARCHAR / NCHAR | bytes|
| TIMESTAMP | int |
| JSON and other types | 不支持 |

### 开发示例

本文内容由浅入深包括 5 个示例程序，同时也包含大量实用的 debug 技巧。

注意：**UDF 内无法通过 print 函数输出日志，需要自己写文件或用 python 内置的 logging 库写文件**。

#### 示例一

编写一个只接收一个整数的 UDF 函数： 输入 n， 输出 ln(n^2 + 1)。
首先编写一个 Python 文件，存在系统某个目录，比如 /root/udf/myfun.py 内容如下。

```python
from math import log

def init():
    pass

def destroy():
    pass

def process(block):
    rows, _ = block.shape()
    return [log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
```

这个文件包含 3 个函数， init 和 destroy 都是空函数，它们是 UDF 的生命周期函数，即使什么都不做也要定义。最关键的是 process 函数， 它接受一个数据块，这个数据块对象有两个方法。
1. shape() 返回数据块的行数和列数
2. data(i, j) 返回 i 行 j 列的数据

标量函数的 process 方法传入的数据块有多少行，就需要返回多少行数据。上述代码忽略列数，因为只需对每行的第一列做计算。

接下来创建对应的 UDF 函数，在 TDengine CLI 中执行下面语句。

```sql
create function myfun as '/root/udf/myfun.py' outputtype double language 'Python'
```
其输出如下。

```shell
taos> create function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
Create OK, 0 row(s) affected (0.005202s)
```

看起来很顺利，接下来查看系统中所有的自定义函数，确认创建成功。

```text
taos> show functions;
              name              |
=================================
 myfun                          |
Query OK, 1 row(s) in set (0.005767s)
```

生成测试数据，可以在 TDengine CLI 中执行下述命令。

```sql
create database test;
create table t(ts timestamp, v1 int, v2 int, v3 int);
insert into t values('2023-05-01 12:13:14', 1, 2, 3);
insert into t values('2023-05-03 08:09:10', 2, 3, 4);
insert into t values('2023-05-10 07:06:05', 3, 4, 5);
```

测试 myfun 函数。

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.011088s)
```

不幸的是执行失败了，什么原因呢？查看 udfd 进程的日志。

```shell
tail -10 /var/log/taos/udfd.log
```

发现以下错误信息。

```text
05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
```

错误很明确：没有加载到 Python 插件 libtaospyudf.so，如果遇到此错误，请参考前面的准备环境一节。

修复环境错误后再次执行，如下。

```sql
taos> select myfun(v1) from t;
         myfun(v1)         |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

至此，我们完成了第一个 UDF 😊，并学会了简单的 debug 方法。

#### 示例二

上面的 myfun 虽然测试测试通过了，但是有两个缺点。

1. 这个标量函数只接受 1 列数据作为输入，如果用户传入了多列也不会抛异常。

```sql
taos> select myfun(v1, v2) from t;
       myfun(v1, v2)       |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

2. 没有处理 null 值。我们期望如果输入有 null，则会抛异常终止执行。因此 process 函数改进如下。

```python
def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception(f"require 1 parameter but given {cols}")
    return [ None if block.data(i, 0) is None else log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
```

执行如下语句更新已有的 UDF。

```sql
create or replace function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
```

再传入 myfun 两个参数，就会执行失败了。

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.014643s)
```

自定义的异常信息打印在插件的日志文件 /var/log/taos/taospyudf.log 中。

```text
2023-05-24 23:21:06.790 ERROR [1666188] [doPyUdfScalarProc@507] call pyUdfScalar proc function. context 0x7faade26d180. error: Exception: require 1 parameter but given 2

At:
  /var/lib/taos//.udf/myfun_3_1884e1281d9.py(12): process

```

至此，我们学会了如何更新 UDF，并查看 UDF 输出的错误日志。
（注：如果 UDF 更新后未生效，在 TDengine 3.0.5.0 以前（不含）的版本中需要重启 taosd，在 3.0.5.0 及之后的版本中不需要重启 taosd 即可生效。）

#### 示例三

输入（x1, x2, ..., xn）, 输出每个值和它们的序号的乘积的和：1 *  x1 + 2 * x2 + ... + n * xn。如果 x1 至 xn 中包含 null，则结果为 null。

本例与示例一的区别是，可以接受任意多列作为输入，且要处理每一列的值。编写 UDF 文件 /root/udf/nsum.py。

```python
def init():
    pass

def destroy():
    pass

def process(block):
    rows, cols = block.shape()
    result = []
    for i in range(rows):
        total = 0
        for j in range(cols):
            v = block.data(i, j)
            if v is None:
                total = None
                break
            total += (j + 1) * block.data(i, j)
        result.append(total)
    return result
```

创建 UDF。

```sql
create function nsum as '/root/udf/nsum.py' outputtype double language 'Python';
```

测试 UDF。

```sql
taos> insert into t values('2023-05-25 09:09:15', 6, null, 8);
Insert OK, 1 row(s) affected (0.003675s)

taos> select ts, v1, v2, v3,  nsum(v1, v2, v3) from t;
           ts            |     v1      |     v2      |     v3      |     nsum(v1, v2, v3)      |
================================================================================================
 2023-05-01 12:13:14.000 |           1 |           2 |           3 |              14.000000000 |
 2023-05-03 08:09:10.000 |           2 |           3 |           4 |              20.000000000 |
 2023-05-10 07:06:05.000 |           3 |           4 |           5 |              26.000000000 |
 2023-05-25 09:09:15.000 |           6 |        NULL |           8 |                      NULL |
Query OK, 4 row(s) in set (0.010653s)
```

#### 示例四

编写一个 UDF，输入一个时间戳，输出距离这个时间最近的下一个周日。比如今天是 2023-05-25， 则下一个周日是 2023-05-28。
完成这个函数要用到第三方库 momen。先安装这个库。

```shell
pip3 install moment
```

然后编写 UDF 文件 /root/udf/nextsunday.py。

```python
import moment

def init():
    pass

def destroy():
    pass


def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception("require only 1 parameter")
    if not type(block.data(0, 0)) is int:
        raise Exception("type error")
    return [moment.unix(block.data(i, 0)).replace(weekday=7).format('YYYY-MM-DD')
            for i in range(rows)]
```

UDF 框架会将 TDengine 的 timestamp 类型映射为 Python 的 int 类型，所以这个函数只接受一个表示毫秒数的整数。process 方法先做参数检查，然后用 moment 包替换时间的星期为星期日，最后格式化输出。输出的字符串长度是固定的 10 个字符长，因此可以这样创建 UDF 函数。

```sql
create function nextsunday as '/root/udf/nextsunday.py' outputtype binary(10) language 'Python';
```

此时测试函数，如果你是用 systemctl 启动的 taosd，肯定会遇到错误。

```sql
taos> select ts, nextsunday(ts) from t;

DB error: udf function execution failure (1.123615s)
```

```shell
tail -20 taospyudf.log  
2023-05-25 11:42:34.541 ERROR [1679419] [PyUdf::PyUdf@217] py udf load module failure. error ModuleNotFoundError: No module named 'moment'
```

这是因为 “moment” 所在位置不在 python udf 插件默认的库搜索路径中。怎么确认这一点呢？通过以下命令搜索 taospyudf.log。

```shell
grep 'sys path' taospyudf.log  | tail -1
```

输出如下

```text
2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
```

发现 python udf 插件默认搜索的第三方库安装路径是： /lib/python3/dist-packages，而 moment 默认安装到了 /usr/local/lib/python3.8/dist-packages。下面我们修改 python udf 插件默认的库搜索路径。
先打开 python3 命令行，查看当前的 sys.path。

```python
>>> import sys
>>> ":".join(sys.path)
'/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages'
```

复制上面脚本的输出的字符串，然后编辑 /var/taos/taos.cfg 加入以下配置。

```shell
UdfdLdLibPath /usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages
```

保存后执行 systemctl restart taosd, 再测试就不报错了。

```sql
taos> select ts, nextsunday(ts) from t;
           ts            | nextsunday(ts) |
===========================================
 2023-05-01 12:13:14.000 | 2023-05-07     |
 2023-05-03 08:09:10.000 | 2023-05-07     |
 2023-05-10 07:06:05.000 | 2023-05-14     |
 2023-05-25 09:09:15.000 | 2023-05-28     |
Query OK, 4 row(s) in set (1.011474s)
```

#### 示例五

编写一个聚合函数，计算某一列最大值和最小值的差。
聚合函数与标量函数的区别是：标量函数是多行输入对应多个输出，聚合函数是多行输入对应一个输出。聚合函数的执行过程有点像经典的 map-reduce 框架的执行过程，框架把数据分成若干块，每个 mapper 处理一个块，reducer 再把 mapper 的结果做聚合。不一样的地方在于，对于 TDengine Python UDF 中的 reduce 函数既有 map 的功能又有 reduce 的功能。reduce 函数接受两个参数：一个是自己要处理的数据，一个是别的任务执行 reduce 函数的处理结果。如下面的示例 /root/udf/myspread.py。

```python
import io
import math
import pickle

LOG_FILE: io.TextIOBase = None

def init():
    global LOG_FILE
    LOG_FILE = open("/var/log/taos/spread.log", "wt")
    log("init function myspead success")

def log(o):
    LOG_FILE.write(str(o) + '\n')

def destroy():
    log("close log file: spread.log")
    LOG_FILE.close()

def start():
    return pickle.dumps((-math.inf, math.inf))

def reduce(block, buf):
    max_number, min_number = pickle.loads(buf)
    log(f"initial max_number={max_number}, min_number={min_number}")
    rows, _ = block.shape()
    for i in range(rows):
        v = block.data(i, 0)
        if v > max_number:
            log(f"max_number={v}")
            max_number = v
        if v < min_number:
            log(f"min_number={v}")
            min_number = v
    return pickle.dumps((max_number, min_number))

def finish(buf):
    max_number, min_number = pickle.loads(buf)
    return max_number - min_number
```

在这个示例中，我们不但定义了一个聚合函数，还增加了记录执行日志的功能。
1. init 函数打开一个文件用于记录日志
2. log 函数记录日志，自动将传入的对象转成字符串，加换行符输出
3. destroy 函数在执行结束后关闭日志文件
4. start 函数返回初始的 buffer，用来存聚合函数的中间结果，把最大值初始化为负无穷大，最小值初始化为正无穷大
5. reduce 函数处理每个数据块并聚合结果
6. finish 函数将 buffer 转换成最终的输出 

执行下面 SQL 语句创建对应的 UDF。

```sql
create or replace aggregate function myspread as '/root/udf/myspread.py' outputtype double bufsize 128 language 'Python';
```

这个 SQL 语句与创建标量函数的 SQL 语句有两个重要区别。
1. 增加了 aggregate 关键字
2. 增加了 bufsize 关键字，用来指定存储中间结果的内存大小，这个数值可以大于实际使用的数值。本例中间结果是两个浮点数组成的 tuple，序列化后实际占用大小只有 32 个字节，但指定的 bufsize 是128，可以用 python 命令行打印实际占用的字节数

```python
>>> len(pickle.dumps((12345.6789, 23456789.9877)))
32
```

测试这个函数，可以看到 myspread 的输出结果和内置的 spread 函数的输出结果是一致的。

```sql
taos> select myspread(v1) from t;
       myspread(v1)        |
============================
               5.000000000 |
Query OK, 1 row(s) in set (0.013486s)

taos> select spread(v1) from t;
        spread(v1)         |
============================
               5.000000000 |
Query OK, 1 row(s) in set (0.005501s)
```

最后，查看执行日志，可以看到 reduce 函数被执行了 3 次，执行过程中 max 值被更新了 4 次，min 值只被更新 1 次。

```shell
root@slave11 /var/log/taos $ cat spread.log
init function myspead success
initial max_number=-inf, min_number=inf
max_number=1
min_number=1
initial max_number=1, min_number=1
max_number=2
max_number=3
initial max_number=3, min_number=1
max_number=6
close log file: spread.log
```

通过这个示例，我们学会了如何定义聚合函数，并打印自定义的日志信息。

### 更多 Python UDF 示例代码
#### 标量函数示例 [pybitand](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pybitand.py)

pybitand 实现多列的按位与功能。如果只有一列，返回这一列。pybitand 忽略空值。

<details>
<summary>pybitand.py</summary>

```Python
{{#include tests/script/sh/pybitand.py}}
```

</details>

#### 聚合函数示例 [pyl2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pyl2norm.py)

pyl2norm 实现了输入列的所有数据的二阶范数，即对每个数据先平方，再累加求和，最后开方。

<details>
<summary>pyl2norm.py</summary>

```c
{{#include tests/script/sh/pyl2norm.py}}
```

</details>

#### 聚合函数示例 [pycumsum](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pycumsum.py)

pycumsum 使用 numpy 计算输入列所有数据的累积和。
<details>
<summary>pycumsum.py</summary>

```c
{{#include tests/script/sh/pycumsum.py}}
```

</details>

## 管理 UDF 

在集群中管理 UDF 的过程涉及创建、使用和维护这些函数。用户可以通过 SQL 在集群中创建和管理 UDF，一旦创建成功，集群的所有用户都可以在 SQL 中使用这些函数。由于 UDF 存储在集群的 mnode 上，因此即使重启集群，已经创建的 UDF 也仍然可用。

在创建 UDF 时，需要区分标量函数和聚合函数。标量函数接受零个或多个输入参数，并返回一个单一的值。聚合函数接受一组输入值，并通过对这些值进行某种计算（如求和、计数等）来返回一个单一的值。如果创建时声明了错误的函数类别，则通过 SQL 调用函数时会报错。

此外，用户需要确保输入数据类型与 UDF 程序匹配，UDF 输出的数据类型与 outputtype 匹配。这意味着在创建 UDF 时，需要为输入参数和输出值指定正确的数据类型。这有助于确保在调用 UDF 时，输入数据能够正确地传递给 UDF，并且 UDF 的输出值与预期的数据类型相匹配。

### 创建标量函数

创建标量函数的 SQL 语法如下。
```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```
各参数说明如下。
- or replace：如果函数已经存在，则会修改已有的函数属性。
- function_name：标量函数在SQL中被调用时的函数名。
- language：支持 C 语言和 Python 语言（3.7 及以上版本），默认为 C。
- library_path：如果编程语言是 C，则路径是包含 UDF 实现的动态链接库的库文件绝对路径，通常指向一个 so 文件。如果编程语言是 Python，则路径是包含 UDF
实现的 Python 文件路径。路径需要用英文单引号或英文双引号括起来。
- output_type：函数计算结果的数据类型名称。

### 创建聚合函数

创建聚合函数的 SQL 语法如下。

```sql
CREATE [OR REPLACE] AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type BUFSIZE buffer_size LANGUAGE 'Python';
```

其中，buffer_size 表示中间计算结果的缓冲区大小，单位是字节。其他参数的含义与标量函数相同。

如下 SQL 创建一个名为 l2norm 的 UDF。
```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
```

### 删除 UDF

删除指定名称的 UDF 的 SQL 语法如下。
```sql
DROP FUNCTION function_name;
```

### 查看 UDF

显示集群中当前可用的所有 UDF 的 SQL 如下。
```sql
show functions;
```

### 查看函数信息
  
同名的 UDF 每更新一次，版本号会增加 1。   
```sql
select * from ins_functions \G;     
```

