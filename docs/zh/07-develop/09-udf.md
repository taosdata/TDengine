---
sidebar_label: 用户定义函数
title: UDF（用户定义函数）
description: "支持用户编码的聚合函数和标量函数，在查询中嵌入并使用用户定义函数，拓展查询的能力和功能。"
---

在有些应用场景中，应用逻辑需要的查询无法直接使用系统内置的函数来表示。利用 UDF(User Defined Function) 功能，TDengine 可以插入用户编写的处理代码并在查询中使用它们，就能够很方便地解决特殊应用场景中的使用需求。 UDF 通常以数据表中的一列数据做为输入，同时支持以嵌套子查询的结果作为输入。

用户可以通过 UDF 实现两类函数：标量函数和聚合函数。标量函数对每行数据输出一个值，如求绝对值 abs，正弦函数 sin，字符串拼接函数 concat 等。聚合函数对多行数据进行输出一个值，如求平均数 avg，最大值 max 等。

TDengine 支持通过 C/Python 语言进行 UDF 定义。接下来结合示例讲解 UDF 的使用方法。

## 用 C 语言实现 UDF

使用 C 语言实现 UDF 时，需要实现规定的接口函数
- 标量函数需要实现标量接口函数 scalarfn 。
- 聚合函数需要实现聚合接口函数 aggfn_start ， aggfn ， aggfn_finish。
- 如果需要初始化，实现 udf_init；如果需要清理工作，实现udf_destroy。

接口函数的名称是 UDF 名称，或者是 UDF 名称和特定后缀（_start, _finish, _init, _destroy)的连接。列表中的scalarfn，aggfn, udf需要替换成udf函数名。

### 用 C 语言实现标量函数
标量函数实现模板如下
```c
#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

// initialization function. if no initialization, we can skip definition of it. The initialization function shall be concatenation of the udf name and _init suffix
// @return error number defined in taoserror.h
int32_t scalarfn_init() {
    // initialization.
    return TSDB_CODE_SUCCESS;
}

// scalar function main computation function
// @param inputDataBlock, input data block composed of multiple columns with each column defined by SUdfColumn
// @param resultColumn, output column
// @return error number defined in taoserror.h
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn* resultColumn) {
    // read data from inputDataBlock and process, then output to resultColumn.
    return TSDB_CODE_SUCCESS;
}

// cleanup function. if no cleanup related processing, we can skip definition of it. The destroy function shall be concatenation of the udf name and _destroy suffix.
// @return error number defined in taoserror.h
int32_t scalarfn_destroy() {
    // clean up
    return TSDB_CODE_SUCCESS;
}
```
scalarfn 为函数名的占位符，需要替换成函数名，如bit_and。

### 用 C 语言实现聚合函数

聚合函数的实现模板如下
```c
#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

// Initialization function. if no initialization, we can skip definition of it. The initialization function shall be concatenation of the udf name and _init suffix
// @return error number defined in taoserror.h
int32_t aggfn_init() {
    // initialization.
    return TSDB_CODE_SUCCESS;
}

// aggregate start function. The intermediate value or the state(@interBuf) is initialized in this function. The function name shall be concatenation of udf name and _start suffix
// @param interbuf intermediate value to initialize
// @return error number defined in taoserror.h
int32_t aggfn_start(SUdfInterBuf* interBuf) {
    // initialize intermediate value in interBuf
    return TSDB_CODE_SUCCESS;
}

// aggregate reduce function. This function aggregate old state(@interbuf) and one data bock(inputBlock) and output a new state(@newInterBuf).
// @param inputBlock input data block
// @param interBuf old state
// @param newInterBuf new state
// @return error number defined in taoserror.h
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
    // read from inputBlock and interBuf and output to newInterBuf
    return TSDB_CODE_SUCCESS;
}

// aggregate function finish function. This function transforms the intermediate value(@interBuf) into the final output(@result). The function name must be concatenation of aggfn and _finish suffix.
// @interBuf : intermediate value
// @result: final result
// @return error number defined in taoserror.h
int32_t int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result) {
    // read data from inputDataBlock and process, then output to result
    return TSDB_CODE_SUCCESS;
}

// cleanup function. if no cleanup related processing, we can skip definition of it. The destroy function shall be concatenation of the udf name and _destroy suffix.
// @return error number defined in taoserror.h
int32_t aggfn_destroy() {
    // clean up
    return TSDB_CODE_SUCCESS;
}
```
aggfn为函数名的占位符，需要修改为自己的函数名，如l2norm。

### C 语言 UDF 接口函数定义

接口函数的名称是 udf 名称，或者是 udf 名称和特定后缀（_start, _finish, _init, _destroy)的连接。以下描述中函数名称中的 scalarfn，aggfn, udf 需要替换成udf函数名。

接口函数返回值表示是否成功。如果返回值是 TSDB_CODE_SUCCESS，表示操作成功，否则返回的是错误代码。错误代码定义在 taoserror.h，和 taos.h 中的API共享错误码的定义。例如， TSDB_CODE_UDF_INVALID_INPUT 表示输入无效输入。TSDB_CODE_OUT_OF_MEMORY 表示内存不足。

接口函数参数类型见数据结构定义。

#### 标量函数接口

 `int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn)` 
 
 其中 scalarFn 是函数名的占位符。这个函数对数据块进行标量计算，通过设置resultColumn结构体中的变量设置值

参数的具体含义是：
  - inputDataBlock: 输入的数据块
  - resultColumn: 输出列 

#### 聚合函数接口

`int32_t aggfn_start(SUdfInterBuf *interBuf)`

`int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf)`

`int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result)`

其中 aggfn 是函数名的占位符。首先调用aggfn_start生成结果buffer，然后相关的数据会被分为多个行数据块，对每个数据块调用 aggfn 用数据块更新中间结果，最后再调用 aggfn_finish 从中间结果产生最终结果，最终结果只能含 0 或 1 条结果数据。

参数的具体含义是：
  - interBuf：中间结果 buffer。
  - inputBlock：输入的数据块。
  - newInterBuf：新的中间结果buffer。
  - result：最终结果。


#### 初始化和销毁接口
`int32_t udf_init()`

`int32_t udf_destroy()`

其中 udf 是函数名的占位符。udf_init 完成初始化工作。 udf_destroy 完成清理工作。如果没有初始化工作，无需定义udf_init函数。如果没有清理工作，无需定义udf_destroy函数。


### C 语言 UDF 数据结构
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
数据结构说明如下：

- SUdfDataBlock 数据块包含行数 numOfRows 和列数 numCols。udfCols[i] (0 <= i <= numCols-1)表示每一列数据，类型为SUdfColumn*。
- SUdfColumn 包含列的数据类型定义 colMeta 和列的数据 colData。
- SUdfColumnMeta 成员定义同 taos.h 数据类型定义。
- SUdfColumnData 数据可以变长，varLenCol 定义变长数据，fixLenCol 定义定长数据。 
- SUdfInterBuf 定义中间结构 buffer，以及 buffer 中结果个数 numOfResult

为了更好的操作以上数据结构，提供了一些便利函数，定义在 taosudf.h。

### 编译 C UDF

用户定义函数的 C 语言源代码无法直接被 TDengine 系统使用，而是需要先编译为 动态链接库，之后才能载入 TDengine 系统。

例如，按照上一章节描述的规则准备好了用户定义函数的源代码 bit_and.c，以 Linux 为例可以执行如下指令编译得到动态链接库文件：

```bash
gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
```

这样就准备好了动态链接库 libbitand.so 文件，可以供后文创建 UDF 时使用了。为了保证可靠的系统运行，编译器 GCC 推荐使用 7.5 及以上版本。

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

max_vol 实现了从多个输入的电压列中找到最大电压，返回由设备ID + 最大电压所在（行，列）+ 最大电压值 组成的组合字符串值

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
select max_vol(vol1,vol2,vol3,deviceid) from battery;
```

<details>
<summary>max_vol.c</summary>

```c
{{#include tests/script/sh/max_vol.c}}
```

</details>

## 用 Python 语言实现 UDF

使用 Python 语言实现 UDF 时，需要实现规定的接口函数
- 标量函数需要实现标量接口函数 process 。
- 聚合函数需要实现聚合接口函数 start ，reduce ，finish。
- 如果需要初始化，实现 init；如果需要清理工作，实现 destroy。

### 用 Python 实现标量函数

标量函数实现模版如下
```Python
def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:
    # process input datablock, 
    # datablock.data(row, col) is to access the python object in location(row,col)
    # return tuple object consisted of object of type outputtype   
```

### 用 Python 实现聚合函数

聚合函数实现模版如下
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
    # use inputs.data(i,j) to access python object of location(i,j)
    # serialize new_state into new_state_bytes
    return new_state_bytes   
def finish(buf: bytes) -> output_type:
    #return obj of type outputtype   
```

### Python UDF 接口函数定义

#### 标量函数接口
```Python
def process(input: datablock) -> tuple[output_type]:
```
- input:datablock 类似二维矩阵，通过成员方法 data(row,col)返回位于 row 行，col 列的 python 对象
- 返回值是一个 Python 对象元组，每个元素类型为输出类型。

#### 聚合函数接口
```Python
def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
```

首先调用 start 生成最初结果 buffer，然后输入数据会被分为多个行数据块，对每个数据块 inputs 和当前中间结果 buf 调用 reduce，得到新的中间结果，最后再调用 finish 从中间结果 buf 产生最终输出，最终输出只能含 0 或 1 条数据。


#### 初始化和销毁接口
```Python
def init()
def destroy()
```

其中 init 完成初始化工作。 destroy 完成清理工作。

### Python 和 TDengine之间的数据类型映射

下表描述了TDengine SQL数据类型和Python数据类型的映射。任何类型的NULL值都映射成Python的None值。

|  **TDengine SQL数据类型**   | **Python数据类型** |
| :-----------------------: | ------------ |
|TINYINT / SMALLINT / INT  / BIGINT     | int   |
|TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | int |
|FLOAT / DOUBLE | float |
|BOOL | bool |
|BINARY / VARCHAR / NCHAR | bytes|
|TIMESTAMP | int |
|JSON and other types | 不支持 |

### Python UDF 环境的安装
1. 安装 taospyudf 包。此包执行Python UDF程序。
```bash
sudo pip install taospyudf
ldconfig
```
2. 如果 Python UDF 程序执行时，通过 PYTHONPATH 引用其它的包，可以设置 taos.cfg 的 UdfdLdLibPath 变量为PYTHONPATH的内容
 
### Python UDF 示例代码
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

### 聚合函数示例 [pycumsum](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pycumsum.py)

pycumsum 使用 numpy 计算输入列所有数据的累积和。
<details>
<summary>pycumsum.py</summary>

```c
{{#include tests/script/sh/pycumsum.py}}
```

</details>
## 管理和使用 UDF
在使用 UDF 之前需要先将其加入到 TDengine 系统中。关于如何管理和使用 UDF，请参考[管理和使用 UDF](../12-taos-sql/26-udf.md)

