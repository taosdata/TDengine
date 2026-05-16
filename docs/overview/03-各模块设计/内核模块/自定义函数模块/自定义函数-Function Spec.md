# 自定义函数-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-14 | 2025-01-14 | 1.0 | 任新胜 | 新建 |
| 2026-01-14 | 2026-01-14 | 1.1 | 廖浩均 | 重构文档 |

## 2. 背景

TDengine 提供了丰富的内置函数供用户选择使用，但是，在某些特殊的应用场景中，用户依然会想使用一些特殊的和业务关系更紧密的函数来实现特定的功能。允许编写用户自定义函数（UDF），以便解决特殊应用场景中的使用需求，是 TDengine 需要具备的一个能力。

## 3. 定义

1. **标量函数: **标量函数是一种将输入数据转换为输出数据的函数，通常用于对单个数据值进行计算和转换。
2. **聚合函数: **聚合函数是一种特殊的函数，用于对数据进行分组和计算，从而生成汇总信息。
3. **结构化查询语言（Structured Query Language，SQL）：**是一种用于管理和操作关系型数据库的标准编程语言。 它允许用户存储、更新、删除、搜索和检索数据库中的数据。 SQL 被广泛应用于各种数据中心应用程序中，是 ISO 和 ANSI 等标准化机构认可的国际标准。

## 4. 行为说明

### 4.1 用 C 语言开发 UDF

#### 4.1.1 使用流程

1. 首先准备 UDF 函数需要的实现文件，例如 my_function.c，具体方法 4.2 — 4.4 具体说明
2. 编译得到动态链接库文件，例如 my_function.so
3. 使用动态链接库文件创建自定义函数，例如 my_function
4. 创建成功后即可像内置函数一样访问
以上为 udf 函数使用的基本流程，其中最主要的是第一步，4.2 — 4.4  将详细说明如何准备一个完善的 udf 实现文件，4.5 是具体的实现代码示例。4.6 将介绍如何使用准备好的 udf 实现文件。
**说明：**UDF 函数也分为标量函数和聚合函数两类，分别有不同的接口定义和实现方法，因此以下部分会将两者分开说明。

#### 4.1.2 接口定义

##### 4.1.2.1 汇总说明

使用 C/C++ 语言实现 UDF 时，需要实现规定的接口函数，注意 udf 为实际函数名，各函数中需保持一致
1. 标量函数需要实现标量接口函数 udf 。
2. 聚合函数需要实现聚合接口函数 udf_start、udf、udf_finish。
3. 如果需要初始化，实现 udf_init。
4. 如果需要清理工作，实现 udf_destroy。

##### 4.1.2.2 初始化和销毁接口

初始化和销毁接口是标量函数和聚合函数共同使用的接口，相关 API 如下。
```c
int32_t udf_init()
int32_t udf_destroy()
```

其中，udf_init 函数完成初始化工作，udf_destroy 函数完成清理工作。如果没有初始化工作，无须定义 udf_init 函数；如果没有清理工作，无须定义 udf_destroy 函数。

##### 4.1.2.3 标量函数

标量函数是一种将输入数据转换为输出数据的函数，通常用于对单个数据值进行计算和转换。标量函数的接口函数原型如下。和上文的 udf 一样，scalarfn 需替换为实际函数名。
```c
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn);
```

主要参数说明如下。
1. inputDataBlock：输入的数据块。
2. resultColumn：输出列。

##### 4.1.2.4 聚合函数

聚合函数是一种特殊的函数，用于对数据进行分组和计算，从而生成汇总信息。聚合函数的工作原理如下。
1. 初始化结果缓冲区：首先调用 aggfn_start 函数，生成一个结果缓冲区（result buffer），用于存储中间结果。
2. 分组数据：相关数据会被分为多个行数据块（row data block），每个行数据块包含一组具有相同分组键（grouping key）的数据。
3. 更新中间结果：对于每个数据块，调用 aggfn 函数更新中间结果。aggfn 函数会根据聚合函数的类型（如 sum、avg、count 等）对数据进行相应的计算，并将计算结 果存储在结果缓冲区中。
4. 生成最终结果：在所有数据块的中间结果更新完成后，调用 aggfn_ﬁnish 函数从结果缓冲区中提取最终结果。最终结果只包含 0 条或 1 条数据，具体取决于聚 合函数的类型和输入数据。
聚合函数的接口函数原型如下。
```c
int32_t aggfn_start(SUdfInterBuf *interBuf);
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf);
int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result);
```

和前文 udf 一样，其中 aggfn 是函数名的占位符。首先调用 aggfn_start 生成结果 buffer，然后相关的数据会被分为多个行数据块，对每个数据块调用 aggfn 用数据块更新中间结果，最后再调用 aggfn_finish 从中间结果产生最终结果，最终结果只能含 0 或 1 条结果数据。
主要参数说明如下。
1. interBuf：中间结果缓存区。
2. inputBlock：输入的数据块。
3. newInterBuf：新的中间结果缓冲区。
4. result：最终结果。

#### 4.1.3 函数模板

##### 4.1.3.1 标量函数模板

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

##### 4.1.3.2 聚合函数模板

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

#### 4.1.4 C UDF 实现

##### 4.1.4.1 UDF 数据结构说明

udf 实现，主要是对传入数据的读取，自定义处理后将结果写入输出缓冲区。整个过程是对以下数据结构的读写
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

1. SUdfDataBlock 数据块包含行数 numOfRows 和列数 numCols。udfCols[i] (0 <= i <= numCols-1)表示每一列数据，类型为SUdfColumn*。
2. SUdfColumn 包含列的数据类型定义 colMeta 和列的数据 colData。
3. SUdfColumnMeta 成员定义同 taos.h 数据类型定义。
4. SUdfColumnData 数据可以变长，varLenCol 定义变长数据，fixLenCol 定义定长数据。
5. SUdfInterBuf 定义中间结构 buffer，以及 buffer 中结果个数 numOfResult

##### 4.1.4.2 UDF 实现便利函数

为了更好的操作以上数据结构，提供一些便利函数，定义在 taosudf.h .
```c
static char *udfColDataGetData(const SUdfColumn *pColumn, int32_t row)；
static int32_t udfColDataGetDataLen(const SUdfColumn *pColumn, int32_t row);
static bool udfColDataIsNull(const SUdfColumn *pColumn, int32_t row);
static int32_t udfColEnsureCapacity(SUdfColumn *pColumn, int32_t newCapacity);
static int32_t udfColDataSetNull(SUdfColumn *pColumn, int32_t row);
static int32_t udfColDataSet(SUdfColumn *pColumn, uint32_t currentRow, const char *pData, bool isNull)
```

#### 4.1.5 示例代码

##### 4.1.5.1 标量函数

bit_add 实现多列的按位与功能。如果只有一列，返回这一列。bit_add 忽略空值。以下为 bit_and.c 文件内容。
```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

DLL_EXPORT int32_t bit_and_init() { return 0; }

DLL_EXPORT int32_t bit_and_destroy() { return 0; }

DLL_EXPORT int32_t bit_and(SUdfDataBlock* block, SUdfColumn* resultCol) {
  udfTrace("block:%p, processing begins, rows:%d cols:%d", block, block->numOfRows, block->numOfCols);

  if (block->numOfCols < 2) {
    udfError("block:%p, cols:%d needs to be greater than 2", block, block->numOfCols);
    return TSDB_CODE_UDF_INVALID_INPUT;
  }

  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (col->colMeta.type != TSDB_DATA_TYPE_INT) {
      udfError("block:%p, col:%d type:%d should be int(%d)", block, i, col->colMeta.type, TSDB_DATA_TYPE_INT);
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }

  SUdfColumnData* resultData = &resultCol->colData;

  for (int32_t i = 0; i < block->numOfRows; ++i) {
    if (udfColDataIsNull(block->udfCols[0], i)) {
      udfColDataSetNull(resultCol, i);
      udfTrace("block:%p, row:%d result is null since col:0 is null", block, i);
      continue;
    }

    int32_t result = *(int32_t*)udfColDataGetData(block->udfCols[0], i);
    udfTrace("block:%p, row:%d col:0 data:%d", block, i, result);

    int32_t j = 1;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        udfColDataSetNull(resultCol, i);
        udfTrace("block:%p, row:%d result is null since col:%d is null", block, i, j);
        break;
      }

      char* colData = udfColDataGetData(block->udfCols[j], i);
      result &= *(int32_t*)colData;
      udfTrace("block:%p, row:%d col:%d data:%d", block, i, j, *(int32_t*)colData);
    }

    if (j == block->numOfCols) {
      udfColDataSet(resultCol, i, (char*)&result, false);
      udfTrace("block:%p, row:%d result is %d", block, i, result);
    }
  }

  resultData->numOfRows = block->numOfRows;
  udfTrace("block:%p, processing completed", block);

  return TSDB_CODE_SUCCESS;
}

```

##### 4.1.5.2 聚合函数，返回数值类型 

l2norm 实现了输入列的所有数据的二阶范数，即对每个数据先平方，再累加求和，最后开方。返回值为数值类型。
```c
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

DLL_EXPORT int32_t l2norm_init() { return 0; }

DLL_EXPORT int32_t l2norm_destroy() { return 0; }

DLL_EXPORT int32_t l2norm_start(SUdfInterBuf* buf) {
  int32_t bufLen = sizeof(double);
  if (buf->bufLen < bufLen) {
    udfError("failed to execute udf since input buflen:%d < %d", buf->bufLen, bufLen);
    return TSDB_CODE_UDF_INVALID_BUFSIZE;
  }

  udfTrace("start aggregation, buflen:%d used:%d", buf->bufLen, bufLen);
  *(int64_t*)(buf->buf) = 0;
  buf->bufLen = bufLen;
  buf->numOfResult = 0;
  return 0;
}

DLL_EXPORT int32_t l2norm(SUdfDataBlock* block, SUdfInterBuf* interBuf, SUdfInterBuf* newInterBuf) {
  udfTrace("block:%p, processing begins, cols:%d rows:%d", block, block->numOfCols, block->numOfRows);

  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (col->colMeta.type != TSDB_DATA_TYPE_INT && col->colMeta.type != TSDB_DATA_TYPE_DOUBLE) {
      udfError("block:%p, col:%d type:%d should be int(%d) or double(%d)", block, i, col->colMeta.type,
               TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE);
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }

  double sumSquares = *(double*)interBuf->buf;
  int8_t numNotNull = 0;

  for (int32_t i = 0; i < block->numOfCols; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn* col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        udfTrace("block:%p, col:%d row:%d is null", block, i, j);
        continue;
      }

      switch (col->colMeta.type) {
        case TSDB_DATA_TYPE_INT: {
          char*   cell = udfColDataGetData(col, j);
          int32_t num = *(int32_t*)cell;
          sumSquares += (double)num * num;
          udfTrace("block:%p, col:%d row:%d data:%d", block, i, j, num);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          char*  cell = udfColDataGetData(col, j);
          double num = *(double*)cell;
          sumSquares += num * num;
          udfTrace("block:%p, col:%d row:%d data:%f", block, i, j, num);
          break;
        }
        default:
          break;
      }
      ++numNotNull;
    }
    udfTrace("block:%p, col:%d result is %f", block, i, sumSquares);
  }

  *(double*)(newInterBuf->buf) = sumSquares;
  newInterBuf->bufLen = sizeof(double);
  newInterBuf->numOfResult = 1;

  udfTrace("block:%p, result is %f", block, sumSquares);
  return 0;
}

DLL_EXPORT int32_t l2norm_finish(SUdfInterBuf* buf, SUdfInterBuf* resultData) {
  double sumSquares = *(double*)(buf->buf);
  *(double*)(resultData->buf) = sqrt(sumSquares);
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;

  udfTrace("end aggregation, result is %f", *(double*)(resultData->buf));
  return 0;
}

```

##### 4.1.5.3 聚合函数，返回字符串类型

max_vol 实现了从多个输入的电压列中找到最大电压，返回由设备 ID + 最大电压所在（行，列）+ 最大电压值 组成的组合字符串值
```c
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

#define STR_MAX_LEN 256  // inter buffer length

DLL_EXPORT int32_t max_vol_init() { return 0; }

DLL_EXPORT int32_t max_vol_destroy() { return 0; }

DLL_EXPORT int32_t max_vol_start(SUdfInterBuf *buf) {
  int32_t bufLen = sizeof(float) + STR_MAX_LEN;
  if (buf->bufLen < bufLen) {
    udfError("failed to execute udf since input buflen:%d < %d", buf->bufLen, bufLen);
    return TSDB_CODE_UDF_INVALID_BUFSIZE;
  }

  udfTrace("start aggregation, buflen:%d used:%d", buf->bufLen, bufLen);
  memset(buf->buf, 0, sizeof(float) + STR_MAX_LEN);
  *((float *)buf->buf) = INT32_MIN;
  buf->bufLen = bufLen;
  buf->numOfResult = 0;
  return 0;
}

DLL_EXPORT int32_t max_vol(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
  udfTrace("block:%p, processing begins, cols:%d rows:%d", block, block->numOfCols, block->numOfRows);

  float maxValue = *(float *)interBuf->buf;
  char  strBuff[STR_MAX_LEN] = "inter1buf";

  if (block->numOfCols < 2) {
    udfError("block:%p, cols:%d needs to be greater than 2", block, block->numOfCols);
    return TSDB_CODE_UDF_INVALID_INPUT;
  }

  // check data type
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn *col = block->udfCols[i];
    if (i == block->numOfCols - 1) {
      // last column is device id , must varchar
      if (col->colMeta.type != TSDB_DATA_TYPE_VARCHAR) {
        udfError("block:%p, col:%d type:%d should be varchar(%d)", block, i, col->colMeta.type, TSDB_DATA_TYPE_VARCHAR);
        return TSDB_CODE_UDF_INVALID_INPUT;
      }
    } else {
      if (col->colMeta.type != TSDB_DATA_TYPE_FLOAT) {
        udfError("block:%p, col:%d type:%d should be float(%d)", block, i, col->colMeta.type, TSDB_DATA_TYPE_FLOAT);
        return TSDB_CODE_UDF_INVALID_INPUT;
      }
    }
  }

  // calc max voltage
  SUdfColumn *lastCol = block->udfCols[block->numOfCols - 1];
  for (int32_t i = 0; i < block->numOfCols - 1; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn *col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        udfTrace("block:%p, col:%d row:%d is null", block, i, j);
        continue;
      }

      char *data = udfColDataGetData(col, j);
      float voltage = *(float *)data;

      if (voltage <= maxValue) {
        udfTrace("block:%p, col:%d row:%d data:%f", block, i, j, voltage);
      } else {
        maxValue = voltage;
        char   *valData = udfColDataGetData(lastCol, j);
        int32_t valDataLen = udfColDataGetDataLen(lastCol, j);

        // get device id
        char   *deviceId = valData + sizeof(uint16_t);
        int32_t deviceIdLen = valDataLen < (STR_MAX_LEN - 1) ? valDataLen : (STR_MAX_LEN - 1);

        strncpy(strBuff, deviceId, deviceIdLen);
        snprintf(strBuff + deviceIdLen, STR_MAX_LEN - deviceIdLen, "_(%d,%d)_%f", j, i, maxValue);
        udfTrace("block:%p, col:%d row:%d data:%f, as max_val:%s", block, i, j, voltage, strBuff);
      }
    }
  }

  *(float *)newInterBuf->buf = maxValue;
  strncpy(newInterBuf->buf + sizeof(float), strBuff, STR_MAX_LEN);
  newInterBuf->bufLen = sizeof(float) + strlen(strBuff) + 1;
  newInterBuf->numOfResult = 1;

  udfTrace("block:%p, result is %s", block, strBuff);
  return 0;
}

DLL_EXPORT int32_t max_vol_finish(SUdfInterBuf *buf, SUdfInterBuf *resultData) {
  char *str = buf->buf + sizeof(float);
  // copy to des
  char *des = resultData->buf + sizeof(uint16_t);
  strcpy(des, str);

  // set binary type len
  uint16_t len = strlen(str);
  *((uint16_t *)resultData->buf) = len;

  // set buf len
  resultData->bufLen = len + sizeof(uint16_t);
  // set row count
  resultData->numOfResult = 1;

  udfTrace("end aggregation, result is %s", str);
  return 0;
}

```

### 4.2 用 python 开发 UDF

#### 4.2.1 准备环境

准备环境的具体步骤如下：
第1步，准备好 Python 运行环境。
第2步，安装 Python 包 taospyudf。命令如下。
```shell
pip3 install taospyudf
```

第3步，执行命令 ldconfig。
第4步，启动 taosd 服务。
安装过程中会编译 C++ 源码，因此系统上要有 cmake 和 gcc。编译生成的 libtaospyudf.so 文件自动会被复制到 /usr/local/lib/ 目录，因此如果是非 root 用户，安装时需加 sudo。安装完可以检查这个目录是否有了这个文件:
```shell
root@slave11 ~/udf $ ls -l /usr/local/lib/libtaos*
-rw-r--r-- 1 root root 671344 May 24 22:54 /usr/local/lib/libtaospyudf.so
```

#### 4.2.2 接口定义

##### 4.2.2.1 总体说明

当使用 Python 语言开发 UDF 时，需要实现规定的接口函数。具体要求如下。
1. 标量函数需要实现标量接口函数 process。
2. 聚合函数需要实现聚合接口函数 start、reduce、finish。
3. 如果需要初始化，则应实现函数 init。
4. 如果需要清理工作，则实现函数 destroy。

##### 4.2.2.2 初始化和销毁接口

初始化和销毁的接口如下。
```python
def init()
def destroy()
```

参数说明：
1. init 完成初始化工作
2. destroy 完成清理工作
**注意** 用 Python 开发 UDF 时必须定义 init 函数和 destroy 函数

##### 4.2.2.3 标量函数接口

标量函数的接口如下。
```python
def process(input: datablock) -> tuple[output_type]:
```

主要参数说明如下：
1. input:datablock 类似二维矩阵，通过成员方法 data(row, col) 读取位于 row 行、col 列的 python 对象
2. 返回值是一个 Python 对象元组，每个元素类型为输出类型。

##### 4.2.2.4 聚合函数接口

聚合函数的接口如下。
```python
def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
```

上述代码定义了 3 个函数，分别用于实现一个自定义的聚合函数。具体过程如下。
首先，调用 start 函数生成最初的结果缓冲区。这个结果缓冲区用于存储聚合函数的内部状态，随着输入数据的处理而不断更新。
然后，输入数据会被分为多个行数据块。对于每个行数据块，调用 reduce 函数，并将当前行数据块（inputs）和当前的中间结果（buf）作为参数传递。reduce 函数会根据输入数据和当前状态来更新聚合函数的内部状态，并返回新的中间结果。
最后，当所有行数据块都处理完毕后，调用 finish 函数。这个函数接收最终的中间结果（buf）作为参数，并从中生成最终的输出。由于聚合函数的特性，最终输出只能包含 0 条或 1 条数据。这个输出结果将作为聚合函数的计算结果返回给调用者。

#### 4.2.3 函数模板

##### 4.2.3.1 标量函数模板

用Python语言开发标量函数的模板如下。
```c
def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:  
```

##### 4.2.3.2 聚合函数模板

用Python语言开发聚合函数的模板如下。
```c
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

#### 4.2.4 数据类型映射

下表描述了TDengine SQL 数据类型和 Python 数据类型的映射。任何类型的 NULL 值都映射成 Python 的 None 值。
| TDengine SQL数据类型 | Python数据类型 |
| --- | --- |
| TINYINT / SMALLINT / INT / BIGINT | int |
| TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | int |
| FLOAT / DOUBLE | float |
| BOOL | bool |
| BINARY / VARCHAR / NCHAR | bytes |
| TIMESTAMP | int |
| JSON and other types | 不支持 |

#### 4.2.5 示例代码

##### 4.2.5.1 标量函数示例

pybitand 实现多列的按位与功能。如果只有一列，返回这一列。pybitand 忽略空值
pybitand.py 文件内容如下：
```c
def init():
    pass

def process(block):
    (rows, cols) = block.shape()
    result = []
    for i in range(rows):
        r = 2 ** 32 - 1
        for j in range(cols):
            cell = block.data(i,j)
            if cell is None:
                result.append(None)
                break
            else:
                r = r & cell
        else:
            result.append(r)
    return result

def destroy():
    pass

```

##### 4.2.5.2 聚合函数示例

pyl2norm 实现了输入列的所有数据的二阶范数，即对每个数据先平方，再累加求和，最后开方。
pyl2norm.py 文件如下：
```c
import json
import math

def init():
    pass

def destroy():
    pass

def start():
    return json.dumps(0.0).encode('utf-8')

def finish(buf):
    sum_squares = json.loads(buf)
    result = math.sqrt(sum_squares)
    return result

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    sum_squares = json.loads(buf)
    
    for i in range(rows):
        for j in range(cols):
            cell = datablock.data(i,j)
            if cell is not None:
                sum_squares += cell * cell
    return json.dumps(sum_squares).encode('utf-8') 

```

##### 4.2.5.3 聚合函数示例

pycumsum 使用 numpy 计算输入列所有数据的累积和。pycumsum.py 文件如下
```c
import pickle
import numpy as np

def init():
    pass

def destroy():
    pass

def start():
    return pickle.dumps(0.0)

def finish(buf):
    return pickle.loads(buf)

def reduce(datablock, buf):
    (rows, cols) = datablock.shape()
    state = pickle.loads(buf)
    row = []
    for i in range(rows):
        for j in range(cols):
            cell = datablock.data(i, j)
            if cell is not None:
                row.append(datablock.data(i, j))
    if len(row) > 1:
        new_state = np.cumsum(row)[-1]
    else:
        new_state = state
    return pickle.dumps(new_state)

```

### 4.3 UDF 管理与使用

#### 4.3.1 基本说明

在集群中管理 UDF 的过程涉及创建、使用和维护这些函数。用户可以通过 SQL 在集群中创建和管理 UDF，一旦创建成功，集群的所有用户都可以在 SQL 中使用这些函数。由于 UDF 存储在集群的 mnode 上，因此即使重启集群，已经创建的 UDF 也仍然可用。
在创建 UDF 时，需要区分标量函数和聚合函数。标量函数接受零个或多个输入参数，并返回一个单一的值。聚合函数接受一组输入值，并通过对这些值进行某种计算（如求和、计数等）来返回一个单一的值。如果创建时声明了错误的函数类别，则通过 SQL 调用函数时会报错。
此外，用户需要确保输入数据类型与 UDF 程序匹配，UDF 输出的数据类型与 outputtype 匹配。这意味着在创建 UDF 时，需要为输入参数和输出值指定正确的数据类型。这有助于确保在调用 UDF 时，输入数据能够正确地传递给 UDF，并且 UDF 的输出值与预期的数据类型相匹配。

#### 4.3.2 创建标量函数

创建标量函数的 SQL 语法如下。
```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```

各参数说明如下。
1. or replace：如果函数已经存在，则会修改已有的函数属性。
2. function_name：标量函数在SQL中被调用时的函数名。
3. language：支持 C 语言和 Python 语言（3.7 及以上版本），默认为 C。
4. library_path：如果编程语言是 C，则路径是包含 UDF 实现的动态链接库的库文件绝对路径，通常指向一个 so 文件。如果编程语言是 Python，则路径是包含 UDF 实现的 Python 文件路径。路径需要用英文单引号或英文双引号括起来。
5. output_type：函数计算结果的数据类型名称。

#### 4.3.3 创建聚合函数

创建聚合函数的 SQL 语法如下。
```sql
CREATE [OR REPLACE] AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type BUFSIZE buffer_size LANGUAGE 'Python';
```

其中，buffer_size 表示中间计算结果的缓冲区大小，单位是字节。其他参数的含义与标量函数相同。
如下 SQL 创建一个名为 l2norm 的 UDF。
```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
```

#### 4.3.4 删除UDF

删除指定名称的 UDF 的 SQL 语法如下。
```sql
DROP FUNCTION function_name;
```

#### 4.3.5 查看 UDF

显示集群中当前可用的所有 UDF 的 SQL 如下。
```sql
show functions;
```

#### 4.3.6 查看函数信息

同名的 UDF 每更新一次，版本号会增加 1。
```sql
select * from ins_functions \G;   
```

## 5. 性能

1. udf 自定义函数出现错误和异常，不应该影响 taosd 的运行。
2. 执行速度受 udf 用户自定义实现的影响

## 6. 安全

自定义函数运行的进程空间与主进程相互分割，互不影响，通过 IPC 机制进行交流和沟通。
1. 隔离执行环境：自定义函数模块必须在独立的、资源受限的沙箱环境中运行，即一个独立的进程空间中运行。
2. 最小权限：每个模块明确定义并强制执行最小权限集，模块仅能访问其功能所必需的系统资源。
3. 输入验证：所有传递给自定义函数模块的输入参数必须经过严格的验证和净化。这包括类型检查、长度限制（防止缓冲区溢出）、内容过滤（如SQL注入、XSS脚本检查）。
4. 安全内存：系统提供的API或模块内部若涉及内存操作，应强制使用安全版本函数（如memcpy_s, memset_s），确保进行边界检查，防止缓冲区溢出。
5. 故障隔离与恢复：单个模块的崩溃或异常不得导致整个宿主系统或其他模块失效。系统应具备快速重启失败模块或切换到安全状态的能力。
6. 安全机制（如系统调用拦截、内存检查）引入的性能开销应在设计可接受范围内，不应严重影响系统核心功能。
7. 安全审计日志：所有安全相关事件，包括模块加载、权限检查结果等，都必须记录到不可篡改的审计日志中，日志应包含时间戳、模块标识、操作详情和结果。

## 7. 兼容性

1. 对外暴露的接口约定，不因版本变化改动，保证所有实现 UDF 的版本兼容
2. 版本变化，不影响已经使用的 UDF实现
3. 更新版本，已经创建并使用的 UDF 继续有效

## 8. 运维

1. UDF 功能使用了一个新的进程 udfd，会随TDengine server 安装包安装。
2. udfd 进程由 taosd 进程启动，taosd 进程停止时，udfd 进程停止。udfd 异常退出，taosd 进程会自动重新启动udfd。

## 9. 使用场景

内置函数无法满足用户需求，客户想要编写和使用自定义处理函数的场景。

## 10. 约束和限制

1. UDF 内无法通过 print 函数输出日志，需要自己写文件或使用日志库写文件。
2. 在创建 UDF 时，需要区分标量函数和聚合函数。如果创建时声明了错误的函数类别，则通过 SQL 调用函数时会报错。

## 11. 常见错误和排查

如果碰到执行报错：DB error: udf function execution failure
需配合日志查看问题，日志目录： /var/log/taos/taospyudf.log  
1. 加载插件失败，报错如下。需重新检查安装环境，确认  libtaospyudf.so  是否安装成功。
```c
05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
```

1. udf 错误调用，例如参数个数不对，可能报错
```c
[doPyUdfScalarProc@507] call pyUdfScalar proc function.
```

1. Python 开发的 UDF 插件，可能会碰到依赖库加载失败的情况，输出如下
```plaintext
2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
```

因为 python udf 插件默认搜索的第三方库安装路径是： /lib/python3/dist-packages，而 moment 默认安装到了 /usr/local/lib/python3.8/dist-packages。下面我们修改 python udf 插件默认的库搜索路径。 先打开 python3 命令行，查看当前的 sys.path。
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

## 12. 可观测性

无

## 13. 安装和卸载

无

## 14. 文档

需要在官方文档【开发指南】中增加【UDF】一章，用于用户自定义函数说明。

## 15. 参考文档

无

## 16. 附录

无
