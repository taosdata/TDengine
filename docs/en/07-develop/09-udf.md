---
title: User-Defined Functions (UDF)
sidebar_label: UDF
description: This document describes how to create user-defined functions (UDF), your own scalar and aggregate functions that can expand the query capabilities of TDengine.
---

The built-in functions of TDengine may not be sufficient for the use cases of every application. In this case, you can define custom functions for use in TDengine queries. These are known as user-defined functions (UDF). A user-defined function takes one column of data or the result of a subquery as its input.

User-defined functions can be scalar functions or aggregate functions. Scalar functions, such as `abs`, `sin`, and `concat`, output a value for every row of data. Aggregate functions, such as `avg` and `max` output one value for multiple rows of data.

TDengine supports user-defined functions written in C or Python. This document describes the usage of user-defined functions.

## Implement a UDF in C 

When you create a user-defined function, you must implement standard interface functions:
- For scalar functions, implement the `scalarfn` interface function.
- For aggregate functions, implement the `aggfn_start`, `aggfn`, and `aggfn_finish` interface functions.
- To initialize your function, implement the `udf_init` function. To terminate your function, implement the `udf_destroy` function.

There are strict naming conventions for these interface functions. The names of the start, finish, init, and destroy interfaces must be <udf-name\>_start, <udf-name\>_finish, <udf-name\>_init, and <udf-name\>_destroy, respectively. Replace `scalarfn`, `aggfn`, and `udf` with the name of your user-defined function.

### Implementing a Scalar Function in C
The implementation of a scalar function is described as follows:
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
Replace `scalarfn` with the name of your function.

### Implementing an Aggregate Function in C

The implementation of an aggregate function is described as follows:
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
Replace `aggfn` with the name of your function.

### UDF Interface Definition in C

There are strict naming conventions for interface functions. The names of the start, finish, init, and destroy interfaces must be <udf-name\>_start, <udf-name\>_finish, <udf-name\>_init, and <udf-name\>_destroy, respectively. Replace `scalarfn`, `aggfn`, and `udf` with the name of your user-defined function.

Interface functions return a value that indicates whether the operation was successful. If an operation fails, the interface function returns an error code. Otherwise, it returns TSDB_CODE_SUCCESS. The error codes are defined in `taoserror.h` and in the common API error codes in `taos.h`. For example, TSDB_CODE_UDF_INVALID_INPUT indicates invalid input. TSDB_CODE_OUT_OF_MEMORY indicates insufficient memory.

For information about the parameters for interface functions, see Data Model

#### Scalar Interface
 `int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn)` 
 
 Replace `scalarfn` with the name of your function. This function performs scalar calculations on data blocks. You can configure a value through the parameters in the `resultColumn` structure.

The parameters in the function are defined as follows:
  - inputDataBlock: The data block to input.
  - resultColumn: The column to output. The column to output. 

#### Aggregate Interface

`int32_t aggfn_start(SUdfInterBuf *interBuf)`

`int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf)`

`int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result)`

Replace `aggfn` with the name of your function. In the function, aggfn_start is called to generate a result buffer. Data is then divided between multiple blocks, and the `aggfn` function is called on each block to update the result. Finally, aggfn_finish is called to generate the final results from the intermediate results. The final result contains only one or zero data points.

The parameters in the function are defined as follows:
  - interBuf: The intermediate result buffer.
  - inputBlock: The data block to input.
  - newInterBuf: The new intermediate result buffer.
  - result: The final result.


#### Initialization and Cleanup Interface
`int32_t udf_init()`

`int32_t udf_destroy()`

Replace `udf` with the name of your function. udf_init initializes the function. udf_destroy terminates the function. If it is not necessary to initialize your function, udf_init is not required. If it is not necessary to terminate your function, udf_destroy is not required.


### Data Structures for UDF in C
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
The data structure is described as follows:

- The SUdfDataBlock block includes the number of rows (numOfRows) and the number of columns (numCols). udfCols[i] (0 <= i <= numCols-1) indicates that each column is of type SUdfColumn.
- SUdfColumn includes the definition of the data type of the column (colMeta) and the data in the column (colData).
- The member definitions of SUdfColumnMeta are the same as the data type definitions in `taos.h`.
- The data in SUdfColumnData can become longer. varLenCol indicates variable-length data, and fixLenCol indicates fixed-length data. 
- SUdfInterBuf defines the intermediate structure `buffer` and the number of results in the buffer `numOfResult`.

Additional functions are defined in `taosudf.h` to make it easier to work with these structures.

### Compiling C UDF

To use your user-defined function in TDengine, first, compile it to a shared library.

For example, the sample UDF `bit_and.c` can be compiled into a DLL as follows:

```bash
gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
```

The generated DLL file `libbitand.so` can now be used to implement your function. Note: GCC 7.5 or later is required.

### UDF Sample Code in C

#### Scalar function: [bit_and](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/bit_and.c)

The bit_and function implements bitwise addition for multiple columns. If there is only one column, the column is returned. The bit_and function ignores null values.

<details>
<summary>bit_and.c</summary>

```c
{{#include tests/script/sh/bit_and.c}}
```

</details>

#### Aggregate function 1: [l2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/l2norm.c)

The l2norm function finds the second-order norm for all data in the input column. This squares the values, takes a cumulative sum, and finds the square root.

<details>
<summary>l2norm.c</summary>

```c
{{#include tests/script/sh/l2norm.c}}
```

</details>

#### Aggregate function 2: [max_vol](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/max_vol.c)

The max_vol function returns a string concatenating the deviceId column, the row number and column number of the maximum voltage and the maximum voltage given several voltage columns as input.

Create Table:
```bash
create table battery(ts timestamp, vol1 float, vol2 float, vol3 float, deviceId varchar(16));
```
Create the UDF:
```bash
create aggregate function max_vol as '/root/udf/libmaxvol.so' outputtype binary(64) bufsize 10240 language 'C'; 
```
Use the UDF in the query：
```bash
select max_vol(vol1,vol2,vol3,deviceid) from battery;
```

<details>
<summary>max_vol.c</summary>

```c
{{#include tests/script/sh/max_vol.c}}
```

</details>

## Implement a UDF in Python

Implement the specified interface functions when implementing a UDF in Python.
- implement `process` function for the scalar UDF。
- implement `start`, `reduce`, `finish` for the aggregate UDF。
- implement `init` for initialization and `destroy` for termination。

### Implement a Scalar UDF in Python

The implementation of a scalar UDF is described as follows:

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

### Implement an Aggregate UDF in Python

The implementation of an aggregate function is described as follows:

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
    # use inputs.data(i,j) to access python ojbect of location(i,j)
    # serialize new_state into new_state_bytes
    return new_state_bytes   
def finish(buf: bytes) -> output_type:
    #return obj of type outputtype   
```

### Python UDF Interface Definition

#### Scalar interface
```Python
def process(input: datablock) -> tuple[output_type]:
```
- `input` is a data block two-dimension matrix-like object, of which method `data(row, col)` returns the Python object located at location (`row`, `col`)
- return a Python tuple object, of which each item is a Python object of type `output_type`

#### Aggregate Interface
```Python
def start() -> bytes:
def reduce(input: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
```

- first `start()` is called to return the initial result in type `bytes`
- then the input data are divided into multiple data blocks and for each block `input`, `reduce` is called with the data block `input` and the current result `buf` bytes and generates a new intermediate result buffer. 
- finally, the `finish` function is called on the intermediate result `buf` and outputs 0 or 1 data of type `output_type`


#### Initialization and Cleanup Interface
```Python
def init()
def destroy()
```
Implement `init` for initialization and `destroy` for termination. 

### Data Mapping between TDengine SQL and Python UDF

The following table describes the mapping between TDengine SQL data type and Python UDF Data Type. The `NULL` value of all TDengine SQL types is mapped to the `None` value in Python.

|  **TDengine SQL Data Type**   | **Python Data Type** |
| :-----------------------: | ------------ |
|TINYINT / SMALLINT / INT  / BIGINT     | int   |
|TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | int |
|FLOAT / DOUBLE | float |
|BOOL | bool |
|BINARY / VARCHAR / NCHAR | bytes|
|TIMESTAMP | int |
|JSON and other types | Not Supported |

### Installing Python UDF
1. Install Python package `taospyudf` that executes Python UDF
```bash
sudo pip install taospyudf
ldconfig
```
2. If PYTHONPATH is needed to find Python packages when the Python UDF executes, include the PYTHONPATH contents into the udfdLdLibPath variable of the taos.cfg configuration file
 
### Python UDF Sample Code
#### Scalar Function [pybitand](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pybitand.py)

The `pybitand` function implements bitwise addition for multiple columns. If there is only one column, the column is returned. The `pybitand` function ignores null values.

<details>
<summary>pybitand.py</summary>

```Python
{{#include tests/script/sh/pybitand.py}}
```

</details>

#### Aggregate Function [pyl2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pyl2norm.py)

The `pyl2norm` function finds the second-order norm for all data in the input column. This squares the values, takes a cumulative sum, and finds the square root.
<details>
<summary>pyl2norm.py</summary>

```c
{{#include tests/script/sh/pyl2norm.py}}
```

</details>

## Manage and Use UDF
You need to add UDF to TDengine before using it in SQL queries. For more information about how to manage UDF and how to invoke UDF, please see [Manage and Use UDF](../12-taos-sql/26-udf.md).
