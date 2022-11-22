---
sidebar_label: UDF
title: User-Defined Functions (UDF)
description: "You can define your own scalar and aggregate functions to expand the query capabilities of TDengine."
---

The built-in functions of TDengine may not be sufficient for the use cases of every application. In this case, you can define custom functions for use in TDengine queries. These are known as user-defined functions (UDF). A user-defined function takes one column of data or the result of a subquery as its input.

TDengine supports user-defined functions written in C or C++. This document describes the usage of user-defined functions.

User-defined functions can be scalar functions or aggregate functions. Scalar functions, such as `abs`, `sin`, and `concat`, output a value for every row of data. Aggregate functions, such as `avg` and `max` output one value for multiple rows of data.

When you create a user-defined function, you must implement standard interface functions:
- For scalar functions, implement the `scalarfn` interface function.
- For aggregate functions, implement the `aggfn_start`, `aggfn`, and `aggfn_finish` interface functions.
- To initialize your function, implement the `udf_init` function. To terminate your function, implement the `udf_destroy` function.

There are strict naming conventions for these interface functions. The names of the start, finish, init, and destroy interfaces must be <udf-name\>_start, <udf-name\>_finish, <udf-name\>_init, and <udf-name\>_destroy, respectively. Replace `scalarfn`, `aggfn`, and `udf` with the name of your user-defined function.

## Implementing a Scalar Function
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

## Implementing an Aggregate Function

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
// @param interbuf intermediate value to intialize
// @return error number defined in taoserror.h
int32_t aggfn_start(SUdfInterBuf* interBuf) {
    // initialize intermediate value in interBuf
    return TSDB_CODE_SUCESS;
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

## Interface Functions

There are strict naming conventions for interface functions. The names of the start, finish, init, and destroy interfaces must be <udf-name\>_start, <udf-name\>_finish, <udf-name\>_init, and <udf-name\>_destroy, respectively. Replace `scalarfn`, `aggfn`, and `udf` with the name of your user-defined function.

Interface functions return a value that indicates whether the operation was successful. If an operation fails, the interface function returns an error code. Otherwise, it returns TSDB_CODE_SUCCESS. The error codes are defined in `taoserror.h` and in the common API error codes in `taos.h`. For example, TSDB_CODE_UDF_INVALID_INPUT indicates invalid input. TSDB_CODE_OUT_OF_MEMORY indicates insufficient memory.

For information about the parameters for interface functions, see Data Model

### Interfaces for Scalar Functions

 `int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn)` 
 
 Replace `scalarfn` with the name of your function. This function performs scalar calculations on data blocks. You can configure a value through the parameters in the `resultColumn` structure.

The parameters in the function are defined as follows:
  - inputDataBlock: The data block to input.
  - resultColumn: The column to output. The column to output. 

### Interfaces for Aggregate Functions

`int32_t aggfn_start(SUdfInterBuf *interBuf)`

`int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf)`

`int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result)`

Replace `aggfn` with the name of your function. In the function, aggfn_start is called to generate a result buffer. Data is then divided between multiple blocks, and aggfn is called on each block to update the result. Finally, aggfn_finish is called to generate final results from the intermediate results. The final result contains only one or zero data points.

The parameters in the function are defined as follows:
  - interBuf: The intermediate result buffer.
  - inputBlock: The data block to input.
  - newInterBuf: The new intermediate result buffer.
  - result: The final result.


### Initializing and Terminating User-Defined Functions
`int32_t udf_init()`

`int32_t udf_destroy()`

Replace `udf`with the name of your function. udf_init initializes the function. udf_destroy terminates the function. If it is not necessary to initialize your function, udf_init is not required. If it is not necessary to terminate your function, udf_destroy is not required.


## Data Structure of User-Defined Functions
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

- The SUdfDataBlock block includes the number of rows (numOfRows) and number of columns (numCols). udfCols[i] (0 <= i <= numCols-1) indicates that each column is of type SUdfColumn.
- SUdfColumn includes the definition of the data type of the column (colMeta) and the data in the column (colData).
- The member definitions of SUdfColumnMeta are the same as the data type definitions in `taos.h`.
- The data in SUdfColumnData can become longer. varLenCol indicates variable-length data, and fixLenCol indicates fixed-length data. 
- SUdfInterBuf defines the intermediate structure `buffer` and the number of results in the buffer `numOfResult`.

Additional functions are defined in `taosudf.h` to make it easier to work with these structures.

## Compile UDF

To use your user-defined function in TDengine, first compile it to a dynamically linked library (DLL).

For example, the sample UDF `bit_and.c` can be compiled into a DLL as follows:

```bash
gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
```

The generated DLL file `libbitand.so` can now be used to implement your function. Note: GCC 7.5 or later is required.

## Manage and Use User-Defined Functions
After compiling your function into a DLL, you add it to TDengine. For more information, see [User-Defined Functions](../12-taos-sql/26-udf.md).

## Sample Code

### Sample scalar function: [bit_and](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/bit_and.c)

The bit_and function implements bitwise addition for multiple columns. If there is only one column, the column is returned. The bit_and function ignores null values.

<details>
<summary>bit_and.c</summary>

```c
{{#include tests/script/sh/bit_and.c}}
```

</details>

### Sample aggregate function: [l2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/l2norm.c)

The l2norm function finds the second-order norm for all data in the input column. This squares the values, takes a cumulative sum, and finds the square root.

<details>
<summary>l2norm.c</summary>

```c
{{#include tests/script/sh/l2norm.c}}
```

</details>
