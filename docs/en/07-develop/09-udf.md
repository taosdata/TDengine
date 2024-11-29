---
sidebar_label: User-Defined Functions
title: User-Defined Functions (UDF)
slug: /developer-guide/user-defined-functions
---

## Introduction to UDF

In certain application scenarios, the query functions required by the application logic cannot be directly implemented using built-in functions. TDengine allows the writing of User-Defined Functions (UDF) to address the specific needs in such scenarios. Once the UDF is successfully registered in the cluster, it can be called in SQL just like system-built-in functions, with no difference in usage. UDFs are divided into scalar functions and aggregate functions. Scalar functions output a value for each row of data, such as calculating the absolute value (abs), sine function (sin), string concatenation function (concat), etc. Aggregate functions output a value for multiple rows of data, such as calculating the average (avg) or maximum value (max).

TDengine supports writing UDFs in both C and Python programming languages. UDFs written in C have performance almost identical to built-in functions, while those written in Python can leverage the rich Python computation libraries. To prevent exceptions during UDF execution from affecting database services, TDengine uses process separation technology to execute UDFs in another process. Even if a user-defined UDF crashes, it will not affect the normal operation of TDengine.

## Developing UDFs in C

When implementing UDFs in C, it is necessary to implement the specified interface functions:

- Scalar functions need to implement the scalar interface function `scalarfn`.
- Aggregate functions need to implement the aggregate interface functions `aggfn_start`, `aggfn`, `aggfn_finish`.
- If initialization is required, implement `udf_init`.
- If cleanup is required, implement `udf_destroy`.

### Interface Definition

The name of the interface function is the UDF name or a combination of the UDF name and specific suffixes (\_start, \_finish, \_init, \_destroy). The function names described later, such as `scalarfn` and `aggfn`, need to be replaced with the UDF name.

#### Scalar Function Interface

A scalar function is a function that converts input data to output data, typically used for calculations and transformations on a single data value. The prototype for the scalar function interface is as follows.

```c
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn);
```

The main parameter descriptions are as follows:

- `inputDataBlock`: The input data block.
- `resultColumn`: The output column.

#### Aggregate Function Interface

An aggregate function is a special function used to group and calculate data to generate summary information. The workings of an aggregate function are as follows:

- Initialize the result buffer: First, call the `aggfn_start` function to generate a result buffer (result buffer) for storing intermediate results.
- Group data: Relevant data will be divided into multiple row data blocks, each containing a set of data with the same grouping key.
- Update intermediate results: For each data block, call the `aggfn` function to update the intermediate results. The `aggfn` function will compute the data according to the type of aggregate function (such as sum, avg, count, etc.) and store the calculation results in the result buffer.
- Generate final results: After updating the intermediate results of all data blocks, call the `aggfn_finish` function to extract the final result from the result buffer. The final result will contain either 0 or 1 piece of data, depending on the type of aggregate function and the input data.

The prototype for the aggregate function interface is as follows.

```c
int32_t aggfn_start(SUdfInterBuf *interBuf);
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf);
int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result);
```

Where `aggfn` is a placeholder for the function name. First, call `aggfn_start` to generate the result buffer, then the relevant data will be divided into multiple row data blocks, and the `aggfn` function will be called for each data block to update the intermediate results. Finally, call `aggfn_finish` to produce the final result from the intermediate results, which can only contain 0 or 1 result data.

The main parameter descriptions are as follows:

- `interBuf`: The intermediate result buffer.
- `inputBlock`: The input data block.
- `newInterBuf`: The new intermediate result buffer.
- `result`: The final result.

#### Initialization and Destruction Interfaces

The initialization and destruction interfaces are shared by both scalar and aggregate functions, with the relevant APIs as follows.

```c
int32_t udf_init()
int32_t udf_destroy()
```

The `udf_init` function performs initialization, while the `udf_destroy` function handles cleanup. If there is no initialization work, there is no need to define the `udf_init` function; if there is no cleanup work, there is no need to define the `udf_destroy` function.

### Scalar Function Template

The template for developing scalar functions in C is as follows.

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

### Aggregate Function Template

The template for developing aggregate functions in C is as follows.

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

### Compilation

In TDengine, to implement UDFs, you need to write C source code and compile it into a dynamic link library file according to TDengine's specifications. Following the previously described rules, prepare the source code for the UDF `bit_and.c`. For the Linux operating system, execute the following command to compile and obtain the dynamic link library file.

```shell
gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
```

To ensure reliable operation, it is recommended to use GCC version 7.5 or above.

### C UDF Data Structures

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

The structure descriptions are as follows:

- `SUdfDataBlock` contains the number of rows `numOfRows` and the number of columns `numCols`. `udfCols[i]` (0 \<= i \<= `numCols`-1) represents each column of data, of type `SUdfColumn*`.
- `SUdfColumn` contains the data type definition `colMeta` and the data `colData`.
- The members of `SUdfColumnMeta` are defined similarly to the data type definitions in `taos.h`.
- `SUdfColumnData` can be of variable length, with `varLenCol` defining variable length data and `fixLenCol` defining fixed length data.
- `SUdfInterBuf` defines the intermediate structure buffer and the number of results in the buffer `numOfResult`.

To better operate on the above data structures, some utility functions are provided, defined in `taosudf.h`.

### C UDF Example Code

#### Scalar Function Example [bit_and](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/bit_and.c)

`bit_add` implements the bitwise AND function for multiple columns. If there is only one column, it returns that column. `bit_add` ignores null values.

<details>
<summary>bit_and.c</summary>

```c
{{#include tests/script/sh/bit_and.c}}
```

</details>

#### Aggregate Function Example 1 Return Value as Numeric Type [l2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/l2norm.c)

`l2norm` implements the second-order norm of all data in the input column, which means squaring each data point, summing them, and then taking the square root.

<details>
<summary>l2norm.c</summary>

```c
{{#include tests/script/sh/l2norm.c}}
```

</details>

#### Aggregate Function Example 2 Return Value as String Type [max_vol](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/max_vol.c)

`max_vol` finds the maximum voltage from multiple input voltage columns and returns a combined string value composed of device ID + the location (row, column) of the maximum voltage + the maximum voltage value.

Create table:

```bash
create table battery(ts timestamp, vol1 float, vol2 float, vol3 float, deviceId varchar(16));
```

Create custom function:

```bash
create aggregate function max_vol as '/root/udf/libmaxvol.so' outputtype binary(64) bufsize 10240 language 'C'; 
```

Use custom function:

```bash
select max_vol(vol1, vol2, vol3, deviceid) from battery;
```

<details>
<summary>max_vol.c</summary>

```c
{{#include tests/script/sh/max_vol.c}}
```

</details>

## Developing UDFs in Python

### Preparing the Environment

The specific steps to prepare the environment are as follows:

- Step 1: Prepare the Python runtime environment.
- Step 2: Install the Python package `taospyudf`. The command is as follows.

    ```shell
    pip3 install taospyudf
    ```

- Step 3: Execute the command `ldconfig`.
- Step 4: Start the `taosd` service.

During installation, C++ source code will be compiled, so the system must have `cmake` and `gcc`. The compiled file `libtaospyudf.so` will be automatically copied to the `/usr/local/lib/` directory, so if you are a non-root user, you need to add `sudo` during installation. After installation, you can check if this file exists in the directory:

```shell
root@slave11 ~/udf $ ls -l /usr/local/lib/libtaos*
-rw-r--r-- 1 root root 671344 May 24 22:54 /usr/local/lib/libtaospyudf.so
```

### Interface Definition

When developing UDFs using Python, you need to implement the specified interface functions. The specific requirements are as follows.

- Scalar functions need to implement the scalar interface function `process`.
- Aggregate functions need to implement the aggregate interface functions `start`, `reduce`, and `finish`.
- If initialization is required, implement the `init` function.
- If cleanup is required, implement the `destroy` function.

#### Scalar Function Interface

The interface for scalar functions is as follows.

```Python
def process(input: datablock) -> tuple[output_type]:
```

The main parameter description is as follows:

- `input`: `datablock` similar to a two-dimensional matrix, which reads the Python object located at row `row` and column `col` through the member method `data(row, col)`.
- The return value is a tuple of Python objects, with each element of the output type.

#### Aggregate Function Interface

The interface for aggregate functions is as follows.

```Python
def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
```

The above code defines three functions used to implement a custom aggregate function. The specific process is as follows.

First, call the `start` function to generate the initial result buffer. This result buffer is used to store the internal state of the aggregate function and will be continuously updated as the input data is processed.

Then, the input data will be divided into multiple row data blocks. For each row data block, the `reduce` function will be called, passing the current row data block (`inputs`) and the current intermediate result (`buf`) as parameters. The `reduce` function will update the internal state of the aggregate function based on the input data and current state, returning the new intermediate result.

Finally, when all row data blocks are processed, the `finish` function will be called. This function receives the final intermediate result (`buf`) as a parameter and generates the final output from it. Due to the nature of aggregate functions, the final output can only contain 0 or 1 piece of data. This output result will be returned to the caller as the computation result of the aggregate function.

#### Initialization and Destruction Interfaces

The initialization and destruction interfaces are as follows.

```Python
def init()
def destroy()
```

Parameter descriptions:

- `init`: Completes initialization work.
- `destroy`: Completes cleanup work.

:::note

When developing UDFs in Python, it is necessary to define the `init` and `destroy` functions.

:::

### Scalar Function Template

The template for developing scalar functions in Python is as follows.

```Python
def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:  
```

### Aggregate Function Template

The template for developing aggregate functions in Python is as follows.

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

### Data Type Mapping

The following table describes the mapping between TDengine SQL data types and Python data types. Any type of NULL value is mapped to Python's `None` value.

|  **TDengine SQL Data Type**   | **Python Data Type** |
| :-----------------------: | ------------ |
| TINYINT / SMALLINT / INT / BIGINT | int |
| TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | int |
| FLOAT / DOUBLE | float |
| BOOL | bool |
| BINARY / VARCHAR / NCHAR | bytes|
| TIMESTAMP | int |
| JSON and other types | Not supported |

### Development Examples

This article contains five example programs, progressing from simple to complex, and also includes a wealth of practical debugging tips.

:::note

Logging cannot be output through the `print` function within UDFs; you need to write to files yourself or use Python's built-in logging library to write to files.

:::

#### Example One

Write a UDF function that only accepts a single integer: input `n`, output `ln(n^2 + 1)`.
First, write a Python file located in a certain system directory, such as `/root/udf/myfun.py`, with the following content.

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

This file contains three functions: `init` and `destroy` are both empty functions; they are the lifecycle functions of the UDF and need to be defined even if they do nothing. The key function is `process`, which accepts a data block; this data block object has two methods.

1. `shape()` returns the number of rows and columns in the data block.
2. `data(i, j)` returns the data located at row `i` and column `j`.

The `process` method of the scalar function needs to return as many rows of data as the number of rows in the input data block. The above code ignores the number of columns because it only needs to calculate the first column of each row.

Next, create the corresponding UDF function by executing the following statement in the TDengine CLI.

```sql
create function myfun as '/root/udf/myfun.py' outputtype double language 'Python'
```

The output is as follows.

```shell
taos> create function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
Create OK, 0 row(s) affected (0.005202s)
```

It seems to go smoothly. Next, check all custom functions in the system to confirm that the creation was successful.

```text
taos> show functions;
              name              |
=================================
 myfun                          |
Query OK, 1 row(s) in set (0.005767s)
```

Generate test data by executing the following commands in the TDengine CLI.

```sql
create database test;
create table t(ts timestamp, v1 int, v2 int, v3 int);
insert into t values('2023-05-01 12:13:14', 1, 2, 3);
insert into t values('2023-05-03 08:09:10', 2, 3, 4);
insert into t values('2023-05-10 07:06:05', 3, 4, 5);
```

Test the `myfun` function.

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.011088s)
```

Unfortunately, the execution failed. What is the reason? Check the logs of the `udfd` process.

```shell
tail -10 /var/log/taos/udfd.log
```

The following error message is found.

```text
05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
```

The error is clear: the Python plugin `libtaospyudf.so` could not be loaded. If you encounter this error, please refer to the preparation environment section above.

After fixing the environment error, execute the command again as follows.

```sql
taos> select myfun(v1) from t;
         myfun(v1)         |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

Thus, we have completed the first UDF and learned some simple debugging methods.

#### Example Two

Although the above `myfun` passed the test, it has two shortcomings.

1. This scalar function only accepts one column of data as input; if the user passes in multiple columns, it will not raise an exception.

```sql
taos> select myfun(v1, v2) from t;
       myfun(v1, v2)       |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

2. It does not handle null values. We expect that if there are null values in the input, it will raise an exception and terminate execution. Therefore, the `process` function is improved as follows.

```python
def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception(f"require 1 parameter but given {cols}")
    return [ None if block.data(i, 0) is None else log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
```

Execute the following statement to update the existing UDF.

```sql
create or replace function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
```

Passing two parameters to `myfun` will now cause it to fail.

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.014643s)
```

The custom exception message is printed in the plugin's log file `/var/log/taos/taospyudf.log`.

```text
2023-05-24 23:21:06.790 ERROR [1666188] [doPyUdfScalarProc@507] call pyUdfScalar proc function. context 0x7faade26d180. error: Exception: require 1 parameter but given 2

At:
  /var/lib/taos//.udf/myfun_3_1884e1281d9.py(12): process
```

Thus, we have learned how to update the UDF and check the error logs produced by the UDF.
(Note: If the UDF does not take effect after being updated, in versions of TDengine prior to 3.0.5.0, it is necessary to restart `taosd`; in versions 3.0.5.0 and later, there is no need to restart `taosd` for it to take effect.)

#### Example Three

Input (x1, x2, ..., xn), output the sum of each value multiplied by its index: `1 *  x1 + 2 * x2 + ... + n * xn`. If x1 to xn contains null, the result is null.

The difference from Example One is that this can accept any number of columns as input and needs to process the values of each column. Write the UDF file `/root/udf/nsum.py`.

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

Create the UDF.

```sql
create function nsum as '/root/udf/nsum.py' outputtype double language 'Python';
```

Test the UDF.

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

#### Example Four

Write a UDF that takes a timestamp as input and outputs the next Sunday closest to that time. For example, if today is 2023-05-25, the next Sunday would be 2023-05-28. This function will use the third-party library `moment`. First, install this library.

```shell
pip3 install moment
```

Then, write the UDF file `/root/udf/nextsunday.py`.

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

The UDF framework will map TDengine's timestamp type to Python's int type, so this function only accepts an integer representing milliseconds. The `process` method first performs parameter checks, then uses the `moment` package to replace the weekday of the time with Sunday, and finally formats the output. The output string has a fixed length of 10 characters, so the UDF function can be created as follows.

```sql
create function nextsunday as '/root/udf/nextsunday.py' outputtype binary(10) language 'Python';
```

At this point, test the function; if you started `taosd` using `systemctl`, you will definitely encounter an error.

```sql
taos> select ts, nextsunday(ts) from t;

DB error: udf function execution failure (1.123615s)
```

```shell
tail -20 taospyudf.log  
2023-05-25 11:42:34.541 ERROR [1679419] [PyUdf::PyUdf@217] py udf load module failure. error ModuleNotFoundError: No module named 'moment'
```

This is because the location of "moment" is not in the default library search path of the Python UDF plugin. How can we confirm this? By searching `taospyudf.log` with the following command.

```shell
grep 'sys path' taospyudf.log  | tail -1
```

The output is as follows:

```text
2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
```

It shows that the default search path for third-party libraries in the Python UDF plugin is: `/lib/python3/dist-packages`, while `moment` is installed by default in `/usr/local/lib/python3.8/dist-packages`. Now, we will modify the default library search path for the Python UDF plugin.
First, open the Python 3 command line and check the current `sys.path`.

```python
>>> import sys
>>> ":".join(sys.path)
'/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages'
```

Copy the output string from the above script, then edit `/var/taos/taos.cfg` to add the following configuration.

```shell
UdfdLdLibPath /usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages
```

After saving, execute `systemctl restart taosd`, and then test again without errors.

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

#### Example Five

Write an aggregate function to calculate the difference between the maximum and minimum values of a certain column.

The difference between aggregate functions and scalar functions is that scalar functions correspond to multiple outputs for multiple rows of input, while aggregate functions correspond to a single output for multiple rows of input. The execution process of aggregate functions is somewhat like the execution process of the classic map-reduce framework, which divides the data into several blocks, with each mapper processing one block, and the reducer aggregating the results from the mappers. The difference is that in TDengine's Python UDF, the `reduce` function has both map and reduce functionalities. The `reduce` function takes two parameters: one is the data to be processed, and the other is the result of the reduce function executed by another task. The following example demonstrates this in `/root/udf/myspread.py`.

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

In this example, we not only define an aggregate function but also add logging functionality to record execution logs.

1. The `init` function opens a file for logging.
2. The `log` function records logs, automatically converting the passed object to a string and adding a newline character.
3. The `destroy` function closes the log file after execution.
4. The `start` function returns the initial buffer for storing intermediate results of the aggregate function, initializing the maximum value to negative infinity and the minimum value to positive infinity.
5. The `reduce` function processes each data block and aggregates the results.
6. The `finish` function converts the buffer into the final output.

Execute the following SQL statement to create the corresponding UDF.

```sql
create or replace aggregate function myspread as '/root/udf/myspread.py' outputtype double bufsize 128 language 'Python';
```

This SQL statement has two important differences from the SQL statement for creating scalar functions.

1. The `aggregate` keyword has been added.
2. The `bufsize` keyword has been added to specify the memory size for storing intermediate results, in bytes. This value can be greater than the actual size used. In this case, the intermediate result consists of a tuple of two floating-point numbers, and after serialization, it occupies only 32 bytes. However, the specified `bufsize` is 128, which can be printed using Python to show the actual number of bytes used.

```python
>>> len(pickle.dumps((12345.6789, 23456789.9877)))
32
```

Test this function, and you will see that the output of `myspread` is consistent with the output of the built-in `spread` function.

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

Finally, check the execution log, and you will see that the `reduce` function was executed three times, with the `max` value being updated four times and the `min` value being updated once.

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

Through this example, we learned how to define aggregate functions and print custom log information.

### More Python UDF Example Code

#### Scalar Function Example [pybitand](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pybitand.py)

`pybitand` implements the bitwise AND function for multiple columns. If there is only one column, it returns that column. `pybitand` ignores null values.

<details>
<summary>pybitand.py</summary>

```Python
{{#include tests/script/sh/pybitand.py}}
```

</details>

#### Aggregate Function Example [pyl2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pyl2norm.py)

`pyl2norm` implements the second-order norm of all data in the input column, which means squaring each data point, summing them, and then taking the square root.

<details>
<summary>pyl2norm.py</summary>

```c
{{#include tests/script/sh/pyl2norm.py}}
```

</details>

#### Aggregate Function Example [pycumsum](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pycumsum.py)

`pycumsum` calculates the cumulative sum of all data in the input column using `numpy`.
<details>
<summary>pycumsum.py</summary>

```c
{{#include tests/script/sh/pycumsum.py}}
```

</details>

## Managing UDFs

The process of managing UDFs in the cluster involves creating, using, and maintaining these functions. Users can create and manage UDFs in the cluster via SQL, and once created, all users in the cluster can use these functions in SQL. Since UDFs are stored on the mnode of the cluster, they remain available even after the cluster is restarted.

When creating UDFs, it is necessary to distinguish between scalar functions and aggregate functions. Scalar functions accept zero or more input parameters and return a single value. Aggregate functions accept a set of input values and return a single value through some computation (such as summation, counting, etc.). If the wrong function type is declared during creation, an error will occur when calling the function via SQL.

Additionally, users need to ensure that the input data types match the UDF program, and that the UDF output data types match the `outputtype`. This means that when creating a UDF, the correct data types must be specified for both input parameters and output values. This helps ensure that when calling the UDF, the input data can be correctly passed to the UDF, and that the UDF's output value matches the expected data type.

### Creating Scalar Functions

The SQL syntax for creating scalar functions is as follows.

```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```

Parameter descriptions are as follows:

- `or replace`: If the function already exists, it will modify the existing function properties.
- `function_name`: The name of the scalar function when called in SQL.
- `language`: Supports C language and Python language (version 3.7 and above), defaulting to C.
- `library_path`: If the programming language is C, the path is the absolute path to the dynamic link library containing the UDF implementation, usually pointing to a `.so` file. If the programming language is Python, the path is the file path containing the UDF implementation in Python. The path needs to be enclosed in single or double quotes.
- `output_type`: The data type name of the function's computation result.

### Creating Aggregate Functions

The SQL syntax for creating aggregate functions is as follows.

```sql
CREATE [OR REPLACE] AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type BUFSIZE buffer_size LANGUAGE 'Python';
```

Where `buffer_size` indicates the buffer size for intermediate calculation results, measured in bytes. The meanings of other parameters are the same as for scalar functions.

The following SQL creates a UDF named `l2norm`.

```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
```

### Deleting UDFs

The SQL syntax for deleting a UDF with the specified name is as follows.

```sql
DROP FUNCTION function_name;
```

### Viewing UDFs

The SQL to display all currently available UDFs in the cluster is as follows.

```sql
show functions;
```

### Viewing Function Information

The version number of the UDF increases by 1 each time it is updated.

```sql
select * from ins_functions \G;     
```
