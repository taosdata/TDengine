---
sidebar_label: User-Defined Functions
title: User-Defined Functions (UDF)
slug: /developer-guide/user-defined-functions
---

## Introduction to UDF

In some application scenarios, the query functionality required by the application logic cannot be directly implemented using built-in functions. TDengine allows the writing of user-defined functions (UDFs) to address the needs of special application scenarios. Once successfully registered in the cluster, UDFs can be called in SQL just like system built-in functions, with no difference in usage. UDFs are divided into scalar functions and aggregate functions. Scalar functions output a value for each row of data, such as absolute value (abs), sine function (sin), string concatenation function (concat), etc. Aggregate functions output a value for multiple rows of data, such as average (avg), maximum value (max), etc.

TDengine supports writing UDFs in two programming languages: C and Python. UDFs written in C have performance nearly identical to built-in functions, while those written in Python can utilize the rich Python computation libraries. To prevent exceptions during UDF execution from affecting the database service, TDengine uses process isolation technology, executing UDFs in a separate process. Even if a user-written UDF crashes, it will not affect the normal operation of TDengine.

## Developing UDFs in C Language

When implementing UDFs in C language, you need to implement the specified interface functions:

- Scalar functions need to implement the scalar interface function scalarfn.
- Aggregate functions need to implement the aggregate interface functions `aggfn_start`, `aggfn`, `aggfn_finish`.
- If initialization is needed, implement `udf_init`.
- If cleanup is needed, implement `udf_destroy`.

### Interface Definition

The interface function names are the UDF name, or the UDF name connected with specific suffixes (`_start`,`_finish`, `_init`,`_destroy`). Function names described later in the content, such as `scalarfn`, `aggfn`, should be replaced with the UDF name.

#### Scalar Function Interface

A scalar function is a function that converts input data into output data, typically used for calculating and transforming a single data value. The prototype of the scalar function interface is as follows.

```c
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn);
```

Key parameter descriptions are as follows:

- inputDataBlock: The input data block.
- resultColumn: The output column.

#### Aggregate Function Interface

An aggregate function is a special type of function used for grouping and calculating data to generate summary information. The working principle of aggregate functions is as follows:

- Initialize the result buffer: First, the `aggfn_start` function is called to generate a result buffer for storing intermediate results.
- Group data: Related data is divided into multiple row data blocks, each containing a group of data with the same grouping key.
- Update intermediate results: For each data block, the `aggfn` function is called to update the intermediate results. The `aggfn` function performs calculations according to the type of aggregate function (such as sum, avg, count, etc.) and stores the results in the result buffer.
- Generate the final result: After updating the intermediate results of all data blocks, the `aggfn_finish` function is called to extract the final result from the result buffer. The final result contains either 0 or 1 data row, depending on the type of aggregate function and the input data.

The prototype of the aggregate function interface is as follows.

```c
int32_t aggfn_start(SUdfInterBuf *interBuf);
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf);
int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result);
```

Key parameter descriptions are as follows:

- `interBuf`: Intermediate result buffer.
- `inputBlock`: The input data block.
- `newInterBuf`: New intermediate result buffer.
- `result`: The final result.

#### Initialization and Destruction Interface

The initialization and destruction interfaces are common interfaces used by both scalar and aggregate functions, with the following APIs.

```c
int32_t udf_init()
int32_t udf_destroy()
```

Among them, the `udf_init` function completes the initialization work, and the `udf_destroy` function completes the cleanup work. If there is no initialization work, there is no need to define the `udf_init` function; if there is no cleanup work, there is no need to define the `udf_destroy` function.

### Scalar Function Template

The template for developing scalar functions in C language is as follows.

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

The template for developing aggregate functions in C language is as follows.

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

In TDengine, to implement UDF, you need to write C language source code and compile it into a dynamic link library file according to TDengine's specifications.
Prepare the UDF source code `bit_and.c` as described earlier. For example, on a Linux operating system, execute the following command to compile into a dynamic link library file.

```shell
gcc -g -O0 -fPIC -shared bit_and.c -o libbitand.so
```

It is recommended to use GCC version 7.5 or above to ensure reliable operation.

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

The data structures are described as follows:

- `SUdfDataBlock` contains the number of rows `numOfRows` and the number of columns `numOfCols`. `udfCols[i]` (0 \<= i \<= numCols-1) represents each column's data, type `SUdfColumn*`.
- `SUdfColumn` includes the column's data type definition `colMeta` and the column's data `colData`.
- `SUdfColumnMeta` members are defined similarly to data type definitions in `taos.h`.
- `SUdfColumnData` can be variable-length, `varLenCol` defines variable-length data, and `fixLenCol` defines fixed-length data.
- `SUdfInterBuf` defines an intermediate structure buffer and the number of results in the buffer `numOfResult`

To better operate the above data structures, some convenience functions are provided, defined in `taosudf.h`.

### C UDF Example Code

#### Scalar Function Example [bit_and](https://github.com/taosdata/TDengine/blob/3.0/test/cases/12-UDFs/sh/bit_and.c)

`bit_and` implements the bitwise AND function for multiple columns. If there is only one column, it returns that column. `bit_and` ignores null values.

<details>
<summary>bit_and.c</summary>

```c
{{#include test/cases/12-UDFs/sh/bit_and.c}}
```

</details>

#### Aggregate Function Example 1 Returning Numeric Type [l2norm](https://github.com/taosdata/TDengine/blob/3.0/test/cases/12-UDFs/sh/l2norm.c)

`l2norm` implements the second-order norm of all data in the input columns, i.e., squaring each data point, then summing them up, and finally taking the square root.

<details>
<summary>l2norm.c</summary>

```c
{{#include test/cases/12-UDFs/sh/l2norm.c}}
```

</details>

#### Aggregate Function Example 2 Returning String Type [max_vol](https://github.com/taosdata/TDengine/blob/3.0/test/cases/12-UDFs/sh/max_vol.c)

`max_vol` implements finding the maximum voltage from multiple input voltage columns, returning a composite string value consisting of the device ID + the position (row, column) of the maximum voltage + the maximum voltage value.

Create table:

```shell
create table battery(ts timestamp, vol1 float, vol2 float, vol3 float, deviceId varchar(16));
```

Create custom function:

```shell
create aggregate function max_vol as '/root/udf/libmaxvol.so' outputtype binary(64) bufsize 10240 language 'C'; 
```

Use custom function:

```shell
select max_vol(vol1, vol2, vol3, deviceid) from battery;
```

<details>
<summary>max_vol.c</summary>

```c
{{#include test/cases/12-UDFs/sh/max_vol.c}}
```

</details>

#### Aggregate Function Example 3 Split string and calculate average value [extract_avg](https://github.com/taosdata/TDengine/blob/3.0/test/cases/12-UDFs/sh/extract_avg.c)

The `extract_avg` function converts a comma-separated string sequence into a set of numerical values, counts the results of all rows, and calculates the final average. Note when implementing:

- `interBuf->numOfResult` needs to return 1 or 0 and cannot be used for count.
- Count can use additional caches, such as the `SumCount` structure.
- Use `varDataVal` to obtain the string.

Create table:

```shell
create table scores(ts timestamp, varStr varchar(128));
```

Create custom function:

```shell
create aggregate function extract_avg as '/root/udf/libextract_avg.so' outputtype double bufsize 16 language 'C'; 
```

Use custom function:

```shell
select extract_avg(valStr) from scores;
```

Generate `.so` file

```bash
gcc -g -O0 -fPIC -shared extract_vag.c -o libextract_avg.so
```

<details>
<summary>extract_avg.c</summary>

```c
{{#include test/cases/12-UDFs/sh/extract_avg.c}}
```

</details>

## Developing UDFs in Python Language

### Environment Setup

The specific steps to prepare the environment are as follows:

- Step 1, prepare the Python runtime environment. If you compile and install Python locally, be sure to enable the `--enable-shared` option, otherwise the subsequent installation of taospyudf will fail due to failure to generate a shared library.
- Step 2, install the Python package taospyudf. The command is as follows.

    ```shell
    pip3 install taospyudf
    ```

- Step 3, execute the command ldconfig.
- Step 4, start the taosd service.

The installation process will compile C++ source code, so cmake and gcc must be present on the system. The compiled libtaospyudf.so file will automatically be copied to the /usr/local/lib/ directory, so if you are not a root user, you need to add sudo during installation. After installation, you can check if this file is in the directory:

```shell
root@server11 ~/udf $ ls -l /usr/local/lib/libtaos*
-rw-r--r-- 1 root root 671344 May 24 22:54 /usr/local/lib/libtaospyudf.so
```

### Interface Definition

When developing UDFs in Python, you need to implement the specified interface functions. The specific requirements are as follows.

- Scalar functions need to implement the scalar interface function process.
- Aggregate functions need to implement the aggregate interface functions start, reduce, finish.
- If initialization is needed, the init function should be implemented.
- If cleanup work is needed, implement the destroy function.

#### Scalar Function Interface

The interface for scalar functions is as follows.

```python
def process(input: datablock) -> tuple[output_type]:
```

The main parameters are as follows:

- input: datablock is similar to a two-dimensional matrix, read the python object located at row and col through the member method data(row, col)
- The return value is a tuple of Python objects, each element type as the output type.

#### Aggregate Function Interface

The interface for aggregate functions is as follows.

```python
def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
```

The above code defines 3 functions, each used to implement a custom aggregate function. The specific process is as follows.

First, the start function is called to generate the initial result buffer. This result buffer is used to store the internal state of the aggregate function, which is continuously updated as input data is processed.

Then, the input data is divided into multiple row data blocks. For each row data block, the reduce function is called, and the current row data block (inputs) and the current intermediate result (buf) are passed as parameters. The reduce function updates the internal state of the aggregate function based on the input data and current state, and returns a new intermediate result.

Finally, when all row data blocks have been processed, the finish function is called. This function takes the final intermediate result (buf) as a parameter and generates the final output from it. Due to the nature of aggregate functions, the final output can only contain 0 or 1 data entries. This output result is returned to the caller as the result of the aggregate function calculation.

#### Initialization and Destruction Interface

The interfaces for initialization and destruction are as follows.

```python
def init()
def destroy()
```

Parameter description:

- `init` completes the initialization work
- `destroy` completes the cleanup work

**Note** When developing UDFs in Python, you must define both `init` and `destroy` functions

### Scalar Function Template

The template for developing scalar functions in Python is as follows.

```python
def init():
    # initialization
def destroy():
    # destroy
def process(input: datablock) -> tuple[output_type]:  
```

### Aggregate Function Template

The template for developing aggregate functions in Python is as follows.

```python
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

The table below describes the mapping between TDengine SQL data types and Python data types. Any type of NULL value is mapped to Python's None value.

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

This article includes 5 example programs, ranging from basic to advanced, and also contains numerous practical debugging tips.

Note: **Within UDF, logging cannot be done using the print function; you must write to a file or use Python's built-in logging library.**

#### Example One

Write a UDF function that only accepts a single integer: Input n, output ln(n^2 + 1).
First, write a Python file, located in a system directory, such as `/root/udf/myfun.py` with the following content.

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

This file contains 3 functions, `init` and `destroy` are empty functions, they are the lifecycle functions of UDF, even if they do nothing, they must be defined. The most crucial is the `process` function, which accepts a data block. This data block object has two methods.

1. `shape()` returns the number of rows and columns of the data block
1. `data(i, j)` returns the data at row i, column j

The scalar function's `process` method must return as many rows of data as there are in the data block. The above code ignores the number of columns, as it only needs to compute each row's first column.

Next, create the corresponding UDF function, execute the following statement in the TDengine CLI.

```sql
create function myfun as '/root/udf/myfun.py' outputtype double language 'Python'
```

```shell
taos> create function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
Create OK, 0 row(s) affected (0.005202s)
```

It looks smooth, next let's check all the custom functions in the system to confirm it was created successfully.

```text
taos> show functions;
              name              |
=================================
 myfun                          |
Query OK, 1 row(s) in set (0.005767s)
```

Generate test data, you can execute the following commands in the TDengine CLI.

```sql
create database test;
create table t(ts timestamp, v1 int, v2 int, v3 int);
insert into t values('2023-05-01 12:13:14', 1, 2, 3);
insert into t values('2023-05-03 08:09:10', 2, 3, 4);
insert into t values('2023-05-10 07:06:05', 3, 4, 5);
```

Test the myfun function.

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.011088s)
```

Unfortunately, the execution failed. What could be the reason? Check the taosudf process logs.

```shell
tail -10 /var/log/taos/taosudf.log
```

Found the following error messages.

```text
05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
```

The error is clear: the Python plugin `libtaospyudf.so` was not loaded. If you encounter this error, please refer to the previous section on setting up the environment.

After fixing the environment error, execute again as follows.

```sql
taos> select myfun(v1) from t;
         myfun(v1)         |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

With this, we have completed our first UDF and learned some basic debugging methods.

#### Example 2

Although the myfun function passed the test, it has two drawbacks.

1. This scalar function only accepts 1 column of data as input, and it will not throw an exception if multiple columns are passed.

```sql
taos> select myfun(v1, v2) from t;
       myfun(v1, v2)       |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

1. It does not handle null values. We expect that if the input contains null, it will throw an exception and terminate execution. Therefore, the process function is improved as follows.

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

Passing two arguments to myfun will result in a failure.

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.014643s)
```

Custom exception messages are logged in the plugin log file `/var/log/taos/taospyudf.log`.

```text
2023-05-24 23:21:06.790 ERROR [1666188] [doPyUdfScalarProc@507] call pyUdfScalar proc function. context 0x7faade26d180. error: Exception: require 1 parameter but given 2

At:
  /var/lib/taos//.udf/myfun_3_1884e1281d9.py(12): process

```

Thus, we have learned how to update UDFs and view the error logs output by UDFs.
(Note: If the UDF does not take effect after an update, in versions prior to TDengine 3.0.5.0 (not inclusive), it is necessary to restart taosd, while in version 3.0.5.0 and later, restarting taosd is not required for the update to take effect.)

#### Example Three

Input (x1, x2, ..., xn), output the sum of each value and its index multiplied: `1 *x1 + 2* x2 + ... + n * xn`. If x1 to xn contain null, the result is null.

This example differs from Example One in that it can accept any number of columns as input and needs to process each column's value. Write the UDF file /root/udf/nsum.py.

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

Write a UDF that takes a timestamp as input and outputs the next closest Sunday. For example, if today is 2023-05-25, then the next Sunday is 2023-05-28.
To complete this function, you need to use the third-party library moment. First, install this library.

```shell
pip3 install moment
```

Then write the UDF file `/root/udf/nextsunday.py`.

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

The UDF framework maps TDengine's timestamp type to Python's int type, so this function only accepts an integer representing milliseconds. The process method first checks the parameters, then uses the moment package to replace the day of the week with Sunday, and finally formats the output. The output string length is fixed at 10 characters long, so you can create the UDF function like this.

```sql
create function nextsunday as '/root/udf/nextsunday.py' outputtype binary(10) language 'Python';
```

At this point, test the function. If you started taosd with systemctl, you will definitely encounter an error.

```sql
taos> select ts, nextsunday(ts) from t;

DB error: udf function execution failure (1.123615s)
```

```shell
tail -20 taospyudf.log  
2023-05-25 11:42:34.541 ERROR [1679419] [PyUdf::PyUdf@217] py udf load module failure. error ModuleNotFoundError: No module named 'moment'
```

This is because the location of "moment" is not in the default library search path of the python udf plugin. How to confirm this? Search `taospyudf.log` with the following command.

```shell
grep 'sys path' taospyudf.log  | tail -1
```

The output is as follows

```text
2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
```

It is found that the default third-party library installation path searched by the python udf plugin is: `/lib/python3/dist-packages`, while moment is installed by default in `/usr/local/lib/python3.8/dist-packages`. Next, we modify the default library search path of the python udf plugin.
First, open the python3 command line and check the current sys.path.

```python
>>> import sys
>>> ":".join(sys.path)
'/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages'
```

Copy the output string from the script above, then edit `/var/taos/taos.cfg` and add the following configuration.

```shell
UdfdLdLibPath /usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages
```

After saving, execute `systemctl restart taosd`, then test again and there will be no errors.

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

Write an aggregate function to calculate the difference between the maximum and minimum values of a column.
The difference between aggregate functions and scalar functions is: scalar functions have multiple outputs corresponding to multiple rows of input, whereas aggregate functions have a single output corresponding to multiple rows of input. The execution process of an aggregate function is somewhat similar to the classic map-reduce framework, where the framework divides the data into several chunks, each mapper handles a chunk, and the reducer aggregates the results of the mappers. The difference is that, in the TDengine Python UDF, the reduce function has both map and reduce capabilities. The reduce function takes two parameters: one is the data it needs to process, and the other is the result of other tasks executing the reduce function. See the following example `/root/udf/myspread.py`.

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

In this example, we not only defined an aggregate function but also added the functionality to record execution logs.

1. The `init` function opens a file for logging.
1. The `log` function records logs, automatically converting the incoming object into a string and appending a newline.
1. The `destroy` function closes the log file after execution.
1. The `start` function returns the initial buffer to store intermediate results of the aggregate function, initializing the maximum value as negative infinity and the minimum value as positive infinity.
1. The `reduce` function processes each data block and aggregates the results.
1. The `finish` function converts the buffer into the final output.

Execute the following SQL statement to create the corresponding UDF.

```sql
create or replace aggregate function myspread as '/root/udf/myspread.py' outputtype double bufsize 128 language 'Python';
```

This SQL statement has two important differences from the SQL statement used to create scalar functions.

1. Added the `aggregate` keyword.
1. Added the `bufsize` keyword, which is used to specify the memory size for storing intermediate results. This value can be larger than the actual usage. In this example, the intermediate result is a tuple consisting of two floating-point arrays, which actually occupies only 32 bytes when serialized, but the specified `bufsize` is 128. You can use the Python command line to print the actual number of bytes used.

```python
>>> len(pickle.dumps((12345.6789, 23456789.9877)))
32
```

To test this function, you can see that the output of `myspread` is consistent with that of the built-in `spread` function.

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

Finally, by checking the execution log, you can see that the reduce function was executed 3 times, during which the max value was updated 4 times, and the min value was updated only once.

```shell
root@server11 /var/log/taos $ cat spread.log
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

#### Scalar Function Example [pybitand](https://github.com/taosdata/TDengine/blob/3.0/test/cases/12-UDFs/sh/pybitand.py)

`pybitand` implements the bitwise AND function for multiple columns. If there is only one column, it returns that column. `pybitand` ignores null values.

<details>
<summary>pybitand.py</summary>

```python
{{#include test/cases/12-UDFs/sh/pybitand.py}}
```

</details>

#### Aggregate Function Example [pyl2norm](https://github.com/taosdata/TDengine/blob/3.0/test/cases/12-UDFs/sh/pyl2norm.py)

`pyl2norm` calculates the second-order norm of all data in the input column, i.e., squares each data point, then sums them up, and finally takes the square root.

<details>
<summary>pyl2norm.py</summary>

```python
{{#include test/cases/12-UDFs/sh/pyl2norm.py}}
```

</details>

#### Aggregate Function Example [pycumsum](https://github.com/taosdata/TDengine/blob/3.0/test/cases/12-UDFs/sh/pycumsum.py)

`pycumsum` uses numpy to calculate the cumulative sum of all data in the input column.
<details>
<summary>pycumsum.py</summary>

```python
{{#include test/cases/12-UDFs/sh/pycumsum.py}}
```

</details>

## Managing UDFs

The process of managing UDFs in a cluster involves creating, using, and maintaining these functions. Users can create and manage UDFs in the cluster through SQL. Once created, all users in the cluster can use these functions in SQL. Since UDFs are stored on the cluster's mnode, they remain available even after the cluster is restarted.

When creating UDFs, it is necessary to distinguish between scalar functions and aggregate functions. Scalar functions accept zero or more input parameters and return a single value. Aggregate functions accept a set of input values and return a single value by performing some calculation (such as summing, counting, etc.) on these values. If the wrong function category is declared during creation, an error will be reported when the function is called through SQL.

Additionally, users need to ensure that the input data type matches the UDF program, and the output data type of the UDF matches the `outputtype`. This means that when creating a UDF, you need to specify the correct data types for input parameters and output values. This helps ensure that when the UDF is called, the input data is correctly passed to the UDF, and the output values match the expected data types.

### Creating Scalar Functions

The SQL syntax for creating scalar functions is as follows.

```sql
CREATE [OR REPLACE] FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```

The parameters are explained as follows.

- or replace: If the function already exists, it modifies the existing function properties.
- function_name: The name of the scalar function when called in SQL.
- language: Supports C and Python languages (version 3.7 and above), default is C.
- library_path: If the programming language is C, the path is the absolute path to the library file containing the UDF implementation dynamic link library, usually pointing to a .so file. If the programming language is Python, the path is the path to the Python file containing the UDF implementation. The path needs to be enclosed in single or double quotes in English.
- output_type: The data type name of the function computation result.

### Creating Aggregate Functions

The SQL syntax for creating aggregate functions is as follows.

```sql
CREATE [OR REPLACE] AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type BUFSIZE buffer_size LANGUAGE 'Python';
```

Here, `buffer_size` represents the size of the buffer for intermediate calculation results, in bytes. The meanings of other parameters are the same as those for scalar functions.

The following SQL creates a UDF named `l2norm`.

```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
```

### Deleting UDFs

The SQL syntax for deleting a UDF with a specified name is as follows.

```sql
DROP FUNCTION function_name;
```

### Viewing UDFs

The SQL to display all currently available UDFs in the cluster is as follows.

```sql
show functions;
```

### Viewing Function Information

Each update of a UDF with the same name increases the version number by 1.

```sql
select * from ins_functions \G;     
```
