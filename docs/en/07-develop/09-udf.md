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

There are strict naming conventions for these interface functions. The names of the start, finish, init, and destroy interfaces must be `_start`, `_finish`, `_init`, and `_destroy`, respectively. Replace `scalarfn`, `aggfn`, and `udf` with the name of your user-defined function.

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

There are strict naming conventions for interface functions. The names of the start, finish, init, and destroy interfaces must be &lt;udf-name&gt;_start, &lt;udf-name&gt;_finish, &lt;udf-name&gt;_init, and &lt;udf-name&gt;_destroy, respectively. Replace `scalarfn`, `aggfn`, and `udf` with the name of your user-defined function.

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

- The SUdfDataBlock block includes the number of rows (numOfRows) and the number of columns (numCols). udfCols[i] (0 &lt;= i &lt;= numCols-1) indicates that each column is of type SUdfColumn.
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
Use the UDF in the query:
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

### Prepare Environment
  
1. Prepare Python Environment

Please follow standard procedure of python environment preparation.
   
2. Install Python package `taospyudf`

```shell
pip3 install taospyudf
```

During this process, some C++ code needs to be compiled. So it's required to have `cmake` and `gcc` on your system. The compiled `libtaospyudf.so` will be automatically copied to `/usr/local/lib` path. If you are not root user, please use `sudo`. After installation is done, please check using the command below.

```shell
root@slave11 ~/udf $ ls -l /usr/local/lib/libtaos*
-rw-r--r-- 1 root root 671344 May 24 22:54 /usr/local/lib/libtaospyudf.so
```

Then execute the command below.

```shell
ldconfig
```

3. If you want to utilize some 3rd party python packages in your Python UDF, please set configuration parameter `UdfdLdLibPath` to the value of `PYTHONPATH` before starting `taosd`.

4. Launch `taosd` service
   
Please refer to [Get Started](../../get-started)

### Interface definition

#### Introduction to Interface

Implement the specified interface functions when implementing a UDF in Python.
- implement `process` function for the scalar UDF.
- implement `start`, `reduce`, `finish` for the aggregate UDF.
- implement `init` for initialization and `destroy` for termination.

#### Scalar UDF Interface

The implementation of a scalar UDF is described as follows:

```Python
def process(input: datablock) -> tuple[output_type]:   
```

Description: this function processes datablock, which is the input; you can use datablock.data(row, col) to access the python object at location(row,col); the output is a tuple object consisted of objects of type outputtype

#### Aggregate UDF Interface

The implementation of an aggregate function is described as follows:

```Python
def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes   
def finish(buf: bytes) -> output_type:
```

Description: first the start() is invoked to generate the initial result `buffer`; then the input data is divided into multiple row blocks, and reduce() is invoked for each block `inputs` and current intermediate result `buf`; finally finish() is invoked to generate the final result from intermediate `buf`, the final result can only contains 0 or 1 data.

#### Initialization and Cleanup Interface

```python
def init()
def destroy()
```

Description: init() does the work of initialization before processing any data; destroy() does the work of cleanup after the data is processed.

### Python UDF Template

#### Scalar Template

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

Note：process() must be implemented, init() and destroy() must be defined too but they can do nothing.

#### Aggregate Template

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

Note: aggregate UDF requires init(), destroy(), start(), reduce() and finish() to be implemented. start() generates the initial result in buffer, then the input data is divided into multiple row data blocks, reduce() is invoked for each data block `inputs` and intermediate `buf`, finally finish() is invoked to generate final result from the intermediate result `buf`.

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

### Development Guide

In this section we will demonstrate 5 examples of developing UDF in Python language. In this guide, you will learn the development skills from easy case to hard case, the examples include: 
1. A scalar function which accepts only one integer as input and outputs ln(n^2 + 1)。
2. A scalar function which accepts n integers, like（x1, x2, ..., xn）and output the sum of the product of each input and its sequence number, i.e. x1 + 2 * x2 + ... + n * xn。
3. A scalar function which accepts a timestamp and output the next closest Sunday of the timestamp. In this case, we will demonstrate how to use 3rd party library `moment`. 
4. An aggregate function which calculates the difference between the maximum and the minimum of a specific column, i.e. same functionality of built-in spread().

In the guide, some debugging skills of using Python UDF will be explained too. 

We assume you are using Linux system and already have TDengine 3.0.4.0+ and Python 3.7+.

Note:**You can't use print() function to output log inside a UDF, you have to write the log to a specific file or use logging module of Python.**

#### Sample 1: Simplest UDF

This scalar UDF accepts an integer as input and output ln(n^2 + 1).

Firstly, please compose a Python source code file in your system and save it, e.g. `/root/udf/myfun.py`, the code is like below.

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

This program consists of 3 functions, init() and destroy() do nothing, but they have to be defined even though there is nothing to do in them because they are critical parts of a python UDF. The most important function is process(), which accepts a data block and the data block object has two methods: 
1. shape() returns the number of rows and the number of columns of the data block
2. data(i, j) returns the value at (i,j) in the block
   
The output of the process() function of a scalar UDF returns exactly same number of data as the number of input rows. We will ignore the number of columns because we just want to compute on the first column.

Then, we create the UDF using the SQL command below.

```sql
create function myfun as '/root/udf/myfun.py' outputtype double language 'Python'
```

Here is the output example, it may change a little depending on your version being used.

```shell
 taos> create function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
Create OK, 0 row(s) affected (0.005202s)
```

Then, we used the `show` command to prove the creation of the UDF is successful.

```text
taos> show functions;
              name              |
=================================
 myfun                          |
Query OK, 1 row(s) in set (0.005767s)
```

Next, we can try to test the function. Before executing the UDF, we need to prepare some data using the command below in TDengine CLI.

```sql
create database test;
create table t(ts timestamp, v1 int, v2 int, v3 int);
insert into t values('2023-05-01 12:13:14', 1, 2, 3);
insert into t values('2023-05-03 08:09:10', 2, 3, 4);
insert into t values('2023-05-10 07:06:05', 3, 4, 5);
```

Execute the UDF to test it:

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.011088s)
```

Unfortunately, the UDF execution failed. We need to check the log `udfd` daemon to find out why.

```shell
tail -10 /var/log/taos/udfd.log
```

Below is the output.

```text
05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
```

From the error message we can find out that `libtaospyudf.so` was not loaded successfully. Please refer to the [Prepare Environment] section.

After correcting environment issues, execute the UDF:

```sql
taos> select myfun(v1) from t;
         myfun(v1)         |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

Now, we have finished the first PDF in Python, and learned some basic debugging skills.

#### Sample 2: Abnormal Processing

The `myfun` UDF example in sample 1 has passed, but it has two drawbacks.

1. It the program accepts only one column of data as input, but it doesn't throw exception if you passes multiple columns.

```sql
taos> select myfun(v1, v2) from t;
       myfun(v1, v2)       |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

2. `null` value is not processed. We expect the program to throw exception and terminate if `null` is passed as input.

So, we try to optimize the process() function as below.

```python
def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception(f"require 1 parameter but given {cols}")
    return [ None if block.data(i, 0) is None else log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
```

The update the UDF with command below.

```sql
create or replace function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
```

At this time, if we pass two arguments to `myfun`, the execution would fail.

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.014643s)
```

However, the exception is not shown to end user, but displayed in the log file `/var/log/taos/taospyudf.log`

```text
2023-05-24 23:21:06.790 ERROR [1666188] [doPyUdfScalarProc@507] call pyUdfScalar proc function. context 0x7faade26d180. error: Exception: require 1 parameter but given 2

At:
  /var/lib/taos//.udf/myfun_3_1884e1281d9.py(12): process

```

Now, we have learned how to update a UDF and check the log of a UDF.

Note: Prior to TDengine 3.0.5.0 (excluding), updating a UDF requires to restart `taosd` service. After 3.0.5.0, restarting is not required. 

#### Sample 3: UDF with n arguments 

A UDF which accepts n integers, likee (x1, x2, ..., xn) and output the sum of the product of each value and its sequence number: 1 *  x1 + 2 * x2 + ... + n * xn. If there is `null` in the input, then the result is `null`. The difference from sample 1 is that it can accept any number of columns as input and process each column. Assume the program is written in /root/udf/nsum.py:

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

Crate and test the UDF:

```sql
create function nsum as '/root/udf/nsum.py' outputtype double language 'Python';
```

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

#### Sample 4: Utilize 3rd party package

A UDF which accepts a timestamp and output the next closed Sunday. This sample requires to use third party package `moment`, you need to install it firstly.

```shell
pip3 install moment
```

Then compose the Python code in /root/udf/nextsunday.py

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

UDF framework will map the TDengine timestamp to Python int type, so this function only accepts an integer representing millisecond. process() firstly validates the parameters, then use `moment` to replace the time, format the result and output. 

Create and test the UDF.

```sql
create function nextsunday as '/root/udf/nextsunday.py' outputtype binary(10) language 'Python';
```

If your `taosd` is started using `systemd`, you may encounter the error below. Next we will show how to debug.

```sql
taos> select ts, nextsunday(ts) from t;

DB error: udf function execution failure (1.123615s)
```

```shell
 tail -20 taospyudf.log  
2023-05-25 11:42:34.541 ERROR [1679419] [PyUdf::PyUdf@217] py udf load module failure. error ModuleNotFoundError: No module named 'moment'
```

This is because `moment` doesn't exist in the default library search path of python UDF, please check the log file `taosdpyudf.log`. 

```shell
grep 'sys path' taospyudf.log  | tail -1
```

```text
2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
```

You may find that the default library search path is `/lib/python3/dist-packages` (just for example, it may be different in your system), but `moment` is installed to `/usr/local/lib/python3.8/dist-packages` (for example, it may be different in your system). Then we change the library search path of python UDF.

Check `sys.path`, which must include the packages you install with pip3 command previously, as shown below:

```python
>>> import sys
>>> ":".join(sys.path)
'/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages'
```

Copy the output and edit /var/taos/taos.cfg to add below configuration parameter.

```shell
UdfdLdLibPath /usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages
```

Save it, then restart `taosd`, using `systemctl restart taosd`, and test again, it will succeed this time.

Note: If your cluster consists of multiple `taosd` instances, you have to repeat same process for each of them.

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

#### Sample 5: Aggregate Function

An aggregate function which calculates the difference of the maximum and the minimum in a column. An aggregate funnction takes multiple rows as input and output only one data. The execution process of an aggregate UDF is like map-reduce, the framework divides the input into multiple parts, each mapper processes one block and the reducer aggregates the result of the mappers. The reduce() of Python UDF has the functionality of both map() and reduce(). The reduce() takes two arguments: the data to be processed; and the result of other tasks executing reduce(). For example, assume the code is in `/root/udf/myspread.py`.

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

In this example, we implemented an aggregate function, and added some logging. 
1. init() opens a file for logging 
2. log() is the function for logging, it converts the input object to string and output with an end of line
3. destroy() closes the log file \
4. start() returns the initial buffer for storing the intermediate result 
5. reduce() processes each data block and aggregates the result
6. finish() converts the final buffer() to final result\ 

Create the UDF.

```sql
create or replace aggregate function myspread as '/root/udf/myspread.py' outputtype double bufsize 128 language 'Python';
```

This SQL command has two important different points from the command creating scalar UDF.
1. keyword `aggregate` is used
2. keyword `bufsize` is used to specify the memory size for storing the intermediate result. In this example, the result is 32 bytes, but we specified 128 bytes for `bufsize`. You can use the `python` CLI to print actual size.

```python
>>> len(pickle.dumps((12345.6789, 23456789.9877)))
32
```

Test this function, you can see the result is same as built-in spread() function. \

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

At last, check the log file, we can see that the reduce() function is executed 3 times, max value is updated 3 times and min value is updated only one time.

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

### SQL Commands

1. Create Scalar UDF

```sql
CREATE FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```

2. Create Aggregate UDF

```sql
CREATE AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```

3. Update Scalar UDF

```sql
CREATE OR REPLACE FUNCTION function_name AS OUTPUTTYPE int LANGUAGE 'Python';
```

4. Update Aggregate UDF
   
```sql
CREATE OR REPLACE AGGREGATE FUNCTION function_name AS OUTPUTTYPE BUFSIZE buf_size int LANGUAGE 'Python';
```

Note: If keyword `AGGREGATE` used, the UDF will be treated as aggregate UDF despite what it was before; Similarly, if there is no keyword `aggregate`, the UDF will be treated as scalar function despite what it was before.

5. Show the UDF
  
The version of a UDF is increased by one every time it's updated.
  
```sql
select * from ins_functions \G;     
```

6. Show and Drop existing UDF

```sql
SHOW functions;
DROP FUNCTION function_name;
```

### More Python UDF Samples

#### Scalar Function [pybitand](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pybitand.py)

The `pybitand` function implements bitwise addition for multiple columns. If there is only one column, the column is returned. The `pybitand` function ignores null values.

<details>
<summary>pybitand.py</summary>

```Python
{{#include tests/script/sh/pybitand.py}}
```

</details>

#### Aggregate Function [pyl2norm](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pyl2norm.py)

The `pyl2norm` function finds the second-order norm for all data in the input columns. This squares the values, takes a cumulative sum, and finds the square root.
<details>
<summary>pyl2norm.py</summary>

```c
{{#include tests/script/sh/pyl2norm.py}}
```

</details>

#### Aggregate Function [pycumsum](https://github.com/taosdata/TDengine/blob/3.0/tests/script/sh/pycumsum.py)

The `pycumsum` function finds the cumulative sum for all data in the input columns.
<details>
<summary>pycumsum.py</summary>

```c
{{#include tests/script/sh/pycumsum.py}}
```

</details>
## Manage and Use UDF
You need to add UDF to TDengine before using it in SQL queries. For more information about how to manage UDF and how to invoke UDF, please see [Manage and Use UDF](../taos-sql/udf/).
