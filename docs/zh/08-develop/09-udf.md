---
sidebar_label: UDF
title: 用户自定义函数
toc_max_heading_level: 4
---

## UDF 简介

在某些应用场景中，应用逻辑需要的查询功能无法直接使用TDengine内置的函数来实现。TDengine允许编写用户自定义函数（UDF），以便解决特殊应用场景中的使用需求。UDF在集群中注册成功后，可以像系统内置函数一样在SQL中调用，就使用角度而言没有任何区别。UDF分为标量函数和聚合函数。标量函数对每行数据输出一个值，如求绝对值abs、正弦函数sin、字符串拼接函数concat等。聚合函数对多行数据输出一个值，如求平均数avg、取最大值max等。

TDengine支持用C和Python两种编程语言编写UDF。C语言编写的UDF与内置函数的性能几乎相同，Python语言编写的UDF可以利用丰富的Python运算库。为了避免UDF执行中发生异常影响数据库服务，TDengine使用了进程分离技术，把UDF的执行放到另一个进程中完成，即使用户编写的UDF崩溃，也不会影响TDengine的正常运行。

## 用 C 语言开发 UDF

使用 C 语言实现 UDF 时，需要实现规定的接口函数
- 标量函数需要实现标量接口函数 scalarfn 。
- 聚合函数需要实现聚合接口函数 aggfn_start ， aggfn ， aggfn_finish。
- 如果需要初始化，实现 udf_init；如果需要清理工作，实现udf_destroy。

接口函数的名称是 UDF 名称，或者是 UDF 名称和特定后缀（`_start`, `_finish`, `_init`, `_destroy`)的连接。列表中的scalarfn，aggfn, udf需要替换成udf函数名。

### 接口定义

在TDengine中，UDF的接口函数名称可以是UDF名称，也可以是UDF名称和特定后缀（如_start、_finish、_init、_destroy）的连接。后面内容中描述的函数名称，例如scalarfn、aggfn，需要替换成UDF名称。。

#### 标量函数接口

标量函数是一种将输入数据转换为输出数据的函数，通常用于对单个数据值进行计算和转换。标量函数的接口函数原型如下。

```c
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn *resultColumn)
```
主要参数说明如下。
- inputDataBlock：输入的数据块。
- resultColumn：输出列。

#### 聚合函数接口

聚合函数是一种特殊的函数，用于对数据进行分组和计算，从而生成汇总信息。聚合函数的工作原理如下。
- 初始化结果缓冲区：首先调用aggfn_start函数，生成一个结果缓冲区（result buffer），用于存储中间结果。
- 分组数据：相关数据会被分为多个行数据块（row data block），每个行数据块包含一组具有相同分组键（grouping key）的数据。
- 更新中间结果：对于每个数据块，调用aggfn函数更新中间结果。aggfn函数会根据聚合函数的类型（如sum、avg、count等）对数据进行相应的计算，并将计算结
果存储在结果缓冲区中。
- 生成最终结果：在所有数据块的中间结果更新完成后，调用aggfn_ﬁnish函数从结果缓冲区中提取最终结果。最终结果通常只包含0条或1条数据，具体取决于聚
合函数的类型和输入数据。

聚合函数的接口函数原型如下。

```c
int32_t aggfn_start(SUdfInterBuf *interBuf)
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf)
int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result)
```


其中 aggfn 是函数名的占位符。首先调用aggfn_start生成结果buffer，然后相关的数据会被分为多个行数据块，对每个数据块调用 aggfn 用数据块更新中间结果，最后再调用 aggfn_finish 从中间结果产生最终结果，最终结果只能含 0 或 1 条结果数据。

主要参数说明如下。
- interBuf：中间结果缓存区。
- inputBlock：输入的数据块。
- newInterBuf：新的中间结果缓冲区。
- result：最终结果。


#### 初始化和销毁接口

初始化和销毁接口是标量函数和聚合函数共同使用的接口，相关API如下。

```c
int32_t udf_init()
int32_t udf_destroy()
```

其中，udf_init函数完成初始化工作，udf_destroy函数完成清理工作。如果没有初始化工作，无须定义udf_init函数；如果没有清理工作，无须定义udf_destroy函数。

### 标量函数模板

用C语言开发标量函数的模板如下。
```c
int32_t scalarfn_init() {
    return TSDB_CODE_SUCCESS;
}
int32_t scalarfn(SUdfDataBlock* inputDataBlock, SUdfColumn* resultColumn) {
    return TSDB_CODE_SUCCESS;
}
int32_t scalarfn_destroy() {
    return TSDB_CODE_SUCCESS;
}
```
### 聚合函数模板

用C语言开发聚合函数的模板如下。
```c
int32_t aggfn_init() {
    return TSDB_CODE_SUCCESS;
}
int32_t aggfn_start(SUdfInterBuf* interBuf) {
    return TSDB_CODE_SUCCESS;
}
int32_t aggfn(SUdfDataBlock* inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
    return TSDB_CODE_SUCCESS;
}
int32_t int32_t aggfn_finish(SUdfInterBuf* interBuf, SUdfInterBuf *result) {
    return TSDB_CODE_SUCCESS;
}
int32_t aggfn_destroy() {
    return TSDB_CODE_SUCCESS;
}
```

### 编译

在TDengine中，为了实现UDF，需要编写C语言源代码，并按照TDengine的规范编译为动态链接库文件。
按照前面描述的规则，准备UDF的源代码bit_and.c。以Linux操作系统为例，执行如下指令，编译得到动态链接库文件。
```shell
gcc-g-O0-fPIC-sharedbit_and.c-olibbitand.so
```

为了保证可靠运行，推荐使用7.5及以上版本的GCC。

## 用 Python 语言开发 UDF

### 准备环境
  
准备环境的具体步骤如下：
- 第1步，准备好Python运行环境。
- 第2步，安装Python包taospyudf。命令如下。
    ```shell
    pip3 install taospyudf
    ```
- 第3步，执行命令ldconfig。
- 第4步，启动taosd服务。

### 接口定义

当使用Python语言开发UDF时，需要实现规定的接口函数。具体要求如下。
- 标量函数需要实现标量接口函数process。
- 聚合函数需要实现聚合接口函数start、reduce、finish。
- 如果需要初始化，则应实现函数init。
- 如果需要清理工作，则实现函数destroy。

#### 标量函数接口

标量函数的接口如下。
```Python
def process(input: datablock) -> tuple[output_type]:
```

主要参数说明如下：
- input:datablock 类似二维矩阵，通过成员方法 data(row,col)返回位于 row 行，col 列的 python 对象
- 返回值是一个 Python 对象元组，每个元素类型为输出类型。

#### 聚合函数接口

聚合函数的接口如下。
```Python
def start() -> bytes:
def reduce(inputs: datablock, buf: bytes) -> bytes
def finish(buf: bytes) -> output_type:
```

上述代码定义了3个函数，分别用于实现一个自定义的聚合函数。具体过程如下。

首先，调用start函数生成最初的结果缓冲区。这个结果缓冲区用于存储聚合函数的内部状态，随着输入数据的处理而不断更新。

然后，输入数据会被分为多个行数据块。对于每个行数据块，调用reduce函数，并将当前行数据块（inputs）和当前的中间结果（buf）作为参数传递。reduce函数会根据输入数据和当前状态来更新聚合函数的内部状态，并返回新的中间结果

最后，当所有行数据块都处理完毕后，调用finish函数。这个函数接收最终的中间结果（buf）作为参数，并从中生成最终的输出。由于聚合函数的特性，最终输出只能包含0条或1条数据。这个输出结果将作为聚合函数的计算结果返回给调用者。

#### 初始化和销毁接口

初始化和销毁的接口如下。
```Python
def init()
def destroy()
```

参数说明：
- init 完成初始化工作
- destroy 完成清理工作

**注意** 用Python开发UDF时必须定义init函数和destroy函数

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
    # use inputs.data(i,j) to access python object of location(i,j)
    # serialize new_state into new_state_bytes
    return new_state_bytes   
def finish(buf: bytes) -> output_type:
    #return obj of type outputtype   
```

### 数据类型映射

下表描述了TDengine SQL 数据类型和 Python 数据类型的映射。任何类型的 NULL 值都映射成 Python 的 None 值。

|  **TDengine SQL数据类型**   | **Python数据类型** |
| :-----------------------: | ------------ |
|TINYINT / SMALLINT / INT  / BIGINT     | int   |
|TINYINT UNSIGNED / SMALLINT UNSIGNED / INT UNSIGNED / BIGINT UNSIGNED | int |
|FLOAT / DOUBLE | float |
|BOOL | bool |
|BINARY / VARCHAR / NCHAR | bytes|
|TIMESTAMP | int |
|JSON and other types | 不支持 |

### 开发示例

本文内容由浅入深包括 5 个示例程序，同时也包含大量实用的 debug 技巧。

注意：**UDF 内无法通过 print 函数输出日志，需要自己写文件或用 python 内置的 logging 库写文件**。

#### 示例一

编写一个只接收一个整数的 UDF 函数： 输入 n， 输出 ln(n^2 + 1)。
首先编写一个 Python 文件，存在系统某个目录，比如 /root/udf/myfun.py 内容如下

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

这个文件包含 3 个函数， init 和 destroy 都是空函数，它们是 UDF 的生命周期函数，即使什么都不做也要定义。最关键的是 process 函数， 它接受一个数据块，这个数据块对象有两个方法：
1. shape() 返回数据块的行数和列数
2. data(i, j) 返回 i 行 j 列的数据
标量函数的 process 方法传人的数据块有多少行，就需要返回多少个数据。上述代码中我们忽略的列数，因为我们只想对每行的第一个数做计算。
接下来我们创建对应的 UDF 函数，在 TDengine CLI 中执行下面语句：

```sql
create function myfun as '/root/udf/myfun.py' outputtype double language 'Python'
```
其输出如下

```shell
 taos> create function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
Create OK, 0 row(s) affected (0.005202s)
```

看起来很顺利，接下来 show 一下系统中所有的自定义函数，确认创建成功：

```text
taos> show functions;
              name              |
=================================
 myfun                          |
Query OK, 1 row(s) in set (0.005767s)
```

接下来就来测试一下这个函数，测试之前先执行下面的 SQL 命令，制造些测试数据，在 TDengine CLI 中执行下述命令

```sql
create database test;
create table t(ts timestamp, v1 int, v2 int, v3 int);
insert into t values('2023-05-01 12:13:14', 1, 2, 3);
insert into t values('2023-05-03 08:09:10', 2, 3, 4);
insert into t values('2023-05-10 07:06:05', 3, 4, 5);
```

测试 myfun 函数：

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.011088s)
```

不幸的是执行失败了，什么原因呢？
查看 udfd 进程的日志

```shell
tail -10 /var/log/taos/udfd.log
```

发现以下错误信息：

```text
05/24 22:46:28.733545 01665799 UDF ERROR can not load library libtaospyudf.so. error: operation not permitted
05/24 22:46:28.733561 01665799 UDF ERROR can not load python plugin. lib path libtaospyudf.so
```

错误很明确：没有加载到 Python 插件 libtaospyudf.so，如果遇到此错误，请参考前面的准备环境一节。

修复环境错误后再次执行，如下：

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

上面的 myfun 虽然测试测试通过了，但是有两个缺点：

1. 这个标量函数只接受 1 列数据作为输入，如果用户传入了多列也不会抛异常。

```sql
taos> select myfun(v1, v2) from t;
       myfun(v1, v2)       |
============================
               0.693147181 |
               1.609437912 |
               2.302585093 |
```

2. 没有处理 null 值。我们期望如果输入有 null，则会抛异常终止执行。
因此 process 函数改进如下：

```python
def process(block):
    rows, cols = block.shape()
    if cols > 1:
        raise Exception(f"require 1 parameter but given {cols}")
    return [ None if block.data(i, 0) is None else log(block.data(i, 0) ** 2 + 1) for i in range(rows)]
```

然后执行下面的语句更新已有的 UDF：

```sql
create or replace function myfun as '/root/udf/myfun.py' outputtype double language 'Python';
```

再传入 myfun 两个参数，就会执行失败了

```sql
taos> select myfun(v1, v2) from t;

DB error: udf function execution failure (0.014643s)
```

但遗憾的是我们自定义的异常信息没有展示给用户，而是在插件的日志文件 /var/log/taos/taospyudf.log  中：

```text
2023-05-24 23:21:06.790 ERROR [1666188] [doPyUdfScalarProc@507] call pyUdfScalar proc function. context 0x7faade26d180. error: Exception: require 1 parameter but given 2

At:
  /var/lib/taos//.udf/myfun_3_1884e1281d9.py(12): process

```

至此，我们学会了如何更新 UDF，并查看 UDF 输出的错误日志。
（注：如果 UDF 更新后未生效，在 TDengine 3.0.5.0 以前（不含）的版本中需要重启 taosd，在 3.0.5.0 及之后的版本中不需要重启 taosd 即可生效。）

#### 示例三

编写一个 UDF：输入（x1, x2, ..., xn）, 输出每个值和它们的序号的乘积的和： 1 *  x1 + 2 * x2 + ... + n * xn。如果 x1 至 xn 中包含 null，则结果为 null。
这个示例与示例一的区别是，可以接受任意多列作为输入，且要处理每一列的值。编写 UDF 文件 /root/udf/nsum.py：

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

创建 UDF：

```sql
create function nsum as '/root/udf/nsum.py' outputtype double language 'Python';
```

测试 UDF：

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
完成这个函数要用到第三方库 momen。先安装这个库：

```shell
pip3 install moment
```

然后编写 UDF 文件 /root/udf/nextsunday.py

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

UDF 框架会将 TDengine 的 timestamp 类型映射为 Python 的 int 类型，所以这个函数只接受一个表示毫秒数的整数。process 方法先做参数检查，然后用 moment 包替换时间的星期为星期日，最后格式化输出。输出的字符串长度是固定的10个字符长，因此可以这样创建 UDF 函数：

```sql
create function nextsunday as '/root/udf/nextsunday.py' outputtype binary(10) language 'Python';
```

此时测试函数，如果你是用 systemctl 启动的 taosd，肯定会遇到错误：

```sql
taos> select ts, nextsunday(ts) from t;

DB error: udf function execution failure (1.123615s)
```

```shell
 tail -20 taospyudf.log  
2023-05-25 11:42:34.541 ERROR [1679419] [PyUdf::PyUdf@217] py udf load module failure. error ModuleNotFoundError: No module named 'moment'
```

这是因为 “moment” 所在位置不在 python udf 插件默认的库搜索路径中。怎么确认这一点呢？通过以下命令搜索 taospyudf.log:

```shell
grep 'sys path' taospyudf.log  | tail -1
```

输出如下

```text
2023-05-25 10:58:48.554 INFO  [1679419] [doPyOpen@592] python sys path: ['', '/lib/python38.zip', '/lib/python3.8', '/lib/python3.8/lib-dynload', '/lib/python3/dist-packages', '/var/lib/taos//.udf']
```

发现 python udf 插件默认搜索的第三方库安装路径是： /lib/python3/dist-packages，而 moment 默认安装到了 /usr/local/lib/python3.8/dist-packages。下面我们修改 python udf 插件默认的库搜索路径。
先打开 python3 命令行，查看当前的 sys.path

```python
>>> import sys
>>> ":".join(sys.path)
'/usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages'
```

复制上面脚本的输出的字符串，然后编辑 /var/taos/taos.cfg 加入以下配置：

```shell
UdfdLdLibPath /usr/lib/python3.8:/usr/lib/python3.8/lib-dynload:/usr/local/lib/python3.8/dist-packages:/usr/lib/python3/dist-packages
```

保存后执行 systemctl restart taosd, 再测试就不报错了：

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
聚合函数与标量函数的区别是：标量函数是多行输入对应多个输出，聚合函数是多行输入对应一个输出。聚合函数的执行过程有点像经典的 map-reduce 框架的执行过程，框架把数据分成若干块，每个 mapper 处理一个块，reducer 再把 mapper 的结果做聚合。不一样的地方在于，对于 TDengine Python UDF 中的 reduce 函数既有 map 的功能又有 reduce 的功能。reduce 函数接受两个参数：一个是自己要处理的数据，一个是别的任务执行 reduce 函数的处理结果。如下面的示例 /root/udf/myspread.py:

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

在这个示例中我们不光定义了一个聚合函数，还添加记录执行日志的功能，讲解如下：
1. init 函数不再是空函数，而是打开了一个文件用于写执行日志
2. log 函数是记录日志的工具，自动将传入的对象转成字符串，加换行符输出
3. destroy 函数用来在执行结束关闭文件
4. start 返回了初始的 buffer，用来存聚合函数的中间结果，我们把最大值初始化为负无穷大，最小值初始化为正无穷大
5. reduce 处理每个数据块并聚合结果
6. finish 函数将最终的 buffer 转换成最终的输出 
执行下面的 SQL语句创建对应的 UDF：

```sql
create or replace aggregate function myspread as '/root/udf/myspread.py' outputtype double bufsize 128 language 'Python';
```

这个 SQL 语句与创建标量函数的 SQL 语句有两个重要区别：
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

最后，查看我们自己打印的执行日志，从日志可以看出，reduce 函数被执行了 3 次。执行过程中 max 值被更新了 4 次， min 值只被更新 1 次。

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

## 管理 UDF 

在集群中管理UDF的过程涉及创建、使用和维护这些函数。用户可以通过SQL在集群中创建和管理UDF，一旦创建成功，集群的所有用户都可以在SQL中使用这些函数。由于UDF存储在集群的mnode上，因此即使重启集群，已经创建的UDF也仍然可用。

在创建UDF时，需要区分标量函数和聚合函数。标量函数接受零个或多个输入参数，并返回一个单一的值。聚合函数接受一组输入值，并通过对这些值进行某种计算（如求和、计数等）来返回一个单一的值。如果创建时声明了错误的函数类别，则通过SQL调用函数时会报错。

此外，用户需要确保输入数据类型与UDF程序匹配，UDF输出的数据类型与outputtype匹配。这意味着在创建UDF时，需要为输入参数和输出值指定正确的数据类型。这有助于确保在调用UDF时，输入数据能够正确地传递给UDF，并且UDF的输出值与预期的数据类型相匹配。

### 创建标量函数

创建标量函数的SQL语法如下。
```sql
CREATE FUNCTION function_name AS library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```
各参数说明如下。
- or replace：如果函数已经存在，则会修改已有的函数属性。
- function_name：标量函数在SQL中被调用时的函数名。
- language：支持C语言和Python语言（3.7及以上版本），默认为C。
- library_path：如果编程语言是C，则路径是包含UDF实现的动态链接库的库文件绝对路径，通常指向一个so文件。如果编程语言是Python，则路径是包含UDF
实现的Python文件路径。路径需要用英文单引号或英文双引号括起来。
- output_type：函数计算结果的数据类型名称。


### 创建聚合函数

创建聚合函数的SQL语法如下。
```sql
CREATE AGGREGATE FUNCTION function_name library_path OUTPUTTYPE output_type LANGUAGE 'Python';
```

其中，buffer_size 表示中间计算结果的缓冲区大小，单位是字节。其他参数的含义与标量函数相同。

如下SQL创建一个名为 l2norm 的UDF。
```sql
CREATE AGGREGATE FUNCTION l2norm AS "/home/taos/udf_example/libl2norm.so" OUTPUTTYPE DOUBLE bufsize 8;
```

### 删除 UDF

删除指定名称的 UDF 的 SQL 语法如下：
```sql
DROP FUNCTION function_name;
```

### 查看 UDF

显示集群中当前可用的所有UDF的SQL如下。
```sql
show functions;
```

