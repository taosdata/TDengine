# UDF（用户定义函数）

在有些应用场景中，应用逻辑需要的查询无法直接使用系统内置的函数来表示。利用 UDF 功能，TDengine 可以插入用户编写的处理代码并在查询中使用它们，就能够很方便地解决特殊应用场景中的使用需求。 UDF 通常以数据表中的一列数据做为输入，同时支持以嵌套子查询的结果作为输入。

从 2.2.0.0 版本开始，TDengine 支持通过 C/C++ 语言进行 UDF 定义。接下来结合示例讲解 UDF 的使用方法。

## 用 C/C++ 语言来定义 UDF

TDengine 提供 3 个 UDF 的源代码示例，分别为：
* [add_one.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c)
* [abs_max.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c)
* [demo.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/demo.c)

### 标量函数

[add_one.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c) 是结构最简单的 UDF 实现。其功能为：对传入的一个数据列（可能因 WHERE 子句进行了筛选）中的每一项，都输出 +1 之后的值，并且要求输入的列数据类型为 INT。 

这一具体的处理逻辑在函数 `void add_one(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBuf, char* tsOutput, int* numOfOutput, short otype, short obytes, SUdfInit* buf)` 中定义。这类用于实现 UDF 的基础计算逻辑的函数，我们称为 udfNormalFunc，也就是对行数据块的标量计算函数。需要注意的是，udfNormalFunc 的参数项是固定的，用于按照约束完成与引擎之间的数据交换。

- udfNormalFunc 中各参数的具体含义是：
  * data：输入数据。
  * itype：输入数据的类型。这里采用的是短整型表示法，与各种数据类型对应的值可以参见 [column_meta 中的列类型说明](https://www.taosdata.com/cn/documentation/connector#column_meta)。例如 4 用于表示 INT 型。
  * iBytes：输入数据中每个值会占用的字节数。
  * numOfRows：输入数据的总行数。
  * ts：主键时间戳在输入中的列数据(只读)。
  * dataOutput：输出数据的缓冲区，缓冲区大小为用户指定的输出类型大小 * numOfRows。
  * interBuf：中间计算结果的缓冲区，大小为用户在创建 UDF 时指定的BUFSIZE大小。通常用于计算中间结果与最终结果不一致时使用，由引擎负责分配与释放。
  * tsOutput：主键时间戳在输出时的列数据，如果非空可用于输出结果对应的时间戳。
  * numOfOutput：输出结果的个数（行数）。
  * oType：输出数据的类型。取值含义与 itype 参数一致。
  * oBytes：输出数据中每个值占用的字节数。
  * buf：用于在 UDF 与引擎间的状态控制信息传递块。


### 聚合函数

[abs_max.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c) 实现的是一个聚合函数，功能是对一组数据按绝对值取最大值。

其计算过程为：与所在查询语句相关的数据会被分为多个行数据块，对每个行数据块调用 udfNormalFunc（在本例的实现代码中，实际函数名是 `abs_max`)来生成每个子表的中间结果，再将子表的中间结果调用 udfMergeFunc（本例中，其实际的函数名是 `abs_max_merge`）进行聚合，生成超级表的最终聚合结果或中间结果。聚合查询最后还会通过 udfFinalizeFunc（本例中，其实际的函数名是 `abs_max_finalize`）再把超级表的中间结果处理为最终结果，最终结果只能含0或1条结果数据。

值得注意的是，udfNormalFunc、udfMergeFunc、udfFinalizeFunc 之间，函数名约定使用相同的前缀，此前缀即 udfNormalFunc 的实际函数名。udfMergeFunc 的函数名后缀 `_merge`、udfFinalizeFunc 的函数名后缀 `_finalize`，是 UDF 实现规则的一部分，系统会按照这些函数名后缀来调用相应功能。

- udfMergeFunc 用于对计算中间结果进行聚合，只有针对超级表的聚合查询才需要调用该函数。本例中 udfMergeFunc 对应的实现函数为 `void abs_max_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf)`，其中各参数的具体含义是：
  * data：udfNormalFunc 的输出数据数组，如果使用了 interBuf 那么 data 就是 interBuf 的数组。
  * numOfRows：data 中数据的行数。
  * dataOutput：输出数据的缓冲区，大小等于一条最终结果的大小。如果此时输出还不是最终结果，可以选择输出到 interBuf 中即data中。
  * numOfOutput：输出结果的个数（行数）。
  * buf：用于在 UDF 与引擎间的状态控制信息传递块。

- udfFinalizeFunc 用于对计算结果进行最终计算，通常用于有 interBuf 使用的场景。本例中 udfFinalizeFunc 对应的实现函数为 `void abs_max_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf)`，其中各参数的具体含义是：
  * dataOutput：输出数据的缓冲区。
  * interBuf：中间结算结果缓冲区，可作为输入。
  * numOfOutput：输出数据的个数，对聚合函数来说只能是0或者1。
  * buf：用于在 UDF 与引擎间的状态控制信息传递块。


### 其他 UDF 函数

用户 UDF 程序除了需要实现上面几个函数外，还有两个用于初始化和释放 UDF 与引擎间的状态控制信息传递块的函数。具体来说，也即对应 udfInitFunc 和 udfDestroyFunc。其函数名命名规则同样是采取以 udfNormalFunc 的实际函数名为前缀，以 `_init` 和 `_destroy` 为后缀。系统会在初始化和资源释放时调用对应名称的函数。

- udfInitFunc 用于初始化状态控制信息传递块。上例中 udfInitFunc 对应的实现函数为 `int abs_max_init(SUdfInit* buf)`，其中各参数的具体含义是：
  * buf：用于在  UDF 与引擎间的状态控制信息传递块。

- udfDestroyFunc 用于释放状态控制信息传递块。上例中 udfDestroyFunc 对应的实现函数为 `void abs_max_destroy(SUdfInit* buf)`，其中各参数的具体含义是：
  * buf：用于在  UDF 与引擎间的状态控制信息传递块。

目前该功能暂时没有实际意义，待后续扩展使用。

### UDF 实现方式的规则总结

根据 UDF 函数类型的不同，用户所要实现的功能函数也不同：
* 标量函数：UDF 中需实现 udfNormalFunc。
* 聚合函数：UDF 中需实现 udfNormalFunc、udfMergeFunc（对超级表查询）、udfFinalizeFunc。

需要注意的是，如果对应的函数不需要具体的功能，也需要实现一个空函数。

## 编译 UDF

用户定义函数的 C 语言源代码无法直接被 TDengine 系统使用，而是需要先编译为 .so 链接库，之后才能载入 TDengine 系统。

例如，按照上一章节描述的规则准备好了用户定义函数的源代码 add_one.c，那么可以执行如下指令编译得到动态链接库文件：
```bash
gcc -g -O0 -fPIC -shared add_one.c -o add_one.so
```

这样就准备好了动态链接库 add_one.so 文件，可以供后文创建 UDF 时使用了。

## 在系统中管理和使用 UDF

### 创建 UDF

用户可以通过 SQL 指令在系统中加载客户端所在主机上的 UDF 函数库（不能通过 RESTful 接口或 HTTP 管理界面来进行这一过程）。一旦创建成功，则当前 TDengine 集群的所有用户都可以在 SQL 指令中使用这些函数。UDF 存储在系统的 MNode 节点上，因此即使重启 TDengine 系统，已经创建的 UDF 也仍然可用。

在创建 UDF 时，需要区分标量函数和聚合函数。如果创建时声明了错误的函数类别，则可能导致通过 SQL 指令调用函数时出错。此外， UDF 支持输入与输出类型不一致，用户需要保证输入数据类型与 UDF 程序匹配，UDF 输出数据类型与 OUTPUTTYPE 匹配。

- 创建标量函数：`CREATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) [ BUFSIZE B ];`
  * ids(X)：标量函数未来在 SQL 指令中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  * ids(Y)：包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  * typename(Z)：此函数计算结果的数据类型，与上文中 udfNormalFunc 的 itype 参数不同，这里不是使用数字表示法，而是直接写类型名称即可；
  * B：中间计算结果的缓冲区大小，单位是字节，最小 0，最大 512，如果不使用可以不设置。

  例如，如下语句可以把 add_one.so 创建为系统中可用的 UDF：
  ```sql
  CREATE FUNCTION add_one AS "/home/taos/udf_example/add_one.so" OUTPUTTYPE INT;
  ```

- 创建聚合函数：`CREATE AGGREGATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) [ BUFSIZE B ];`
  * ids(X)：聚合函数未来在 SQL 指令中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  * ids(Y)：包含 UDF 函数实现的动态链接库的库文件绝对路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  * typename(Z)：此函数计算结果的数据类型，与上文中 udfNormalFunc 的 itype 参数不同，这里不是使用数字表示法，而是直接写类型名称即可；
  * B：中间计算结果的缓冲区大小，单位是字节，最小 0，最大 512，如果不使用可以不设置。

  关于中间计算结果的使用，可以参考示例程序[demo.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/demo.c)
  
  例如，如下语句可以把 demo.so 创建为系统中可用的 UDF：
  ```sql
  CREATE AGGREGATE FUNCTION demo AS "/home/taos/udf_example/demo.so" OUTPUTTYPE DOUBLE bufsize 14;
  ```

### 管理 UDF

- 删除指定名称的用户定义函数：`DROP FUNCTION ids(X);`
  * ids(X)：此参数的含义与 CREATE 指令中的 ids(X) 参数一致，也即要删除的函数的名字，例如 `DROP FUNCTION add_one;`。
- 显示系统中当前可用的所有 UDF：`SHOW FUNCTIONS;`

### 调用 UDF

在 SQL 指令中，可以直接以在系统中创建 UDF 时赋予的函数名来调用用户定义函数。例如：
```sql
SELECT X(c) FROM table/stable;
```

表示对名为 c 的数据列调用名为 X 的用户定义函数。SQL 指令中用户定义函数可以配合 WHERE 等查询特性来使用。

## UDF 的一些使用限制

在当前版本下，使用 UDF 存在如下这些限制：
1. 在创建和调用 UDF 时，服务端和客户端都只支持 Linux 操作系统；
2. UDF 不能与系统内建的 SQL 函数混合使用，暂不支持在一条 SQL 语句中使用多个不同名的 UDF ；
3. UDF 只支持以单个数据列作为输入；
4. UDF 只要创建成功，就会被持久化存储到 MNode 节点中；
5. 无法通过 RESTful 接口来创建 UDF；
6. UDF 在 SQL 中定义的函数名，必须与 .so 库文件实现中的接口函数名前缀保持一致，也即必须是 udfNormalFunc 的名称，而且不可与 TDengine 中已有的内建 SQL 函数重名。
