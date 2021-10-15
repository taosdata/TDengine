# UDF（用户定义函数）

在有些应用场景中，应用逻辑需要的查询无法直接使用系统内置的函数来表示。利用 UDF 功能，TDengine 可以插入用户编写的处理代码并在查询中使用它们，就能够很方便地解决特殊应用场景中的使用需求。

从 2.2.0.0 版本开始，TDengine 支持通过 C/C++ 语言进行 UDF 定义。接下来结合示例讲解 UDF 的使用方法。

## 用 C/C++ 语言来定义 UDF

TDengine 提供 3 个 UDF 的源代码示例，分别为：
* [add_one.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c)
* [abs_max.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c)
* [sum_double.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/sum_double.c)

### 无需中间变量的标量函数

[add_one.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c) 是结构最简单的 UDF 实现。其功能为：对传入的一个数据列（可能因 WHERE 子句进行了筛选）中的每一项，都输出 +1 之后的值，并且要求输入的列数据类型为 INT。

这一具体的处理逻辑在函数 `void add_one(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBUf, char* tsOutput, int* numOfOutput, short otype, short obytes, SUdfInit* buf)` 中定义。这类用于实现 UDF 的基础计算逻辑的函数，我们称为 udfNormalFunc，也就是对行数据块的标量计算函数。需要注意的是，udfNormalFunc 的参数项是固定的，用于按照约束完成与引擎之间的数据交换。

- udfNormalFunc 中各参数的具体含义是：
  * data：存有输入的数据。
  * itype：输入数据的类型。这里采用的是短整型表示法，与各种数据类型对应的值可以参见 [column_meta 中的列类型说明](https://www.taosdata.com/cn/documentation/connector#column_meta)。例如 4 用于表示 INT 型。
  * iBytes：输入数据中每个值会占用的字节数。
  * numOfRows：输入数据的总行数。
  * ts：主键时间戳在输入中的列数据。
  * dataOutput：输出数据的缓冲区。
  * interBuf：系统使用的中间临时缓冲区，通常用户逻辑无需对 interBuf 进行处理。
  * tsOutput：主键时间戳在输出时的列数据。
  * numOfOutput：输出数据的个数。
  * oType：输出数据的类型。取值含义与 itype 参数一致。
  * oBytes：输出数据中每个值会占用的字节数。
  * buf：计算过程的中间变量缓冲区。

其中 buf 参数需要用到一个自定义结构体 SUdfInit。在这个例子中，因为 add_one 的计算过程无需用到中间变量缓存，所以可以把 SUdfInit 定义成一个空结构体。

### 无需中间变量的聚合函数

[abs_max.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c) 实现的是一个聚合函数，功能是对一组数据按绝对值取最大值。

其计算过程为：与所在查询语句相关的数据会被分为多个行数据块，对每个行数据块调用 udfNormalFunc（在本例的实现代码中，实际函数名是 `abs_max`)，再将每个数据块的计算结果调用 udfMergeFunc（本例中，其实际的函数名是 `abs_max_merge`）进行聚合，生成每个子表的聚合结果。如果查询指令涉及超级表，那么最后还会通过 udfFinalizeFunc（本例中，其实际的函数名是 `abs_max_finalize`）再把子表的计算结果聚合为超级表的计算结果。

值得注意的是，udfNormalFunc、udfMergeFunc、udfFinalizeFunc 之间，函数名约定使用相同的前缀，此前缀即 udfNormalFunc 的实际函数名。udfMergeFunc 的函数名后缀 `_merge`、udfFinalizeFunc 的函数名后缀 `_finalize`，是 UDF 实现规则的一部分，系统会按照这些函数名后缀来调用相应功能。

- udfMergeFunc 用于对计算中间结果进行聚合。本例中 udfMergeFunc 对应的实现函数为 `void abs_max_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf)`，其中各参数的具体含义是：
  * data：udfNormalFunc 的输出组合在一起的数据，也就成为了 udfMergeFunc 的输入。
  * numOfRows：data 中数据的行数。
  * dataOutput：输出数据的缓冲区。
  * numOfOutput：输出数据的个数。
  * buf：计算过程的中间变量缓冲区。

- udfFinalizeFunc 用于对计算结果进行最终聚合。本例中 udfFinalizeFunc 对应的实现函数为 `void abs_max_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf)`，其中各参数的具体含义是：
  * dataOutput：输出数据的缓冲区。对 udfFinalizeFunc 来说，其输入数据也来自于这里。
  * interBuf：系统使用的中间临时缓冲区，与 udfNormalFunc 中的同名参数含义一致。
  * numOfOutput：输出数据的个数。
  * buf：计算过程的中间变量缓冲区。

同样因为 abs_max 的计算过程无需用到中间变量缓存，所以同样是可以把 SUdfInit 定义成一个空结构体。

### 使用中间变量的聚合函数

[sum_double.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/sum_double.c) 也是一个聚合函数，功能是对一组数据输出求和结果的倍数。

出于功能演示的目的，在这个用户定义函数的实现方法中，用到了中间变量缓冲区 buf。因此，在这个源代码文件中，SUdfInit 就不再是一个空的结构体，而是定义了缓冲区的具体存储内容。

也正是因为用到了中间变量缓冲区，因此就需要对这一缓冲区进行初始化和资源释放。具体来说，也即对应 udfInitFunc（本例中，其实际的函数名是 `sum_double_init`）和 udfDestroyFunc（本例中，其实际的函数名是 `sum_double_destroy`）。其函数名命名规则同样是采取以 udfNormalFunc 的实际函数名为前缀，以 `_init` 和 `_destroy` 为后缀。系统会在初始化和资源释放时调用对应名称的函数。

- udfInitFunc 用于初始化中间变量缓冲区中的变量和内容。本例中 udfInitFunc 对应的实现函数为 `int sum_double_init(SUdfInit* buf)`，其中各参数的具体含义是：
  * buf：计算过程的中间变量缓冲区。

- udfDestroyFunc 用于释放中间变量缓冲区中的变量和内容。本例中 udfDestroyFunc 对应的实现函数为 `void sum_double_destroy(SUdfInit* buf)`，其中各参数的具体含义是：
  * buf：计算过程的中间变量缓冲区。

注意，UDF 的实现过程中需要小心处理对中间变量缓冲区的使用，如果使用不当则有可能导致内存泄露或对资源的过度占用，甚至导致系统服务进程崩溃等。

### UDF 实现方式的规则总结

根据所要实现的 UDF 类型不同，用户所要实现的功能函数内容也会有所区别：
* 无需中间变量的标量函数：结构体 SUdfInit 可以为空，需实现 udfNormalFunc。
* 无需中间变量的聚合函数：结构体 SUdfInit 可以为空，需实现 udfNormalFunc、udfMergeFunc、udfFinalizeFunc。
* 使用中间变量的标量函数：结构体 SUdfInit 需要具体定义，并需实现 udfNormalFunc、udfInitFunc、udfDestroyFunc。
* 使用中间变量的聚合函数：结构体 SUdfInit 需要具体定义，并需实现 udfNormalFunc、udfInitFunc、udfDestroyFunc、udfMergeFunc、udfFinalizeFunc。

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

在创建 UDF 时，需要区分标量函数和聚合函数。如果创建时声明了错误的函数类别，则可能导致通过 SQL 指令调用函数时出错。

- 创建标量函数：`CREATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) bufsize B;`
  * ids(X)：标量函数未来在 SQL 指令中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  * ids(Y)：包含 UDF 函数实现的动态链接库的库文件路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  * typename(Z)：此函数计算结果的数据类型，与上文中 udfNormalFunc 的 itype 参数不同，这里不是使用数字表示法，而是直接写类型名称即可；
  * B：系统使用的中间临时缓冲区大小，单位是字节，最小 0，最大 512，通常可以设置为 128。

  例如，如下语句可以把 add_one.so 创建为系统中可用的 UDF：
  ```sql
  CREATE FUNCTION add_one AS "/home/taos/udf_example/add_one.so" OUTPUTTYPE INT;
  ```

- 创建聚合函数：`CREATE AGGREGATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) bufsize B;`
  * ids(X)：聚合函数未来在 SQL 指令中被调用时的函数名，必须与函数实现中 udfNormalFunc 的实际名称一致；
  * ids(Y)：包含 UDF 函数实现的动态链接库的库文件路径（指的是库文件在当前客户端所在主机上的保存路径，通常是指向一个 .so 文件），这个路径需要用英文单引号或英文双引号括起来；
  * typename(Z)：此函数计算结果的数据类型，与上文中 udfNormalFunc 的 itype 参数不同，这里不是使用数字表示法，而是直接写类型名称即可；
  * B：系统使用的中间临时缓冲区大小，单位是字节，最小 0，最大 512，通常可以设置为 128。

  例如，如下语句可以把 abs_max.so 创建为系统中可用的 UDF：
  ```sql
  CREATE AGGREGATE FUNCTION abs_max AS "/home/taos/udf_example/abs_max.so" OUTPUTTYPE BIGINT bufsize 128;
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
2. UDF 不能与系统内建的 SQL 函数混合使用；
3. UDF 只支持以单个数据列作为输入；
4. UDF 只要创建成功，就会被持久化存储到 MNode 节点中；
5. 无法通过 RESTful 接口来创建 UDF；
6. UDF 在 SQL 中定义的函数名，必须与 .so 库文件实现中的接口函数名前缀保持一致，也即必须是 udfNormalFunc 的名称，而且不可与 TDengine 中已有的内建 SQL 函数重名。
