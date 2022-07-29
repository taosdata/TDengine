---
sidebar_label: 自定义函数
title: 用户自定义函数
---

除了 TDengine 的内置函数以外，用户还可以编写自己的函数逻辑并加入TDengine系统中。

## 创建函数

```sql
CREATE [AGGREGATE] FUNCTION func_name AS library_path OUTPUTTYPE type_name [BUFSIZE value]
```

语法说明：

AGGREGATE：标识此函数是标量函数还是聚集函数。
func_name：函数名，必须与函数实现中udfNormalFunc的实际名称一致。
library_path：包含UDF函数实现的动态链接库的绝对路径，是在客户端侧主机上的绝对路径。
OUTPUTTYPE：标识此函数的返回类型。
BUFSIZE：中间结果的缓冲区大小，单位是字节。不设置则默认为0。最大不可超过512字节。

关于如何开发自定义函数，请参考 [UDF使用说明](../../develop/udf)。

## 删除自定义函数

```sql
DROP FUNCTION func_name
```