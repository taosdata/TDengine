# Data Compact

### 1. 概要

TDengine 面向多种写入场景，而很多写入场景下，TDengine 的存储会导致数据存储的放大或数据文件的空洞等。一方面影响数据的存储效率，另一方面也会影响查询效率。为了解决上述问题，TDengine 需要提供一个数据的 COMPACT 功能，将存储的数据文件重新整理，删除文件空洞和无效数据，提高数据的组织度，从而提高存储和查询的效率。

### 2. 语法

```sql
COMPACT DATABASE db_name [start with 'XXXX'] [end with 'YYYY']；
```

*需要扩展的功能请在评论中添加。*

### 3. 效果

- 扫描并压缩 DB 中所有 VGROUP 中 VNODE 的所有数据文件
- COMPACT 为异步，执行 COMPACT 命令后不会等 COMPACT 结束就会返回。如果上一个 COMPACT 没有完成则再发起一个 COMPACT 任务，则会等上一个任务完成后再返回。
- COMPCAT 会删除被删除数据以及被删除的表的数据
- COMPACT 会合并多个 STT 文件
- COMPACT 可能阻塞写入，但不阻塞查询
- COMPACT 进程不可观测
- 可通过 start with 关键字指定 COMPACT 数据的起始时间
- 可通过 end with 关键字指定 COMPACT 数据的终止时间

### 4. 后期改进工作

1. 添加 COMPACT 命令的多种配置参数，如只 COMPACT 一定时间范围的数据
2. COMPACT 进程可观测并返回各种参数
3. COMPACT 不阻塞写入
