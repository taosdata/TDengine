# 统计指定 DB 的用量信息-Function Spec

## 1. 背景

   目前无法通过 SQL 查看 DB 磁盘占用空间、压缩率等信息，因此开发该功能方便运维。 

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/07/30 | 0.1 | 邓怡豪 | 初稿 |
| 2024/07/31 | 0.2 | 邓怡豪 | 经过 wade review, 做了比较大的调整，主要是ins_disk_usage 系统表内容的调整 |
| 2024/08/02 | 0.3 | 邓怡豪 | 1. 删除了3项和其他table 重复的字段 1. 按照实现方案，更新一些性能/限制的内容 1. dbname下，一个快捷SQL, 用来查看当前是DB的磁盘占用，及其数据压缩率 |
|  |  |  |  |

## 3. 定义

   无

## 4. 行为说明

### 4.1 SQL 语法

#### 4.1.1 基础SQL 

```sql
 select expr from information_schema.ins_disk_usage [where condtion]
```

    **行为说明**： 查看各个vgroup 的各个组件磁盘占用情况，并且可以通过查询语句计算压缩率等。 
```sql
 taos> select * from ins_disk_usage where db_name = "test";
  db_name|vgroup_id| wal | data1| data2 |data3| cache_rdb| table_meta| s3 | raw_data|
=============================================================================================================================================================================
   test|13 | 112 | 256 | 2000| 3000| 4000| 8000| 9000| 10000
Query OK, 1 row(s) in set (0.003510s)
```

#### 4.1.2 快捷命令（直接查看当前DB上数据占用磁盘大小）

```sql
taos> show disk_info;
           _db_usage            |
=================================
 Compress_radio: NULL           |
 Disk_occupied: 4k              |
Query OK, 2 row(s) in set (0.013209s)

taos> 
```

    **行为说明**： 输出两行，分别为 Compression_radio, Disk_occupied, 具体字段内容如下：   
    **compression_ratio**: 类型为double 类型，代表当前库的压缩率, 如果估算出来raw_data 的size 小于1k,  则显示为NULL。
    **disk_occupied**:  类型为整数类型，代表当前用户**数据占用大小。 **  
    该命令本质上等同于 
 ` select sum(data1 + data2 + data3)/sum(raw_data), sum(data1 + data2 + data3) from information_schema.ins_disk_usage where db_name="dbname" `

### 4.2 新增系统表

  在 information_schema 系统库中添加 一个名为 ins_disk_usage 系统表，字段为如下列表

| 字段 | 含义 | 类型 | 单位 | 备注 |
| --- | --- | --- | --- | --- |
| db_name | Db 的名称 | Varchar | 无 | 准确值 |
| vgroup_id | Vgroup 的ID号 | 整数类型 | 无 | 准确值 |
| wal | wal 的大小 | 长整数类型 | K | 准确值 |
| data1 | 一级存储上磁盘占用大小 | 长整数类型 | K | 同上， 不包含WAL 值 |
| data2 | 二级存储上磁盘占用大小 | 长整数类型 | K | 准确值，不包含WAL值 |
| data3 | 三级存储上磁盘占用大小 | 长整数类型 | K | 准确值， |
| cache_rdb | last/last_row占用磁盘的大小 | 长整数类型 | K | 同上 |
| table_meta | Table Meta 占用磁盘大小 | 长整数类型 | K | 同上 |
| s3 | s3 上占用的数据的大小 | 长整数类型 | K | 相对准确值 |
| raw_data | 预估出来的真实数据的大小，不含mem table | 长整数类型 | k | 估算值 |

### 4.3  使用示例

```sql

taos> select sum(data1 + data2 + data3) from information_schema.ins_disk_usage where vgroup_id = 13
    > ;
Query OK, 0 row(s) in set (0.018433s)

taos> select sum(data1 + data2 + data3) from information_schema.ins_disk_usage where vgroup_id = 2 ;
 sum(data1 + data2 + data3) |
=============================
      0.000000000000000e+00 |
Query OK, 1 row(s) in set (0.014729s)

taos> select sum(data1 + data2 + data3) from information_schema.ins_disk_usage where vgroup_id = 3 ;
 sum(data1 + data2 + data3) |
=============================
      4.000000000000000e+00 |
Query OK, 1 row(s) in set (0.013212s)
```

## 5. 性能

    **主要性能瓶颈**:  需要到各个vnode上获取其基本的磁盘占用、表的数量、行数信息，行数统计等信息，需要读header block信息，其中主要瓶颈是读 header block。 
   ** 预计执行时间:   **预估为秒级，性能和`show table distributed stableName`类似， 如果 DB 上超级表或者普通表的数目为 N， 执行单个 `show table distributed table`时间为T，那么执行该查询的时间则为 N * T。  

## 6. 兼容性

-  只增加一个虚拟表，且这个表没有持久化，不更改存储结构。 
-  没有增加新消息。 
      **因此**： **可以在线升级、也可以回退，不存在兼容性问题**

## 7. 运维

     无

## 8. 使用场景

    查看磁盘占用、压缩率等信息。 

## 9. 约束和限制

约束： 本质上是一个运维工具， 且资源消耗比较大，不建议频繁调用，
限制：
-  如果有大量的删表、删数据行为，不能准确计算得到压缩率等信息，需要等到完全compact 完全结束之后，才能得到一个相对准确的压缩率，如果在compact 还没有结束时，查询得到的磁盘占用可能过大。
-  本实现统计是按**实际文件占用进行统计**且忽略了**目录本身的大小， **因此和用 `du ` 命令直接统计目录的大小存在一定的区别，两者差距一般小于1M。 
-  多副本情况下，只统计单副本的DB的占用大小
-  如果要统计单个DB 实际的磁盘占用可以用 `select sum(data1+data2+data3+wal+cache_rdb + table_meta) from ins_disk_usage where dbname = "test"`
-  文件大小的磁盘占用大小

## 10. 常见错误和排查

  无

## 11. 可观测性

  无

## 12. 安装和卸载

 无

## 13. 文档

  需要修改公开文档

## 14. 参考文档

   [需求说明：统计指定 DB 的用量信息](https://taosdata.feishu.cn/wiki/NMW3wXDAXigD6WkTBjccVgomnQf)
   [统计指定 DB 的用量信息-方案对比](https://taosdata.feishu.cn/wiki/OgoowxzGniYpBgkLNfNcK0UlnEc)

## 15. 附录

 无
