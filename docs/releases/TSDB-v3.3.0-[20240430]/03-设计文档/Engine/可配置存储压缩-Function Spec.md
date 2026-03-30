# 可配置存储压缩-Function Spec

## 1. 背景

很多客户在选型时考虑总拥有成本，其中磁盘空间占用比较大。存储压缩算法增强后，TDengine 的压缩比预期可再提升一倍以上。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 修改内容 |
| --- | --- | --- | --- |
| 2024/2/6 | 0.1 | 邓怡豪 | 解决 Wade 的 comments |
| 2024/2/21 | 0.2 | 邓怡豪 | 解决 Wade 的 comments |
| 2024/2/26 | 0.3 | 邓怡豪 | 根据 Meeting Review 的 comments 修改，主要是增加各数据类型的编码方法、压缩方法、压缩级别的可选值和默认值矩阵 |
|  |  |  |  |

## 3. 定义

### 3.1 压缩等级定义

   ** 一级压缩：**对数据进行编码，本质也是一种压缩
    **二级压缩：**对数据块进行压缩。
    **三级压缩：**对多个数据块进行压缩

### 3.2 压缩算法级别 

    在本文中特指二级压缩算法内部的级别，比 如 lz4，至少 8 个 level  可选，每个 level 下都有不同表现，本质是压缩率、压缩速度、解压速度之间的 tradeoff，为了避免选择困难，特简化定义为如下三种级别： 
   ** high：**压缩率最高，压缩速度和解压速度相对最差。
   ** low：** 压缩速度和解压速度最好，压缩率相对最低。
    **medium：**兼顾压缩率、压缩速度和解压速度。

### 3.3 压缩算法列表

1. 编码算法列表（一级压缩）
     simple8b, bit-packing,delta-i, delta-d, disabled( simple8B、XOR、RLE、disabled)
1. 压缩算法列表（二级压缩）
      lz4、zlib、zstd、tsz、xz、disabled 
1. 各个数据类型的默认压缩算法列表和适用范围

| 数据类型 | 可选编码算法 | 编码算法默认值 | 可选压缩算法 | 压缩算法默认值 | 压缩等级默认值 |
| --- | --- | --- | --- | --- | --- |
| null | 无编码 | 无 | 无 | 无 | 无 |
| tinyint/utinyint | simple8b | simple8b | lz4/zlib/zstd/xz | l4 | medium |
| smallint/usmallint | simple8b | simple8b | 同上 | 同上 | 同上 |
| int/uint | simple8b | simple8b | 同上 | 同上 | 同上 |
| timestamp | delta-i | delta-i | 同上 | 同上 | 同上 |
| bigint//ubigint | simple8b/delta-i | delta-i | 同上 | 同上 | 同上 |
| float | delta-d | delta-d | lz4/zlib/zstd/xz/tsz | lz4 | 同上 |
| double | delta-d | delta-d | lz4/zlib/zstd/xz/tsz | lz4 | 同上 |
| binary | disabled | disabled | lz4/zlib/zstd/xz | lz4 | 同上 |
| nchar | disabled | disabled | 同上 | lz4 | 同上 |
| bool | bit-packing | bit-packing | 同上 | lz4 | 同上 |

说明： 针对浮点类型，如果配置为tsz, 其精度由taosd的全局配置决定，如果配置为tsz, 但是没有配置有损压缩标志，默认使用lz4进行压缩。 

## 4. 行为说明

### 4.1 概述

压缩算法的标志被存储在超级表或者普通表的元信息中，具体行为如下：
**写入**： 生成数据文件的时候，按照列所指定的压缩算法压缩后写入文件。重整文件的时候，按照数据块上已存在的压缩方式进行解压，并用当前列所指定的压缩算法生成新的数据文件。 
**查询**： 按照数据块内部存储的压缩算法进行解压，并读取数据。 
**Note**:   这里压缩算法指一级压缩和二级压缩

### 4.2 SQL语法

##### 4.2.0.1 建表时指定压缩方式

```sql
CREATE [dbname.]tabname (colName colType [ENCODE 'encode_type'] [COMPRESS 'compress_type' [LEVEL 'high/mid/low'], [, other cerate_definition]...])
```

**参数说明**
tabname：超级表或者普通表名称
encode_type:   一级压缩，默认值暂时未定，需要针对各种数据类型，找出最合适的算法
compress_type:  二级压缩，默认值暂时未定，需要针对各种数据类型，找出最合适的算法
level:  特指二级压缩的级别，默认值为medium,  支持简写为 'h'/'l'/'m'
**功能说明**
   创建超级表/普通表的时候指定普通列的压缩方式，子表各个列的压缩算法继承自超级表，如果不指定压缩算法，则用默认值进行压缩。 
** 举例说明**
```sql
create TABLE t (
    ts TIMESTAMP ENCODE 'Simpple8B' COMPRESS 'lz4' level 'high',
    i int ENCODE 'XOR' COMPRESS 'gzip' Level 'high',
    j float COMPRESS 'tsz' Level 'medium',
    k int ENCODE 'XOR', 
    m BLOB COMPRESS 'disabled',
    n double 
)
```

ts 列：用 'Simpple8B' 进行一级压缩，用 'lz4' 进行二级压缩, 压缩级别为 'high'
i 列：  用 'XOR' 进行一级压缩，用 'gzip' 进行二级压缩，且压缩级别为 'high'
j 列：**  **用默认编码算法进行一级压缩，用 'tsz' 进行二级压缩，且压缩级别为 ‘medium’
K 列：用 'XOR' 进行一级压缩，用默认压缩算法进行压缩，且压缩级别为默认值 'medium'
m 列：用默认编码算法进行一级压缩，不进行二级压缩
n 列： 用默认编码算法进行一级压缩，用默认压缩算法进行二级压缩, 且压缩级别为默认值 'medium'

##### 4.2.0.2 ~~compact 时指定压缩（~~~~待~~~~定~~~~）~~

```sql
COMPACT VGROUP 2 COMPRESS 'lz4' RANGE(ts1, ts2); 
COMPACT DATABASE d1 COMPRESS 'gzip' BLOCK '100MB';
COMPACT DATABASE d2 COMPRESS 'disabled';
```

 **功能说明**
1. 进行三级压缩，数据文件拆分多个子块后对子块压缩，在 COMPACT 的时候进行
2. 不可观察，做完 COMPACT 之后，无法从任何元信息查看当前子块的压缩算法，子块本身带有自描述信息

##### 4.2.0.3 查看列的压缩方式

```sql
DESCRIBE [dbname.]tabName 
```

 **参数说明**
    tabName:  表名，可以为超级表、子表、普通表
 **功能说明**
    在原有的 describe table 输出信息中增加三列，一列为编码方式，一列为压缩方式， 最后一列为压缩 level
 **举例说明**
```sql
taos> describe meters;
             field              |          type          |   length    |        note        |     encode     |    compress    |     level      |
================================================================================================================================================
 ts                             | TIMESTAMP              |           8 |                    | delta-i        | lz4            | medium         |
 current                        | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
 voltage                        | INT                    |           4 |                    | simple8b       | lz4            | medium         |
 phase                          | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
 groupid                        | INT                    |           4 | TAG                | disabled       | disabled       | disabled       |
 location                       | VARCHAR                |          24 | TAG                | disabled       | disabled       | disabled       |
Query OK, 6 row(s) in set (0.003480s)
```

  _ts：一级压缩方式为 RLE ,  二级压缩方式 gzip,  压缩级别为 medium
  c0:  一级压缩方式为 XOR,  二级压缩方式为 disabled, 即不进行二级压缩
  t0:    tag 列不支持进行一级压缩和二级压缩，统一输出为 disabled

##### 4.2.0.4 更改列的压缩方式

```sql {wrap}
ALTER TABLE [db_name.]tabName MODIFY COLUMN colName [ENCODE 'ecode_type'] [COMPRESS 'compress_type'] [LEVEL "high"]
```

**参数说明**
 tabName:** ** 表名，可以为超级表、普通表
 colName:  待更改压缩算法的列, 只能为普通列
**功能说明**
 该命令只改变元数据存储的压缩算法标志，只有在 flush data 或者 compact 的时候，才使用新的压缩算法对数据进行压缩，并且该压缩算法的标志会被持久化到 data header  中，之后有两种情况: 
1. 如果需要和旧的 data （假设为A）进行 merge ，需要用 A header 中存储的压缩算法进行解压，并用新的压缩算法对最新 merge 生成的 data 进行压缩。 
2. 如果不需要和旧的 data 进行 merge，则新数据用新的压缩算法进行压缩， 旧的数据不受影响。

## 5. 性能

 性能暂时无法估计，并且和具体的压缩算法及其数据类型相关性很大，暂时给一个参考值，之后需要实测

| 压缩级别 | CPU 占用（越小越好） | 写入影响(越小越好) | 查询影响（越小越好） | 磁盘空间（越小越好） |
| --- | --- | --- | --- | --- |
| 无压缩 | 1 | 1 | 1 | 1 |
| high | 1.3 | 1.3 | 1.3 | 小于0.5 |
| midium | 1.2 | 1.2 | 1.2 | 约等于0.5 |
| low | 1.1 | 1.1 | 1.1 | 大于0.5 小于0.7 |

## 6. 兼容性

1. 不会对已存在的数据产生影响
2. 不支持回退，这是由于超级表/普通表的元信息发生了变更，如果回退，之前的版本无法识别这个
**主要场景列举**

| 场景 | 兼容性 |
| --- | --- |
| 存在超级表/普通表， 无数据 | 兼容 |
| 存在超级表/普通表，且已经写入数据 | 兼容 |
| 新创建超级表/普通表 | 兼容 |
| TSZ 压缩 | 是 |  | 兼容 |

## 7. 运维

  暂无

## 8. 使用场景

 希望减少磁盘空间占用并能够接受写入/查询延时有一定程度的增加

## 9. 约束和限制

1. 创建子表和自动建表均不能指定压缩方式
2. 由于支持动态更改列的压缩算法，describe table 所显示列的压缩算法和 data 数据实际用到的压缩算法并不完全一致，需要等数据完全 compact 之后，两者才能一致
3. 压缩只对普通列起作用， 对 tag 列不起作用, 如果创建表的时候，对 tag 列指定编码或者压缩方式，直接报错，如果使用动态修改 tag 列的编码方式或者压缩方式，也是报错

## 10. 常见错误和排查

  暂无
