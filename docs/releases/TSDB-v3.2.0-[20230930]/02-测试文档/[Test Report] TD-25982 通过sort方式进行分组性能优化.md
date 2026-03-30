# [Test Report] TD-25982 通过sort方式进行分组性能优化

### 1. 概述：

此次优化主要是在partition by 查询时，将Partition Node替换为Sort Node以减少随机的磁盘访问次数。具体优化可参考：[Partition by + Slimit/Limit相关性能优化](https://taosdata.feishu.cn/docx/Ka8OdSOSpo4OuXxsveicDBrDnPb) 
SQL变化：
在select之后添加了 sort_for_group() hint。

### 2. 测试环境：

102.168.1.63：
CPU: Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz（2）40核
Mem: DDR4 16GB* 16
Disk: 895GB

### 3. 测试用例：

**测试版本：**
V3.1.0.2   vs   当前3.0最新代码
**数据集：**
- 1000表, 1000行数据, 共100万行.
- 1000表, 10万行数据, 共1亿行.
Partition 列分别为:
- c0 BIGINT, 唯一值共有1千/1万个. 数据集一为1千, 数据集2为1万. c3类似.
- c1 INT, 唯一值共有256.
- c2 BIGINT, 唯一值共有512.
- c3 VARCHAR, 唯一值共有1千/1万个.
用例集：

#### 3.1 Partition by column 无interval ,无 slimit， 无agg

| 数据集 |  | Partition by c0 select c0 from st partition by c0; | Partition by c1 select c1 from st partition by c1; | Partition by c2 select c2 from st partition by c2; | Partition by c3 select c2 from st partition by c3; | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 100w行 | 优化前 | 385.543ms | 345.566s | 375.037ms | 353.390ms |  |
|  | 优化后 | 516.282ms | 480.293ms | 491.362ms | 580.626ms |  |
| 1亿行 | 优化前 | 127428.385ms | 29074.699ms | 30527.115ms | 130260.248ms |  |
|  | 优化后 | 44072.928ms | 39530.360ms | 41422.549ms | 53914.523ms |  |

#### 3.2 Partition by column 无interval ,无 slimit，带agg

| 数据集 |  | Partition by c0 select count(*), c0 from st partition by c0; | Partition by c1 select count(*), c1 from st partition by c1; | Partition by c2 select count(*), c2 from st partition by c2; | Partition by c3 select count(*), c3 from st partition by c3; | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 100w行 | 优化前 | 110.508ms | 105.566ms | 98.472ms | 141.533ms |  |
|  | 优化后 | 184.346ms | 175.256ms | 166.027ms | 253.888ms |  |
| 1亿行 | 优化前 | 105554.832ms | 5959.969ms | 6997.864ms | 106635.154ms |  |
|  | 优化后 | 17657.893ms | 13725.435ms | 14695.033ms | 24960.583ms |  |

#### 3.3 Partition by column + interval

| 数据集 |  | Partition by c0 select count(*), c0 from st partition by c0 interval(1m); | Partition by c1 select count(*), c1 from st partition by c1 interval(1m); | Partition by c2 select count(*), c2 from st partition by c2 interval(1m); | Partition by c3 select count(*), c3 from st partition by c3 interval(1m); | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 100w行 | 优化前 | 115.790ms | 123.780ms | 119.591ms | 151.288ms |  |
|  | 优化后 | 227.690ms | 216.453ms | 219.453ms | 281.945ms |  |
| 1亿行 | 优化前 | 96953.594ms | 7112.903ms | 8112.939ms | 106126.588ms |  |
|  | 优化后 | 26047.809ms | 27622.068ms | 27690.269ms | 34878.296ms |  |

### 4. 总结：

1. 在数据集较小时, 分组个数较少时, 即partition操作对磁盘操作很少时, partition性能高于sort.
2. 在数据集较大时，分组个数较少时，partition node对磁盘的读写性能较快, partition性能高于sort
3. 在数据集较大时，分组个数较多时，sort性能高于partition
