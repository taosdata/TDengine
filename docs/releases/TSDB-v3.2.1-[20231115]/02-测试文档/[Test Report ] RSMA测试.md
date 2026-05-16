# [Test Report ] RSMA测试

## 1. 测试结论

本功能需要重构，暂不测试。

## 2. 测试功能描述

最初的RSMA 描述：[Log In - Confluence](https://jira.taosdata.com:18090/pages/viewpage.action?pageId=151193191)
新的用户手册：[rsma功能](https://taosdata.feishu.cn/wiki/SVf3wv8VriIxRUkKPUzcLGmWnig) 
跟开礼讨论，该功能的直接使用场景是监控，测试可基于监控来验证性能。
之前测试的方案：https://jira.taosdata.com:18090/pages/viewpage.action?pageId=158205577

## 3. 实际应用场景

这几天测试下来，感觉功能设计出来，用法比较麻烦。 @张玮绚
1. 现有的库和超级表都需要重建，对旧数据无法使用。
2. 多列模型中，如果某数据列不是 double float，无法使用 avg 和 sum，即新建超级表就会提示失败。这样对多列总不符合预期的数据列，就无法使用该功能。
3. 每个超级表只能配置一个rollup 函数。

## 4. 功能测试内容 

本次涉及的主要是语法变更

### 4.1 SQL语法

1. Create database 的边界和异常，包括语法，单位，interval/keep值，level1/2/3和 keep123 的关系
```sql

## 5. check grammar

## 6. check unit

## 7. check value range

## 8. check relationships

create database db_rsma retentions  -:7d,1m:30d,30m:365d;   
create database db_rsma retentions  0:7d,1m:30d,30m:365d;   
create database db_rsma retentions  -1:7d,1m:30d,30m:365d;   
create database db_rsma retentions  -:7d,1m:1d,30m:365d;   
create database db_rsma retentions  -:7d,1m:5d,30m:1d;   
create database db_rsma retentions  -:7d,5m:1m,30m:1d;   
create database db_rsma retentions  -:7d,5m:30m,1m:1d;   

```

1. 数值类型计算函数只支持double/float类型；
```sql
f"create stable {dbname}.stb11 (ts timestamp, c_int int) tags (tag1 int) rollup(sum)",
f"create stable {dbname}.stb11 (ts timestamp, c_bint bigint) tags (tag1 int) rollup(sum)",
f"create stable {dbname}.stb11 (ts timestamp, c_bool bool) tags (tag1 int) rollup(sum)",
f"create stable {dbname}.stb11 (ts timestamp, c_binary binary(10)) tags (tag1 int) rollup(sum)",
f"create stable {dbname}.stb11 (ts timestamp, c_nchar nchar(10)) tags (tag1 int) rollup(sum)",
```




### 8.1 interval 和 keep 的边界值

### 8.2 过期数据测试（还在开发

   - 添加小于 level1 keep 时间戳的测试数据，预计是查询不到数据。
1. 重复添加上述同样过期数据，预期是 taosd 正常运行，查询不到数据。

### 8.3 删除数据测试（还在开发

 taosBenchmark  -f 1-insert/benchmark-tbl-rsma-alter.json 

### 8.4 多级存储测试（还在开发

本期不做

### 8.5 副本变更

### 8.6 compact

### 8.7 Split

### 8.8 redistribute

## 9. 性能测试结果

对比不添加 rsma 的库，验证性能变化。
1. 创建 rsma 库，写入数据
2. 创建普通库，设置duration 值和 rsma 库的level1的 keep值一致，写入数据
3. 对比数据文件大小，对比写入速度带下
4. 总共 1 亿条数据，1000个子表，每张子表10w 条

|  | d_rsma | d_normal | query-sql |
| --- | --- | --- | --- |
| duration | default/10days | default/10days |  |
| retentions | -:10d,1m:15d,1h:30d | * |  |
| stt | 2 | 2 |  |
| vgroups | 6 | 6 |  |
| records/s | 10w | 65w | 这里需要关注下 @徐开礼 ，通过 taosBenchmark 观察，写入速度是随着数据增多而下降，这带来的写入速度的降低不一定是可以接受的。从写入速度6869 rows/s下降到3995 rows/s. |
| query-delay | 0.436993s | 0.555822s | select count(* )from d0.st_min where ts > now-9d ; |
| data-size | 2350M | 395M | 这里需要关注下 @徐开礼 |

Data sizes 对比
```typescript
root@yw86 /home/chr $ du 2.6/TDinternal/sim/dnode1/data/vnode/vnode[2-8]  -sh
87M     2.6/TDinternal/sim/dnode1/data/vnode/vnode2
78M     2.6/TDinternal/sim/dnode1/data/vnode/vnode3
77M     2.6/TDinternal/sim/dnode1/data/vnode/vnode4
73M     2.6/TDinternal/sim/dnode1/data/vnode/vnode5
66M     2.6/TDinternal/sim/dnode1/data/vnode/vnode6
89M     2.6/TDinternal/sim/dnode1/data/vnode/vnode7
root@yw86 /home/chr $ du 2.6/TDinternal/sim/dnode1/data/vnode/vnode[3-4][0-9]  -sh
408M    2.6/TDinternal/sim/dnode1/data/vnode/vnode38
335M    2.6/TDinternal/sim/dnode1/data/vnode/vnode39
396M    2.6/TDinternal/sim/dnode1/data/vnode/vnode40
366M    2.6/TDinternal/sim/dnode1/data/vnode/vnode41
380M    2.6/TDinternal/sim/dnode1/data/vnode/vnode42
400M    2.6/TDinternal/sim/dnode1/data/vnode/vnode43
```


## 10. 测试结果

### 10.1 性能测试结果

### 10.2 性能测试结果
