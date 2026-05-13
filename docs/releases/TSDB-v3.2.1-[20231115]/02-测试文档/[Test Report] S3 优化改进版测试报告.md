# [Test Report] S3 优化改进版测试报告

## 一、测试目的

本次是对 S3 存储功能的第三次改进，目标就是让产品能够达到商业化的水平

## 二、本次改进的内容

1、允许写入最后一级存储
2、支持标准化的 S3 接口，可对接本地部署的 S3
3、 对性能进行了优化

## 三、测试方案设计

       1、使用 taosBenchmark 测试写入功能
       2、查询测试基本的聚合查询，投影查询及 GROUP BY、interval 查询及点查及小范围查
       3、性能验证主要通过没上传 S3 时的性能和上传到 查询 后的性能做对比

## 四、验证内容

### 1、功能测试：

| 测试分类 | 测试内容 | 预期 | 结果 | 修复后 |
| --- | --- | --- | --- | --- |
| 写入 | taosBenchmark 写入，三级存储，10张子表，每子表 100W 数据 | 写入过程无报错 | 符合预期 |  |
| 上传文件 | 数据写入完成后, 经过 s3UploadDelaySec 参数配置的时间后，.data 文件开始上传 S3 服务器 | s3UploadDelaySec 能够正确生效，同时文件能够正确上传 | 符合预期 |  |
| 聚合查询 | 文件上传至S3 后，查询 count(*) sum(*) avg 函数 | 聚合查询功能都正常 | 符合预期 |  |
| 配置成三级存储，测试最后一级存储上传至 S3 |  |
| 配置成二级存储，测试最后一级存储上传至 S3 |  |
| 数据过期 | 过期后数据文件能否在 S3 被删除 | 可以被删除 | 符合预期 |  |
| 数据正确性检查 | 子表的时间间隔是 1000，根据此进行验证数据正确性 select count(*) from (select diff(ts) as dif from meters partition by tbname) where dif != 1000; | 返回0 | 符合预期 |  |
|  |  |  |  |  |
| 数据删除 | 先查询出某段时间在S3上数据的数量，删除这段时间内所有数据，再查询此段数据数量 | 预期这段时间内数据都被删除，再查询数据应为零 | 符合预期 |  |
| 删除表 | 查询到超级表总数量，再查询到要删除子表总数量，删除子表，再查询超级表总数量 | 预期等于原来的数量减去删除子表的数量，验证删除表后的数量的正确性 | 符合预期 |  |
| 删除数据库 | 删除指定数据库 | 预期是S3上数据库对应的文件也能被删除 | 符合预期 |  |
|  | 再S3 服务无法提供的情况下删除数据库 | 预期是数据库能被删除，但S3 上的文件无法删除 | 符合预期 |  |
| 异常测试 | 查询数据中间停止 taosd | 查询自动终止，无崩溃现象 | 符合预期 |  |
|  | LEVEL 2 上的 DATA 文件已经上传到S3, 停止 S3 服务，查询 first 和 last | 预期 查询 first 无法正常返回结果，因为first.记录所有的S3 服务已经停止 ，查询 last 可以正常返回，因为 last 查询的记录在LEVEL 0上，同时无崩溃现象 | 符合预期 |  |
|  | 停止S3 服务，在数据库下执行写入 LEVEL2 及查询，trim、 compact、 flush、数据库的操作 | 预期都能正常操作，和没配置S3 无差别 | 符合预期 |  |
|  | 在上传或下载期间停止服务 TAOSD 服务 | 预期是等待一小段断开连接的时间后 TAOSD 服务正常退出 | 不通过，目前看会长时间阻塞 | 取消 |
|  | 在上传期间中断网络 | 预期是停止上传，无崩溃，其它功能都正常 | 崩溃 TD-27394 | 已解决 |
|  | 在上传期间写入数据及查询数据 | 预期是功能都正常 | 符合预期 |  |
|  | 三节点其中两个节点配置了S3, 另外一个节点没有配置 | 预期这种情况是可以正常运行，S3 是以 DNODE 为单位进行配置的 |  |  |
| 集群测试 | 集群环境下多副本数据库写入，需要不同节点配置不同的BUCKET名称，进行数据写入及查询，数据删除，DATA 文件上传 | 预期这些操作都能正常执行 |  |  |
| 命令兼容性 | compact database 执行 | 正常执行，不报错 | 符合预期 |  |
|  |  |  |  |  |
|  | Trim database | 预期把需要上传的文件上传到 S3 | 符合预期 |  |
|  | Split vgroup | 预期是禁止使用 | 符合预期 |  |
|  | Restore vnode on dnode | 只删除 0 级上的 dnode2 上的vnode 文件夹，预期是可以完全恢复出来 | 符合预期 |  |
|  |  | 删除 1 级上的 dnode2 上的 vnode 文件夹，预期是可以完全恢复出来 | 符合预期 |  |
|  |  | 删除 2 级上的 dnode2 上的 vnode 文件夹，预期是可以完全恢复出来 | 符合预期 |  |
|  | Balance vgroups leader | 功能正常 | 符合预期 |  |
|  | Alter database db replica 改变副本数 | 功能正常 | 修复后符合预期 |  |


### 2、性能测试

####   (一) 腾讯云 环境（100M 带宽 8核 16G内存）：

1) BlockSize 配置不同值
BlockCacheSize = 16
SQL: Select * from d0;

| 测试分类 | 值 | 下载速度 | 结果 |
| --- | --- | --- | --- |
| BlockSize | 默认值(-1) | 400K/s | 756s |
| 1x | 4096 | 42M/s | 26s |
| 10x | 40960 | 45M/s | 31s |
| 100x | 409600 | 55M/s | 36s |

       结论： 从上图数据可以看出，并不是 BlockSize 越大越好，如果过大，会下载一些用不到的，所以反而更慢了
2) BlockCacheSize 配置不同值
     BlockSize = 4096
     SQL: Select * from d0;

|  |
|  |
| 下载速度 | 用时 | 速度速度 | 用时 |
| BlockCacheSize | 默认值(16) | 42M/s | 26s | 33M/s | 23s |
| 4x | 64 | 41M/s | 26s | 20k | 18s |
| 8x | 128 | 39M/s | 26s | 20k | 18s |

     结论： 从上图数据可以看出，BlockCacheSize 设置为原来的 4 倍 64 后，第二次的下载量几乎没有了，基本上把所有块都缓存在本地了，说明缓存功能是生效的

####   (二) 自建 S3 本地服务（100M 带宽 8核 16G内存）：

    192.168.1.51
     本环境客户端与服务器在同一台机器，所以网络带宽可以理解为无限大，在这个环境下测试的主要目的是在排除网络带宽的影响因素后，验证 HTTP 获取数据这套机器和直接访问磁盘获取数据的差距有多大。
    1张超级表，10个子表，每个子表100W 数据
    单副本测试

| 测试分类 | SQL | 返回行数 | 第一次查询 | 第二次查询 | S3 第一次 | S3 第二次 |
| --- | --- | --- | --- | --- | --- | --- |
| 投影查询 | Select * from d0 | 100W 行 | 18.6s | 18.5s | 17.7s | 17.7s |
|  | Select * from meters | 1000W 行 | 126s | 122s | 160s | 141s |
| 聚合查询 | Select count(*) from meters | 1 行 | 0.109s | 0.109s | 0.082s | 0.081s |
|  | select count(*) from d0; | 1行 | 0.030s | 0.029s | 0.012s | 0.012s |
|  | Select count(*) from meters where c0 >49338311 | 5022000 行 4970000行 | 0.637s | 0.638s | 5.71s | 5.93s |
|  | Select avg(c0) from meters | 49931089.1466 49503575.67 | 0.20s | 0.20s | 0.19s | 0.19s |
|  | Select sum(c0) from meters; | 499310891466000（结果） 495035756708000 | 0.19s | 0.19s | 0.20s | 0.19s |
|  | Select first (ts) from meters | 1 行 | 0.018s | 0.019s | 0.058s | 0.01s |
|  | Select last(ts) from meters; | 1 行 | 0.038s | 0.022s | 0.015s | 0.015s |
| 小范围查 | select * from meters where ts >='2023-11-04 00:00:00' and ts <='2023-11-05 00:00:00'; | 864010行 | 13.25s | 12.44s | 12.72s | 12.84s |
| 点查 | select * from meters where ts ='2023-11-05 00:00:00'; | 0.24s | 0.23s | 0.30s | 0.30s | 0.30s |
| 分组查询 | select count(*) from (select diff(ts) as dif from meters partition by tbname) where dif != 1000; | 0 行 | 8.22s | 8.12s | 19.2s | 18.8s |
|  | select tbname,count(*) from meters group by tbname; | 10行 | 0.13s | 0.138s | 0.13s | 0.13ss |
|  | Select count(*) from meters interval(60s) | 16667 | 0.44s | 0.45s | 5.39s | 5.40s |
|  | select * from (select count(*) as cnt from meters interval(60s)) where cnt != 600; | 1 行 结果：400 | 0.40s | 0.37s | 5.18s | 5.20s |


三副本测试：


| 测试分类 | SQL | 返回行数 | 上传 S3 前 | 上传 S3 后 |
| --- | --- | --- | --- | --- |
| 投影查询 | Select * from d0 | 100W 行 | 18.8s | 18.94s |
|  | Select * from meters | 1000W 行 | 126s | 120s |
| 聚合查询 | Select count(*) from meters | 1 行 | 0.14s | 0.13s |
|  | select count(*) from d0; | 1行 | 0.020s | 0.020s |
|  | Select count(*) from meters where c0 >49338311 | 4929541 行 行 | 11.1s | 11.4s |
|  | Select avg(c0) from meters | 49484959.376375675201416 | 0.29s | 0.23s |
|  | Select sum(c0) from meters; | 493697880819231（结果） | 0.28s | 0.23s |
|  | Select first (ts) from meters | 1 行 2023-10-25 00:00:00 | 0.013s | 0.056s |
|  | Select last(ts) from meters; | 1 行 2023-11-05 13:46:39 | 0.024s | 0.01s |
| 小范围查 | select * from meters where ts >='2023-11-04 00:00:00' and ts <='2023-11-05 00:00:00'; | 864010行 | 12.28s | 12.26s |
| 点查 | select * from meters where ts ='2023-11-05 00:00:00'; | 10 行 | 0.31s | 0.38s |
| 分组查询 | select count(*) from (select diff(ts) as dif from meters partition by tbname) where dif != 1000; | 0 行 | 7.96s | 28.92s |
|  | select tbname,count(*) from meters group by tbname; | 10行 | 0.21s | 0.17s |
|  | Select count(*) from meters interval(60s) | 16667 | 0.54s | 10.92s |
|  | select * from (select count(*) as cnt from meters interval(60s)) where cnt != 600; | 1 行 结果：400 | 8.47s | 30.67s |

        **结论：**
        红色部分为上传至 S3 后性能下降，下降的原因是这种查询情景下访问 HTTP特别频繁，每次访问一次HTTP 都需要 20ms, 都在上百次的访问，所以比在本地要慢很多，如数据。
       以上测试数据说明，在请求数据的次数不频繁的场景下，两者的速度是相当的，在需要大量频繁访问数据的场景下，HTTP 方式还是有一定延时，要慢不少。
       以上测试数据同时也表明，使用 S3 的性能主要受带宽的影响，在上面环境中带宽无限大的情况下，大部分查询是和在本地是一样的。

#### （三）新增 PAGE CACHE 模式的性能测试：

PAGE CACHE 模式比原来的 BLOCK CACHE 模式到 S3 上请求数据的精度更细，可以达到更精准访问，本次切换为了 PAGE CACHE 模式后，进行了性能测试
192.168.1.51 TAOS.CFG 配置如下:
dataDir /root/proj/s3/sim/dnode1/data0 0 1
dataDir /root/proj/s3/sim/dnode1/data1 1 0
dataDir /root/proj/s3/sim/dnode1/data2 2 0
logDir /root/proj/s3/sim/dnode1/log
s3EndPoint     http://192.168.1.51:9000
s3AccessKey    S0ccu99QXjElwfEvkFEi:epZX3gc3sheFMz4kBGwAuy7H6KTskEJaXDXmIL8L
s3BucketName   bucket-51-d1
s3UploadDelaySec 600
s3PageCacheSize 256 或 4096    即 1M 或 16 M     

   **测试结果：**

| 测试分类 | SQL | 返回行数 | 上传 S3 前 | 上传 S3 后 BLOCK CACHE | 上传 S3 后 PAGE CACHE= 1M | 上传 S3 后 PageCache= 16M |
| --- | --- | --- | --- | --- | --- | --- |
| 投影查询 | Select * from d0 | 100W 行 | 17.4s | 18.94s | 287s | 22s |
|  | Select * from meters | 1000W 行 | 140s | 120s | 458s 后 kill query | 156s |
| 聚合查询 | Select count(*) from meters | 1 行 | 0.13s | 0.13s | 0.13s | 0.12s |
|  | select count(*) from d0; | 1行 | 0.026s | 0.020s | 0.027s | 0.025s |
|  | Select count(*) from meters where c0 >49338311 | 4989000 行 行 | 1.01s | 11.4s | 14.1s | 13.9s |
|  | Select avg(c0) from meters | 49647100.30059 | 0.24s | 0.23s | 0.23s | 0.24s |
|  | Select sum(c0) from meters; | 496471003006000（结果） | 0.25s | 0.23s | 0.23s | 0.24s |
|  | Select first (ts) from meters | 1 行 2023-11-05 00:00:00 | 0.015s | 0.056s | 0.22s | 0.02s |
|  | Select last(ts) from meters; | 1 行 2023-11-16 13:46:39 | 0.024s | 0.01s | 0.02s | 0.03 |
| 小范围查 | select * from meters where ts >='2023-11-04 00:00:00' and ts <='2023-11-05 00:00:00'; | 10行 | 0.32s | 12.26s | 4.94s | 0.48s |
| 点查 | select * from meters where ts ='2023-11-05 00:00:00'; | 10 行 | 0.32s | 0.38s | 4.83s | 0.47s |
| 分组查询 | select count(*) from (select diff(ts) as dif from meters partition by tbname) where dif != 1000; | 0 行 | 8.4s | 28.92s | 14.64s | 14.9s |
|  | select tbname,count(*) from meters group by tbname; | 10行 | 0.19s | 0.17s | 0.19s | 0.18s |
|  | Select count(*) from meters interval(60s); | 16667 | 0.48s | 10.92s | 4.99s | 0.53s |
|  | select * from (select count(*) as cnt from meters interval(60s)) where cnt != 600; | 1 行 结果：400 | 0.49s | 30.67s | 4.90s | 0.53s |

（红色部分为性能变差，绿色部分为性能变好）
结论：
  1） PAGE CACHE 模式可以有效提升  interval 和 带条件过滤的 parition by 的性能，比 BLOCK CACHE 模式提升10 倍以上
 2）PAGE CACHE 配置为小缓存下（1M），会严重影响投影查询的性能, 配置为 16M 后，与本地访问速度几乎相同。

## 五、测试结论

        基本功能测试没有问题，但也存在一些在启用 s3 后难以解决的问题，详见下节使用须知。

## 六、使用须知

1. 最后一级在配置的时间上限到达后上传至 S3，如果此后再次写入最后一级上的时间范围内的数据时客户端会返回成功，查询也能查询到。但在内部无法落盘，如果系统重启这部分数据会丢失。使用时请注意：
   - 尽量一次性导入历史数据，避免多次导入同一时间段的数据
   - 如果上述条件无法实现，请配置足够大的上传延时，相当于要有足够大的本地磁盘缓存
2. 查询性能受两个因素影响较大：
   - 网络带宽，如果网络带宽足够大，可以达到和在本地磁盘相同的性能。查询 s3 上的数据的性能和网络带宽正相关。
   - 与 S3 服务的交互次数，如果所访问的数据集所涉及的文件块比较分散，则有可能会和 S3 服务之间有较多的 HTTP 交互，每次交互即使数据量很小耗时也在 20 ms 以上，总的延迟会较大。 所涉及的查询场景主要是分组聚合。
