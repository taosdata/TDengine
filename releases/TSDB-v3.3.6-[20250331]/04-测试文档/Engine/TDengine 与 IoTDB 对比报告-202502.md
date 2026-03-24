# TDengine 与 IoTDB 对比报告-202502

## 一、测试环境

    ** 测试服务器**：192.168.1.61 
    ** 操作系统**： Linux ubuntu 20
    ** 硬件配置：**
         CPU: 40C Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz
         内存  256G
         硬盘：500G **SSD**
 **     ****测试样本数据****：    **
        数据是一个厂房内，有 100 台类型相同的设备，设备每 1 毫秒输出一次电压、电流及当前设备运行状态三个采集值，每个设备采集了 1000W 次做为测试样本数据集。
        样本数据集以 CSV 文本格式记录保存，总大小 67 G。
 **     对比软件版本：**
      latest:
             3.0 分支 （2025-02-19 合入）
      new：
             TDengine 3.3.5.2 （2025-01-17 发布）
              IoTDB 1.3.3             (2024-11-21 发布)
old：【只是方便把旧版本的测试数据放到本文档中】
TDengine 3.1.1.0 （2023-8发布）
        IoTDB 1.1.2             (2023-7发布)
**      部署：**
           new版本均为单机部署安装[root@u1-61 /data4/guoxy/td_iotdb]

## 二、参数配置

1. TDengine 仅设置了vgroup参数为20。其余参数采用默认值。
2. IoTDB参数都采用默认值。

## 三、写入性能 

  把 67G 的 CSV 采集数据导入到 TDengine 和 IoTDB 中，性能指标如下：
    1）导入过程使用5线程串行导入
    2）双方都使用默认参数

| 产品 | new用时【5并发】 | new数据压缩后大小 | old用时【单线程】 | old数据压缩后大小 |
| --- | --- | --- | --- | --- |
| TDengine | 21分钟[11:15:49--11:36:40] | 785M | 50分钟 | 793M |
| IoTDB | 88分钟[10:53:53--12:21:50] | 754M | 9小时 | 1.6G |

   **结论：**
      1）TDengine 在数据写入性能方面的各项指标远超 IoTDB。
      2）但压缩率方面从比 IoTDB 高一倍降到了比对方占用空间还大了约5%，说明对方在存储优化方面提升明显。

  测试步骤：
```bash {wrap}
192.168.1.61:
cd /data4/guoxy/td_iotdb/iotdb
./start_iotdb.sh  ===启动脚本
./stop_iotdb.sh  ===停止脚本
./import_iotdb.sh  ===数据导入脚本
/data4/guoxy/td_iotdb/iotdb/code/apache-iotdb-1.3.3-all-bin/sbin/start-cli.sh ==启动cli

cd /data4/guoxy/td_iotdb/tdengine
./start.sh  ===启动脚本
taos -f create_db.sql  ===创建db、stable、table
./import_taos.sh  ===数据导入脚本
```


## 四、查询性能 

note：为了和以前的结论结构一致，暂未调整1、2、3几种查询语句及所在位置，只更新了查询的耗时变化。
           另外测试机器的不同，所以new两者之间对比，old两者之间对比。

### 1、TDengine 好于 IoTDB 的查询

|  | 查询 | TDengine 用时（秒）-latest | TDengine 用时（秒）-3.3.5.2 | IoTDB 用时（秒）-new | TDengine 用时（秒）-old | IoTDB 用时（秒）-old | TD SQL | IoTDB SQL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 投影查询 limt 1000 | 0.048357s | 0.048357s | 19.387s | 0.06s | 15.7s | select * from meters limit 1000; | select * from root.test.meters.* limit 1000; |
| 投影查询 limit 10w | 1.407692s | 1.407692s | timeout | 41s | 1876s | select * from meters limit 100000; | select * from root.test.meters.* limit 100000; |
| 投影查询 limit 100w | 1702.704081s 37.634871s(输出到/dev/null) 4.421656s(explain执行，不打印） | 1702.704081s 37.634871s(输出到/dev/null) 4.421656s(explain执行，不打印） | Timeout | 387s | Timeout | select * from meters limit 10000000; | select * from root.test.meters.* limit 1000000; |
| 子表列过滤 | 0.303078s | 0.303078s | 0.873s | 1.18s | 1.56s | SELECT count(current) from test.d0 where current<8.12; | select count(current) from root.test.meters.d0** where current<8.12; |
| 多个子表列过滤 | 0.900713s | 0.900713s | 0.073s【他们有bug，过滤为0】 | 1.5s | Timeout | SELECT count(current) from test.meters where current<8.12 and tbname in('d0','d1','d2','d3'); | select count(current) from root.test.meters.** where device in('d0','d1','d2','d3') and current<8.12; |
| 超级表列过滤 | 6.199853s | 6.199853s | 16.816s | 14.3s | Tmeout | SELECT count(current) from test.meters where current<8.12; | select count(current) from root.test.meters.** where current<8.12; |
| 分组聚合+过滤条件 | 超级表下 min | 2.721435s | 7.017016s | 8.032s | 14.8s | Timeout | Select min(voltage) from meters where current>0 partition by tbname ; | select min_value(voltage) from root.test.meters.* where current>0 align by device; |
|  | 超级表下 max | 2.793605s | 6.695612s | 4.426s | 14.7s | Timeout | select max(voltage) from meters where ts >= '2017-07-14' and ts <'2017-07-15' and current>0 partition by tbname interval(1h); | select max_value(voltage) from root.test.meters.* where current>0 group by ([2017-07-14,2017-07-15),1h) align by device; |
| 超级表 last | 0.030428s[未开缓存] 0.003252s[开启缓存] | 0.030428s[未开缓存] 0.003252s[开启缓存] | 0.050s | 0.003s | 0.021s | Select last(voltage) from meters; | select last voltage from root.test.meters.* ; |
| 超级表 last(*) | 0.321918s[未开缓存] 0.004036s[开启缓存] | 0.321918s[未开缓存] 0.004036s[开启缓存] | 0.073s | 0.0045s | 0.04s | Select last(*) from meters ; | select last * from root.test.meters.* ; |


### 2、两者相差不多的查询

|  | 查询 | TDengine 用时（秒）-latest | TDengine 用时（秒）-3.3.5.2 | IoTDB 用时（秒）-new | TDengine 用时（秒）-old | IoTDB 用时（秒）-old | TD SQL | IoTDB SQL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 超级表某个时间段内的采集个数统计 | 0.030496s | 0.030496s | 0.143s | 0.068s | 0.062s | select count(current) from meters where ts >'2017-07-14 11:40:00' and ts < '2017-07-14 12:26:39'; | select count(current) from root.test.meters.** where time >2017-07-14 11:40:00 and time < 2017-07-14 12:26:39; |
| 超级表某个时间段内的采集最大值统计 | 0.031865s | 0.031865s | 0.061s | 0.056s | 0.061s | select max(current) from meters where ts >'2017-07-14 11:40:00' and ts < '2017-07-14 12:26:39'; | select max_value(current) from root.test.meters.** where time >2017-07-14 11:40:00 and time < 2017-07-14 12:26:39; |
| 静态数量统计 | 子表数量统计 | 0.018170s | 0.018170s | 0.022s | 0.009s | 0.013s | show tables; | show devices; |


### 3、TDengine 不及 IoTDB 的查询

|  | 查询 | TDengine 用时（秒）-latest | TDengine 用时（秒）-3.3.5.2 | IoTDB 用时（秒）-new | TDengine 用时（秒）-old | IoTDB 用时（秒）-old | TD SQL | IoTDB SQL |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 超级表数量统计 | 0.040959s | 0.040959s | 0.107s | 0.076s | 0.030s | select count(*) from meters; | select count(*) from root.test.meters.**; |
| 超级表平均值统计 | 0.039031s | 0.056365s | 0.047s | 0.099s | 0.019s | select avg(current) from meters; | select avg(current) from root.test.meters.**; |
| 超级表最小值统计 | 0.039391s | 0.060238s | 0.049s | 0.12s | 0.027s | Select min(voltage) from meters; | select min_value(voltage) from root.test.meters.* ; |
| 分组查询 | Group by tbname 进行分组统计 | 0.045559s | 0.259315s | 0.111s | 1.19s | 0.09s | select count(*) from meters group by tbname; | select count(*) from root.test.meters.* align by device; |
| 超级表 interval(1h) 统计数量 | 0.150859s | 0.421976s | 0.633s | 5.8s | 0.607s | select count(voltage) from meters where ts >= '2017-07-14' and ts <'2017-07-15' partition by tbname interval(1m); | select count(voltage) from root.test.meters.* group by ([2017-07-14,2017-07-15),1m) align by device; |
| 超级表 interval(1h) 统计最小值 | 0.124360s | 0.341967s | 0.175s | 5.66s | 0.617s | select min(voltage) from meters where ts >= '2017-07-14' and ts <'2017-07-15' partition by tbname interval(1h); | select min_value(voltage) from root.test.meters.* group by ([2017-07-14,2017-07-15),1h) align by device; |
| 时间列排序 | Limit offset | 35.227953s | 35.227953s | timeout | 100秒以上 | 6.2s | SELECT ts,current,tbname from test.meters where ts > '2017-07-14 12:40:00' order by ts desc limit 100 offset 10000000; | SELECT current from root.test.meters.** where time > 2017-07-14 12:40:00 order by time desc limit 100 offset 10000000 align by device; |


### 4、 结论

**   **
    （为了和以前的结论结构一致，暂未调整上面1、2、3几种查询语句及所在位置）从3.0分支最新测试结果看，所有查询性能都优于 IoTDB。
   旧的文档结果请参考：[TDengine 与 IoTDB 对比报告](https://taosdata.feishu.cn/wiki/ORFswBO4ciXZEDkwlcjc7bYHnzd)
[TDengine 与 IoTDB集群对比报告](https://taosdata.feishu.cn/wiki/WIYmwfPXFimfvSkrHwpcyfGnnpn)
[TDengine 与 IoTDB集群多表低频对比报告](https://taosdata.feishu.cn/wiki/MEC7wkpadilX5LkApCncmCEgnbd)
