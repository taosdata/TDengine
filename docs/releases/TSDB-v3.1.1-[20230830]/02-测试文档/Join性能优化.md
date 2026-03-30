# Join性能优化

## 1. 1:数据准备

利用客户博创联动的客户数据进行改造，同时用taosdump分别导入到2.6和3.0两个库中。
具体数据信息参考：[[博创联动] 查询结果正确性对比校验](https://taosdata.feishu.cn/wiki/wikcnd5jpHmWxa4xCcvO3BYLvbp) 
同时分别建立6个vgroup，使2.6和3.0尽量分布一致。
```sql
2.6版本
taos> show vgroups;
    vgId     |   tables    |  status  |   onlines   | v1_dnode | v1_status | compacting  |
==========================================================================================
           9 |       17000 | ready    |           1 |        1 | leader    |           0 |
          10 |       17000 | ready    |           1 |        1 | leader    |           0 |
          11 |       17000 | ready    |           1 |        1 | leader    |           0 |
          12 |       17000 | ready    |           1 |        1 | leader    |           0 |
          13 |       16636 | ready    |           1 |        1 | leader    |           0 |
          14 |       16000 | ready    |           1 |        1 | leader    |           0 |
Query OK, 6 row(s) in set (0.003018s)

3.0版本
taos> show vgroups;
  vgroup_id  |            db_name             |   tables    | v1_dnode |  v1_status  | v2_dnode |  v2_status  | v3_dnode |  v3_status  | v4_dnode |  v4_status  |  cacheload  | cacheelements | tsma |
======================================================================================================================================================================================================
          10 | report                         |       16765 |        1 | leader      | NULL     | NULL        | NULL     | NULL        | NULL     | NULL        |           0 |             0 |    0 |
          11 | report                         |       16879 |        1 | leader      | NULL     | NULL        | NULL     | NULL        | NULL     | NULL        |           0 |             0 |    0 |
          12 | report                         |       16651 |        1 | leader      | NULL     | NULL        | NULL     | NULL        | NULL     | NULL        |           0 |             0 |    0 |
          13 | report                         |       16797 |        1 | leader      | NULL     | NULL        | NULL     | NULL        | NULL     | NULL        |           0 |             0 |    0 |
          14 | report                         |       16963 |        1 | leader      | NULL     | NULL        | NULL     | NULL        | NULL     | NULL        |           0 |             0 |    0 |
          15 | report                         |       16581 |        1 | leader      | NULL     | NULL        | NULL     | NULL        | NULL     | NULL        |           0 |             0 |    0 |
Query OK, 6 row(s) in set (0.007154s)

```


## 2. 2:测试结果

由于测试机器和测试版本的变化，因此记录一下不同时期不同机器的查询时间对比，也能看出新3.0比旧3.0都有几百倍的提升，同时也比2.6提升了不少。
测试语句（只有sql9-sql16是join相关）和旧的测试时间，都是从[[博创联动] 查询结果正确性对比校验](https://taosdata.feishu.cn/wiki/wikcnd5jpHmWxa4xCcvO3BYLvbp) 里面copy过来的，供参考。

| **sql** | **2.6旧机器查询耗时** | **3.0旧机器查询耗时** | **2.6新机器查询耗时** | **3.0新机器查询耗时** | **备注** |
| --- | --- | --- | --- | --- | --- |
| 版本信息 | 23年3月初2.6版本 | 23年3月初3.0版本 | 23年8月25号版本 | 23年8.25号版本 |  |
| **sql9** | **0.723s** | **273.381s** | **2.162s** | **0.721s** |  |
| **sql10** | **0.617s** | **237.935s** | **2.679s** | **0.653s** |  |
| **sql11** | **0.666s** | **144.354s** | **1.780s** | **0.558s** |  |
| **sql12** | **0.622s** | **57.043s** | **2.739s** | **0.253s** |  |
| **sql13** | **0.667s** | **122.234** | **2.145s** | **0.475s** |  |
| **sql14** | **0.600s** | **114.288s** | **2.498s** | **0.346s** |  |
| **sql15** | **0.669s** | **140.131s** | **1.452s** | **0.512s** |  |
| **sql16** | **0.633s** | **137.562s** | **2.648s** | **0.407s** |  |
