# Data In AVEVA Historian Test (Obsolete)

## 1. Jira

TD-25998


TD-25999


## 2. Limitation

- ~~historian的live表是连接实际采集器进行数据写入，没法直接通过SQLServer进行数据写入，本次测试不开展实时数据写入测试。~~ Live库在客户环境中验证。
- historian的history表中允许同一个tagname存在多条datetime相同但value不同的数据，而datetime在TDengine中对应的主键时间戳是唯一的，这就导致存在historian多条数据同步至TDengine中只有一条的情况。
- 连接器通过SQLServer的查询获取结果，查询时间窗口采用**左闭右开区间**的方式，而当每个查询时间窗口的起始点时间戳没有数据时，因为SQLServer的查询特性，会在查询结果中新增一条该时间戳的插值记录，这样写入TDengine中的数据可能存在比historian中对应表的数据多的情况。

## 3. Functional Case

| Type | Description | Expected Results | Result | Memo |
| --- | --- | --- | --- | --- |
| connection | 正确配置地址、端口号，用户名、密码 | 连通性校验通过 | Pass |  |
|  | 配置地址，用户名，密码；端口号设置为空；
SQLServer开启1433端口 | 连通性校验通过 | Pass |  |
|  | 配置地址，用户名，密码；端口号设置为空；
SQLServer不开启1433端口 | 连通性校验不通过，拒绝访问 | Pass |  |
|  | 配置错误的用户名或密码 | 连通性校验不通过，拒绝访问 | Pass |  |
|  | 配置错误的端口号 | 连通性校验不通过，拒绝访问 | Pass |  |
| megrate | sanity流程 | 流程正常执行，以completed结束 | Pass |  |
|  | 单tagName，无agent | 写入数据正常 | Pass |  |
|  | 单tagName，有agent | 写入数据正常 | Pass |  |
|  | 多个tagName，配置子表命名规则 | 每个tagName对应一个子表 | Pass |  |
|  | 使用“*”，配置子表命名规则 | 除sys开头的tag均被同步 | Pass |  |
|  | 只能选择history表 | live表被禁灰 | Pass |  |
|  | 除datetime外，至少存在1个列和1个标签 | 全部列设置为列/标签/None时，前端校验报错 | Pass |  |
| sync | sanity流程 | 流程正常执行，不会主动结束 | Pass |  |
|  | 单tagName，无agent | 写入数据正常 | Pass |  |
|  | 单tagName，有agent | 写入数据正常 | Pass |  |
|  | 多个tagName，配置子表命名规则 | 每个tagName对应一个子表 | Pass |  |
|  | 使用“*”，配置子表命名规则 | 除sys开头的tag均被同步 | Pass |  |
|  | 可选择history和live表 |  | Pass |  |
|  | 起始时间大于终止时间会弹出异常 |  | Pass |  |
| 异常处理 | 任务执行时，设置taosx和数据源间网络100%丢包 | 任务保持running，恢复网络后，数据正常写入 | Pass |  |

## 4. Issue

TD-27818


TD-27819


TD-27820

## 5. Reliability

## 6. Performance

## 7. Compatibility

## 8. Reference

[taosX AVEVA™ Historian Source](https://taosdata.feishu.cn/wiki/R92NwYTvKiL84Gk4qVdcTtGMnjb) 
安装参考：[AVEVA™ Historian 2020.R2.SP1 Research Report](https://taosdata.feishu.cn/wiki/TjYfwPHo0iUr5JkWr3Ic3lhpndc)
安装包在nas: /public/Wonderware/InstallationPackage
