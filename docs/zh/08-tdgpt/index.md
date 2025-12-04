---
sidebar_label: 体验 TDgpt
title: 体验 TDgpt
description: 通过公共数据库体验 TDgpt。
---

TDgpt 是与 TDengine 主进程 taosd 适配的外置式时序数据分析智能体，能够将时序数据分析服务无缝集成在 TDengine 的查询执行流程中。TDgpt 是一个无状态的平台，其内置了经典的统计分析模型库 Statsmodel，内嵌了 torch/Keras 等机器/深度学习框架库，而且在云服务上 TDgpt 内置支持了涛思自研时序数据基础大模型（TDtsfm）和 TimeMoE 时序基础模型。

云服务上的公共数据库**时序数据预测分析数据集**预先准备好了三个数据集（登录云服务，点击屏幕左下侧“数据库集市”），用于体验时序数据基础模型强大的预测能力和泛化能力。两个时序模型均为在该数据集上进行训练。内置的数据集存储在 forecast 数据库里面，该数据库包括三个数据表。

- electricity_demand_sub：不同时间段的电力需求数据 electricity_demands。
- exchange_2_cpc_results_sub：来源于 NAB 的公开数据集 cpc。
- exchange_2_cpm_results_sub：来源于 NAB 的公开数据集 cpm。

TDgpt 集成了时序基础模型的预测能力，使用 SQL 语句可以轻松调用时序基础模型的预测能力。

1. 通过如下 SQL 语句即可调用涛思时序基础模型预测数据。

    ``` SQL
    select _FROWTS, forecast(val, 'algo=tdtsfm_1,start=1324915200000,rows=300') from forecast.electricity_demand_sub;
    ```

2. 通过如下 SQL 可以调用 TimeMoE 时序基础模型预测数据。

    ``` SQL
    select _FROWTS, forecast(val, 'algo=timemoe-fc,start=1324915200000,rows=300') from forecast.electricity_demand_sub;
    ```
  
3. 查询完成以后，可以通过点击**图表**里面的**绘图**按钮显示数据图形。

用户可以执行 `show anodes full;` 查询当前系统预置的算法列表，各算法的使用方法可以参考 [TDgpt 文档](https://docs.taosdata.com/advanced/TDgpt/introduction/)。
