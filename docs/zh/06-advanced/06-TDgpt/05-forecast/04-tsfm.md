---
title: "时序基础模型"
sidebar_label: "时序基础模型"
---

TDgpt 安装包内置涛思数据时序基础模型和 Time-MoE [^1]两个时序基础模型。
从 3.3.6.4 版本开始，TDgpt 拓展了针对 moirai [^2], chronos[^3], timesfm [^4]三个基础时序模型的支持，需要注意的是如果要使用这三个模型，需要您在本地部署
相应的时序基础模型服务。上述三个模型的部署方式，请参考[部署时序基础模型](../09-dev/04-tsfm/index.md) 的内容。

## 功能概述

时序数据基础模型是专门训练用以处理时间序列数据预测和异常检测、数据补齐等高级时序数据分析功能的基础模型，时序基础模型继承了大模型的优良泛化能力，无需要设置复杂的输入参数，即可根据输入数据进行预测分析。

| 序号  | 参数       | 说明                   |
|-----|----------|----------------------|
| 1   | tdtsfm_1 | 涛思时序数据基础模型 v1.0      |
| 2   | time-moe | MoE 时序基础模型           |
| 3   | moirai   | SalesForce 开源的时序基础模型 |
| 4   | chronos  | Amazon 开源的时序基础模型     |
| 5   | timesfm  | Google 开源的时序基础模型     |


TDgpt 集成时序基础模型的预测能力，无需设置模型相关参数，使用 SQL 语句即可轻松调用时序基础模型的进行预测。

- 以下 SQL 语句调用涛思时序基础模型（tdtfsm）预测数据，返回 10 条预测记录。

```SQL
SELECT _frowts, FORECAST(i32, "algo=tdtsfm_1,rows=10") from foo
```

- 以下 SQL 语句调用 TimeMoE 时序基础模型预测数据，并返回 10 条预测记录。

```SQL
SELECT _frowts, FORECAST(i32, "algo=timemoe-fc,rows=10") from foo
```

## 参考文献

[^1]: Time-MoE: Billion-Scale Time Series Foundation Models with Mixture of Experts. [[paper](https://arxiv.org/abs/2409.16040)] [[GitHub Repo](https://github.com/Time-MoE/Time-MoE)]
[^2]: Moirai-MoE: Empowering Time Series Foundation Models with Sparse Mixture of Experts. [[paper](https://arxiv.org/abs/2410.10469)] [[GitHub Repo](https://github.com/SalesforceAIResearch/uni2ts)]
[^3]: Chronos: Learning the Language of Time Series. [[paper](https://arxiv.org/abs/2403.07815)] [[GitHub Repo](https://github.com/amazon-science/chronos-forecasting)]
[^4]: A decoder-only foundation model for time-series forecasting. [[paper](https://arxiv.org/abs/2310.10688)] [[GitHub Repo](https://github.com/google-research/timesfm/)]
