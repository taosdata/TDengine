---
title: "调用时序基础模型"
sidebar_label: "调用时序基础模型"
---

调用时序基础模型的预测能力，需要您部署相应的时序基础模型服务，模型的部署方式请参考[部署时序基础模型](../../06-TDgpt/06-dev/04-tsfm/index.md) 的内容。

## 功能概述

时序数据基础模型是专门训练用以处理时间序列数据预测和异常检测、数据补齐等高级时序数据分析功能的基础模型，时序基础模型继承了大模型的优良泛化能力，无需要设置复杂的输入参数，即可根据输入数据进行预测分析。

| 序号  | 参数       | 说明                   |
|-----|----------|----------------------|
| 1   | tdtsfm_1 | 涛思时序数据基础模型 v1.0      |
| 2   | time-moe[^1] | MoE 时序基础模型           |
| 3   | moirai[^2]   | SalesForce 开源的时序基础模型 |
| 4   | chronos[^3]  | Amazon 开源的时序基础模型     |
| 5   | timesfm[^4]  | Google 开源的时序基础模型     |
| 6   | moment[^5]   | CMU 开源的时序基础模型     |

TDgpt 集成时序基础模型的预测能力，无需设置模型运行参数，即可直接调用时序基础模型进行预测。

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
[^5]: MOMENT: A Family of Open Time-series Foundation Models. [[paper](https://arxiv.org/abs/2402.03885)] [[GitHub Repo](https://github.com/moment-timeseries-foundation-model/moment)]
