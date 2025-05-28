---
title: "prophet"
sidebar_label: "prophet"
---

本节说明 prophet 时序数据预测分析模型使用方法。

## 功能概述

Prophet 是由 Facebook（Meta）开源的时间序列预测工具，它专为处理具有明显趋势变化、强季节性以及节假日效应的业务数据而设计。在 TDengine 的 TD-GPT 系统中，Prophet 已作为可选预测算法被集成，用户无需复杂的代码编写，通过简单的 SQL 查询即可直接调用该模型，实现对时间序列数据的快速预测分析。

## 可用参数列表

### 以下为 TDengine 中 Prophet 支持的常规参数:

| 参数名                    | 默认值       | 类型     | 说明                                                         |
|---------------------------|--------------|----------|--------------------------------------------------------------|
| p_cfg_tpl                  | 无       | string   | 自定义参数模板名称，不指定则不生效          |


### 模型配置
为满足多样化的预测需求，TDengine 支持通过 yaml 文件进行 Prophet 模型的参数配置，用户可以方便地配置节假日、定制 Prophet 行为等
#### 配置目录
配置文件通常位于 taosanode 安装目录下的cfg文件夹

例如，若 taosanode 安装目录为```/usr/local/taos/taosanode```，那么配置文件目录为```/usr/local/taos/taosanode/cfg```

#### 配置文件
- ***文件路径确定规则***：如果p_cfg_tpl值为demo，且 taosanode 安装目录为```/usr/local/taos/taosanode```，那么模型读取的配置文件完整路径为```/usr/local/taos/taosanode/cfg/prophet_demo.yaml```

- ***默认行为***：如果```p_cfg_tpl```未传值，那么模型将使用 ```Prophet`` 的默认行为进行预测

#### 配置文件示例

```yaml
saturating_cap_max: false          # 是否自动用输入数据最大值作为饱和上限（cap）
saturating_cap: null               # 手动指定饱和上限值，优先级高于 saturating_cap_max
saturating_cap_scale: null         # 饱和上限的缩放倍数，例如 1.2 表示 cap = 最大值 * 1.2

saturating_floor_min: false        # 是否自动用输入数据最小值作为饱和下限（floor）
saturating_floor: null             # 手动指定饱和下限值，优先级高于 saturating_floor_min
saturating_floor_scale: null       # 饱和下限的缩放倍数，例如 0.8 表示 floor = 最小值 * 0.8

seasonality_mode: "additive"       # 季节性模型类型，additive 或 multiplicative
seasonality_prior_scale: 10.0      # 控制季节性灵活度，值越大模型越灵活
holidays_prior_scale: 10.0         # 控制节假日效应强度，值越大影响越明显
changepoint_prior_scale: 0.05      # 趋势变点敏感度，值越大模型对趋势突变越敏感

daily_seasonality: true            # 是否启用日季节性
weekly_seasonality: false           # 是否启用周季节性
yearly_seasonality: false           # 是否启用年季节性

holidays:
  - holiday: "New Year"           # 节假日名称
    dates:
      - "2025-01-01"
      - "2025-01-02"
    lower_window: 0               # 节假日前影响的天数（负数表示节日前几天）
    upper_window: 1               # 节假日后影响的天数

  - holiday: "Labor Day"
    dates:
      - "2025-05-01"
      - "2025-05-02"
      - "2025-05-03"
    lower_window: 0
    upper_window: 0

  - holiday: "Custom Holiday"
    dates:
      - "2025-08-15"
    lower_window: -1              # 节假日前1天也视为假日影响期
    upper_window: 2               # 节假日后2天视为假日影响期
```

#### 完整参数

以下为 TDengine 中 Prophet 支持的参数，这些参数会影响模型的基础预测行为
##### 初始化参数

| 参数名                    | 默认值       | 类型     | 说明                                                         |
|---------------------------|--------------|----------|--------------------------------------------------------------|
| growth                    | linear       | string   | 趋势模型类型：`linear`（线性）或 `logistic`（饱和）          |
| changepoint_prior_scale   | 0.05         | float    | 趋势突变敏感度，值越大越敏感                                  |
| changepoint_range         | 0.8          | float    | 控制变点搜索范围的比例                                        |
| daily_seasonality         | auto         | bool     | 是否启用日季节性建模                                          |
| weekly_seasonality        | auto         | bool     | 是否启用周季节性建模                                          |
| yearly_seasonality        | auto         | bool     | 是否启用年季节性建模                                          |
| seasonality_mode          | additive     | string   | 季节性模式：`additive` 或 `multiplicative`                   |
| seasonality_prior_scale   | 10.0         | float    | 控制季节性复杂度                                              |
| holidays_prior_scale      | 10.0         | float    | 控制节假日影响强度                                            |
| mcmc_samples              | 0            | int      | MCMC 采样数量，启用贝叶斯时使用                            |
| holidays                  | -            | string   | 自定义节假日列表（可为 json 编码或已注册模板）               |

##### 重采样参数

| 参数名           | 默认值 | 类型   | 说明                                      |
|------------------|--------|--------|-------------------------------------------|
| resample          | 无      | string | 采样频率，如 `1S`, `1T`, `1H`, `1D` 等     |
| resample_mode     | mean   | string | 采样方法：mean, max, min, sum 等          |

##### 饱和参数

| 参数名                      | 默认值       | 类型   | 说明                                                      |
|-----------------------------|--------------|--------|-----------------------------------------------------------|
| saturating_cap_max          | false        | bool   | 自动设置上限为历史最大值                                  |
| saturating_cap              | -            | float  | 手动设置上限值                                            |
| saturating_cap_scale        | -            | float  | 上限缩放倍数                                              |
| saturating_floor_min        | false        | bool   | 自动设置下限为历史最小值                                  |
| saturating_floor            | -            | float  | 手动设置下限值                                            |
| saturating_floor_scale      | -            | float  | 下限缩放倍数       |

### 查询示例

针对 temperature 列进行数据预测, 从时间点1748361600000开始，输出10个预测点，时间点间隔是60秒；p_cfg_tpl 是模型参数模板；

```
FORECAST(temperature,"algo=prophet,rows=10,start=1748361600000,every=60,p_cfg_tpl=demo")
```

完整的调用 SQL 语句如下：

```SQL
SELECT _frowts, _fhigh, _frowts, FORECAST(temperature,"algo=prophet,rows=10,start=1748361600000,every=60,p_cfg_tpl=demo") FROM (SELECT * FROM test.boiler_temp WHERE ts >= '2025-05-21 00:00:00' AND ts < '2025-05-25 00:00:00' ORDER BY ts DESC LIMIT 10 ) foo;
```

### 参考文献

- https://facebook.github.io/prophet/docs/quick_start.html#python-api