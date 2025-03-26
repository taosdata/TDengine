---
sidebar_label: Explore TDgpt
title: Explore TDgpt
description: Explore TDgpt through public databases.
---

TDgpt is an external time-series data analysis agent that is compatible with TDengine's main process taosd, enabling seamless integration of time-series data analysis services into TDengine's query execution process. TDgpt is a stateless platform that includes the classic statistical analysis model library Statsmodel, embeds machine/deep learning framework libraries such as torch/Keras, and on cloud services, TDgpt has built-in support for TaoData's self-developed time-series foundation models (TDtsfm) and TimeMoE time-series foundation model.

The public database **Time Series Prediction Analysis Dataset** on the cloud service has prepared three datasets (login to the TDengine Cloud, click **DB Mart** on the lower left of the screen) to experience the powerful prediction and generalization capabilities of the time-series foundation models. Both time-series models are trained on these datasets. The built-in datasets are stored in the forecast database, which includes three data tables.

- electricity_demand_sub: Electricity demand data (electricity_demands) from different time periods.
- exchange_2_cpc_results_sub: Public dataset cpc from NAB.
- exchange_2_cpm_results_sub: Public dataset cpm from NAB.

TDgpt integrates the prediction capabilities of time-series foundation models, allowing you to easily invoke these capabilities using SQL statements.

1. You can invoke TaoData's time-series foundation model to predict data using the following SQL statement:

    ``` SQL
    select _FROWTS, forecast(val, 'algo=tdtsfm_1,start=1324915200000,rows=300') from forecast.electricity_demand_sub;
    ```

2. You can invoke the TimeMoE time-series foundation model to predict data using the following SQL:

    ``` SQL
    select _FROWTS, forecast(val, 'algo=timemoe-fc,start=1324915200000,rows=300') from forecast.electricity_demand_sub;
    ```
  
3. After the query is complete, you can display the data graph by clicking the Draw button in the Chart section.

Users can execute `show anodes full;` to query the list of pre-configured algorithms in the current system. For usage methods of each algorithm, please refer to the [TDgpt Document](https://docs.tdengine.com/advanced/TDgpt/introduction/)。
