---
title: LSTM
sidebar_label: LSTM
---

This document describes how to use LSTM in TDgpt.

##  Description

Long short-term memory (LSTM) is a special type of recurrent neural network (RNN) well-suited for tasks such as time-series data processing and natural language processing. Its unique gating mechanism allows it to effectively capture long-term dependencies. and address the gradient vanishing problem found in traditional RNNs, enabling more accurate predictions on sequential data. However, it does not directly provide confidence interval results for its computations.

The complete SQL statement is shown as follows:

```SQL
SELECT _frowts, FORECAST(i32, "algo=lstm,alpha=95,period=10,start_p=1,max_p=5,start_q=1,max_q=5") from foo
```

```json5
{
"rows": fc_rows,  // Rows returned
"period": period, // Period of results (equivalent to input period)
"alpha": alpha,   // Confidence interval of results (equivalent to input confidence interval)
"algo": "lstm",  // Algorithm
"mse": mse,       // Mean square error (MSE) of model generated for input time series
"res": res        // Results in column format
}
```

## References

- [1] Hochreiter S. Long Short-term Memory[J]. Neural Computation MIT-Press, 1997.