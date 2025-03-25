---
title: MLP
sidebar_label: MLP
---

This document describes how to use MLP in TDgpt.

##  Description

MLP (Multilayer Perceptron) is a classic neural network model that can learn nonlinear relationships from historical data, capture patterns in time-series data, and make future value predictions. It performs feature extraction and mapping through multiple fully connected layers, generating prediction results based on the input historical data. Since it does not directly account for trends or seasonal variations, it typically requires data preprocessing to improve performance. It is well-suited for handling nonlinear and complex time-series problems.

The complete SQL statement is shown as follows:

```SQL
SELECT _frowts, FORECAST(i32, "algo=mlp") from foo
```

```json5
{
"rows": fc_rows,  // Rows returned
"period": period, // Period of results (equivalent to input period)
"alpha": alpha,   // Confidence interval of results (equivalent to input confidence interval)
"algo": "mlp",  // Algorithm
"mse": mse,       // Mean square error (MSE) of model generated for input time series
"res": res        // Results in column format
}
```

## References

- [1]Rumelhart D E, Hinton G E, Williams R J. Learning representations by back-propagating errors[J]. nature, 1986, 323(6088): 533-536.
- [2]Rosenblatt F. The perceptron: a probabilistic model for information storage and organization in the brain[J]. Psychological review, 1958, 65(6): 386.
- [3]LeCun Y, Bottou L, Bengio Y, et al. Gradient-based learning applied to document recognition[J]. Proceedings of the IEEE, 1998, 86(11): 2278-2324.