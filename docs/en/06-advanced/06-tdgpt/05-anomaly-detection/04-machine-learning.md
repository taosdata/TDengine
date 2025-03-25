---
title: Machine Learning Algorithms
sidebar_label: Machine Learning Algorithms
---

TDgpt includes a built-in autoencoder for anomaly detection.

This algorithm is suitable for detecting anomalies in periodic time-series data. It must be pre-trained on your time-series data.

The trained model is saved to the `ad_autoencoder` directory. You then specify the model in your SQL statement.

```SQL
--- Add the name of the model `ad_autoencoder_foo` in the options of the anomaly window and detect anomalies in the dataset `foo` using the autoencoder algorithm.
SELECT COUNT(*), _WSTART
FROM foo
ANOMALY_WINDOW(col1, 'algo=encoder, model=ad_autoencoder_foo');
```

The following algorithms are in development:

- Isolation Forest
- One-Class Support Vector Machines (SVM)
- Prophet

## References

1. https://en.wikipedia.org/wiki/Autoencoder
