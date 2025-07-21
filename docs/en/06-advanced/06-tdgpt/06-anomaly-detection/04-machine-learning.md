---
title: Machine Learning Algorithms
sidebar_label: Machine Learning Algorithms
---

TDgpt includes a deep learning model for anomaly detection that is built with an autoencoder. This model has been pretrained on the [art_daily_small_noise dataset](https://raw.githubusercontent.com/numenta/NAB/master/data/artificialNoAnomaly/art_daily_small_noise.csv) from NAB. For more information about training models, see [Preparing Models](../../dev/ml/).

Note that the model is not installed by default. To use the model, open the `/var/lib/taos/taosanode/model/` directory on your anode and create the `sample-ad-autoencoder` subdirectory. Download the model files from [our GitHub repository](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/model/sample-ad-autoencoder/) and save both files to that subdirectory. Then restart the taosanode service. For specific instructions, see [Add Machine Learning Models to TDgpt](../../dev/ml/).

The `model` directory structure is as follows:

```bash
.
└── model
    └── sample-ad-autoencoder
        ├── sample-ad-autoencoder.keras
        └── sample-ad-autoencoder.info
```

```SQL
--- In the anomaly window, set the algorithm to `sample_ad_model` and the model to `sample-ad-autoencoder`.
SELECT _wstart, count(*) 
FROM foo anomaly_window(val, 'algo=sample_ad_model,model=sample-ad-autoencoder');
```

 Note that this model works well only when it is pretrained. Using it with datasets on which it has not been trained will likely produce poor results.

The following algorithms are in development:

- Isolation Forest
- One-Class Support Vector Machines (SVM)
- Prophet

### References

1. [Autoencoder](https://en.wikipedia.org/wiki/Autoencoder)
