---
title: Machine Learning Algorithms
sidebar_label: Machine Learning Algorithms
---

TDgpt includes a deep learning model for anomaly detection that is built with an autoencoder. This model has been pretrained on the [art_daily_small_noise dataset](https://raw.githubusercontent.com/numenta/NAB/master/data/artificialNoAnomaly/art_daily_small_noise.csv) from NAB. For more information about training models, see [Preparing Models](../../01-tdgpt/06-dev/02-ml/index.md).

The sample model and its adapter are not loaded by default. To use them:

1. Download the files from [sample-ad-autoencoder](https://github.com/taosdata/TDengine/tree/main/tools/tdgpt/model/sample-ad-autoencoder/) to `/var/lib/taos/taosanode/model/sample-ad-autoencoder/`.
2. Copy the sample adapter [misc/autoencoder.py](https://github.com/taosdata/TDengine/blob/main/tools/tdgpt/taosanalytics/misc/autoencoder.py) to `taosanalytics/algo/ad/`.
3. Restart taosanode and run `UPDATE ALL ANODES`.

For details, see [Add Machine Learning Models to TDgpt](../../01-tdgpt/06-dev/02-ml/index.md).

The `model` directory structure is as follows:

```bash
.
└── model
    └── sample-ad-autoencoder
        ├── sample-ad-autoencoder.keras
        └── sample-ad-autoencoder.info
```

```sql
--- In the anomaly window, set the algorithm to `sample_ad_model` and the model to `sample-ad-autoencoder`.
SELECT _wstart, count(*) 
FROM foo anomaly_window(val, 'algo=sample_ad_model,model=sample-ad-autoencoder');
```

Note that this model works well only when it is pretrained. Using it with datasets on which it has not been trained will likely produce poor results.

The following algorithms are in development:

- Isolation Forest
- One-Class Support Vector Machines (SVM)

### References

1. [Autoencoder](https://en.wikipedia.org/wiki/Autoencoder)
