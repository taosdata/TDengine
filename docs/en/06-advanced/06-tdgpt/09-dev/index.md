---
title: Algorithm Developer's Guide
sidebar_label: Algorithm Developer's Guide
---
TDgpt is an extensible agent for advanced time-series data analytics. You can follow the steps described in this document to develop your own analytics algorithms and add them to the platform. Your applications can then use SQL statements to invoke these algorithms. Custom algorithms must be developed in Python.
The anode adds algorithms semi-dynamically. When the anode is started, it scans specified directories for files that meet its requirements and adds those files to the platform. To add an algorithm to your TDgpt, perform the following steps:

1. Develop an analytics algorithm according to the TDgpt requirements.
2. Place the source code files in the appropriate directory and restart the anode.
3. Run the `CREATE ANODE` statement to add the anode to your TDengine cluster.

Your algorithm has been added to TDgpt and can be used in SQL statements. Because TDgpt is decoupled from TDengine, adding or upgrading algorithms on the anode does not affect the TDengine server (taosd). On the application side, it is necessary only to update your SQL statements to start using new or upgraded algorithms.

This extensibility makes TDgpt suitable for a wide range of use cases. You can add any algorithms needed by your use cases on demand and invoke them via SQL. You can also update algorithms without making significant changes to your applications.

This document describes how to add algorithms to an anode and invoke them with SQL statements.

## Directory Structure

The directory structure of an anode is described as follows:

```bash
.
├── bin
├── cfg
├── lib
│   └── taosanalytics
│       ├── algo
│       │   ├── ad
│       │   └── fc
│       ├── misc
│       └── test
├── log -> /var/log/taos/taosanode
├── model -> /var/lib/taos/taosanode/model
└── venv -> /var/lib/taos/taosanode/venv

```

|Directory|Description|
|---|---|
|taosanalytics| Source code, including the `algo` subdirectory for algorithms, the `test` subdirectory for unit and integration tests, and the `misc` subdirectory for other files. Within the `algo` subdirectory, the `ad` subdirectory includes anomaly detection algorithms, and the `fc` subdirectory includes forecasting algorithms.|
|venv| Virtual Python environment |
|model|Trained models for datasets|
|cfg|Configuration files|

## Limitations

- Place Python source code for anomaly detection in the `./taos/algo/ad` directory.
- Place Python source code for forecasting in the `./taos/algo/fc` directory.

### Class Naming Rules

The anode adds algorithms automatically. Your algorithm must therefore consist of appropriately named Python files. Algorithm files must start with an underscore (`_`) and end with `Service`. For example: `_KsigmaService` is the name of the k-sigma anomaly detection algorithm.

### Class Inheritance Rules

- All anomaly detection algorithms must inherit `AbstractAnomalyDetectionService` and implement the `execute` method.
- All forecasting algorithms must inherit `AbstractForecastService` and implement the `execute` method.

### Class Property Initialization

Your classes must initialize the following properties:

- `name`: identifier of the algorithm. Use lowercase letters only. This identifier is displayed when you use the `SHOW` statement to display available algorithms.
- `desc`: basic description of the algorithm.

```SQL
--- The `algo` key takes the defined `name` value.
SELECT COUNT(*)
FROM foo ANOMALY_WINDOW(col_name, 'algo=name')
```
