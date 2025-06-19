---
sidebar_label: Introduction
title: Introduction
---

import Image from '@theme/IdealImage';
import tdgptArch from '../../assets/tdgpt-01.png';

## Introduction

Numerous algorithms have been proposed to perform time-series forecasting, anomaly detection, imputation, and classification, with varying technical characteristics suited for different scenarios.

Typically, these analysis algorithms are packaged as toolkits in high-level programming languages (such as Python or R) and are widely distributed and used through open-source channels. This model helps software developers integrate complex analysis algorithms into their systems and greatly lowers the barrier to using advanced algorithms.

Database system developers have also attempted to integrate data analysis algorithm models directly into database systems. By building machine learning libraries (e.g., Spark's MLlib), they aim to leverage mature analytical techniques to enhance the advanced data analysis capabilities of databases or analytical computing engines.

The rapid development of artificial intelligence (AI) has brought new opportunities to time-series data analysis. Efficiently applying AI capabilities to this field also presents new challenges for databases. To this end, TDengine has introduced TDgpt, an intelligent agent for time-series analytics. With TDgpt, you can use statistical analysis algorithms, machine learning models, deep learning models, foundational models for time-series data, and large language models via SQL statements. TDgpt exposes the analytical capabilities of these algorithms and models through SQL and applies them to your time-series data using new windows and functions.

## Technical Features

TDgpt is an external agent that integrates seamlessly with TDengine's main process taosd. It allows time-series analysis services to be embedded directly into TDengine's query execution flow.
TDgpt is a stateless platform that includes the classic statsmodels library of statistical analysis models as well as embedded frameworks such as PyTorch and Keras for machine and deep learning. In addition, it can directly invoke TDengine's proprietary foundation model TDtsfm through request forwarding and adaptation.

As an analytics agent, TDgpt will also support integration with third-party time-series model-as-a-service (MaaS) platforms in the future. By modifying just a single parameter (algo), you will be able to access cutting-edge time-series model services.
TDgpt is an open system to which you can easily add your own algorithms for forecasting, anomaly detection, imputation, and classification. Once added, the new algorithms can be used simply by changing the corresponding parameters in the SQL statement,
with no need to modify a single line of application code.

## System Architecture

TDgpt is composed of one or more stateless analysis nodes, called AI nodes (anodes). These anodes can be deployed as needed across the TDengine cluster in appropriate hardware environments (for example, on compute nodes equipped with GPUs) depending on the requirements of the algorithms being used.
TDgpt provides a unified interface and invocation method for different types of analysis algorithms. Based on user-specified parameters, it calls advanced algorithm packages and other analytical tools, then returns the results to TDengine's main process (taosd) in a predefined format.
TDgpt consists of four main components:
-- Built-in analytics libraries: Includes libraries such as statsmodels, pyculiarity, and pmdarima, offering ready-to-use models for forecasting and anomaly detection.
-- Built-in machine learning libraries: Includes libraries like PyTorch, Keras, and Scikit-learn to run pre-trained machine and deep learning models within TDgpt's process space. The training process can be managed using end-to-end open-source ML frameworks such as Merlion or Kats, and trained models can be deployed by uploading them to a designated TDgpt directory.

- Request adapter for general-purpose LLMs: Converts time-series forecasting requests into prompts for general-purpose LLMs such as Llama in a MaaS manner. (Note: This functionality is not open source.)
- Adapter for locally deployed time-series models: Sends requests directly to models like Time-MoE and TDtsfm that are specifically designed for time-series data. Compared to general-purpose LLMs, these models do not require prompt engineering, are lighter-weight, and are easier to deploy locally with lower hardware requirements. In addition, the adapter can also connect to cloud-based time-series MaaS systems such as TimeGPT, enabling localized analysis powered by cloud-hosted models.

<figure>
<Image img={tdgptArch} alt="TDgpt Architecture"/>
</figure>

During query execution, the vnode in TDengine forwards any elements involving advanced time-series data analytics directly to the anode. Once the analysis is completed, the results are assembled and embedded back into the query execution process.

## Advanced Analytics Services

The services provided by TDgpt are described as follows:

- Anomaly detection: This service is provided via a new **anomaly window** that has been introduced into TDengine. An anomaly window is a special type of event window, defined by the anomaly detection algorithm as a time window during which an anomaly is occurring. This window differs from an event window in that the algorithm determines when it opens and closes instead of expressions input by the user. The query operations supported by other windows are also supported for anomaly windows.
- Time-series forecasting: The FORECAST function invokes a specified (or default) forecasting algorithm to predict future time-series data based on input historical data.
- Data imputation: To be released in July 2025
- Time-series classification: To be released in July 2025

## Custom Algorithms

TDgpt is an extensible platform to which you can add your own algorithms and models using the process described in [Algorithm Developer's Guide](../dev/). After adding an algorithm, you can access it through SQL statements just like the built-in algorithms. It is not necessary to make updates to your applications.

Custom algorithms must be developed in Python. The anode adds algorithms dynamically. When the anode is started, it scans specified directories for files that meet its requirements and adds those files to the platform. To add an algorithm to your TDgpt, perform the following steps:

1. Develop an analytics algorithm according to the TDgpt requirements.
2. Place the source code files in the appropriate directory and restart the anode.
3. Refresh the algorithm cache table.
You can then use your new algorithm in SQL statements.

## Algorithm Evaluation

TDengine Enterprise includes a tool that evaluates the effectiveness of different algorithms and models. You can use this tool on any algorithm or model in TDgpt, including built-in and custom forecasting and anomaly detection algorithms and models. The tool uses quantitative metrics to evaluate the accuracy and performance of each algorithm with a given dataset in TDengine.

## Model Management

Trained models for machine learning frameworks such as PyTorch, TensorFlow, and Keras must be placed in the designated directory on the anode. The anode automatically detects and loads models from this directory.
TDengine Enterprise includes a model manager that integrates seamlessly with open-source end-to-end ML frameworks for time-series data such as Merlion and Kats.

## Processing Performance

Time-series analytics is a CPU-intensive workflow. Using a more powerful CPU or GPU can improve performance.
Machine and deep learning models in TDgpt are run through PyTorch, and you can use standard methods of improving performance, for example deploying TDgpt on a machine with more RAM and uing a torch model that can use GPUs.
You can add different algorithms and models to different anodes to enable concurrent processing.

## Operations and Maintenance

With TDengine OSS, permissions and resource management are not provided for TDgpt.
TDgpt is deployed as a Flask service through uWSGI. You can monitor its status by opening the port in uWSGI.

### References

1. [Merlion](https://opensource.salesforce.com/Merlion/latest/index.html)
1. [Kats](https://facebookresearch.github.io/Kats/)
1. [StatsModels](https://www.statsmodels.org/stable/index.html)
1. [Keras](https://keras.io/guides/)
1. [PyTorch](https://pytorch.org/)
1. [Scikit-learn](https://scikit-learn.org/stable/index.html)
1. [Time-MoE](https://github.com/Time-MoE/Time-MoE)
1. [TimeGPT](https://docs.nixtla.io/docs/getting-started-about_timegpt)
1. [DeepSeek](https://www.deepseek.com/)
1. [Llama](https://www.llama.com/docs/overview/)
1. [Spark MLlib](https://spark.apache.org/docs/latest/ml-guide.html)
