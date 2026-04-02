---
title: System Requirements
sidebar_label: System Requirements
---

The system requirements on this page apply to the machine running the anode for TDgpt. Note that these are guidelines, and actual requirements scale with model size, request concurrency (QPS), context/window length, and any local caching/feature store footprint.

System requirements also differ depending on whether you use inference only or also perform training or fine-tuning.

## Inference Only

Typical use cases: forecasting, anomaly detection, conversational analytics, automated insights/report generation.

### Minimum Specifications

- CPU: 8 cores
- Memory: 16 GB
- Storage: 200 GB SSD
- GPU: Optional

:::note

TDgpt can operate without a GPU. However, adding a GPU will improve latency and throughput.

:::

### Recommended Specifications

- CPU: 16 cores
- Memory: 32 GB
- Storage: 500 GB SSD
- GPU: NVIDIA GPU with 24 GB VRAM

## Training or Fine-Tuning

Typical use cases: model fine-tuning, retraining, or local validation of model updates.

### Minimum Specifications

- CPU: 16 cores
- Memory: 32 GB
- Storage: 500 GB SSD
- GPU: NVIDIA GPU with 24 GB VRAM

### Recommended Specifications

- CPU: 32 cores
- Memory: 64 GB
- Storage: 1 TB
- GPU: At least one NVIDIA GPU with 24 GB VRAM

## Network Requirements

If the anode pulls large volumes of time-series data or features from TDengine TSDB or TDengine IDMP, a 1 Gbps network is required. For high-throughput environments, 10 Gbps is recommended.
