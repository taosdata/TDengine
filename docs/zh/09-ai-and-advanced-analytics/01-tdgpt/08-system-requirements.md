---
title: 系统要求
sidebar_label: 系统要求
---

本页的系统要求适用于运行 TDgpt 的 Anode 所在主机。以下为参考规格，实际需求会随模型规模、请求并发（QPS）、上下文/窗口长度，以及本地缓存或特征存储占用等因素变化。

仅做推理，与同时进行训练或微调时，系统要求也不同。

## 仅推理

典型场景：预测、异常检测、对话式分析、自动化洞察/报告生成。

### 最低配置

- CPU：8 核
- 内存：16 GB
- 存储：200 GB SSD
- GPU：可选

:::note

TDgpt 可在无 GPU 环境下运行。配置 GPU 可降低时延并提高吞吐。

:::

### 推荐配置

- CPU：16 核
- 内存：32 GB
- 存储：500 GB SSD
- GPU：NVIDIA GPU，显存 24 GB

## 训练或微调

典型场景：模型微调、再训练，或在本地验证模型更新。

### 最低配置

- CPU：16 核
- 内存：32 GB
- 存储：500 GB SSD
- GPU：NVIDIA GPU，显存 24 GB

### 推荐配置

- CPU：32 核
- 内存：64 GB
- 存储：1 TB
- GPU：至少一块 NVIDIA GPU，显存 24 GB

## 网络要求

若 Anode 需从 TDengine TSDB 或 TDengine IDMP 拉取大量时序数据或特征，网络带宽需达到 1 Gbps。高吞吐环境建议使用 10 Gbps。
