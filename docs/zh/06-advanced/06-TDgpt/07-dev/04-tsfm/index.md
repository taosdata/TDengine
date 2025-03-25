---
sidebar_label: 添加时序基础模型
title: 添加时序基础模型
---

本章主要介绍部署并使用开源时序基础模型（Time Series Fundation Model, TSFM），由众多研究机构及企业开源时序基础模型极大地简化了时序数据分析的复杂程度，在数据分析算法、机器学习和传统深度学习模型之外，提供了一个时间序列数据高级分析的新选择。

TDgpt 已经内置原生支持了两个时序基础模型涛思时序基础模型 (TDtsfm v1.0) 和 Time-MoE。但是越来越多的开源或商业时序模型需要用户按需将其整合到 TDgpt 中，本章将以支持 Time-MoE 模型为例，说明如何将一个独立部署的 MaaS 服务整合到 TDgpt 中，并通过 SQL 语句调用其时序数据分析能力。


```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
