---
sidebar_label: 边缘网关
sidebar_position: 3.5
title: 边缘网关接入
description: 将 Neuron / NeuronEX 边缘网关中已配置的采集数据一键写入 TDengine
---

import NeuronPrerequisites from '../../13-ecosystem-integrations/05-iot/resource/_neuron-prerequisites.mdx'
import NeuronMapping from '../../13-ecosystem-integrations/05-iot/resource/_neuron-mapping.mdx'
import NeuronOneClick from '../../13-ecosystem-integrations/05-iot/resource/_neuron-one-click.mdx'

## 概述

边缘网关接入面向已在 Neuron / NeuronEX 网关上完成设备采集配置的场景：在 TDengine Explorer 的「数据写入 → 边缘网关」页签中填写网关地址和凭据、确认映射后，Explorer 自动完成数据流、写入规则和采集订阅的配置，采集数据随即持续写入当前集群。

与本章其他接入方式不同，边缘网关接入不创建 taosX 接入任务，也无需配置解析与映射规则——设备、点位、数据类型的定义直接取自网关侧已有的采集配置。数据面为 Neuron → eKuiper → taosAdapter，通过无模式写入自动创建超级表和子表；配置完成后数据链路不依赖 Explorer 持续运行。

目前支持 NeuronEX 及开源 Neuron + eKuiper 组合。

<NeuronPrerequisites />

<NeuronMapping />

<NeuronOneClick />

## 更多说明

- 需要自定义标签、业务属性或数据转换时，使用 [Neuron 手工接入](../../13-ecosystem-integrations/05-iot/03-Neuron.md#手工接入)。
- 入库验证 SQL、运行监控、断链缓存（Store-and-Forward）、mTLS 与高可用部署、性能参考及常见问题，见 [Neuron 集成](../../13-ecosystem-integrations/05-iot/03-Neuron.md)。
