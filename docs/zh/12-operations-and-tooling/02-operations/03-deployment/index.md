---
sidebar_label: 集群部署
title: 集群部署
---

import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

由于 TDengine 设计之初就采用了分布式架构，具有强大的水平扩展能力，以满足不断增长的数据处理需求，因此 TDengine 支持集群，并将此核心功能开源。用户可以根据实际环境和需求选择 4 种部署方式—手动部署、Docker 部署、Kubernetes 部署和 Helm 部署。

<DocCardList items={useCurrentSidebarCategory().items}/>
