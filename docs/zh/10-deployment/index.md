---
sidebar_label: 部署集群
title: 部署集群
description: 部署 TDengine 集群的多种方式
---

TDengine 支持集群，提供水平扩展的能力。如果需要获得更高的处理能力，只需要多增加节点即可。TDengine 采用虚拟节点技术，将一个节点虚拟化为多个虚拟节点，以实现负载均衡。同时，TDengine可以将多个节点上的虚拟节点组成虚拟节点组，通过多副本机制，以保证供系统的高可用。TDengine的集群功能完全开源。

本章节主要介绍如何在主机上人工部署集群，docker部署，以及如何使用 Kubernetes 和 Helm部署集群。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
