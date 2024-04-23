---
title: Deployment
description: This document describes how to deploy a TDengine cluster on a server, on Kubernetes, and by using Helm.
---

TDengine has a native distributed design and provides the ability to scale out. A few nodes can form a TDengine cluster. If you need higher processing power, you just need to add more nodes into the cluster. TDengine uses virtual node technology to virtualize a node into multiple virtual nodes to achieve load balancing. At the same time, TDengine can group virtual nodes on different nodes into virtual node groups, and use the replication mechanism to ensure the high availability of the system. The cluster feature of TDengine is completely open source.

This document describes how to manually deploy a cluster on a host directly and deploy a cluster with Docker, Kubernetes or Helm.

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
