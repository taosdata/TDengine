---
title: 立即开始
description: '快速设置 TDengine 环境并体验其高效写入和查询'
---

import xiaot from './tdengine.webp'

TDengine 完整的软件包包括服务端（taosd）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、应用驱动（taosc）、命令行程序 (CLI，taos) 和一些工具软件。TDengine 除了提供多种语言的连接器之外，还通过 [taosAdapter](../reference/taosadapter) 提供 [RESTful 接口](../connector/rest-api)。

本章主要介绍如何利用 Docker 或者安装包快速设置 TDengine 环境并体验其高效写入和查询。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```

### 开发者技术交流群

微信扫描下面二维码，加“小 T”为好友，即可加入“物联网大数据技术前沿群”，与大家共同交流物联网大数据技术应用、TDengine 使用问题和技巧等话题。

<img src={xiaot} alt="小 T 的二维码" width="200" />
