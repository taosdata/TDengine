---
title: 立即开始
description: '快速设置 TDengine 环境并体验其高效写入和查询'
---

import xiaot from './xiaot.webp'
import xiaot_new from './xiaot-20231007.png'
import channel from './channel.webp'
import official_account from './official-account.webp'

TDengine 完整的软件包包括服务端（taosd）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、应用驱动（taosc）、命令行程序 (CLI，taos) 和一些工具软件。TDengine 除了提供多种语言的连接器之外，还通过 [taosAdapter](../reference/taosadapter) 提供 [RESTful 接口](../connector/rest-api)。

本章主要介绍如何利用 Docker 或者安装包快速设置 TDengine 环境并体验其高效写入和查询。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```

## 加入 TDengine 官方社区

微信扫描以下二维码，学习了解 TDengine 的最新技术，与大家共同交流物联网大数据技术应用、TDengine 使用问题和技巧等话题。

<table width="100%">
<tr align="center">
<td style={{padding:'1em 3em',border:0}}><img src={xiaot_new} alt="小 T 的二维码" width="200" /></td>
<td style={{padding:'1em 3em',border:0}}><img src={channel} alt="TDengine 微信视频号" width="200" /></td>
<td style={{padding:'1em 3em',border:0}}><img src={official_account} alt="TDengine 微信公众号" width="200" /></td>
</tr>
<tr align="center">
<td style={{padding:'1em 3em',border:0}}>加入 TDengine 微信群<br/>了解学习最新物联网技术</td>
<td style={{padding:'1em 3em',border:0}}>关注 TDengine 视频号<br/>收看技术直播与教学视频</td>
<td style={{padding:'1em 3em',border:0}}>关注 TDengine 公众号<br/>阅读技术文章与行业案例</td>
</tr>
</table>
