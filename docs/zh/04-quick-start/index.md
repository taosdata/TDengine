---
sidebar_label: 快速上手
title: 快速上手
description: TDengine TSDB 快速上手
toc_max_heading_level: 4
---

import xiaot_new from './assets/xiaot-20231007.png'
import channel from './assets/channel.webp'
import official_account from './assets/official-account.webp'

TDengine TSDB 完整软件包包括服务端 `taosd`、用于与第三方系统对接并提供 RESTful 接口的 `taosAdapter`、应用驱动 `taosc`、命令行程序 `taos shell`，以及一些工具软件。除多语言连接器外，TDengine 还通过 [taosAdapter](../12-operations-and-tooling/03-components/03-taosadapter.md) 提供 [RESTful 接口](../10-developer-guide/08-connectors-reference/10-rest-api.mdx)。

先完成环境安装，再按侧栏顺序体验基本概念、数据建模、写入和查询等能力。

## 下载与安装

你可以任选以下一种方式快速设置 TDengine 环境，并体验其高效写入和查询：

- [用 Docker 快速体验 TDengine TSDB](./01-download-and-install/01-docker.md)
- [用安装包快速体验 TDengine TSDB](./01-download-and-install/02-package.md)
- [用云服务快速体验 TDengine TSDB](./01-download-and-install/03-cloud.md)

## 加入 TDengine 官方社区

使用微信扫描以下二维码，获取 TDengine 最新技术动态，并与社区用户交流物联网大数据技术应用、TDengine 使用问题和实践技巧。

<table width="100%">
<tr align="center">
<td style={{padding:'1em 3em',border:0}}><img src={xiaot_new} alt="小 T 的二维码" width="200" /></td>
<td style={{padding:'1em 3em',border:0}}><img src={channel} alt="TDengine 微信视频号" width="200" /></td>
<td style={{padding:'1em 3em',border:0}}><img src={official_account} alt="TDengine 微信公众号" width="200" /></td>
</tr>
<tr align="center">
<td style={{padding:'1em 3em',border:0}}>加入 TDengine 微信群<br/>交流最新物联网技术</td>
<td style={{padding:'1em 3em',border:0}}>关注 TDengine 视频号<br/>观看技术直播与教学视频</td>
<td style={{padding:'1em 3em',border:0}}>关注 TDengine 公众号<br/>阅读技术文章与行业案例</td>
</tr>
</table>
