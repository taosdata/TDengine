---
sidebar_label: 巡检工具
title: 巡检工具
---

本文档旨在介绍 TDengine TSDB 安装部署前后配套的巡检工具。

相关工具的功能简介：

| **工具名称** | **功能简介** |
|:--|:----------|
| **安装前检查**   | 部署前对 TDengine TSDB 安装部署的依赖环境进行安装前检查 |
| **安装前配置** | 部署前对 TDengine TSDB 安装部署的依赖环境进行安装前预配置 |
| **安装部署**   | 指定环境安装部署 TDengine TSDB |
| **例行巡检**   | 基于 TDengine TSDB 环境，进行例行巡检和告警 |
| **基准性能测试**   | 指定环境执行 IO 和网络的基准性能测试 |

## 使用限制

巡检工具目前仅支持企业版用户，暂不对社区版开放

## 支持的平台
>
> **💡 Note:** 目前仅在下列平台验证通过，若在其它平台或版本上运行巡检工具，遇到问题请联系对接的交付人员。

- Kylin V10
- Ubuntu (20.04.2)
- CentOS (7.9)
- LinxOS (6.0.99)
- openEuler (23.09)
- Debian（12）

## 支持的 TDengine TSDB 版本

- 3.1.x.x 及以上版本

## 运行前提条件

- 运行工具需要 root 权限。如果要由普通用户运行该工具，则需要 sudo 权限。
- 配置免密登录的环境，运行工具的当前节点也需要配置自身免密。
- 运行工具环境 glibc 版本要求 : glibc 2.17 及以上版本 ( x64 架构 ), glibc 2.27 及以上版本 ( ARM 架构 )。
- 配置的 FQDN 和 IP 地址必须提前配置到 /etc/hosts 文件。
- 在脚本执行期间，必须保证能通过 RESTful 远程连接 TDengine TSDB 服务（仅针对例行巡检工具）。
- 集群节点之间的 SSH 服务必须启用。如果无法启用 SSH 服务，则只能通过 local 模式在每个节点上单独运行工具。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
