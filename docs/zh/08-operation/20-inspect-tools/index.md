---
sidebar_label: 巡检工具
title: 巡检工具
---

本文档旨在介绍 TDengine 安装部署前后配套的巡检工具。

相关工具的功能简介： 

| **工具名称** | **功能简介** |
|:--|:----------|
| **安装前检查**   | 部署前对 TDengine 安装部署的依赖要素进行安装前检查 |
| **安装前预配置** | 部署前对 TDengine 安装部署的依赖要素进行安装前预配置 |
| **安装部署**   | 指定环境安装部署 TDengine |
| **例行巡检**   | 基于 TDengine 环境，进行例行巡检和告警 |   

## 支持的平台
- Kylin V10
- Ubuntu 20.04.2
- CentOS 7.9
- LinxOS 6.0.99
- openEuler 23.09

## 支持的 TDengine 版本
- 3.1.1.x
- 3.3.3.x
- 3.3.4.x

## 运行前提条件
 - 运行工具需要 root 权限。如果要由普通用户运行该工具，则需要 sudo 权限。
 - 运行工具环境要求安装 glibc 2.17 及以上版本。
 - 配置的 FQDN 和 IP 地址必须提前配置到 /etc/hosts 文件。
 - 在脚本执行期间，必须保证能通过 RESTful 远程连接 TDengine 服务。
 - 集群节点之间的 SSH 服务必须启用。如果无法启动 SSH 服务，则只能通过 local 模式在每个节点上单独运行工具。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```