---
sidebar_label: TDengine 发布历史
title: TDengine 发布历史及下载链接
description: TDengine 发布历史、Release Notes 及下载链接
---

## TDengine 版本规则说明

TDengine 版本号由四个数字组成，中间由点号分隔，定义如下
- `[Major+].[Major].[Feature].[Maintenance]`
- `Major+`：产品有重大重构，不能直接升级，如有升级需要请联系 TDengine 客户支持团队
- `Major`：有重大新特性，升级时不支持滚动升级，且升级后不可回退，如从 3.2.3.0 升级到 3.3.0.0 后不能回退
- `Feature`：有新特性，升级时不支持滚动升级，但从相同的 Major Release 不同的 Feature Release 升级后可以回退，如从 3.3.0.0 升级到 3.3.1.0 后可以回退到 3.3.0.0。客户端驱动(libtaos.so) 与服务端需要同步升级。
- `Maintenance`: 没有新特性，有且只有缺陷修复，支持滚动升级，升级后可以回退
- 滚动升级：由三个或以上节点组成且使用三副本的集群，每次停止一个节点、对其进行升级、然后重新启动，按此过程循环完成对集群中所有节点的升级，在升级期间集群仍然可以对外提供服务。对于不支持滚动升级的版本变化，需要先停止整个集群、升级集群中所有节点、最后启动整个集群，在升级期间集群不能对外提供服务。

## TDengine 2.x 下载

TDengine 2.x 各版本安装包请访问[这里](https://www.taosdata.com/all-downloads)

## TDengine 3.x 下载

TDengine 3.x 各版本安装包下载链接如下：

import Release from "/components/ReleaseV3";

## 3.3.3.0

<Release type="tdengine" version="3.3.3.0" />

## 3.3.2.0

<Release type="tdengine" version="3.3.2.0" />

## 3.3.1.0

<Release type="tdengine" version="3.3.1.0" />

## 3.3.0.3

<Release type="tdengine" version="3.3.0.3" />

## 3.3.0.0

<Release type="tdengine" version="3.3.0.0" />

## 3.2.3.0

<Release type="tdengine" version="3.2.3.0" />

## 3.2.2.0

<Release type="tdengine" version="3.2.2.0" />

## 3.2.1.0

<Release type="tdengine" version="3.2.1.0" />

## 3.2.0.0

<Release type="tdengine" version="3.2.0.0" />

## 3.1.1.0

<Release type="tdengine" version="3.1.1.0" />

## 3.1.0.3

<Release type="tdengine" version="3.1.0.3" />

## 3.1.0.2

<Release type="tdengine" version="3.1.0.2" />

## 3.1.0.0

<Release type="tdengine" version="3.1.0.0" />

## 3.0.7.1

<Release type="tdengine" version="3.0.7.1" />

## 3.0.7.0

<Release type="tdengine" version="3.0.7.0" />

## 3.0.6.0

<Release type="tdengine" version="3.0.6.0" />

## 3.0.5.1

<Release type="tdengine" version="3.0.5.1" />

## 3.0.5.0

<Release type="tdengine" version="3.0.5.0" />

## 3.0.4.2

<Release type="tdengine" version="3.0.4.2" />

## 3.0.4.1

<Release type="tdengine" version="3.0.4.1" />

## 3.0.4.0

<Release type="tdengine" version="3.0.4.0" />

## 3.0.3.2

<Release type="tdengine" version="3.0.3.2" />

## 3.0.3.1

<Release type="tdengine" version="3.0.3.1" />

## 3.0.3.0

<Release type="tdengine" version="3.0.3.0" />


## 3.0.2.6

<Release type="tdengine" version="3.0.2.6" />

## 3.0.2.5

<Release type="tdengine" version="3.0.2.5" />

## 3.0.2.4

<Release type="tdengine" version="3.0.2.4" />

## 3.0.2.3

<Release type="tdengine" version="3.0.2.3" />

## 3.0.2.2

<Release type="tdengine" version="3.0.2.2" />

## 3.0.2.1

<Release type="tdengine" version="3.0.2.1" />

## 3.0.2.0

<Release type="tdengine" version="3.0.2.0" />

## 3.0.1.8

<Release type="tdengine" version="3.0.1.8" />

## 3.0.1.7

<Release type="tdengine" version="3.0.1.7" />

## 3.0.1.6

<Release type="tdengine" version="3.0.1.6" />

## 3.0.1.5

<Release type="tdengine" version="3.0.1.5" />

## 3.0.1.4

<Release type="tdengine" version="3.0.1.4" />

## 3.0.1.3

<Release type="tdengine" version="3.0.1.3" />

## 3.0.1.2

<Release type="tdengine" version="3.0.1.2" />

## 3.0.1.1

<Release type="tdengine" version="3.0.1.1" />

## 3.0.1.0

<Release type="tdengine" version="3.0.1.0" />
