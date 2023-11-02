---
title: 部署 taosKeeper 
sidebar_label: 部署 taosKeeper
---

本节讲述如何部署 taosKeeper, taosKeeper 是用于接收业务集群产生的监控数据并将其写入监控集群的一个代理服务。

## 安装

taosKeeper 无需独立安装。在安装了 TDengine server 之后，系统中就已经具备了 taosKeeper。但如果想在不同的服务器上分别部署 TDengine 集群 (taosd 组件) 和 taosKeeper，则需要在这些服务器上都安装 TDengine server 安装包。具体请参数 [安装](../install)。关于 taosKeeper 的配置和使用细节，请参考 [taosKeeper](../../reference/taosKeeper)。

## 部署

部署 taosKeeper 请参考 [taosKeeper](../../reference/taosKeeper/#配置和运行方式)