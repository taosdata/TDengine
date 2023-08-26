---
title: 部署 taosAdapter 
sidebar_label: 部署 taosAdapter
---

本节讲述如何部署 taosAdapter 以为 TDengine 集群提供 RESTful 和 WebSocket 访问接口。

## 安装

taosAdapter 无需独立安装。在安装了 TDengine server 之后，系统中就已经具备了 taosAdapter。但如果想在不同的服务器上分别部署 TDengine 集群 (taosd 组件) 和 taosAdapter，则需要在这些服务器上都安装 TDengine server 安装包。具体请参数 [安装](../install)。关于 taosAdapter 的配置和使用细节，请参考 [taosAdapter](../../reference/taosAdapter)。

## 单一实例部署

部署 taosAdapter 的单一实例非常简单，请参考 [部署taosAdapter](../../reference/taosadapter/#taosadapter-部署方法)

## 多实例部署

部署多个 taosAdapter 的主要目的：1. 提升系统吞吐量，避免 taosAdapter 自身成为系统瓶颈；2. 提升系统的健壮性和高可用能力，当有一个实例因为某种故障而不能再提供服务时，进入业务系统的请求可以被自动路由到其它实例。 部署多个实例时需要解决负载均衡问题，避免某个节点过载而其它节点闲置。

部署多个 taosAdapter 实例需要先分别部署成功多个实例，其步骤与部署单一实例完全相同。接下来关键的部分是配置 nginx 。