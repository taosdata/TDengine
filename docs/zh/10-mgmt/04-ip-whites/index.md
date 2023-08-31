---
sidebar_label: IP 白名单
title: IP 白名单
description: 在 TDengine Cloud 实例里面设置允许访问该实例的 IP 白名单
---

TDengine Cloud 能够支持设置实例的 IP 白名单。在管理页面的 **IP 白名单**这个标签页里面，如果没有添加任何 IP 地址，任何客户端都可以访问 TDengine Cloud 当前实例。为了最大程度保证数据安全，客户可以配置 IP 白名单，只允许已经配置的 IP 列表里面的客户端访问该实例。

注意，如果添加了 IP 白名单，就只能是 IP 白名单里面的客户端可以访问 TDengine Cloud 的该实例，其他 IP 的客户端访问该实例对会被 TDengine Cloud 拒绝。
