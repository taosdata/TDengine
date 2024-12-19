---
sidebar_label: Superset
title: 与 Superset 集成
---
‌Apache Superset‌ 是一个现代的企业级商业智能（BI）Web 应用程序，主要用于数据探索和可视化。它由 Apache 软件基金会支持，是一个开源项目，它拥有活跃的社区和丰富的生态系统。Apache Superset 提供了直观的用户界面，使得创建、分享和可视化数据变得简单，同时支持多种数据源和丰富的可视化选项‌。

通过 TDengine 的 Python 连接器, ‌Superset‌ 可支持 TDengine 数据源并提供数据展现、分析等功能

## 安装 Apache Superset

确保已安装 Apache Superset v2.1.0 及以上版本, 如未安装，请到其 [官网](https://superset.apache.org/) 安装

## 安装 TDengine Python 连接器

TDengine Python 连接器从 `v2.1.18` 开始自带 Superset 连接驱动，安装程序会把连接驱动安装到 Superset 相应目录下并向 Superset 提供数据源服务   
Superset 与 TDengine 之间使用 WebSocket 协议连接，所以需另安装支持 WebSocket 连接协议的组件 `taos-ws-py` , 全部安装脚本如下：   
```bash
pip3 install taospy
pip3 install taos-ws-py
```

## Superset 中配置 TDengine 云服务连接

**第 1 步**，进入新建数据库连接页面 "Superset" → "Setting" → "Database Connections" → "+DATABASE"   
**第 2 步**，选择 TDengine 数据库连接。"SUPPORTED DATABASES" 下拉列表中选择 "TDengine" 项。  
:::tip
注意：若下拉列表中无 "TDengine" 项，请检查安装顺序，确保 `TDengine Python 连接器` 在 `Superset` 安装之后再安装。  
:::  
**第 3 步**，"DISPLAY NAME" 中填写连接名称，任意填写即可。   
**第 4 步**，"SQLALCHEMY URL" 项为关键连接信息串，复制以下内容粘贴即可。
```bash
taosws://gw.cloud.taosdata.com?token=0df909712bb345d6ba92253d3e6fb635d609c8ff
```
**第 5 步**，点击 “TEST CONNECTION” 测试连接是否成功，测试通过后点击 “CONNECT” 按钮，完成连接。
       

## 开始使用

TDengine 数据源与其它数据源使用上无差别，这里简单介绍下数据查询：    
1. Superset 界面点击右上角 “+” 号按钮，选择 “SQL query”, 进入查询界面  
2. 左上角 “DATABASE” 下拉列表中选择前面已创建好的 “TDengine” 数据源  
3. “SCHEMA” 下拉列表，选择要操作的数据库名（系统库不显示）  
4. “SEE TABLE SCHEMA” 选择要操作的超级表名或普通表名（子表不显示）  
5. 随后会在下方显示选定表的 SCHEMA 信息  
6. 在 SQL 编辑器区域可输入符合 TDengine 语法的任意 SQL 语句执行

## 示例效果

我们选择 Superset Chart 模板中较流行的两个模板做了效果展示，以智能电表数据为例：  

1. "Aggregate" 类型，展示在第 4 组中指定时间段内每分钟采集电压值(voltage)最大值  

  ![superset-demo1](./superset-demo1.jpeg)

2. "RAW RECORDS" 类型，展示在第 4 组中指定时间段内 current, voltage 的采集值  

  ![superset-demo2](./superset-demo2.jpeg)  
