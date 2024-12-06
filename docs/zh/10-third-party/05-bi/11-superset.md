---
sidebar_label: SuperSet
title: 与 SuperSet 的集成
toc_max_heading_level: 4
---
‌Apache Superset‌ 是一个现代的企业级商业智能（BI）Web 应用程序，主要用于数据探索和可视化。它由 Apache 软件基金会支持，是一个开源项目，拥有活跃的社区和丰富的生态系统。Apache Superset提供了直观的用户界面，使得创建、分享和可视化数据变得简单，同时支持多种数据源和丰富的可视化选项‌。

通过 TDengine 的 Python 连接器, ‌Superset‌ 可支持 TDengine 数据源并提供展现和分析等功能

## 安装 Apache Superset
确保已安装 Apache SuperSet V2.1.0 及以上版本, 如未安装，请到其 [官网](https://superset.apache.org/) 安装

## 安装 TDengine
TDengine 企业版及社区版均可支持，版本要求在 3.0 及以上

## 安装 TDengine Python 连接器
TDengine 的 Python 连接器在 V2.1.17 及之后版本中自带支持 SuperSet 的连接驱动，会自动安装到 SuperSet 目录下并提供数据源服务   
连接使用 WebSocket 协议，需另安装 TDengine 的 taos-ws-py 组件, 全部安装脚本如下：   
```bash
pip3 install taospy
pip3 install taos-ws-py
```

## SuperSet 中配置 TDengine 连接
第1步，进入新建数据库连接页面 
     SuperSet -> Setting-> Database Connections -> +DATABASE   
第2步，选择 TDengine 数据库连接  
   SUPPORTED DATABASES 下拉列表中选择 “TDengine” 项，若下拉列表中没有 TDengine 选项，请确认先安装 SuperSet 后安装 Python 连接器的步骤正确  
第3步，DISPLAY NAME 中给连接起个名字，任意填写即可   
第4步，SQLALCHEMY URL* 项为关键连接信息串，务必填写正确   
```bash
    连接串格式： taosws://用户名:密码@主机名:端口号
```
| 参数名称 | 参数说明 |
|:-------:|:---------:|
| 用户名： | 登录 TDengine 数据库的用户名  
| 密码：   | 登录 TDengine 数据库的密码  
| 主机名： | TDengine 数据库所在主机的名称  
| 端口号： | 提供 WebSocket 服务的端口，默认为：6041  
         

示例：    
本机安装的 TDengine 数据库，提供 WebSocket 服务端口为 6041，使用默认用户名密码，连接串为：    
```bash
taosws://root:taosdata@localhost:6041  
```
第5步，配置好以上关键信息后，可点击 "TEST CONNECTION" 测试连接是否能够成功，测试通过后点 CONNECT 按钮，完成连接
       

## 开始使用
在使用上 TDengine 数据源与其它数据源使用无差别，这里简单介绍下基本数据查询：    
1. SuperSet 界面点击右上角 “+” 号按钮，选择 SQL query, 进入查询界面  
2. 左上角 DATABASE 下拉列表中选择前面已创建好的 TDengine 数据源  
3. SCHEMA 下拉列表，选择要操作的数据库名（系统库不显示）  
4. SEE TABLE SCHEMA 选择要操作的超级表名或普通表名（子表不显示）  
5. 随后会在下面区域显示选定表的 SCHEMA 信息  
6. 在 SQL 编辑器区域可输入符合 TDengine 语法的任意 SQL 语句执行

## 示例效果
我们选择 SuperSet Chart 模板中较流程的两个模板做了效果展示，以智能电表数据为例：  

第一个为 Aggregate 类型，展示在第 4 组中指定时间段内每分钟采集电压值(voltage) 最大值  

![superset-demo1](./superset-demo1.jpeg)

第二个为 RAW RECORDS 类型，展示在第 4 组中指定时间段内 current, voltage 的采集值  

![superset-demo2](./superset-demo2.jpeg)  
