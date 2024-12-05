---
sidebar_label: SuperSet
title: 与 SuperSet 的集成
toc_max_heading_level: 4
---
‌Apache Superset‌ 是一个现代的企业级商业智能（BI）Web应用程序，主要用于数据探索和可视化。它由 Apache 软件基金会支持，是一个开源项目，拥有活跃的社区和丰富的生态系统。Apache Superset提供了直观的用户界面，使得创建、分享和可视化数据变得简单，同时支持多种数据源和丰富的可视化选项‌。

通过 TDengine 的 Python Connector, ‌Apache Superset‌ 可轻松支持查询 TDengine 提供的时序数据，并提供数据展现、分析等功能

## 安装 Apache Superset
确保已安装 Apache SuperSet V2.1.0 及以上版本, 如未安装，请到其 [官网](https://superset.apache.org/) 安装

## 安装 TDengine
TDengine 企业版及社区版均可支持，要求版本在 3.0 及以上即可

## 安装 TDengine Python 连接器
TDengine 的 Python 连接器 V2.1.17 及之后版本中自带支持 SuperSet 的连接驱动，会自动安装到 SuperSet 的安装目录下，连接使用的是 Websocket 协议，所以还需要安装 taos-ws-py 组件, 安装脚本如下：   
```bash
pip3 install taospy
pip3 install taos-ws-py
```

## SuperSet 中配置连接
第1步，进入新建数据库连接页面 
     SuperSet -> Setting-> Database Connections -> +DATABASE   
第2步，选择 TDengine 数据库连接  
   SUPPORTED DATABASES 下拉列表中选择 “TDengine” 项，若下拉列表中没有 TDengine 选项，请检查是否安装包安装顺序，需先安装 SuperSet 后安装 Python 连接器  
第3步，DISPLAY NAME 中给连接起个名字，任意填写即可   
第4步，SQLALCHEMY URL* 项为关键连接信息串，需重点填写。   
    连接串格式： taosws://用户名:密码@主机名:端口号    
    参数说明：   

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
这里只介绍一款做基本的使用方式，方便确认可以获取得 TDengine 中存储的数据  
1. 点周右上角 “+”号按钮，选择 SQL query, 进入查询界面  
2. 左上角 DATABASE 下拉列表中可以看到刚才增加过的各种数据源连接驱动，选择一个你要测试的连接驱动  
3. SCHEMA 下拉列表，对应 TDengine 数据库名（系统表在此不显示）  
4. SEE TABLE SCHEMA 对应 TDengine 超级表名及普通表名，子表不在此显示  
5. 切换上面选择项，在下面相应会显示选择的表的列名及列数据类型信息及标识有索引的字段  
6. 在中间SQL编辑器中可以输入符合 TDengine 语法的任意 SQL 语句执行并显示结果  

## 示例