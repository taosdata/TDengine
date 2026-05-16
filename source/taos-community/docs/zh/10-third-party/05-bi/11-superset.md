---
sidebar_label: Superset
title: 与 Superset 集成
---
‌Apache Superset‌ 是一个现代的企业级商业智能（BI）Web 应用程序，主要用于数据探索和可视化。它由 Apache 软件基金会支持，是一个开源项目，它拥有活跃的社区和丰富的生态系统。Apache Superset 提供了直观的用户界面，使得创建、分享和可视化数据变得简单，同时支持多种数据源和丰富的可视化选项‌。

通过 TDengine TSDB 的 Python 连接器，‌Apache ‌Superset‌ 可支持 TDengine TSDB 数据源并提供数据展现、分析等功能。

## 前置条件

准备以下环境：

- TDengine TSDB 3.2.3.0 或以上版本已安装（企业及社区版均可）。
- taosAdapter 服务正常运行，检查参考 [taosAdapter 使用手册](../../../reference/components/taosadapter)。
- Apache Superset v2.1.0 或以上版本已安装，安装参考 [官方文档](https://superset.apache.org/)。
- Python 连接器 taospy 2.7.18 或以上版本已 [安装](https://pypi.org/project/taospy/)。
- Python 连接器（WebSocket）taos-ws-py 0.3.9 或以上版本已 [安装](https://pypi.org/project/taos-ws-py/)。

## 配置数据源

1. 进入新建数据库连接页“Superset”->“Setting”->“Database Connections”->“+DATABASE”。

2. 选择其它数据库连接，“SUPPORTED DATABASES”下拉列表中选择最后一项“Other”。

3. 在“DISPLAY NAME”中填写连接名称，任意填写即可。

4. 填写数据库连接串。  
    在“SQLALCHEMY URI”中按以下格式填写，务必填写正确。

    ```bash
    taosws://用户名:密码@主机名:端口号
    ```

    | 参数名称 | <center>参数说明</center>          |
    |:------- |:--------------------------------  |
    | 用户名   | 登录 TDengine TSDB 数据库用户名           |  
    | 密码     | 登录 TDengine TSDB 数据库密码            |
    | 主机名   | TDengine TSDB 数据库所在主机名称          |
    | 端口号   | 提供 WebSocket 服务的端口，默认：6041 |  

    示例：

    本机安装 TDengine TSDB 数据库，WebSocket 服务端口 6041，使用默认用户名密码，连接串为：

    ```bash
    taosws://root:taosdata@localhost:6041
    ```

5. 测试连接。  
   点击“TEST CONNECTION”测试连接是否成功，测试通过后点击“CONNECT”按钮，保存配置。

## 数据分析

### 数据准备

TDengine TSDB 数据源与其它数据源使用上无差别，这里简单介绍下数据查询：

1. “Superset”界面点击右上角`+`号按钮，选择`SQL query`，进入查询界面。
2. 左上角“DATABASE”下拉列表中选择前面已创建好的`TDengine`数据源。
3. “SCHEMA”下拉列表，选择要操作的数据库名（系统库不显示）。
4. “SEE TABLE SCHEMA”选择要操作的超级表名或普通表名（子表不显示）。  
5. 随后会在下方显示选定表的“SCHEMA”信息。  
6. 在“SQL”编辑器区域可输入符合 TDengine TSDB 语法的任意 SQL 语句执行。

### 智能电表样例

我们选择【Superset Chart】模板中较流行的两个模板做了效果展示，以智能电表数据为例：  

1. “Aggregate”类型，展示在第 4 组中指定时间段内每分钟采集电压值 (voltage) 最大值。  
![superset-demo1](./superset-demo1.webp)
2. “RAW RECORDS”类型，展示在第 4 组中指定时间段内 current, voltage 的采集值。  
![superset-demo2](./superset-demo2.webp)  
