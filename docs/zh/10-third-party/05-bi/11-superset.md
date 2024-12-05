---
sidebar_label: SuperSet
title: 与 SuperSet 的集成
toc_max_heading_level: 4
---
‌Apache Superset‌ 是一个现代的企业级商业智能（BI）Web应用程序，主要用于数据探索和可视化。它由 Apache 软件基金会支持，是一个开源项目，拥有活跃的社区和丰富的生态系统。Apache Superset提供了直观的用户界面，使得创建、分享和可视化数据变得简单，同时支持多种数据源和丰富的可视化选项‌。

通过 TDengine 的 Python Connector, ‌Apache Superset‌ 可轻松支持查询 TDengine 提供的时序数据，并提供数据展现、分析等功能

## 版本要求
SuperSet 版本： >= 2.1.0   
taospy 连接器版本 >= 2.1.6   
taos-ws-py 连接器版本 >= 2.0.1   
TDengine 版本： >= 3.0 及以上   

## 安装驱动
第 1 步，首先安装 ‌Apache Superset‌ 要求的版本。   
第 2 步，安装 TDengine 的 Python 连接器要求的版本。   
第 3 步，安装 TDengine 的 Python Websocket 连接器要求的版本。   
**注意** 第 1 步和第 2 步不能颠倒，否则会导致 TDengine 无法把驱动文件安装到 SuperSet 指定的目录下。   

## SuperSet 中配置连接驱动

这里我们在 SuperSet 中配置 TDengine 的驱动连接信息

第 1 步 SuperSet 界面中右上角 Setting-> Database Connections 进入数据库连接管理页面。   
第 2 步 右上角按钮 +DATABASE 进入增加数据库连接页面。   
第 3 步 在 SUPPORTED DATABASES 的下拉列表中选择 “TDengine” 项。   
第 4 步 接下来的界面中 DISPLAY NAME 中给连接起个名字，随便填写即可。   
第 5 步 界面中 SQLALCHEMY URL* 项是关键连接信息串，需重点填写。   
       连接串格式： 连接协议://用户名:密码@主机名:端口号
       参数说明：   
         连接协议：   
              taos:     native    连接方式   
              taosws:   websocket 连接方式   
              taosrest: restful   连接方式   
         用户名： 登录 TDengine 数据库的用户名  
         密码：   登录 TDengine 数据库的密码  
         主机名： TDengine 数据库所在主机的名称  
         端口号： 根据不同协议提供登录的端口号  
                taos     协议默认端口 6030  
                taosws   协议默认端口 6041  
                taosrest 协议默认端口 6041  
         示例：以 websocket 协议连接本机的 TDengine 数据库，使用默认用户名密码       
           taosws://root:taosdata@localhost:6041  
第 6 步 配置好以上关键信息后，可点击 "TEST CONNECTION" 测试连接是否能够成功，测试通过后点 CONNECT 按钮，完成连接   
       

## 开始使用
这里只介绍一款做基本的使用方式，方便确认可以获取得 TDengine 中存储的数据  
第1步，点周右上角 “+”号按钮，选择 SQL query, 进入查询界面  
第2步，左上角 DATABASE 下拉列表中可以看到刚才增加过的各种数据源连接驱动，选择一个你要测试的连接驱动  
第3步，SCHEMA 下拉列表，对应 TDengine 数据库名（系统表在此不显示）  
第4步，SEE TABLE SCHEMA 对应 TDengine 超级表名及普通表名，子表不在此显示  
第5步，切换上面选择项，在下面相应会显示选择的表的列名及列数据类型信息及标识有索引的字段  
第6步，在中间SQL编辑器中可以输入符合 TDengine 语法的任意 SQL 语句执行并显示结果  
