---
sidebar_label: Node-RED
title: 与 Node-RED 集成
toc_max_heading_level: 5
---

[Node-RED](https://nodered.org/) 是由 IBM 开发的基于 Node.js 的开源可视化编程工具，通过图形化界面组装丰富节点实现物联网设备、API 及在线服务的连接，支持多协议、跨平台且社区活跃，适用于智能家居、工业自动化等场景的事件驱动应用开发，其主要特点是低代码、可视化。

node-red-node-tdengine 是涛思数据为 Node-RED 开发的官方插件，插件由两个节点组合，tdengine 节点提供 SQL 执行接口，可完成数据写入/查询及元数据管理等。tdengine-consumer 节点提供数据订阅功能，可从指定订阅服务器消费指定 TOPIC 的功能。

## 前置条件

准备以下环境：

- TDengine 3.3.2.0 及以上版本集群已部署并正常运行（企业/社区/云服务版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- Node-RED 3.0.0 及以上版本（ [Node-Red 安装](https://nodered.org/docs/getting-started/)）。
- Node.js 语言连接器 3.1.8 及以上版本。可从 [npmjs.com](https://www.npmjs.com/package/@tdengine/websocket) 下载。


## 配置数据源
插件通过 Node.js 语言连接器访问 TDengine 数据源， 数据源连接遵循 [Node.js 语言连接器](../../../reference/connector/node/)规则，配置步骤如下：

1. 启动 Node-RED 服务，使用浏览器进入 Node-RED 主页。

2. 拖动画布左侧区域内“存储”分类下的 tdengine 或 tdengine-consumer 节点至画布。

3. 选中节点，点击画布右侧区域上方带字典图标的帮助按钮，下方会显示此节点在线帮助信息

4. 双击画布中选中节点，弹出数据源连接属性设置窗口，根据在线帮助指引填写连接信息，填写完成保存。
   
5. 点击右上角“部署按钮” ，订阅节点状态变成绿色，表示数据源配置正确且连接正常。


## 数据分析


### 场景介绍


某生产车间有多台智能电表， 电表每一秒产生一条数据，数据准备存储在 TDengine 数据库中，并能每隔 1 分钟实时输出最新 1 分钟内各智能电表平均电流、电压及用电量。同时要求对电流超过 25A 或电压超过 230V 的设备进行过载报警并把报警信息存储在指定文件中。

我们使用 Node-RED + TDengine 来实现需求，使用 Inject + function 节点来模拟设备一秒产生一条数据，tdengine 节点写入功能来存储数据，统计汇总使用 tdengine 节点查询功能，设备过载实时报警使用 tdengine-consumer 订阅节点来完成。

假设 TDengine 服务器地址： 192.168.2.124 ，WEBSOCKET 端口：6041，使用默认用户名密码登录，模拟三个设备，命名为 d0，d1，d2。

### 数据准备
通过数据库管理工具 taos-CLI , 为采集数据进行手工建模，采用一张设备一张表的模型，建立一张超级表 meters，三个子表 d0，d1，d2。SQL 语句如下：
``` sql
create database test;
create stable test.meters (ts timestamp , current float , voltage int , phase float ) tags (groupid int, location varchar(24));
create table test.d0 using test.meters tags(1, 'workroom1');
create table test.d1 using test.meters tags(2, 'workroom1');
create table test.d2 using test.meters tags(3, 'workroom2');

```

### 数据采集
使用 tdengine 节点, 采集每台设备数据，操作步骤：
- <b>增加存储节点</b> 
  1. 画布左侧区域存储项分类中选择 tdengine 节点，拖动至画布中。
  2. 双击节点打开属性设置，名称命名为 'td-writer'，数据库项右侧点击“+”号图标。
  3. 弹出窗口中，名称命名为 'td124'，连接类型这里我们选择使用字符串连接，输入：
   ``` sql
   ws://root:taosdata@192.168.2.124:6041 
   ```   
  4. 点击“添加”并返回。

- <b>模拟设备产生数据</b> 
  1. 画布左侧区域“功能”项下选择 “function” 节点，拖动至画布 'td-write' 节点前。
  2. 双击节点打开属性设置，名称命名为 ‘write d0’， 下面选项卡选择“运行函数”，填写如下内容后保存返回画布。
   ``` javascript
      // generate rand
      const value2 = Math.floor(Math.random() * (30 - 5 + 1)) + 5; // 5-30
      const value3 = Math.floor(Math.random() * (240 - 198 + 1)) + 198; // 198-240
      const value4 = Math.floor(Math.random() * (3 - 1 + 1)) + 1; // 1-3

      // sql
      msg.topic = `insert into test.d0 values (now, ${value2}, ${value3}, ${value4}) ;`;

      return msg;
   ```
  3. 画布左侧区域“通用”项下选择 “inject” 节点，拖动至画布 ‘write d0’ 前。

   
  4. 双击节点打开属性设置，名称命名为‘inject1’，重复下拉框选择“周期性执行”，周期选择每隔 1 秒。保存返回画布。
   
  5. 相同方法再制作另外两台设备流程。
   
   
- <b>增加信息输出</b> 
  1. 画布左侧区域“通用”项下选择 “debug” 节点，拖动至画布 ‘td-write’ 节点后。
  2. 双击节点打开属性设置，勾选“节点状态”，下拉列表中选择消息数量。


以上节点增加完成后，依次把上面节点按顺序连接起来，形成一条流水线，至此数据采集流程制作完成。

点击右上角 “部署” 按钮发布修改内容，运行成功后可以看到 'td-write' 节点状态变成绿色且 debug 节点的数字在不断变化。
![td-writer](img/td-writer.webp)


### 数据查询
数据查询流程由三个节点（inject/tdengine/debug）组成，完成每隔 1 分钟实时输出最新 1 分钟内各智能电表平均电流、电压及用电量需求。
  1. inject 节点拖动至画布中，双击节点设置属性，名称填写 'query', msg.topic 填写并保存返回画布：
   ``` sql
   select tbname, avg(current),avg(voltage),sum(p) 
     from ( select tbname,current,voltage,current*voltage/60 as p from test.meters  where  ts > now-60s partition by tbname) 
     group by tbname;
   ``` 
  2. tdengine 节点拖动至画布中，双击节点设置属性，数据库选择前面已创建好的数据源 'td124'，保存并返回画布。
  3. debug 节点拖动至画布中，双击节点设置属性，勾选“节点状态”，下拉列表中选择“消息数量”，保存并返回画布。
  4. 依次把以上节点按顺序连接起来，点击 “部署” 按钮发布修改内容。

流程启动成功后可以看到 'td-reader' 节点状态变成“绿色” 表示流程工作正常，debug 输出节点的数字每隔一分钟更新一次，表示不断有数据汇总统计输出了。
  
![td-reader](img/td-reader.webp)
  
### 数据订阅
数据订阅流程由两个节点（/tdengine-consumer/debug）组成，完成设备过载警告提醒功能。
  1. 使用 taos-CLI 手工创建订阅主题 topic_overload ,  SQL 如下：
   ``` sql
   create topic topic_overload as 
        select tbname,* from test.meters 
        where current > 25 or voltage > 230;
   ``` 
  2. tdengine-consumer 节点拖动至画布中，双击节点设置属性，填写如下内容后保存并返回画布。
     - 名称：       td-consumer
     - 订阅服务器：  ws://192.168.2.124:6041
     - 用户名：     root
     - 密码：       taosdata
     - 订阅主题：    topic_overload
     - 消费开始位置：latest
     - 其它项保持默认
   
  3. debug 节点拖动至画布中，双击节点设置属性，勾选“节点状态”，下拉列表中选择“消息数量”，保存并返回画布。
  4. 依次把以上节点按顺序连接起来，点击 “部署” 按钮发布修改内容。
  5. 流程启动成功后可以看到 'td-consumer' 节点状态变成“绿色” 表示流程工作正常，debug 输出节点的数字每隔一分钟更新一次，表示不断有数据汇总统计输出。
  
![td-reader](img/td-consumer.webp)

