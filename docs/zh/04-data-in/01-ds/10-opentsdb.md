---
sidebar_label: OpenTSDB
title: “OpenTSDB”数据源
description: 使用“OpenTSDB”数据源导入数据到 TDengine Cloud 的实例
---

OpenTSDB 数据写入，是通过连接代理把数据从 OpenTSDB 服务器写入到当前选择的 TDengine Cloud 实例。

## 先决条件

- 创建一个空数据库来存储 OpenTSDB 数据。更多信息，请参阅 [数据库](../../../programming/model/#create-database)。
- 确保连接代理运行在与 OpenTSDB 服务器位于同一网络的机器上。更多信息，请参阅 [安装连接代理](../install-agent/)。

## 具体步骤

### 1. 新增数据源

在 TDengine Cloud 中，在左边菜单中打开 **数据写入** 页面，在 **数据源** 选项卡上，单击 **添加数据源**打开新增页面。在**名称**输入框里面填写这个数据源的名称，并选择 **OpenTSDB** 类型，在**代理**选择框里面选择已经创建的代理，如果没有创建代理，请点击旁边的**创建新的代理**按钮去创建新代理。

在**目标数据库**里面选择一个当前所在的 TDengine Cloud 实例里面的数据库作为目标数据库，由于 OpenTSDB 存储数据的时间精度是毫秒，所以这里需要选择一个 _`毫秒精度的数据库`_ 。

### 2. 配置连接信息

在 **连接配置** 区域填写 _`源 OpenTSDB 数据库的连接信息`_，如下图所示：

![OpenTSDB-01zh-FillInTheConnectionInformation.png](./pic/OpenTSDB-03zh-FillInTheConnectionInformation.png '填写源OpenTSDB数据库的连接信息')

可以点击**连通性检查**, 检查 Cloud 实例 与 OpenTSDB 服务之间是否可以连通。

### 3. 配置任务信息

**物理量 Metrics** 是 OpenTSDB 数据库中存储数据的物理量，用户可以指定多个需要同步的物理量，未指定则同步数据库中的全部数据。如果用户指定物理量，需要先点击右侧的 **获取 Metrics** 按钮获取当前源 OpenTSDB 数据库的所有物理量信息，然后在下拉框中进行选择，如下图所示：

![OpenTSDB-02zh-GetAndSelectMetrics.png](./pic/OpenTSDB-06zh-GetAndSelectMetrics.png '获取并选择物理量')

**起始时间** 是指源 OpenTSDB 数据库中数据的起始时间，起始时间的时区使用 explorer 所选时区，此项为必填字段。

**结束时间** 是指源 OpenTSDB 数据库中数据的截止时间，当不指定结束时间时，将持续进行最新数据的同步；当指定结束时间时，将只同步到这个结束时间为止，结束时间的时区使用 explorer 所选时区，此项为可选字段。

**每次读取的时间范围（分钟）** 是连接器从源 OpenTSDB 数据库中单次读取数据时的最大时间范围，这是一个很重要的参数，需要用户结合服务器性能及数据存储密度综合决定。如果范围过小，则同步任务的执行速度会很慢；如果范围过大，则可能因内存使用过高而导致 OpenTSDB 数据库系统故障。

**延迟（秒）** 是一个范围在 1 到 30 之间的整数，为了消除乱序数据的影响，TDengine 总是等待这里指定的时长，然后才读取数据。

### 4. 配置高级选项

**高级选项** 区域是默认折叠的，点击右侧 `>` 可以展开，如下图所示：

![OpenTSDB-03zh-AdvancedOptionsExpandButton.png](./pic/OpenTSDB-07zh-AdvancedOptionsExpandButton.png '高级选项展开按钮')
![OpenTSDB-04zh-AdvancedOptionsExpand.png](./pic/OpenTSDB-08zh-AdvancedOptionsExpand.png '高级选项展开按钮')

### 5. 创建完成

填写完以上信息后，点击 **提交** 按钮，即可启动从 OpenTSDB 到 TDengine 的数据同步。
