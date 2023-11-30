---
title: "TDengine 订阅"
sidebar_label: "TDengine 订阅"
---

本文讲述如何使用 Explorer 订阅另一个集群的数据到本集群。

## 准备工作

在源 TDengine 创建 Topic。例如创建名字为 db_test 的 Topic：
```
create topic db_test as database test;
```

## 步骤

### 第一步： 进入“新增数据源”页面
1. 点击左侧“数据写入”菜单
2. 点击“新增数据源”
![步骤一](./tmq-step1.png)

### 第二步：输入数据源信息
1. 输入任务名称
2. 选择任务类型“TDengine 订阅”
3. 选择目标数据库
4. 以 DSN 形式输入源端数据库连接信息和要订阅的 Topic。例如：tmq+ws://root:taosdata@localhost:6041/topic
5. 完成以上步骤点击“连通性检查”按钮，测试与源端的连通性
![步骤二](./tmq-step2.png)

### 第三步： 订阅设置
1. 选择订阅初始位置。可配置从最早数据（earliest）或最晚（latest）数据开始订阅，默认为 earliest
2. 设置超时时间。支持单位 ms（毫秒），s（秒），m（分钟），h（小时），d（天），M（月），y（年）
3. 点击“新增按钮”
![步骤三](./tmq-step3.png)

### 第四步：监控任务运行情况

提交任务后，回到数据源页面可以查看任务状态。
![](./tmq-step4.png)

点击 “查看”按钮，可以监控任务的动态统计信息。
![](./tmq-step5.png)

也可以点击左侧折叠按钮，展开任务的活动信息。如果任务运行异常，此处可以看到详细的说明。
![](./tmq-step6.png)

## 高级用法

1. FROM DSN 支持多个 Topic，多个 Topic 的名字用逗号分割。例如： `tmq+ws://root:taosdata@localhost:6041/topic1,topic2,topic3`
2. 在 FROM DSN 中，可以用数据库名称、超级表名称或子表名称代替 Topic 名称。例如：`tmq+ws://root:taosdata@localhost:6041/db1,db2,db3`,此时不必要提前创建 Topic，taosX 将自动识别到使用的是数据库名称，并自动在源集群创建订阅数据库的 Topic。
3. FROM DSN 支持 group.id 参数，显示指定订阅用的 group ID。不指定情况下将使用随机生成的 group ID。