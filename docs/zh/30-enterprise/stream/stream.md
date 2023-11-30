## 流计算
通过 Explorer, 您可以轻松地完成对流的管理，从而更好地利用 TDengine 提供的流计算能力。
点击左侧导航栏中的“流计算”，即可跳转至流计算配置管理页面。
您可以通过以下两种方式创建流：流计算向导和自定义 SQL 语句。当前，通过流计算向导创建流时，暂不支持分组功能。通过自定义 SQL 创建流时，您需要了解 TDengine 提供的流计算 SQL 语句的语法，并保证其正确性。
 ![stream-01-流计算入口.jpeg](./pic/stream-01-流计算入口.jpeg "进入流计算页面")

 ### 创建流计算
![stream-02-创建流计算入口.jpeg](./pic/stream-02-创建流计算入口.jpeg "创建流计算入口")
 1. Wizard 方式 
 第一步 填写创建流计算需要的信息，点击 创建 按钮；

![stream-03-创建流计算Wizard.jpeg](./pic/stream-03-创建流计算Wizard.jpeg "创建流计算 Wizard 页面")
![stream-04-创建流计算Wizard.jpeg](./pic/stream-04-创建流计算Wizard.jpeg "创建流计算 Wizard 页面")

第二步 页面出现以下记录，则证明创建成功。
![stream-05-创建流计算成功1.jpeg](./pic/stream-05-创建流计算成功1.jpeg "查看已创建的流计算")

2. Sql 方式

第一步 切换到 SQL 页，直接输入创建流计算 sql, 点击 创建 按钮；
![stream-06-创建流计算Sql.jpeg](./pic/stream-06-创建流计算Sql.jpeg "创建流计算 SQL 页面")

第二步 页面出现以下记录，则证明创建成功。
![stream-07-创建流计算成功2.jpeg](./pic/stream-07-创建流计算成功2.jpeg "查看已创建的流计算")
