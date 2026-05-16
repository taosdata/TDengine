# taosX 测试进度跟踪-v3.3.2.0

开发技术追踪：[3.3.1.0 开发计划追踪](https://taosdata.feishu.cn/wiki/UuDww20QAi34BekNuL9c5Fh9nKc)
请大家在以下表格中阶段性更新各功能最新的测试状态：

|  | Data In: Oracle | OPC: 动态调整点位 | Explorer: 建表时支持复合主键和压缩增强 | License: 支持 TDengine 双活 | Kafka: SSL | Transformer: 支持 UDT 和动态更新 | Transformer： MQTT、Kafka支持json array格式消息解析 | Data In: SQL server | 用户名密码权限导出 | Explorer： 支持Geometry 和 varbinary 数据类型 | 数据源获取示例数据 | PI transform |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Owner | @聂敏慧 | @秦冲 | @聂敏慧 | @宋正勤 | @聂敏慧 | @王旭 | @贾晨阳 | @秦冲 | @聂敏慧 | @聂敏慧 | @聂敏慧 |  |
| Test Spec | [link](https://taosdata.feishu.cn/wiki/ENU2wjrK1idRYPkhcJwcwGRqnvg) | [link](https://taosdata.feishu.cn/wiki/D0iQwgA6miVqX5kPiclctWXVnPg) | [link](https://taosdata.feishu.cn/wiki/RGZkwt7PJiPR5XkVnzUcldg8nBe) | [link](https://taosdata.feishu.cn/wiki/XmUbwN4KciMan3kW3d1cuhSunzf) | n/a | [link](https://taosdata.feishu.cn/wiki/BSZIw8lslimS8Jka9xEcKfESnld) | n/a | [link](https://taosdata.feishu.cn/wiki/GQRCwenMeiBhIdk82HAcrmGHnWb) | [link](https://taosdata.feishu.cn/wiki/RROEw9WdjiVj2DkLLcrcz7sbneg) | [link](https://taosdata.feishu.cn/wiki/QoNvwSJBniw251kP2CZch4x3nYD) | [link](https://taosdata.feishu.cn/wiki/PZVzwBXPkixyJUknxnkcECM8nEc) |  |
| Jira | [oracle](https://jira.taosdata.com:18080/issues/?jql=project%20%3D%20TD%20and%20labels%20in%20(oracle)) | [OPC 动态点位](https://jira.taosdata.com:18080/issues/?jql=summary%20~%20%22taosx%20OPC%20%E5%8A%A8%E6%80%81%E7%82%B9%E4%BD%8D%22) | [explorer 建表支持复合主键和压缩增强](https://jira.taosdata.com:18080/issues/?jql=text%20~%20%22explorer%20%E5%BB%BA%E8%A1%A8%22%20and%20creator%20%3D%20Mia%20) | [双活授权](https://jira.taosdata.com:18080/issues/?jql=summary%20~%20%22%E5%8F%8C%E6%B4%BB%E6%8E%88%E6%9D%83%22) | [kafka](https://jira.taosdata.com:18080/issues/?jql=text%20~%20%22taosx%20kafka%22%20AND%20%20(%22Epic%20Link%22%20%3DTD-28725%20or%20%22Epic%20Link%22%20%3D%20TD-29896%20)%2520and%2520creator%2520%253DMia%2520) | [udt](https://jira.taosdata.com:18080/issues/?jql=project%20%3D%20TD%20AND%20summary%20~%20%22udt%22%20and%20reporter%20%3D%20xwang) | [json array](https://jira.taosdata.com:18080/issues/?jql=labels%20%3Djsonarray%20) | [mssql](https://jira.taosdata.com:18080/issues/?jql=labels%20%3D%20mssql%20and%20labels%20%3D%20taosX%20) |  | [Explorer geometry/varbinary](https://jira.taosdata.com:18080/issues/?jql=text%20~%20%22explorer%20Geometry%2FVarbinary%22) | [MQTT Kafka 示例数据](https://jira.taosdata.com:18080/issues/?jql=text%20~%20%22%20%E7%A4%BA%E4%BE%8B%E6%95%B0%E6%8D%AE%22%20AND%20createdDate%20%3E%3D%202024-06-21%20ORDER%20BY%20summary%20ASC) |  |
| 未解决/总数 | 2/4 | 1/13 | 0/4 | 1/3 | 2/12 | 1/4 | 1/1 | 0/4 |  | 1/1 | 7/7 |  |
| 提测时间 |  | 0513 | 0516 | 0517 |  | 0530 | 0603 | 0605 |  |  | 0607 |  |
| 测试完成 ETA |  |  |  |  |  |  |  |  |  |  |  |  |
| 当前进度 | 90% | 100% | 100% | 100% | 90% | 70% | 100% | 100% |  | 95% | 90% |  |
| 风险和异常 |  | 0515 Server 侧新增的点位数据无法正常入库，志宇反馈解决起来较为复杂。 |  |  |  | 0530 提测时，不包含对外暴露的动态更新 udt 的 API |  |  |  |  | mqtt 尚未提测 |  |
| 0517 |  |  |  | 测试已完成，但还需要在这两个问题修复以后再次验证 |  |  |  |  |  |  |  |  |
| 0523 |  | 测试用例已完成，有一些 improvement 没有修改。还有一个 bug 需要验证。 | 测试已完成，还有一些improvement需要修复验证。 [TD-30006](https://jira.taosdata.com:18080/browse/TD-30006) 影响explorer中对表增加列的功能 |  |  |  |  |  |  |  |  |  |
| 0529 | 测试已完成，还有一些bug需要修复验证 |  |  |  |  |  |  |  |  |  |  |  |
| 0613 |  |  |  |  |  |  |  |  |  |  |  | 已完成多列模式下默认配置的测试； 单列模式还未提测； transform还未开始测试 |
|  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |

## 1. Notes:

- 以下功能会在 3.3.0.1 发布，发布日期不晚于 5 月 31 日
  - Data In: Oracle
  - OPC: 动态调整点位
  - Explorer: 建表时支持复合主键和压缩增强
  - License: 支持 TDengine 双活
