# 优化 DataIn 任务的性能指标 - FS

## 1. 背景

河北电力项目中，用户提出，希望为 Kafka DataIn 任务增加性能指标，以配任务状态是否正常（包括假死状态判断），消费是否正常，以及是否存在数据丢失。
相关的JIRA：
https://jira.taosdata.com:18080/browse/TS-5967

## 2. 变更历史

| **日期** | **版本** | **负责人** | **主要修改内容** |
| --- | --- | --- | --- |
| 2025/4/22 | 0.1 | @杨志宇 | 初稿 |
|  |  |  |  |

## 3. 定义

1. 解析后行数（parsed_rows）：执行“解析”后的 recordBatch 的行数；
2. 过滤器筛掉的行数（filter_skipped_rows）：执行“过滤”操作，减少的行数；
3. 前置合法检查筛掉的行数（check_skipped_rows）：经过前置合法性检查时，执行 Skip 策略减少的行数；
4. 待入库行数（write_ready_rows）：完成 transform 流程后，待写入 TDengine 的行数；
5. 写入成功的行数（processed_rows）：写入 TDengine 成功的行数；
6. 数据异常丢弃的行数（drained_rows）：写入 TDengine 时，遇到已知的数据异常时，执行丢弃（Skip）的行数；
7. 写入失败的行数（failed_rows）：写入 TDengine 时，其他失败失败的行数。

## 4. 行为说明

### 4.1 指标的关系

Flat 类型的数据处理，满足以下指标：
1. 解析后的行数 - 过滤器筛掉的行数 - 前置合法检查筛掉的行数 = 待入库行数
2. 待入库行数 = 写入成功的行数 + 数据异常丢弃的行数 + 写入失败的行数

## 5. 性能

统计 metrics 对性能的影响可以忽略不计。

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

## 9. 约束和限制

无

## 10. 常见错误和排查

无

## 11. 可观测性

对 taos shell, taos Explorer， TDinsight 等基于 UI 或用户交互界面的产品组件是否有影响，如果有则说明清楚这几个组件的行为变化

## 12. 安装和卸载

无

## 13. 文档

无

## 14. 参考文档

1. [20250408需求开发情况](https://taosdata.feishu.cn/wiki/PkRSwY6stivytuklP2wcGUrUnsb)
2. [taosX 可观测性](https://taosdata.feishu.cn/wiki/I5EawNL4ViT082k5RwTcoIEMnRc)
3. [FS - 写入异常处理](https://taosdata.feishu.cn/wiki/TY2vwP511ikOkfkQL0zcHscknJf)
4. [Kafka Datain 任务异常测试 ](https://taosdata.feishu.cn/wiki/HsG4woJu2iJQmRkyOzVcKLPXnHd)

## 15. 附录

无
