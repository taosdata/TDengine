# TDengine 内部占用测点不统计 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-06 | 2026-03-06 | 0.1 | 徐开礼 | 新建 |
|  |  |  |  |  |

## 2. 背景

- TDengine 会创建 log 库和审计库，用于记录操作日志和审计日志。[系统库中的系统表，不应该进行测点统计](https://project.feishu.cn/taosdata_td/feature/detail/6672169603)。

## 3. 定义

### 3.1 测点 

- 在数据采集、工业监控（如 SCADA、IoT）以及数据库系统中，**测点**是指在一个受控对象能够独立产生、记录并反映某一特定属性或状态变化的**基本信息点，**是“最小的数据采集与监控指标”。

### 3.2 测点数量

- TDengine 中，测点数量是所有子表和普通表的排除时间戳主键列之外的普通列数量之和。虚拟表不计算在内，内部所有库的系统表不计算在内，tag 列不计算在内。

## 4. 行为说明

- TDengine 会创建 log 库和审计库，用于记录操作日志和审计日志。这两个系统库中的系统表不应该计入测点数量。

### 4.1 实现方案对比

| 方案 | 描述 | 优点 | 问题及缺点 |
| --- | --- | --- | --- |
| 1 | 对系统表进行标识 | 简单、高效、准确 | 目前，创建系统库中的系统表，均采用标准 SQL 语法。如果通过语法标识系统表，有可能泄漏。即使不考虑易用性，通过硬编码的方式创建系统表，也有泄漏的风险。 |
| 2 | 排除 log 库和审计库中的所有表 | 简单，高效 | 由于不能禁止用户在 log 库和审计库中建表，所以统计有可能不准确。 |
| 3 | 排除 log 库和审计库的已知的系统表 | 简单，准确 | 实现相对复杂。 每次系统表新增时，需要手工修改代码，并且有可能忘记更新。 |
| 4 | 限定 log 库和审计库的建表权限 | 简单，高效，准确 | 为了灵活性和安全性，log 库的创建用户不固定，审计库的名称不固定，且均通过标准SQ L创建。因此，不便于仅为特定用户授予特定库的建表权限。 |

- 上述几种方案均不完美：
```sql
方案 1 不能完全排除泄漏的风险。
方案 2 不能禁止用户在 log/审计库建表。
方案 3 在系统表新增时要修改代码，并且有忘记更新的可能。考虑到系统表的新增不是很频繁，暂计划采用该方案。
方案 4 用户名和库名不固定，不便于仅为特定用户授予特定库的建表权限。
```

- 方案 3 详细说明
```sql {wrap}
1）目前 log库/审计库中，没有普通表；后续新增的系统表，均通过超级表/子表创建，不新增普通表。
2）计算测点时，排除 log库/审议库中已知的系统超级表对应的子表测点；
3）后续有新增的超级表，则由使用方通知 TDengine 更新系统超级表名单；
4）为防止更新不及时，额外冗余 1000 测点，仅用于判断测点数是否超出授权测点时使用，show grants 均展示实际的测点值。
```

### 4.2 系统表列表

- TDengine 3.4.0.0 及后续版本，审计库名称可以指定，不再固定为 audit。
- Log 库和审计库中的系统表，均为超级表/子表，没有普通表。

| 系统库 | 系统表（支持过滤的版本） |
| --- | --- |
| log | 1. ~~cluster_info(3.3)~~ 1. ~~data_dir(3.3)~~ 1. ~~dnodes_info(3.3)~~ 1. ~~d_info(3.3)~~ 1. ~~grants_info(3.3)~~ 1. ~~keeper_monitor(3.3，3.4.0.x 存在)~~ 1. ~~logs(3.3)~~ 1. ~~log_dir(3.3)~~ 1. ~~log_summary(3.3)~~ 1. ~~m_info(3.3)~~ 1. ~~taosadapter_restful_http_request_fail~~~~(3.3)~~ 1. ~~taosadapter_restful_http_request_in_flight(3.3)~~ 1. ~~taosadapter_restful_http_request_summary_milliseconds(3.3)~~ 1. ~~taosadapter_restful_http_request_total(3.3)~~ 1. ~~taosadapter_system_cpu_percent(3.3)~~ 1. ~~taosadapter_system_mem_percent(3.3)~~ 1. ~~temp_dir(3.3)~~ 1. ~~vgroups_info(3.3)~~ 1. ~~vnodes_role(3.3)~~ 1. taosd_dnodes_status（3.4.0.x 新增） 1. adapter_conn_pool（3.4.0.x 新增） 1. taosd_vnodes_info（3.4.0.x 新增） 1. taosd_dnodes_metrics（3.4.0.x 新增） 1. taosd_vgroups_info（3.4.0.x 新增） 1. taos_sql_req（3.4.0.x 新增） 1. taosd_mnodes_info（3.4.0.x 新增） 1. adapter_c_interface（3.4.0.x 新增） 1. taosd_cluster_info（3.4.0.x 新增） 1. taosd_sql_req（3.4.0.x 新增） 1. taosd_dnodes_info（3.4.0.x 新增） 1. adapter_requests（3.4.0.x 新增） 1. taosd_write_metrics（3.4.0.x 新增） 1. adapter_status（3.4.0.x 新增） 1. taos_slow_sql（3.4.0.x 新增） 1. taos_slow_sql_detail（3.4.0.x 新增） 1. taosd_cluster_basic（3.4.0.x 新增） 1. taosd_dnodes_data_dirs（3.4.0.x 新增） 1. taosd_dnodes_log_dirs（3.4.0.x 新增） 1. xnode_agent_activities（3.4.0.x 新增） 1. xnode_task_activities（3.4.0.x 新增） 1. xnode_task_metrics（3.4.0.x 新增） 1. taosx_task_progress（3.4.0.x 新增） 1. taosx_task_csv（3.4.0.x 新增） 1. taosx_task_kinghist（3.4.0.x 新增） 1. taosx_task_tdengine2（3.4.0.x 新增） 1. taosx_task_tdengine3（3.4.0.x 新增） 1. taosx_task_opc_da（3.4.0.x 新增） 1. taosx_task_opc_ua（3.4.0.x 新增） 1. taosx_task_kafka（3.4.0.x 新增） 1. taosx_task_influxdb（3.4.0.x 新增） 1. taosx_task_mqtt（3.4.0.x 新增） 1. taosx_task_avevahistorian（3.4.0.x 新增） 1. taosx_task_opentsdb（3.4.0.x 新增） 1. taosx_task_mysql（3.4.0.x 新增） 1. taosx_task_postgres（3.4.0.x 新增） 1. taosx_task_oracle（3.4.0.x 新增） 1. taosx_task_mssql（3.4.0.x 新增） 1. taosx_task_mongodb（3.4.0.x 新增） 1. taosx_task_sparkplugb（3.4.0.x 新增） 1. taosx_task_orc（3.4.0.x 新增） 1. taosx_task_pulsar（3.4.0.x 新增） 1. taosx_task_pspace（3.4.0.x 新增） |
| audit(3.4.0.0后非固定) | 1. operations(3.3) |

## 5. 性能

- 

## 6. 安全

- 

## 7. 兼容性

- 

## 8. 运维

### 8.1 最佳实践

### 8.2 注意事项

## 9. 使用场景

- 

## 10. 约束和限制

- 

## 11. 常见错误和排查

- 用户操作失败，错误码对照表

| Error code | description | note |
| --- | --- | --- |
|  |  |  |
|  |  |  |

## 12. 可观测性

- 

## 13. 安装和卸载

- 无特殊要求

## 14. 文档

- 

## 15. 参考

- [全链路的数据写入诊断工具 FS](https://taosdata.feishu.cn/wiki/KTA1wdRjAi1EFJkgaGmceqL7n9g)
- [TDengine 监测](https://taosdata.feishu.cn/wiki/B1W1wfUu8iSefQktLI3cRfeHntd)

## 16. 附录

-
