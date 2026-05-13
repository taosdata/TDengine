# Backport 监控功能到3.1分支 Test Spec

## 1. 测试目标

Backport 监控功能到3.1分支后，针对监控功能进行回归测试

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.7.1 | 1.0.0 | 翟坤 | 创建 |
| 2024.7.4 | 2.0.0 | 翟坤 | 更新测试数据 |
| 2024.7.8 | 3.0.0 | 翟坤 | 更新测试结论 |

## 3. 测试结论

测试通过

## 4. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 （测试阻塞，无法进行） | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 9 |
| 严重 Bug 总数 | 0 |

## 5. 已知问题和限制

- Requests统计数据正确性验证：因为统计数据里无法扣除系统底层的数据操作，真实数据会略多于测试数据，比如监控上报数据的写入操作，TDengine底层某些操作需要查询系统表等
- TDinsight上报的DNode Usage->Net数据是以byte为单位，而prometheus上报的数据是以bit为单位，所以TDInsight的数据约为prometheus的八分之一

## 6. 测试资源及环境

### 6.1 功能测试

   测试平台：Linux x64
   测试资源：192.168.0.215

### 6.2 性能测试

无

## 7. 测试范围及重点

- 监控数据正确上报
- TDinsight上监控数据显示正确

## 8. 测试用例

### 8.1 Cluster status

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | OVERIVEW | **显示内容** TDengine Cluster Dashboard (First EP: u0-215:6030, Version: 3.1.1.0) | PASS | N |  |
| 2 | First EP | **显示内容** u0-215:6030 | PASS | N |  |
| 3 | Version | **显示内容** 3.1.1.0 | PASS | N |  |
| 4 | 到期时间小于一周，显示为day | PASS | N |  |
| 5 | 到期时间小于一个月，显示为week | PASS | N |  |
| 6 | 到期时间小于一个年，显示为week | PASS | N |  |
| 7 | 到期时间大于一个年，显示为year | PASS | N |  |
| 8 | 有效期为unlimited，显示为unlimited | PASS | N |  |
| 9 | 配置有效期且有效期 > 50年，显示为unlimited | PASS | N |  |
| 10 | Used Measuring Points | 通过授权命令配置timeseries，从页面上查看used和total的数量与show grants查询的数量一致 | PASS | N |  |
| 11 | Databases | 创建多个database，查看数量是否一致 | PASS | N |  |
| 12 | Stables&Tables | **创建多个超级表和普通表，查看其总和与页面显示是否一致** | PASS | N |  |
| 13 | Connections | 显示数据与**show connections命令的查询结果数量一致** | PASS | N |  |
| 14 | Dnodes | 1. 创建3个节点，页面显示3个dnodes 1. 停止一个节点的process进程，页面显示dnodes_alive=2, tatol=3 | PASS | N |  |
| 15 | Mnodes | 1. 创建3个节点，创建3个mnode 1. 停止一个节点的process进程，页面显示dnodes_alive=2, tatol=3 | PASS | N |  |
| 16 | VGroups | 1. 创建3个节点 1. 停止一个节点的process进程，页面显示dnodes_alive=进程关闭节点对应的vgroup数量, tatol=vgroups总和 | PASS | N |  |
| 17 | Vnodes | 1. 创建3个节点 1. 停止一个节点的process进程，页面显示dnodes_alive=进程关闭节点对应的vnode数量, tatol=vnode总和 | PASS | N |  |
| 18 | DNodes Alive Percent | 根据实际情况计算dnodes在线数量占比正确 | PASS | N |  |
| 19 | Mnodes Alive Percent | 根据实际情况计算mnodes在线数量占比正确 | PASS | N |  |
| 20 | VGroups Alive Percent | 根据实际情况计算vgroups在线数量占比正确 | PASS | N |  |
| 21 | VNodes Alive Percent | 根据实际情况计算vnodes在线数量占比正确 | PASS | N |  |
| 22 | Measuring Point Used | 显示使用的measuring point正确，验证方法同case10 | PASS | N |  |

### 8.2 Dnodes Overview

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Dnodes status | 1. 创建3个节点，页面显示3个dnodes 1. 停止一个节点的process进程，页面显示2个dnode状态为ready，第三个状态为offline | PASS | N |  |
| 2 | DNondes Number | 操作同上一case，曲线显示正确 | PASS | N |  |

### 8.3 MNodes Overview

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | MNodes Status | 1. 创建3个节点，创建3个mnode 1. 停止一个附属节点的process进程，页面显示个mnode状态为leader，一个为follower，最后一个为offline | PASS | N |  |
| 2 | MNodes Number | 操作同上一case，曲线显示正确 | PASS | N |  |

### 8.4 Requests

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Select Request | 1. 通过python脚本连续执行10000次select操作 1. 后台查询监控表中执行查询时间区间上报的count总和大于1万次 1. 查看Select Request面板的平均查询速率曲线绘制合理 | PASS | N |
| 2 | Delete Request | 1. 通过python脚本连续执行10000次delete操作 1. 后台查询监控表中执行查询时间区间上报的count总和大于1万次 1. 查看Delete Request面板的平均删除速率曲线绘制合理 | PASS | N |
| 3 | Insert Request | 1. 通过python脚本连续执行10000次insert操作 1. 后台查询监控表中执行查询时间区间上报的count总和大于10万次 1. 查看Insert Request面板的平均插入速率曲线绘制合理 | PASS | N |
| 4 | Inserted Rows | 1. 通过python脚本连续执行插入10000行数据 1. 后台查询监控表中执行查询时间区间上报的count总和大于1万 1. 查看Inserted Rows面板的平均插入速率曲线绘制合理 | PASS | N |
| 5 | Slow SQL | 执行一个超过5s的sql，后台taos_slow_sql中上报数据正确，页面Slow SQL展示数据正确 | PASS | N |  |

### 8.5 Table Summary

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Stables | select count(*) from information_schema.ins_tables where db_name != 'information_schema' and db_name != 'performance_schema'; | PASS | N |  |
| 2 | Tables总数 | select count(*) from information_schema.ins_stables where db_name != 'information_schema' and db_name != 'performance_schema'; | PASS | N |  |
| 3 | Tables曲线图 | 曲线跟历史table数据对应一致 | PASS | N |  |
| 4 | Tables Number Foreach VGroups | 指定db下创建5张表，通过show vgroups查看表和vgroups的对应关系，验证页面上数据一致 | PASS | N |  |

### 8.6 Dnode Usage

| 测试类型 | 测试场景 | 用例No. | 测试用例名称 | 测试步骤 | 测试结果 | 自动化 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Uptime |  |  | N |  |
| 2 | Has Mnode | 1. 启动3节点集群，一个节点有mnode，页面显示正确 1. 在第二个节点创建mnode，页面显示正确 1. 在第三个节点创建mnode，页面显示正确 | PASS | N |  |
| 3 | CPU cores | 跟系统CPU核数一致 | PASS | N |  |
| 4 | VNodes Number | 通过select dnode_id, count(*) from information_schema.ins_vnodes group by dnode_id验证 | PASS | N |  |
| 5 | Vnode Master | 通过select dnode_id, count(*) from information_schema.ins_vnodes where status='leader' group by dnode_id验证 | PASS | N |  |
| 6 | Current CPU Usage of taosd | 通过taosBenchmark持续写入100亿行数据，cpu使用率跟top命令统计的taosd占用率大体一致 | PASS | N |  |
| 7 | Current Memory Usage of taosd | 通过prometheus的进程监控验证taosd的内存 | PASS | N |  |
| 8 | Max Disk Used | 通过df -h查询taosd对应的磁盘下当前使用率最高的数值，跟TDInsight显示的基本一致 | PASS | N |  |
| 9 | CPU Usage | 1. 通过taosBenchmark持续写入100亿行数据， 1. 通过prometheus的进程监控验证net的曲线波动趋势是否接近，因算法不同无法保证监控数据完全一致 | PASS | N |  |
| 10 | RAM Usage | 1. 通过taosBenchmark持续写入100亿行数据 1. 通过free -h命令查看free和total的内存 1. 通过prometheus的进程监控验证taosd的内存 | PASS | N |  |
| 11 | Disk Used | 通过df -h验证 | PASS | N |  |
| 12 | Disk IO | 1. 通过taosBenchmark持续写入100亿行数据 1. 通过prometheus的进程监控验证net的曲线是否接近，因算法不同无法保证监控数据完全一致 | PASS | N |  |
| 13 | Net | 1. 通过taosBenchmark持续写入100亿行数据 1. 通过prometheus的进程监控验证net的曲线波动趋势是否接近，因算法不同无法保证监控数据完全一致 | PASS | N | TDinsight上报的数据是以byte为单位，而prometheus上报的数据是以bit为单位，所以TDInsight的数据约为prometheus的八分之一 |
