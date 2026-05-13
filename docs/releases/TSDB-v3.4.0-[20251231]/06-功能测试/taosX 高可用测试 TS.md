# taosX 高可用测试 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-02 | - | 0.1 | 霍琳贺 | 初稿 |
| 2026-01-19 |  | 0.2 | @闫宇星 | 添加 agent 相关测试 |

## 2. 测试目标

本测试规范旨在验证 taosX 高可用集群架构的完整性、可靠性和性能，主要测试目标包括：
1. **功能完整性**：验证所有 SQL 命令、API 接口和管理功能的正确性；
2. **高可用性**：验证节点故障时的故障转移（Failover）和任务恢复能力；
3. **负载均衡**：验证任务分片在多节点间的均衡分配和执行；
4. **数据一致性**：验证分布式环境下数据写入的正确性和一致性；
5. **安全性**：验证认证、授权、加密和审计等安全机制；
6. **性能可扩展性**：验证集群扩展对性能的提升效果；
7. **易用性**：验证 SQL 命令和 Explorer UI 的易用性。

## 3. 参考文档

<quote-container>
- [taosX 高可用 - DS](https://taosdata.feishu.cn/wiki/ZIzBw9H8kiFw5Jk3ftUcpP0Xns3)
- [DS - taosX 分布式节点任务配置](https://taosdata.feishu.cn/wiki/DauGwSVKkibFQvkbAiZcVGtFnAb)
</quote-container>

## 4. 测试结论

<quote-container>
测试结论中包含结论和关键数据，但不需罗列过多细节，此处需要把把握信息的详细程度，原则上是外部 Reviewer 能够获得清晰的测试结论且尽量没有冗余信息为标准（这个标准是一句正确的废话，具体实行中需要大家 case by case 来处理）
</quote-container>

## 5. 测试环境

- OS: Linux
- Browser: Chrome

## 6. 功能测试

### 6.1 XNODE 节点管理

| # | 测试用例 | 测试描述 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | 创建 XNode（含用户） | 执行 `CREATE XNODE 'x1:6050' USER __xnode__ PASS 'Ab123456'` | 节点入库，状态 online，自动校验用户 | 通过 |
| 2 | 创建 XNode（复用用户） | 第二个节点省略用户参数 | 复用首个 XNode 用户，节点状态 online | 通过 |
| 3 | 查看 XNode 列表 | `SHOW XNODES` | 返回所有节点及 online/offline 状态 | 通过 |
| 4 | DRAIN 模式 | `DRAIN XNODE <id>` 后观察调度 | 节点不再接收新分片，存量分片被迁移 | 通过 |
| 5 | 删除 XNode（安全下线） | `DROP XNODE '<url>'`，节点有任务时触发清理 | 任务被重分配，节点状态 offline 后删除成功 | 通过 |
| 6 | 删除 XNode（force） | `DROP XNODE FORCE <id>` | 强制删除记录，任务重分配，其余节点可继续运行 | 通过 |

### 6.2 任务管理

| # | 测试用例 | 测试描述 | 预期结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | 创建任务（SQL） | `CREATE XNODE TASK 't1' FROM 'kafka://...' TO database db WITH parser='parser_json'` | 任务入库，状态 running | 通过 |
| 2 | 启动任务 | `START XNODE TASK 't1'` `START XNODE TASK 1` | 任务状态 running，生成分片并分配到可用 XNode | 通过 |
| 3 | 停止任务 | `STOP XNODE TASK 't1'` `STOP XNODE TASK 1` | 任务状态 stopped，分片优雅停止 | 通过 |
| 4 | 删除任务 | `DROP XNODE TASK 't1'` `DROP XNODE TASK 1` | 停止后删除元数据；强制删除跳过运行校验 | 通过 |
| 5 | 并发创建同名任务 | 并发提交同名 `t_dup` | 返回 `Task name already exists`相关错误，且无脏数据 | 通过 |

### 6.3 负载均衡

| 测试用例 | 测试描述 | 预期结果 | 测试结果 |
| --- | --- | --- | --- |
| 任务分配 | 7 分片、3 节点，策略 RoundRobin | 分片分布均衡，无长尾 | 通过 |
| 重新平衡 | `REBALANCE XNODE JOBS WHERE task_id=1` | 分片重新分布，均衡度提升 | 通过 |
| 手动将任务调度到其他节点 | rebalance xnode job 59 with xnode_id 12; | 任务被调度到指定节点 | 通过 |
| 重试与失败标记 | 分片执行失败，达最大重试 | 分片标记 failed，任务 reason 记录最后失败原因 |  |

### 6.4 Agent 管理

| 测试用例 | 测试描述 | 预期结果 | 测试结果 |
| --- | --- | --- | --- |
| 创建 agent | create xnode agent 'name'; | Agent 添加成功 | 通过 |
| 创建 agent 并把状态置为 idle | create xnode agent 'name' with status 'idle'; | Agent 添加成功且状态微 idle | 通过 |
| 查看 agent 列表 | Show xnode agents; | 列出所有 agent 且各字段值正确 | 通过 |
| 修改 agent 状态 | Alter xnode agent 1 with status = 'xxx'; | Agent 修改成功且 show xnode agents 可以看到修改后的状态值 | 通过 |
| 删除 agent | Drop xnode agent 1; | 删除成功且 show 列表为空 | 通过 |

### 6.5 Agent 任务管理

| 测试用例 | 预期结果 | 测试结果 |
| --- | --- | --- |
| 创建并运行一个 agent 任务 | 任务成功运行并写入数据 | 通过 |
| 停止运行中的任务 | 任务成功停止，agent 输出任务停止的日志 | 通过 |
| 启动任务，任务运行中，停止 agent 进程，再启动 | Activity 表依次接收到 transferring，waiting，transferring 日志 | 通过 |
| 任务运行中，删除 agent | 任务停止，agent 断开与 xnode 的连接 | 通过 |

### 6.6 异常用例

| 测试用例 | 测试描述 | 预期结果 |  |
| --- | --- | --- | --- |
| 一个 XNode 重启 | 关闭一台 XNode (taosx) | MNode 判定 offline，分片自动迁移，任务持续运行 | 通过 |
| 多个 XNode 重启 | 三节点关闭其中两台 XNode | MNode 判定两节点 offline，分片自动迁移，任务持续运行 | 通过 |
| 所有 XNode 宕机并重启 | 三节点重启 XNode | 任务中断一段时间并恢复运行 注：由于在 xnode 宕机和重连时，会进行任务迁移，因此所有节点重启后，任务运行所在地址不会是三节点宕机前的地址 | 通过 |
| 一个或多个 XNode 连接故障 | 模拟网络阻塞 | XNode 任务中断，分片自动迁移. 注：服务端添加了 hb ，断开连接后可以很快把任务删除掉，方便 xndoed 把任务调度到其他节点 | 通过 |
| 数据源不可用，任务启动失败后，手动调度到指定节点 | 模拟任务启动失败后的手动调度 | 任务成功调度到指定节点 | 通过 |
| 数据源宕机，重启 | Kafka 数据源停机一段时间后重启 | 任务不发生迁移，重启后任务恢复 注：数据源 kafka 宕机会导致任务退出并重新调度，kafka 恢复后任务正常运行 | 通过 |
| 与数据源的网络断开后恢复 | 模拟 Kafka 数据源网络阻塞 | 任务不发生迁移，网络恢复后写入恢复 |  |
| 与 taosd 的网络连接断开后恢复 | 模拟 xnode 与 taosd 网络阻塞 | 短时间恢复，任务不发生迁移，网络恢复后正常运行 |  |
| Taosd 重启 | `Kill -9 pidof taosd` | 1. 短时间重启，不影响任务运行，taosd 重启后写入恢复 1. taosd 中断时间长，任务会失败，之后继续重试 | 通过 |
| Mnode xnode 管理进程异常退出 | Kill -9 `pidof xnoded` | 重新拉起进程并恢复 | 通过 |
| 心跳超时判定 | 模拟心跳丢失 > 15s | 节点状态转 offline，触发迁移 | 通过 |
| 共享存储抖动 | 注入高延迟 | 调度退避/重试，无大规模失败 |  |
| Mnode 切换 | Mnode 切换 leader, xnoded 伴随 leader 节点重新启动，原节点 xnoded 进程退出 | 原 xnoded 退出，新 leader 节点的 xnoded 进程启动，且运行中的任务 和 job 不受影响 |  |
| Agent 与 xnode 网络连接断开 |  |  |  |
| Xnode 节点宕机并重启 |  |  |  |

## 7. 易用性测试

### 7.1 SQL 易用性测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |

### 7.2 Explorer 易用性测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |

## 8. 长期稳定性测试（可选）

这里用于描述稳定性测试相关的内容。

## 9. 性能测试

### 9.1 性能基准

提供三节点情况下各数据源的性能基准：
1. Kafka ORC 模拟数据源 12 分区负载均衡下的性能基准；
2. MQTT 共享主题订阅的性能基准；
3. TODO

## 10. 安全测试

### 10.1 SQL 语法安全测试

SQL 语法随机测试，对不支持的语法正确报告语法错误，对支持的语法正确解析（返回正确或非语法错误的错误码）。

### 10.2 License 授权安全测试

### 10.3 权限控制安全测试

### 10.4 节点管理、任务管理 SQL 安全测试

### 10.5 渗透测试

## 11. 兼容性测试

1. 因设计变更，新版本 taosx 服务不兼容旧版本
2. 云服务暂时不上线此版本，无需进行云服务兼容性测试

## 12. 已知问题和限制

### 12.1 Mnode 依赖问题

taosx 任务依赖 mnode 的高可用，在 mnode 不可用时：
- 无法创建、修改、删除、启动、停止 taosx 任务；
- 无法进行负载均衡；
- 无法完成故障转移；

### 12.2 数据源故障转移的限制

1. 有文件依赖的数据源，需要保持各节点文件一致性才能进行故障转移，包括但不限于：
   - CSV 数据文件导入；
   - ORC 数据文件导入；
   - 基于 CSV 文件的 OPC-UA/DA 数据导入；
   - 基于 CSV 文件的 KingHistorian 数据导入；
   - 基于 CSV 文件的 TDengine 查询迁移；
   - 自定义 Transform 插件，包括基于文件的 Rhai UDT（User Defined Transformer） 插件和 C SDK 的  UDT 插件；
