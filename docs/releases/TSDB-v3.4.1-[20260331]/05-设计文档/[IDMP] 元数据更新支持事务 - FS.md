# [IDMP] 元数据更新支持事务 - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-25 | 2026-02-25 | 0.1 | 徐开礼 | 新建 |
| 2025-02-26 | 2025-02-26 | 0.2 | 徐开礼 | 根据线下评审进行修改 |

## 2. 背景

- TDengine 目前不支持 begin/commit/rollback 的元数据批处理事务处理框架，因此，基于 TDengine 虚拟表等功能的产品 IDMP 在应用内部通过反向操作的方式模拟实现了批事务，例如，创建失败了则反向删除，删除失败了根据初始创建SQL重建。
- 如果 TDengine 数据库本身支持元数据更新批处理事务框架，IDMP 的实现会更加简单，只需关心产品本身，而不需要处理本应由数据库处理的批事务操作。
- 目前，IDMP 使用的`元数据批事务更新`场景，涉及`虚拟表的创建/删除/修改`，暂不涉及实体表、时序数据和数据库。

## 3. 定义

- **分布式事务： 分布式事务**是指**事务的参与者、资源服务器、服务器等分别位于分布式系统的不同节点上**，需要跨多个独立服务 / 数据库 / 节点共同完成的事务。目标：保证**跨节点、跨服务的操作要么全部成功、要么全部失败**，不出现中间状态，保证数据一致性。
- **CAP 定理：**分布式系统无法同时满足：
```sql {wrap}
C（Consistency）        一致性：所有节点同一时间数据一致
A（Availability）       可用性：服务始终可用
P（Partition tolerance）分区容错性：网络分区不影响运行结论：分布式场景下必须保留 P，只能在 CP 或 AP 之间取舍。
```

- **BASE 理论：**是对 CAP 的工程落地，面向高可用做妥协
```sql {wrap}
Basically Available   基本可用
Soft state            软状态（允许中间状态）
Eventually consistent 最终一致性：放弃强一致，追求最终一致
```

- **ACID（事务本身标准）：**分布式事务也要尽量保证（只是放宽）
```sql {wrap}
A Atomicity 原子性
C Consistency 一致性
I Isolation 隔离性
D Durability 持久性
```

## 4. 行为说明

- 该文档，根据业务方需要的场景，暂只描述了`虚拟子表/虚拟普通表` 的场景，未针对 DB、view、stream、topic 进行展开。

### 4.1 事务语法

| 阶段 | 标准 SQL 语法 | 功能描述 |
| --- | --- | --- |
| 开启 | BEGIN; 或 START TRANSACTION; | 告诉 MNode：接下来的指令不要立即生效，先存入 TID 上下文。 |
| 执行 | CREATE... / ALTER... / DROP... | 这些指令会产生 PREPARED 状态的元数据。 |
| 提交 | COMMIT; | 触发 2PC 的第二阶段，将 PREPARED 状态翻转为 NORMAL。 |
| 撤销 | ROLLBACK; | 触发清理逻辑，擦除所有关联的影子条目或补偿改名。 |

### 4.2 事务中支持的操作

- 第一期，先解决 IDMP 最需要的场景，事务中仅支持以下操作：
```sql
创建虚拟子表/虚拟普通表：CREATE VTABLE
删除虚拟子表/虚拟普通表：DROP VTABLE
修改虚拟子表的标签值：   ALTER VTABLE ... SET TAG (修改子表标签值)
虚拟超级表增加/删除普通列和标签列：ALTER TABLE ... ADD/DROP COLUMN/TAG ...
```

### 4.3 并发事务

- 第一期，整个集群，全局暂只支持一个事务，暂不支持并发事务。
- 如果用户发起多个事务操作，与 IDMP 方商定，暂由 TDengine 等待并延迟返回。这样，后期 TDengine 支持多事务后，应用方不需要改动。

### 4.4 事务操作可见性与冲突

- 在事务内部，先执行的操作对后执行的操作是实时生效。
- 在事务外部，虽然全局只支持一个事务，但是，非事务操作也可能同时操作同一个表对象，从而产生冲突。根据不同的操作类型，行为如下：

#### 4.4.1 事务内“创建” (PREPARED_CREATE) vs. 非事务操作

当事务 A 执行了 `CREATE TABLE t1` 但尚未 `COMMIT` 时，`t1` 处于“影子”状态。
| 非事务操作类型 | 行为说明 | 预期错误/结果 |
| --- | --- | --- |
| SELECT / INSERT/DELETE | 不可见。数据路径找不到该表，视为表不存在。 | Table does not exist |
| DESCRIBE t1 | 不可见。无法查看未提交的 Schema。 | Table does not exist |
| SHOW TABLES | 过滤。结果集中不包含 t1。 | 不显示该行 |
| CREATE TABLE t1 | 冲突。虽然事务未提交，但 t1 已物理占位。 | Resource busy / Conflict |
| ALTER/DELELTE/DROP TABLE t1 | 拦截。由于非事务方看不见该表，自然无法操作。 | Table does not exist |

#### 4.4.2 事务内“删除” (PREPARED_DROP) vs. 非事务操作

当事务 A 执行了 `DROP TABLE t1` 但尚未 `COMMIT` 时，`t1` 处于“逻辑删除”状态。
| 非事务操作类型 | 行为说明 | 预期错误/结果 |
| --- | --- | --- |
| SELECT / INSERT | 只读/延迟。允许访问旧数据，直到事务提交。
这是为了满足 隔离性（Isolation）。在 A 提交前，全局视图仍认为 t1 存在，且应保证正在运行的查询不受影响。 | 正常执行 |
| DELETE | 拦截/冲突。禁止对即将消失的对象进行行级删除操作。系统会保护元数据，防止在删除表的执行过程中产生无效的数据变更日志。 | Resource busy / Conflict |
| DESCRIBE t1 | 可见。返回该表被删除前的 Schema 状态。 | 返回旧 Schema |
| SHOW TABLES | 可见。 | 正常执行 |
| CREATE TABLE t1 | 表名冲突。 | Table already exists |
| DROP TABLE t1 | 抢占冲突。发现该表状态已不是 NORMAL。 | Resource busy / Conflict |
| ALTER TABLE t1 | 拦截。禁止对即将消失的对象进行结构修改。 | Resource busy / Conflict |

#### 4.4.3 事务内“改标签” (PREPARED_ALTER) vs. 非事务操作

当事务 A 执行了 `ALTER TABLE t1 SET TAG...` 但尚未 `COMMIT` 时。
| 非事务操作类型 | 行为说明 | 预期错误/结果 |
| --- | --- | --- |
| SELECT / INSERT/DELETE | 无感。标签修改不影响数据写入路径。 | 正常执行 |
| DESCRIBE t1 | 旧值可见。在 Commit 前，所有人看到的还是旧标签。 | 返回旧 Tag 值 |
| CREATE TABLE t1 | 表名冲突。 | Table already exists |
| ALTER TABLE t1 | 冲突。同一时刻只允许一个 TID 修改该对象的元数据。 | Resource busy / Conflict |
| DROP TABLE t1 | 锁定。由于有活跃事务在修改它，禁止非事务侧删除。 | Resource busy / Conflict |


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

- 复用 show transcations 语法：通过 oper 字段(命名为 __trans__ )，区分批事务还是普通事务。

## 13. 安装和卸载

- 无特殊要求

## 14. 文档

- 需要修改官网文档

## 15. 参考

- [[IDMP] 元数据更新支持事务 DS](https://taosdata.feishu.cn/docx/NOi0dsPyBoBgK2xj62TcVrf7nPh)

## 16. 附录

### 16.1 二期版本计划

以下为二期版本预期支持的功能：
- 支持并发事务
- 事务操作中，支持虚拟超级表的 create/drop
- 事务操作中，支持流计算的 create/drop
- TODO
