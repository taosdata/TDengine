# 传输安全 TS 

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-29 | 2025-12-30 | 1.0 | 邓怡豪 | 添加整体文档 |

## 2. 测试目标

### 2.1 传输安全

1. 增加sasl 功能，主体安全还是依赖于TLS，测试功能是否完整。
2. 增加time-white-list 和白名单功能，传输层只做基本的校验
3. 动态更新TLS 证书
4. 做数据包大小限制和版本控制。 
5. 传输层消息校验，防止消息恶意被耗尽。

### 2.2 Session 控制

1. 验证各个user下session 的并发控制是否符合预期
2. 动态修改user 的session后，客户端能否感知到变化，并动态更新参数限制。

### 2.3 性能对比

1. 查询和写入性能不能下降超过10%。 

## 3. 参考文档

  [传输安全 FS ](https://taosdata.feishu.cn/wiki/UzIcwoxz6ikM7WksBSMclmqrnhd)

## 4. 测试结论

功能符合预期，性别下降不超过10%

## 5. 测试环境

- OS: Linux

## 6. 功能测试

### 6.1 查看内置算法

#### 6.1.1 测试要点

列出所有内置算法，算法个数，各算法内容正确

#### 6.1.2 用例列表

| # | 功能点 | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1. | 传输安全 | test.tls.py | 基本的TLS 功能和动态更新TLS功能正常， 且动态更新TLS 之后，集群状态是否正常 | 通过 |
| 2. | 传输安全 | 之前的版本兼容性测试已经覆盖 | 前后版本消息校验。 | 通过 |
| 3. | 传输安全 | 由其他人相关case 覆盖 | 测试白名单 |  |
| 4. | 传输安全 | 待添加 | 数据包被重放攻击。 |  |
| 3. | session控制 | clientTests.cpp 之testSessionPerUser | 测试单个user 的session 的个数，动态调整之后符合预期 | 通过 |
| 4. | session控制 | clientTests.cpp 之 testSessionConnTime | 测试单connect 的conn time 控制，功能是否符合预期，动态调整之后是否符合预期 | 通过 |
|  | Session控制 | clientTests.cpp 之 testSessionConnIdleTime | 测试单connect 的conn idle time 控制，功能是否符合预期，动态调整之后是否符合预期 | 通过 |
|  | session控制 | clientTests.cpp 之 testSessionConncurentCall | 测试单个user 能创建并发请求个数， 功能是否符合预期，动态调整之后是否符合预期 | 通过 |
|  | Session 控制 | clientTests.cpp 之 testSessionMaxVnodeCall | 测试单个SQL最多所涉及的vnode的个数， 功能是否符合预期，动态调整之后是否符合预期 | 通过 |

### 6.2 用例测试步骤

#### 6.2.1 传输安全
 

| # | 测试项目 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1. | 重新加载TLS证书 | 1. 生成TLS 相关配置，并在taos.cfg 配置路径，和enableTls = 1 1. 启动taosd, 并用taos-shell 进行访问，观察是否正常 1. 在TLS 证书的原有路径下，重新生成TLS 相关证书 1. taos-shell 执行 'alter dnodes reload tls', 观察taosd 的状态，是否正常 1. 新建立一个节点（newDnode），用步骤3 生成一个TLS 证书 1. taos-shell 执行，把newDnode 加载进原有的单一节点中。 1. Taos-shelll观察整个集群的状态。 | 通过 |
| 2. | 增加SASL机制 | 配置enableSasl, 其他该测试同 重新加载TLS的测试。 | 通过 |
| 3. | 数据包大小限制和内容校验 | 1. 属于单元测试中的内部，构建超过512M数据包，对端收到之后，直接关闭连接。 | 通过 |
| 4. | 集群状态下消息认证机制。 | 同上 | 通过 |
| 5. | 稳定新测试 | 配置TLS 和SASL，启动三节点taosd, 用taosBenchmark 写入大量的数据，期间做重新加载TLS 证书，观察写入的的稳定和集群本身的稳定性。 | 通过 |
| 6. | 前后版本兼容性 | 3.4.0.0 之前的客户端无法连接 本版本taosd, 3.4.0.0 之前的taosd 和本版本的taosd 不能组成集群。 | 通过 |
| 7. | 重放攻击 | 待定 |  |


#### 6.2.2 Session 控制

| # | 测试项目 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1. | session_per_user | 1. 用root 用户创建taos_connect名为rootConn, 生成一些测试数据，创建一个user 名为control_user， 并给该用户授予所有的权限 1. rootConn 执行SQL alter user control_user SESSSION_PER_USE ', 等3s 1. 用control_user分别创建11个连接，前10个都正常创建，第一个创建失败，并且返回明确的错误码。关闭用contro_user创建的连接，有创建新连接，可以执行成功。 | 通过 |
| 2. | connect_time | 1. 用root 用户创建taos_connect名为rootConn, 生成一些测试数据，创建一个user 名为control_user， 并给该用户授予所有的权限 1. 用control_use创建一个连接userConn，执行SQL 都成功 1. rootConn 执行SQL 'alter user control_user connect_time 1' （这里为分钟） 1. 等待60s, 用 userConn 执行任何SQL都失败。 | 通过 |
| 3. | connect_idle_time | 1. 用root 用户创建taos_connect名为rootConn, 生成一些测试数据，创建一个user 名为control_user， 并给该用户授予所有的权限 1. 用control_use创建一个连接userConn，执行SQL 都成功 1. rootConn 执行SQL 'alter user control_user connect_idel time 1' （这里为分钟） 1. 等待60s, 用userConn 执行任何SQL都失败。 | 通过 |
| 4. | call_per_session | 1. 用root 用户创建taos_connect名为rootConn, 生成一些测试数据，创建一个user 名为control_user， 并给该用户授予所有的权限 1. 用control_use创建一个连接5个userConn，并且执行SQL，都成功。 1. rootConn 执行SQL alter user control_user SESSSION_PER_USE 2', 等3.0s 1. 用5个userConn，执行SQL，部分成功，部分失败。 | 通过 |
| 5. | vnode_per_call | 1. 用root 用户创建taos_connect名为rootConn, 生成一些测试数据，创建一个user 名为control_user，并给该用户授予所有的权限 1. 用control_use创建一个连接userConn，执行大查询（涉及多个VNODE）, 执行成功， 1. rootConn 执行SQL alter user control_user vnode_per_call 2', 等3s 1. 用userConn，执行一个大查询，直接报错。 1. rootConn 执行SQL alter user control_user vnode_per_call 1024', 等3s 1. 用userConn，执行一个大查询，执行成功。 | 通过 |

##### 

## 7. 易用性测试（无）

## 8. 长期稳定性测试

## 9. 性能测试

#### 9.0.1 测试要点

   - 开启TLS 和无开启TLS 性能对比
   - 不开启session 和开启session控制的性能对比 

#### 9.0.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 传输安全 | 1. 重置数据目录 1. 不开启TLS, 通过taosBenchmark，写入数据，记录完成时间 A 1. 重置数据目录 1. 开启TLS, 通过taosBenchmark，写入数据，记录完成时间 B 1. drop database, 并做trim database, 1. 在各个节点重新生成TLS证书，再次通过taosBenchmark 写入数据, 记录完成时间C。 对比A/B/C的时间差异，B和C的时间基本一致，相比A，整体性能差了5%左右。 | 通过 |
| 2. | Session 控制 | taos.cfg 设置sessionControl = 0， 通过taosBenchmark 写入数据,记录完成时间A。 重置数据目录 taos.cfg 设置sessionControl = 1， 通过taosBenchmark 写入数据,记录完成时间B。 对比开启sessionControl 前后的，A和B的时间差异很小， | 通过 |

## 10. 安全测试

#### 10.0.1 测试要点

   - 无TLS 证书的能否访问引擎。  
   - 数据被劫持

#### 10.0.2 用例列表

## 11. 兼容性测试

  旧版本的的taos/taosd 都不能访问本版本的taosd, 日志中有明显的错误信息。

## 12. 已知问题和限制（可选）
