# TDengine 3.3.4.0 Release

Release Date： 2024/12/31
Version：3.3.4.0
User Manuals: 

## 1. Highlights

1. 流计算支持 Interp()/TWA() 
2. 支持 Azure Blob 对象存储
3. 使用 SQL 命令统计磁盘空间占用情况
4. Flink Source & Sink Connector 

## 2. New Features & Improvements

### 2.1 Engine

#### 2.1.1 新特性

1. 流计算支持 Interp()/TWA()
2. 支持 Azure Blob 对象存储
3. 使用 SQL 命令统计磁盘空间占用情况
4. ~~支持完整的关联查询 ~~
5. 增加更多 SQL 函数

#### 2.1.2 改进使用体验

1. 优化 compact 使用体验，在创建数据库时可以指定有关参数支持自动 compact
2. 动态修改的参数持久化存储，重启后仍然生效
3. 为各种数据类型设置默认的压缩算法

#### 2.1.3 提升性能

1. 提升创建数据库的性能
2. 提升单副本变双副本的性能
3. 提升 where ts in () 过滤条件下的查询性能

#### 2.1.4 提升健壮性

1. 优化大并发查询时节点之间拉取数据的效率、优化对 RPC 连接的使用
2. 禁止 compact 和副本变更操作同时进行
3. 提升事务健壮性
4. 控制服务端的查询内存使用
5. 查询框架各子模块增加参数校验、指针释放后清空及变量初始化
6. 查询框架各子模块的故障注入测试
7. 订阅场景下的压力测试及各 API 的随机组合测试

### 2.2 Tools & Connectors

#### 2.2.1 新特性/优化

1. 支持 STMT2 （ODBC, Rust, Python, Java, C#, Go)
2. taos CLI 命令补全功能的完善
3. ODBC 支持 Kepware
4. Flink Source Connector
5. TDinsight 从 Angular 改为用 React 重新实现

#### 2.2.2 提升健壮性

1. taosBenchmark 重构
2. taosKeeper 企业版和社区版合并，代码加入 TDengine 仓库，CI 整合，清除废弃接口
3. taosAdapter 和 Go 连接器加入 lint 静态代码检查
4. JDBC 连接器用 Jackson 取代 FastJson，提高单元测试代码覆盖率，增加静态代码检查机制

### 2.3 taosX/taosExplorer

#### 2.3.1 新特性/优化

1. 定时导入一个目录下的 CSV
2. 支持压缩的 MQTT 消息
3. taosX GRPC 端口可配置
4. 优化任务展示页面
5. 重新设计  taosX 的 DSN 避免误用或得不到预期结果时的误解
6. 重新设计基于增量备份的备份与恢复功能，可用于穿网闸场景

#### 2.3.2 提升健壮性

1. 使用 cargo clippy 进行静态代码检查
2. 增加跨平台构建工具 cross 进行多平台编译期检查
3. 重构 MQTT Connector
4. 优化任务的状态转换机制
5. Explorer UI 重构
