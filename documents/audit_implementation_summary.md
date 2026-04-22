# 审计直接写入功能实现总结

## 实现概述

本次实现了审计信息直接写入本地数据库的功能，绕过taoskeeper中间件，实现了从 `TDengine Server → HTTP → taoskeeper → audit数据库` 到 `TDengine Server → 直接写入 → audit数据库` 的架构转变。

## 修改文件清单

### 1. 配置相关文件

#### community/include/common/tglobal.h
- 添加全局变量声明: `extern bool tsAuditDirectWrite;`

#### community/source/common/src/tglobal.c
- 添加配置变量定义: `bool tsAuditDirectWrite = false;`（在两个编译分支中都添加）
- 添加配置项注册: `cfgAddBool(pCfg, "auditDirectWrite", ...)`
- 添加配置读取: `tsAuditDirectWrite = pItem->bval;`

### 2. 审计核心文件

#### community/source/libs/audit/inc/auditInt.h
- 在 `SAudit` 结构体中添加字段：
  - `TAOS *taos;` - 数据库连接
  - `TdThreadMutex taosLock;` - 连接锁
  - `int8_t directWriteMode;` - 直接写入模式标志

#### community/source/libs/audit/src/auditMain.c
- 修改 `auditInit()`: 添加直接写入模式初始化调用
- 修改 `auditCleanup()`: 添加直接写入模式清理调用

#### enterprise/src/plugins/audit/src/audit.c
- 添加宏定义：
  - `AUDIT_DB_NAME` - 审计数据库名称
  - `AUDIT_STABLE_NAME` - 审计超级表名称

- 添加新函数：
  - `auditEscapeString()` - SQL字符串转义
  - `auditInitDirectWrite()` - 初始化直接写入模式
  - `auditCleanupDirectWrite()` - 清理直接写入模式
  - `auditEnsureDatabase()` - 确保数据库存在
  - `auditEnsureSuperTable()` - 确保超级表存在
  - `auditDirectWriteRecord()` - 直接写入单条记录
  - `auditDirectWriteBatch()` - 批量直接写入记录

- 修改现有函数：
  - `auditRecordImp()` - 添加直接写入模式分支
  - `auditAddRecordImp()` - 添加直接写入模式分支
  - `auditSendRecordsInBatchImp()` - 添加直接写入模式分支

## 核心实现逻辑

### 1. 初始化流程
```
auditInit()
  └─> auditInitDirectWrite()
        ├─> 初始化 taosLock
        ├─> 设置 directWriteMode = 1
        └─> 设置 taos = NULL (延迟连接)
```

### 2. 写入流程
```
auditRecordImp()
  └─> 检查 tsAuditDirectWrite
        ├─> true: 直接写入模式
        │     ├─> 构建 SAuditRecord
        │     └─> auditDirectWriteRecord()
        │           ├─> 检查/创建数据库连接
        │           ├─> 确保数据库存在
        │           ├─> 确保超级表存在
        │           ├─> 转义SQL字符串
        │           ├─> 构建INSERT SQL
        │           └─> 执行SQL
        └─> false: HTTP模式（原有逻辑）
              └─> 发送HTTP请求到taoskeeper
```

### 3. 清理流程
```
auditCleanup()
  └─> auditCleanupDirectWrite()
        ├─> 关闭数据库连接
        ├─> 销毁 taosLock
        └─> 设置 directWriteMode = 0
```

## 数据库结构

### 数据库
```sql
CREATE DATABASE IF NOT EXISTS audit PRECISION 'ns' WAL_LEVEL 2;
```

### 超级表
```sql
CREATE STABLE IF NOT EXISTS audit.operations (
    ts TIMESTAMP,
    user_name VARCHAR(25),
    operation VARCHAR(20),
    db VARCHAR(65),
    resource VARCHAR(193),
    client_address VARCHAR(64),
    details VARCHAR(50000),
    affected_rows DOUBLE,
    duration DOUBLE
) TAGS (
    cluster_id VARCHAR(64)
);
```

### 子表命名
```
t_operations_{cluster_id}
```

## 配置说明

### 新增配置项
- **auditDirectWrite**: 是否启用直接写入模式
  - 类型: bool
  - 默认值: false
  - 作用域: CFG_SCOPE_SERVER
  - 动态配置: CFG_DYN_ENT_SERVER

### 配置示例
```ini
# 启用审计
audit 1

# 启用直接写入模式
auditDirectWrite 1

# 审计级别
auditLevel 3

# 审计间隔（毫秒）
auditInterval 5000
```

## 关键特性

### 1. 延迟连接
- 数据库连接在第一次写入时才创建
- 避免启动时的连接开销
- 连接失败时自动重试

### 2. 线程安全
- 使用 `taosLock` 保护数据库连接
- 支持多线程并发写入
- 避免连接竞争

### 3. SQL注入防护
- 实现 `auditEscapeString()` 函数
- 转义单引号、双引号、反斜杠
- 防止SQL注入攻击

### 4. 错误处理
- 连接失败时记录错误日志
- SQL执行失败时记录错误信息
- 不影响主业务流程

### 5. 兼容性
- 与原有HTTP模式完全兼容
- 可通过配置随时切换
- 数据库结构与taoskeeper一致

## 性能优势

1. **减少网络开销**: 无需HTTP通信
2. **降低延迟**: 直接写入，无需等待HTTP响应
3. **简化架构**: 不需要部署taoskeeper
4. **提高可靠性**: 减少中间环节

## 测试建议

### 1. 功能测试
- 测试数据库自动创建
- 测试超级表自动创建
- 测试审计记录写入
- 测试SQL转义功能

### 2. 性能测试
- 测试高并发写入
- 测试大量审计记录
- 对比HTTP模式性能

### 3. 稳定性测试
- 测试长时间运行
- 测试连接断开重连
- 测试异常情况处理

### 4. 兼容性测试
- 测试与原有HTTP模式切换
- 测试数据库结构兼容性
- 测试查询功能

## 使用文档

详细使用说明请参考：
- `myDocs/audit_direct_write.md` - 功能说明和使用指南
- `myDocs/test_audit_direct_write.sh` - 测试脚本

## 编译说明

本功能在企业版中实现，需要定义 `TD_ENTERPRISE` 宏：

```bash
# 编译企业版
cd build
cmake .. -DTD_ENTERPRISE=ON
make
```

## 注意事项

1. 直接写入模式与HTTP模式互斥，只能选择其中一种
2. 启用直接写入模式后，不需要配置 `monitorFqdn` 和 `monitorPort`
3. 确保TDengine服务有创建数据库和表的权限
4. 建议定期清理历史审计数据，避免数据库过大

## 后续优化建议

1. **批量写入优化**: 实现真正的批量写入，提高性能
2. **异步写入**: 使用异步队列，避免阻塞主流程
3. **连接池**: 使用连接池管理数据库连接
4. **数据压缩**: 对details字段进行压缩
5. **分区管理**: 自动管理历史数据分区
6. **监控指标**: 添加审计写入性能监控

## 版本信息

- 实现日期: 2026-03-12
- 目标版本: TDengine 3.0+
- 兼容性: 向后兼容，不影响现有功能
