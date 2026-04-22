# 审计直接写入功能说明

## 功能概述

本功能实现了审计信息直接写入本地数据库，绕过taoskeeper中间件。

### 原有架构
```
TDengine Server → HTTP → taoskeeper → audit数据库
```

### 新架构
```
TDengine Server → 直接写入 → audit数据库
```

## 配置说明

在 `taos.cfg` 配置文件中添加以下配置项：

```ini
# 启用审计功能
audit 1

# 启用直接写入模式（绕过taoskeeper）
auditDirectWrite 1

# 审计级别（可选）
auditLevel 3

# 审计间隔（毫秒，可选）
auditInterval 5000
```

## 配置参数详解

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| audit | bool | true | 是否启用审计功能 |
| auditDirectWrite | bool | false | 是否启用直接写入模式 |
| auditLevel | int | 3 | 审计级别（0-5） |
| auditInterval | int | 5000 | 审计间隔（毫秒） |
| auditCreateTable | bool | true | 是否审计创建表操作 |
| enableAuditDelete | bool | true | 是否审计删除操作 |
| enableAuditSelect | bool | true | 是否审计查询操作 |
| enableAuditInsert | bool | true | 是否审计插入操作 |

## 数据库结构

审计数据会写入到 `audit` 数据库中：

### 数据库
- 名称: `audit`
- 精度: 纳秒 (ns)
- WAL级别: 2

### 超级表
- 名称: `operations`
- 列:
  - `ts` (TIMESTAMP): 时间戳
  - `user_name` (VARCHAR(25)): 用户名
  - `operation` (VARCHAR(20)): 操作类型
  - `db` (VARCHAR(65)): 数据库名
  - `resource` (VARCHAR(193)): 资源名称
  - `client_address` (VARCHAR(64)): 客户端地址
  - `details` (VARCHAR(50000)): 详细信息
  - `affected_rows` (DOUBLE): 影响行数
  - `duration` (DOUBLE): 执行时长
- 标签:
  - `cluster_id` (VARCHAR(64)): 集群ID

### 子表
- 命名规则: `t_operations_{cluster_id}`

## 使用示例

### 1. 启用直接写入模式

编辑 `taos.cfg`:
```ini
audit 1
auditDirectWrite 1
```

重启TDengine服务：
```bash
systemctl restart taosd
```

### 2. 查询审计记录

```sql
-- 使用audit数据库
USE audit;

-- 查询所有审计记录
SELECT * FROM operations;

-- 查询特定用户的操作
SELECT * FROM operations WHERE user_name = 'root';

-- 查询特定操作类型
SELECT * FROM operations WHERE operation = 'CREATE_DB';

-- 查询最近的审计记录
SELECT * FROM operations ORDER BY ts DESC LIMIT 100;
```

### 3. 统计分析

```sql
-- 统计各操作类型的数量
SELECT operation, COUNT(*) FROM operations GROUP BY operation;

-- 统计各用户的操作数量
SELECT user_name, COUNT(*) FROM operations GROUP BY user_name;

-- 查询执行时间最长的操作
SELECT * FROM operations ORDER BY duration DESC LIMIT 10;
```

## 优势

1. **性能提升**: 减少HTTP通信开销，直接写入数据库
2. **简化架构**: 不需要部署和维护taoskeeper组件
3. **实时性**: 审计记录实时写入，无需等待批量发送
4. **可靠性**: 减少中间环节，降低数据丢失风险

## 注意事项

1. 启用直接写入模式后，不再需要配置 `monitorFqdn` 和 `monitorPort`
2. 审计数据库会自动创建，无需手动创建
3. 确保TDengine服务有足够的权限创建数据库和表
4. 直接写入模式与HTTP模式互斥，只能选择其中一种

## 兼容性

- 与原有的HTTP模式完全兼容
- 可以通过配置文件随时切换两种模式
- 数据库结构与taoskeeper创建的结构完全一致

## 故障排查

### 问题1: 审计记录没有写入

检查配置：
```bash
grep -E "audit|auditDirectWrite" /etc/taos/taos.cfg
```

查看日志：
```bash
tail -f /var/log/taos/taosdlog.0
```

### 问题2: 数据库连接失败

确认TDengine服务正常运行：
```bash
systemctl status taosd
```

检查数据库是否存在：
```sql
SHOW DATABASES;
```

### 问题3: 权限问题

确保使用root用户或有足够权限的用户：
```sql
SHOW USERS;
```

## 性能建议

1. 根据实际需求调整 `auditLevel`，避免记录过多不必要的操作
2. 定期清理历史审计数据，避免数据库过大
3. 可以通过 `auditInterval` 调整审计频率

## 迁移指南

### 从taoskeeper迁移到直接写入

1. 停止taoskeeper服务
2. 修改 `taos.cfg`，添加 `auditDirectWrite 1`
3. 重启taosd服务
4. 验证审计记录正常写入

### 从直接写入迁移到taoskeeper

1. 修改 `taos.cfg`，设置 `auditDirectWrite 0`
2. 配置 `monitorFqdn` 和 `monitorPort`
3. 启动taoskeeper服务
4. 重启taosd服务
