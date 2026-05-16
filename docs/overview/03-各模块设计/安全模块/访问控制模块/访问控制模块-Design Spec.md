# 访问控制模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-25 | 2025-12-25 | 1.0 | 程洪泽 | 新建 |

## 2. 引言

### 2.1 目的

本文档旨在详细描述 TDengine 数据库访问控制模块的设计与实现。该模块提供了基于角色的访问控制（RBAC）机制，支持细粒度的权限管理和安全策略实施。

### 2.2 范围

本文档涵盖TDengine访问控制模块的核心组件，包括：
- 权限类型和分类体系
- 角色管理机制
- 用户权限验证
- 系统角色和默认权限
- 表级和列级权限控制

### 2.3 受众

- TDengine 开发人员
- 系统架构师
- 安全审计人员
- 数据库管理员

## 3. 术语

| 术语 | 定义 |
| --- | --- |
| **权限（Privilege）** | 执行特定操作的能力，如 SELECT、INSERT、CREATE 等 |
| **角色（Role）** | 权限的集合，可以分配给用户 |
| **系统角色** | 预定义的角色，如 SYSDBA、SYSSEC、SYSAUDIT 等 |
| **对象权限** | 针对特定数据库对象的权限，如表、数据库等 |
| **系统权限** | 全局性的管理权限，如创建用户、修改系统变量等 |
| **权限集（PrivSet）** | 使用位图表示的权限集合 |
| **权限策略** | 包含条件和约束的权限定义 |

## 4. 概述

### 4.1 架构

TDengine 访问控制模块采用分层架构设计：
<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    A[客户端请求] --\u003e B[权限验证层]\n    B --\u003e C[角色管理模块]\n    B --\u003e D[用户管理模块]\n    C --\u003e E[权限策略引擎]\n    D --\u003e E\n    E --\u003e F[权限存储层]\n    F --\u003e G[SDB持久化]\n    \n    subgraph \"核心组件\"\n        C\n        D\n        E\n    end\n    \n    subgraph \"数据存储\"\n        F\n        G\n    end\n","theme":"default","view":"chart"}"/>

### 4.2 技术

- **编程语言**: C 语言
- **数据结构**: 哈希表（SHashObj）、数组（SArray）、位图（SPrivSet）
- **持久化**: SDB（系统数据库）
- **并发控制**: 读写锁（TdThreadRwlock）
- **序列化**: 自定义二进制协议

### 4.3 依赖项

- `thash.h`: 哈希表实现
- `tarray.h`: 动态数组
- `sdb.h`: 系统数据库接口
- `tmsg.h`: 消息处理
- `tutil.h`: 工具函数

## 5. 设计考虑

### 5.1 假设和限制

1. **权限数量限制**: 最多支持 255 种权限类型（EPrivType 枚举）
2. **角色层级**: 当前版本支持简单的角色分配，复杂的角色继承将在未来版本中实现
3. **性能考虑**: 权限验证需要高效，避免影响查询性能
4. **兼容性**: 需要支持从旧版本升级时的权限迁移

### 5.2 设计模式和原则

1. **单一职责原则**: 每个模块负责特定的功能
  - `mndRole`: 角色管理
  - `mndUser`: 用户管理  
  - `mndPrivilege`: 权限验证
1. **开闭原则**: 权限类型可通过枚举扩展，不影响现有代码
2. **组合优于继承**: 使用角色组合权限，而非复杂的继承层次

### 5.3 风险和缓解措施

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| 权限验证性能瓶颈 | 查询延迟增加 | 使用位图运算和哈希表快速查找 |
| 角色权限同步延迟 | 权限更新不及时 | 实现角色最后更新时间戳机制 |
| 权限冲突检测 | 错误的权限组合 | 实现权限冲突检查函数 |
| 升级兼容性问题 | 旧版本权限丢失 | 提供权限迁移工具和版本控制 |

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 权限管理组件（tpriv.h/tpriv.c）

**核心功能**:
- 定义权限类型枚举（EPrivType）
- 实现权限位图操作（SPrivSet）
- 提供权限信息查询接口
- 支持权限冲突检测
**关键接口**:
```c
// 权限位图操作
void privAddType(SPrivSet* privSet, EPrivType type);
void privRemoveType(SPrivSet* privSet, EPrivType type);
bool PRIV_HAS(const SPrivSet* privSet, EPrivType type);

// 权限冲突检查
int32_t checkPrivConflicts(const SPrivSet* privSet, EPrivCategory* pCategory, 
                          EPrivObjType* pObjType, uint8_t* pObjLevel, 
                          EPrivType* conflict0, EPrivType* conflict1);

// 权限迭代器
void privIterInit(SPrivIter* pIter, SPrivSet* privSet);
bool privIterNext(SPrivIter* iter, SPrivInfo** ppPrivInfo);
```

#### 6.1.2 角色管理组件（mndRole.h/mndRole.c）

**核心功能**:
- 角色的创建、修改、删除
- 角色权限的分配和管理
- 系统角色的初始化
- 角色信息的持久化
**关键数据结构**:
```c
typedef struct {
  char    name[TSDB_ROLE_LEN];
  int64_t createdTime;
  int64_t updateTime;
  int64_t uid;
  int64_t version;
  uint8_t flag;  // enable, sys等标志位
  
  SPrivSet sysPrivs;          // 系统权限
  SHashObj* objPrivs;         // 对象权限哈希表
  SHashObj* selectTbs;        // SELECT表权限
  SHashObj* insertTbs;        // INSERT表权限
  SHashObj* updateTbs;        // UPDATE表权限
  SHashObj* deleteTbs;        // DELETE表权限
  SHashObj* parentRoles;      // 父角色（未来支持）
  SHashObj* subRoles;         // 子角色（未来支持）
  SRWLatch lock;              // 读写锁
} SRoleObj;
```

#### 6.1.3 用户管理组件（mndUser.h）

**核心功能**:
- 用户认证和授权
- 用户权限的继承和合并
- 密码和安全策略管理
- IP白名单和时间白名单

#### 6.1.4 权限验证组件（mndPrivilege.h）

**核心功能**:
- 操作权限验证
- 对象权限验证
- 递归权限检查
- 权限冲突解决

### 6.2 关键数据结构

#### 6.2.1 权限位图（SPrivSet）

```c
#define PRIV_GROUP_CNT ((MAX_PRIV_TYPE + 63) / 64)
typedef struct {
  uint64_t set[PRIV_GROUP_CNT];  // 64位数组，支持最多255种权限
} SPrivSet;
```

#### 6.2.2 权限信息（SPrivInfo）

```c
typedef struct {
  EPrivType     privType;    // 权限类型
  EPrivCategory category;    // 权限分类（系统/对象/通用）
  EPrivObjType  objType;     // 对象类型
  int8_t        objLevel;    // 对象层级（0:数据库级，1:表级）
  uint8_t       sysType;     // 系统角色类型
  const char*   name;        // 权限名称
} SPrivInfo;
```

#### 6.2.3 表权限策略（SPrivTblPolicy）

```c
typedef struct {
  SArray* cols;     // 列名数组（NULL表示所有列）
  char*   cond;     // 条件表达式（NULL表示无条件）
  int32_t condLen;  // 条件长度
  int64_t updateUs; // 更新时间
} SPrivTblPolicy;
```

### 6.3 数据库设计

#### 6.3.1 数据模型

**角色表（SDB_ROLE）**:
- 主键：角色名
- 字段：创建时间、更新时间、UID、版本、标志位
- 权限字段：系统权限位图、对象权限哈希、表权限哈希
**用户表（SDB_USER）**:
- 主键：用户名
- 字段：密码信息、账户信息、安全设置
- 权限字段：系统权限、对象权限、角色引用

#### 6.3.2 数据访问层

使用SDB（系统数据库）进行持久化：
```c
// 角色数据编码
SSdbRaw *mndRoleActionEncode(SRoleObj *pRole);

// 角色数据解码
static SSdbRow *mndRoleActionDecode(SSdbRaw *pRaw);

// 数据库操作
static int32_t mndRoleActionInsert(SSdb *pSdb, SRoleObj *pRole);
static int32_t mndRoleActionDelete(SSdb *pSdb, SRoleObj *pRole);
static int32_t mndRoleActionUpdate(SSdb *pSdb, SRoleObj *pOld, SRoleObj *pNew);
```

### 6.4 图表解释

#### 6.4.1 数据流图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant Client\n    participant MNode\n    participant RoleMgmt\n    participant PrivCheck\n    participant SDB\n    \n    Client-\u003e\u003eMNode: 执行操作请求\n    MNode-\u003e\u003ePrivCheck: 检查权限\n    PrivCheck-\u003e\u003eRoleMgmt: 获取用户角色\n    RoleMgmt-\u003e\u003eSDB: 查询角色信息\n    SDB--\u003e\u003eRoleMgmt: 返回角色数据\n    RoleMgmt--\u003e\u003ePrivCheck: 返回权限集合\n    PrivCheck-\u003e\u003ePrivCheck: 验证权限\n    alt 权限足够\n        PrivCheck--\u003e\u003eMNode: 允许操作\n        MNode--\u003e\u003eClient: 执行成功\n    else 权限不足\n        PrivCheck--\u003e\u003eMNode: 拒绝操作\n        MNode--\u003e\u003eClient: 权限错误\n    end\n","theme":"default","view":"chart"}"/>

#### 6.4.2 权限验证流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TD\n    A[开始权限验证] --\u003e B{获取用户对象}\n    B --\u003e C{用户是否存在}\n    C --\u003e|否| D[返回权限错误]\n    C --\u003e|是| E{检查系统权限}\n    E --\u003e|不足| D\n    E --\u003e|足够| F{检查对象权限}\n    F --\u003e|不足| D\n    F --\u003e|足够| G{检查表级权限}\n    G --\u003e|不足| D\n    G --\u003e|足够| H[允许操作]\n    H --\u003e I[结束]\n    D --\u003e I\n","theme":"default","view":"chart"}"/>

#### 6.4.3 角色创建流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TD\n    A[接收创建角色请求] --\u003e B{验证操作者权限}\n    B --\u003e|无权限| C[返回权限错误]\n    B --\u003e|有权限| D{检查角色名有效性}\n    D --\u003e|无效| E[返回格式错误]\n    D --\u003e|有效| F{检查角色是否已存在}\n    F --\u003e|存在| G[返回已存在错误]\n    F --\u003e|不存在| H[创建角色对象]\n    H --\u003e I[分配默认权限]\n    I --\u003e J[序列化角色数据]\n    J --\u003e K[创建事务]\n    K --\u003e L[提交到SDB]\n    L --\u003e M[返回成功]\n","theme":"default","view":"chart"}"/>

#### 6.4.4 状态转换图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    [*] --\u003e 角色未初始化\n    角色未初始化 --\u003e 角色已创建: 创建角色\n    角色已创建 --\u003e 角色已启用: 启用角色\n    角色已创建 --\u003e 角色已锁定: 锁定角色\n    角色已启用 --\u003e 角色已锁定: 锁定操作\n    角色已锁定 --\u003e 角色已启用: 解锁操作\n    角色已启用 --\u003e 角色已删除: 删除角色\n    角色已锁定 --\u003e 角色已删除: 删除角色\n    角色已删除 --\u003e [*]\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 API文档

#### 7.1.1 角色管理API

**创建角色**:
```c
int32_t mndProcessCreateRoleReq(SRpcMsg *pReq);
```

- 请求类型: `TDMT_MND_CREATE_ROLE`
- 权限要求: `PRIV_ROLE_CREATE`
- 参数: 角色名、忽略存在标志等
**删除角色**:
```c
int32_t mndProcessDropRoleReq(SRpcMsg *pReq);
```

- 请求类型: `TDMT_MND_DROP_ROLE`
- 权限要求: `PRIV_ROLE_DROP`
- 参数: 角色名、SQL语句等
**修改角色**:
```c
int32_t mndProcessAlterRoleReq(SRpcMsg *pReq);
```

- 请求类型: `TDMT_MND_ALTER_ROLE`
- 权限要求: `PRIV_ROLE_ALTER`
- 参数: 角色名、修改类型、修改内容等

#### 7.1.2 权限查询API

**查询角色列表**:
```c
static int32_t mndRetrieveRoles(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
```

- 显示表: `TSDB_MGMT_TABLE_ROLE`
- 返回字段: 角色名、启用状态、创建时间、更新时间、角色类型、子角色等
**查询角色权限**:
```c
static int32_t mndRetrievePrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
```

- 显示表: `TSDB_MGMT_TABLE_ROLE_PRIVILEGES`
- 返回字段: 角色名、权限名、对象类型、数据库、表、条件等
**查询列权限**:
```c
static int32_t mndRetrieveColPrivileges(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
```

- 显示表: `TSDB_MGMT_TABLE_ROLE_COL_PRIVILEGES`
- 返回字段: 角色名、权限名、列信息等

### 7.2 用户界面

访问控制模块主要通过以下方式暴露给用户：
1. **SQL命令**: `CREATE ROLE`, `DROP ROLE`, `GRANT`, `REVOKE`等
2. **系统表查询**: 通过`SHOW ROLES`, `SHOW PRIVILEGES`等命令查看
3. **管理接口**: MNode提供的RPC接口

## 8. 安全考虑

### 8.1 安全要求

1. **最小权限原则**: 默认情况下，用户只有最基本的权限
2. **权限分离**: 系统管理员、安全管理员、审计管理员角色分离
3. **密码安全**: 支持密码加密、密码策略、密码历史等
4. **访问控制**: IP白名单、时间白名单、连接限制等
5. **审计日志**: 所有权限变更操作都有审计记录

### 8.2 漏洞缓解

| 漏洞类型 | 缓解措施 |
| --- | --- |
| 权限提升 | 严格的权限验证，防止用户获取未授权权限 |
| 拒绝服务 | 连接数限制，查询超时控制 |
| 信息泄露 | 权限不足时返回通用错误信息 |
| 会话劫持 | 会话超时，Token验证 |

## 9. 性能和可扩展性

### 9.1 性能要求

1. **权限验证延迟**: < 1ms（99%的请求）
2. **角色查询性能**: 支持千级角色快速检索
3. **内存使用**: 角色数据内存缓存，减少磁盘IO
4. **并发支持**: 支持高并发权限验证

### 9.2 可扩展性

1. **水平扩展**: 通过增加MNode实例支持更多用户
2. **权限类型扩展**: 通过枚举扩展支持新的权限类型
3. **角色层级**: 未来支持角色继承和组合
4. **插件架构**: 支持自定义权限验证插件

## 10. 部署和配置

### 10.1 部署流程

1. **初始化系统角色**: 部署时自动创建SYSDBA、SYSSEC等系统角色
2. **权限表创建**: 在SDB中创建角色和权限相关表
3. **默认权限分配**: 为系统角色分配默认权限
4. **升级迁移**: 从旧版本迁移权限数据

### 10.2 版本控制

1. **版本兼容性**: 保持向后兼容，支持从旧版本升级
2. **数据迁移**: 提供权限数据迁移工具
3. **回滚策略**: 支持权限变更的回滚操作

## 11. 监控和维护

### 11.1 监控

**监控工具**:
- 系统表查询: 通过`SHOW ROLES`、`SHOW PRIVILEGES`等命令监控
- 日志分析: 分析权限相关的错误日志和审计日志
- 性能监控: 使用TDengine自带的监控工具监控权限验证性能

### 11.2 日志记录和诊断

**日志级别**:
- **ERROR**: 权限验证失败、角色操作错误等
- **WARN**: 权限冲突、角色状态异常等
- **INFO**: 角色创建、修改、删除等操作记录
- **DEBUG**: 详细的权限验证过程、角色权限变化等
**审计日志**:
所有权限相关的操作都会记录审计日志，包括：
- 角色创建、修改、删除
- 权限授予和撤销
- 用户角色分配
- 权限验证失败
**诊断工具**:
```sql
-- 查看角色状态
SHOW ROLES;

-- 查看角色权限
SHOW ROLE_PRIVILEGES;

-- 查看用户权限
SHOW USER_PRIVILEGES;

-- 查看权限验证错误
SELECT * FROM information_schema.audit_log 
WHERE operation_type LIKE '%PRIV%' 
ORDER BY timestamp DESC LIMIT 100;
```

### 11.3 维护

**日常维护任务**:
1. **角色清理**: 定期清理未使用的角色
2. **权限审计**: 定期审计权限分配，确保符合最小权限原则
3. **密码策略更新**: 根据安全要求更新密码策略
4. **白名单维护**: 更新IP白名单和时间白名单
**故障处理**:
1. **权限验证失败**: 检查角色状态、权限分配、用户状态
2. **角色操作失败**: 检查事务状态、SDB状态、锁状态
3. **性能下降**: 检查权限缓存、优化权限验证逻辑
**升级维护**:
1. **版本升级**: 确保权限数据兼容性，执行必要的迁移脚本
2. **权限迁移**: 从旧版本迁移权限数据到新版本
3. **回滚准备**: 准备权限数据的备份和回滚方案

## 12. 参考资料

1. [访问控制模块-Requirement Spec](https://taosdata.feishu.cn/wiki/Kx5ZwBUDYi6EwBkq4DRcCd0LnYV)
2. [访问控制模块-Function Spec](https://taosdata.feishu.cn/wiki/HSKGwp0KGieFIUkL60EcdMn9nEg)
