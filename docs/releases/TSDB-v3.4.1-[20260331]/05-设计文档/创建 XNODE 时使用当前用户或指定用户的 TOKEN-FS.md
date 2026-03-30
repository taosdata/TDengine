# 创建 XNODE 时使用当前用户或指定用户的 TOKEN-FS

## 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026/1/29 | 2026/1/31 | 1 | 张贵川 | 初始版本 |

## 背景

当前 CREATE XNODE 时，必须指定用户名和密码，不方便用户使用，阻塞自动初始化。本功能增强 XNODE 的认证方式，支持：
1. 使用 Token 认证替代用户名密码
2. 创建 XNODE 时不指定认证信息则自动创建默认 Token
3. 支持动态修改 XNODE 的认证信息（用户名密码或 Token），修改后会自动重启 xnoded 服务

## 定义

- **Token**: 用于 xnoded 连接 taosd 进行认证的字符串凭证
- **xnoded**: XNODE 的守护进程，负责与 taosd 通信
- **默认 Token**: 当创建 XNODE 未指定认证信息时，系统自动生成的 Token

## 行为说明

### 4.1 SQL 语法变更:

#### 4.1.1 CREATE XNODE 支持 TOKEN 参数

**语法**:
```sql {wrap}
CREATE XNODE 'url'
CREATE XNODE 'url' USER name PASS 'password'
CREATE XNODE 'url' TOKEN 'token'
```

**参数说明**:
- **url**: Xnode 节点的地址，格式为 `host:port`，端口号为 taosx GRPC 端口（默认 6055）
- **name** 和 **password**: 用于 xnoded 连接 taosd 的用户名和密码
- **token**: 用于 xnoded 连接 taosd 的认证 Token
**行为说明**:
- 首次创建建议指定 Token 或者用户名和密码，用于守护进程 xnoded 连接 taosd
- 如果未指定 Token 或用户名密码，则系统会自动创建默认 Token
- 创建的默认 Token 用户名为 `xnode`，可通过 `SHOW TOKENS` 查看
**示例**:
```sql {wrap}
-- 不指定认证信息，创建默认 token
taos> CREATE XNODE 'x1:6055';
Create OK, 0 row(s) affected (0.050798s)

-- 使用用户名密码创建
taos> CREATE XNODE 'x1:6055' USER root PASS 'taosdata';
Create OK, 0 row(s) affected (0.050798s)

-- 使用 Token 创建
taos> CREATE XNODE 'x2:6055' TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
Create OK, 0 row(s) affected (0.050798s)
```

#### 4.1.2 ALTER XNODE 修改认证信息

**语法**:
```sql {wrap}
ALTER XNODE SET USER name PASS 'password'
ALTER XNODE SET TOKEN 'token'
```

**参数说明**:
- **token**: 用于连接 taosd 认证的新 Token
- **name**: 新的用户名
- **password**: 新的密码
**行为说明**:
- 修改认证信息会重启守护进程 xnoded
- 该命令修改的是单个 xnoded 守护进程连接 taosd 使用的凭证
- 支持从用户名密码切换到 Token 认证，或反之
- 修改后 xnoded 会使用新的凭证重新连接 taosd
**示例**:
```sql {wrap}
-- 修改 Token
taos> ALTER XNODE SET TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
Query OK, 0 row(s) affected (0.024293s)

-- 修改用户名密码
taos> ALTER XNODE SET USER root PASS 'taosdata';
Query OK, 0 row(s) affected (0.025161s)
```


### 4.2 数据模型变更

#### 4.2.1 SXnodeUserPassObj 结构体扩展

新增 Token 相关字段：
```cpp {wrap}
typedef struct {
  int32_t id;
  int32_t userLen;
  char*   user;
  int32_t passLen;
  char*   pass;
  int32_t tokenLen;    // 新增：token 长度
  char*   token;       // 新增：token 内容
  int64_t createTime;
  int64_t updateTime;
  SRWLatch lock;
} SXnodeUserPassObj;
```

#### 4.2.2 SMCreateXnodeReq 结构体扩展

新增 Token 字段：
```cpp {wrap}
typedef struct {
  int32_t sqlLen;
  int32_t urlLen;
  int32_t userLen;
  int32_t passLen;
  int32_t passIsMd5;
  char*   sql;
  char*   url;
  char*   user;
  char*   pass;
  CowStr  token;       // 新增：token
} SMCreateXnodeReq, SDCreateXnodeReq;
```


#### 4.2.2 SMUpdateXnodeReq 结构体重构

```cpp {wrap}
typedef struct {
  int32_t id;          // 修改为 id（原 xnodeId）
  CowStr  token;       // 新增：token
  CowStr  user;        // 新增：user
  CowStr  pass;        // 新增：pass
  int32_t urlLen;
  int32_t sqlLen;
  char*   url;
  char*   sql;
} SMUpdateXnodeReq;
```

### 4.3 节点类型变更

- 新增 `QUERY_NODE_ALTER_XNODE_STMT` 节点类型
- 移除注释掉的 `QUERY_NODE_UPDATE_XNODE_STMT`

### 4.4 认证方式优先级

创建 XNODE 时的认证方式选择逻辑：
1. 如果指定了 Token (`CREATE XNODE 'url' TOKEN 'xxx'`)，使用 Token 认证
2. 如果指定了用户名密码 (`CREATE XNODE 'url' USER name PASS 'pass'`)，使用用户名密码认证
3. 如果未指定任何认证信息 (`CREATE XNODE 'url'`)，自动创建默认 Token

### 4.5 错误处理

#### 4.5.1 Token 格式错误

```http {wrap}
taos> CREATE XNODE 'x1:6055' TOKEN '';
DB error: Xnode token should not be empty (0.001523s)
```

#### 4.5.2 Token 长度非法

```http {wrap}
taos> CREATE XNODE 'x1:6055' TOKEN 'bjUvkeBfqFsrXBSj8QjnORJcN0nyA6vdkLNAbkI2MhbWPt289OnIQcZHDIDa8SRfdafdafd';
DB error: Xnode token length is illegal (0.001892s)
```

#### 4.5.3 用户名或密码为空

```sql {wrap}
taos> ALTER XNODE SET USER '' PASS 'password';
DB error: xnode user should not be NULL or empty (0.001523s)

taos> ALTER XNODE SET USER root PASS '';
DB error: xnode password should not be NULL or empty (0.001892s)
```

## 性能

1. **创建默认 Token**: 企业版功能，生成默认 Token 的时间开销 < 100ms
2. **修改认证重启**: 修改认证信息后重启 xnoded 的时间约 2000ms（包含优雅关闭和重新启动）
3. **无额外性能损耗**: 认证方式的选择在创建时进行，运行时性能无差异

## 安全

1. **Token 加密**: token 通过加密方式创建
2. **Token 显示**：无任何位置可以打印显示 Token 信息，保证 Token 安全

## 兼容性

1. **向后兼容**: 原有的 `CREATE XNODE 'url' USER name PASS 'password'` 语法继续支持
2. **数据库升级**: 3.4.0.2 版本后才兼容此版本升级
3. **混合认证**: 支持在同一集群中部分 XNODE 使用 Token、部分使用用户名密码

## 运维

部署建议
1. **新部署**: 推荐使用 Token 认证，安全性更高
2. **升级场景**: 存量 XNODE 可继续使用用户名密码，也可通过 ALTER XNODE 切换为 Token
监控建议
1. 监控 xnoded 重启次数（修改认证会触发重启）
2. 监控 `SHOW TOKENS` 中的 name 为 __xnode__ 的 Token 状态
配置变更
无新增配置项。
xnoded 启动时的环境变量变更：
- 新增 `XNODED_TOKEN`: 当使用 Token 认证时传递
- `XNODED_USER_PASS`: 当使用用户名密码认证时传递（原有）

## 使用场景

### 8.1 自动化部署

自动化脚本创建 XNODE 时无需预设用户名密码：
```http {wrap}
-- 自动创建默认 token，无需人工干预
CREATE XNODE 'auto-xnode:6055';

-- 查看自动创建的 token 为 __xnode__
SHOW TOKENS;
```

### 8.2 切换认证方式

从用户名密码切换到 Token 认证：
```sql {wrap}
-- 原使用用户名密码
CREATE XNODE 'x1:6055' USER root PASS 'taosdata';

-- 后续切换到 Token 认证（更安全）
ALTER XNODE SET TOKEN 'C8V3o0ZVvYQ6sMEnjfixjtw0OvN9nIPFAL1HWvSKmHbQsds8vBpVbrEZn2hrzar';
```

### 8.3 密码轮换

定期更新 XNODE 认证信息：
```sql {wrap}
-- 更新密码
ALTER XNODE SET USER root PASS 'new_password_2026';

-- 或更新 Token
ALTER XNODE SET TOKEN 'new_token_generated_2026';
```


### 8.4 凭证泄露应急

Token 或密码泄露后的紧急处理：
```sql {wrap}
-- 立即更换 Token
ALTER XNODE SET TOKEN 'completely_new_token';

-- 或立即更换密码
ALTER XNODE SET USER root PASS 'emergency_new_pass';
```


## 约束和限制

**约束**:
1. Token 长度必须符合系统要求（过长或过短都会报错）
2. ALTER XNODE 一次只能修改一种认证方式（Token 或用户名密码）
3. 修改认证信息后，xnoded 会重启，期间该节点上的任务会暂停
**限制**:
1. 默认 Token 创建是企业版功能（社区版返回 `TSDB_CODE_OPS_NOT_SUPPORT`）
2. 修改认证信息后重启 xnoded 需要约 2s，期间该节点不可用
3. 不支持同时修改多个 XNODE 的认证信息（需要逐个执行 ALTER XNODE）

## 常见错误和排查

**错误 1**: Xnode token should not be empty
**原因**: 指定的 Token 为空字符串
**排查**: 检查 CREATE XNODE TOKEN 'xxx' 中的 Token 值

**错误 2**: Xnode token length is illegal
**原因**: Token 长度不符合要求
**排查**: 确保 Token 长度在有效范围内

**错误 3**: xnode user should not be NULL or empty
**原因**: ALTER XNODE 时用户名为空
**排查**: 检查 ALTER XNODE SET USER 后的用户名

**错误 4**: xnoded 重启后连接失败
**原因**: 新的认证信息有误
**排查**: 
检查 Token 或用户名密码是否正确
检查对应用户是否有足够权限
查看 xnoded 日志获取详细错误信息

## 可观测性

1. **taos shell**: 直接支持新语法
2. **SHOW TOKENS**: 可查看系统 Token，包括 `xnode` 默认用户
3. **日志**: xnoded 重启时会记录相关日志

## 安装和卸载

无特殊要求，正常安装和卸载流程即可。

## 文档

需要修改以下文档：
1. **官网文档**: `docs/zh/14-reference/03-taos-sql/94-datain.md` (已包含在本次提交)
2. **官网文档**: `docs/en/14-reference/03-taos-sql/94-datain.md` (已包含在本次提交)

## 参考文档

- Feishu Feature: [https://project.feishu.cn/taosdata_td/feature/detail/6725312703](https://project.feishu.cn/taosdata_td/feature/detail/6725312703)
- GitHub Commit: 21b5986e29c94448995fadccc03ab83649dae242
- GitHub Commit: d57ad4819e14bab2f870d250a0e78b886d48facb

## 附录

### 16.1 实现要点

#### 16.1.1 Parser 层变更

- `source/libs/parser/inc/sql.y`: 新增 ALTER XNODE 语法规则
- `source/libs/parser/src/parAstCreater.c`: 实现 `createCreateXnodeWithTokenStmt` 和 `createAlterXnodeStmt`
- `source/libs/parser/src/parTranslater.c`: 实现 `translateCreateXnode` 和 `translateAlterXnode`

#### 16.1.2 Mnode 层变更

- `source/dnode/mnode/impl/src/mndXnode.c`: 
  - 新增 `mndStoreXnodeUserPassToken()`: 存储用户密码或 Token
  - 新增 `mndUpdateXnodeUserPassToken()`: 更新用户密码或 Token
  - 新增 `mndRestartXnoded()`: 重启 xnoded
  - 修改 `mndProcessCreateXnodeReq()`: 支持默认 Token 创建逻辑
  - 修改 `mndProcessUpdateXnodeReq()`: 支持 ALTER XNODE 逻辑

#### 16.1.3 Xnode 层变更

- `source/dnode/xnode/src/xnode.c`: 支持 Token 字段传递
- `source/libs/txnode/src/txnodeMgmt.c`: xnoded 启动时支持 TOKEN 环境变量

#### 16.1.4 数据序列化变更

- `source/common/src/msg/xnode.c`: 更新 `SMCreateXnodeReq` 和 `SMUpdateXnodeReq` 的序列化/反序列化
- `source/libs/nodes/src/nodesCodeFuncs.c`: 支持 `SAlterXnodeStmt` 的 JSON 序列化
- `source/libs/nodes/src/nodesUtilFuncs.c`: 支持 `SAlterXnodeStmt` 的内存管理

#### 16.1.5 事务修复

- 修复了多处事务处理中的错误码处理问题
- 统一返回 `TSDB_CODE_ACTION_IN_PROGRESS` 表示异步操作
- 修复了内存分配大小计算问题（`TSDB_XNODE_URL_LEN + 1`）
