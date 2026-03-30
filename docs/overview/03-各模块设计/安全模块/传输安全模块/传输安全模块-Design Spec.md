# 传输安全模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-22 | 2025-12-23 | 1.0 | 程洪泽 | 新建 |

## 2. 引言

### 2.1 目的

本文档旨在定义传输安全模块的详细设计，包括架构设计、组件设计、接口规范、安全考虑、性能要求等。本设计文档基于[传输安全模块-Requirement Spec](https://taosdata.feishu.cn/wiki/Dwxvww5zoidGu6kQxFdc8Vp0ngi)和[身份鉴别模块-Function Spec](https://taosdata.feishu.cn/wiki/WIbGwIzWLitDw6kcumpc43m9nab)，为开发团队提供实现指导。

### 2.2 范围

本文档涵盖传输安全模块的以下功能：
1. TLS 安全性增强（私钥文件加密存储、定期更换）
2. 通信安全性增强（防重放攻击、消息队列安全改造）
3. 关键信息传输（SASL 机制集成）
4. 白名单功能增强（支持禁止 HOST）
5. 连接管理（会话控制、资源限制）
6. 暴力破解检测与防护
7. 动态更新 TLS 证书

### 2.3 受众

- **系统架构师**：了解整体架构设计
- **开发工程师**：实现具体功能模块
- **测试工程师**：验证功能实现
- **运维工程师**：部署和维护传输安全模块
- **安全审计人员**：审查安全设计和实现

## 3. 术语

| 术语/缩写 | 全称/解释 |
| --- | --- |
| TLS | 传输层安全协议（Transport Layer Security） |
| SASL | 简单认证和安全层（Simple Authentication and Security Layer） |
| SCRAM | 加盐挑战响应认证机制（Salted Challenge Response Authentication Mechanism） |
| RPC | 远程过程调用（Remote Procedure Call） |
| SALT | 随机盐值，用于密码学操作增加随机性 |
| CFG_KEY | 配置密钥，用于加密配置文件 |
| SYSSSO | 系统超级安全管理员（System Super Security Officer） |
| SSF | 安全强度因子（Security Strength Factor） |
| Session | 用户通过`taos_connect`建立的数据库会话 |
| Connection | 底层的TCP/TLS连接，一个Session可能对应多个Connection |

## 4. 概述

### 4.1 架构

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    subgraph \"应用层\"\n        A1[业务应用]\n        A2[SQL接口]\n        A3[管理工具]\n    end\n    \n    subgraph \"会话管理层\"\n        B1[连接管理器]\n        B2[会话控制器]\n        B3[资源限制器]\n    end\n    \n    subgraph \"安全层\"\n        C1[TLS安全模块]\n        C2[SASL认证模块]\n        C3[访问控制模块]\n        C4[密钥管理模块]\n    end\n    \n    subgraph \"传输层\"\n        D1[TCP连接池]\n        D2[消息队列]\n        D3[数据包处理器]\n    end\n    \n    A1 --\u003e B1\n    A2 --\u003e B2\n    A3 --\u003e B3\n    B1 --\u003e C1\n    B2 --\u003e C2\n    B3 --\u003e C3\n    C1 --\u003e D1\n    C2 --\u003e D2\n    C3 --\u003e D3\n","theme":"default","view":"chart"}"/>

### 4.2 技术

- 加密技术：TLS 1.3、AES-GCM、HMAC-SHA256、PBKDF2
- 认证技术：SASL 框架、SCRAM-SHA-256 机制
- 开发语言：C/C++（核心模块）
- 第三方库：OpenSSL（TLS 实现）、libsasl2（SASL 实现）
- 网络协议：TCP/IP、RPC 协议

### 4.3 依赖项

- 系统依赖：
  - glibc 2.17 及以上
  - OpenSSL 1.1.1 及以上
  - libsasl2-dev 库
- 内部依赖：
  - 存储安全模块（密钥管理）
  - 身份鉴别模块（用户认证）
  - 访问控制模块（权限管理）
  - 监控模块（日志和指标收集）

## 5. 设计考虑

### 5.1 假设和限制

1. 企业版专属：所有传输安全增强功能仅企业版本支持
2. TLS依赖：动态更新证书功能需要先启用 TLS
3. 性能影响：安全增强带来的性能下降控制在 10% 以内
4. 向后兼容：保持与 3.3.8 版本的兼容性

### 5.2 设计模式和原则

1. 分层架构：分离关注点，每层负责特定功能
2. 策略模式：安全算法可配置，支持多种加密和认证机制

### 5.3 风险和缓解措施

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| 性能下降 | 系统吞吐量降低 | 优化算法实现，使用硬件加速，连接复用 |
| 兼容性问题 | 旧版本客户端无法连接 | 支持协议降级，提供兼容模式 |
| 密钥泄露 | 安全体系被破坏 | 密钥加密存储，定期轮换，访问控制 |
| 单点故障 | 安全模块故障影响整个系统 | 模块化设计，故障隔离，快速恢复 |
| 配置错误 | 安全功能失效 | 配置验证工具，默认安全配置，配置向导 |

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 SASL认证模块

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"classDiagram\n    %% 外部依赖\n    class sasl_conn_t {\n        \u003c\u003cexternal\u003e\u003e\n        void* p\n    }\n    \n    class taoserror_h {\n        \u003c\u003cexternal\u003e\u003e\n        TSDB_CODE_* 错误码定义\n    }\n    \n    class transComm_h {\n        \u003c\u003cexternal\u003e\u003e\n        传输层通用定义和函数\n    }\n    \n    class transLog_h {\n        \u003c\u003cexternal\u003e\u003e\n        日志记录功能\n    }\n    \n    class tversion_h {\n        \u003c\u003cexternal\u003e\u003e\n        版本信息\n    }\n    \n    %% 核心数据结构\n    class SSaslBuffer {\n        +int32_t cap\n        +int32_t len\n        +int8_t invalid\n        +int8_t ref\n        +uint8_t* buf\n        \n        +saslBufferInit(int32_t cap) int32_t\n        +saslBufferAppend(uint8_t* data, int32_t len) int32_t\n        +saslBufferCleanup() void\n        +saslBufferClear() void\n    }\n    \n    class SSaslConn {\n        +int32_t state\n        +int8_t completed\n        +sasl_conn_t* conn\n        +char* authUser\n        +void* pUvConn\n        +SSaslBuffer in\n        +SSaslBuffer out\n        +SSaslBuffer authInfo\n        +int8_t isAuthed\n        +int8_t server\n        +char authId[256]\n        \n        +saslConnCreate(SSaslConn** ppConn, int8_t server) int32_t\n        +saslConnInit(SSaslConn* pConn) int32_t\n        +saslConnCleanup(SSaslConn* pConn) void\n        +saslConnSetState(SSaslConn* pConn, int32_t state) void\n        +saslConnEncode(const char* input, int32_t len, const char** output, unsigned* outputLen) int32_t\n        +saslConnDecode(const char* input, int32_t len, const char** output, unsigned* outputLen) int32_t\n        +saslConnHandleAuth(const char* input, int32_t len) int32_t\n        +saslAuthIsInited(SSaslConn* pConn) int8_t\n    }\n    \n    %% 库级函数\n    class SaslLibrary {\n        \u003c\u003cnamespace\u003e\u003e\n        +saslLibInit() void\n        +saslLibCleanup() void\n    }\n    \n    %% 依赖关系\n    SSaslConn \"1\" *-- \"3\" SSaslBuffer : 包含\n    SSaslConn --\u003e sasl_conn_t : 使用\n    \n    %% 函数依赖\n    SaslLibrary --\u003e taoserror_h : 使用错误码\n    SSaslBuffer --\u003e taoserror_h : 使用错误码\n    SSaslConn --\u003e taoserror_h : 使用错误码\n    SSaslConn --\u003e transComm_h : 依赖传输层定义\n    SSaslConn --\u003e transLog_h : 日志记录\n    SSaslConn --\u003e tversion_h : 版本检查\n    \n    %% 条件编译依赖\n    note for SSaslConn \"条件编译依赖:\\n#if defined(TD_ENTERPRISE) \u0026\u0026 defined(LINUX)\\n#include \u003csasl/sasl.h\u003e\\n#endif\"\n    \n    %% 函数调用关系\n    note for SSaslConn \"核心功能:\\n1. 连接管理 (Create/Init/Cleanup)\\n2. 数据编码/解码 (Encode/Decode)\\n3. 认证处理 (HandleAuth)\\n4. 状态管理 (SetState)\"\n    \n    note for SSaslBuffer \"缓冲区功能:\\n1. 初始化/清理\\n2. 数据追加\\n3. 内容重置\"\n","theme":"default","view":"chart"}"/>

##### 6.1.1.1 组件说明

###### 6.1.1.1.1 核心数据结构

- SSaslBuffer: SASL 数据缓冲区，用于存储加密/解密过程中的数据
- SSaslConn: SASL 连接上下文，维护单个 SASL 会话的所有状态信息

###### 6.1.1.1.2 函数分类

- 库级函数: `saslLibInit()`, `saslLibCleanup()` - 全局初始化和清理
- 连接管理: `saslConnCreate()`, `saslConnInit()`, `saslConnCleanup()`, `saslConnSetState()`
- 数据操作: `saslConnEncode()`, `saslConnDecode()` - 数据加密/解密
- 认证处理: `saslConnHandleAuth()`, `saslAuthIsInited()` - SASL 握手和认证
- 缓冲区管理: `saslBufferInit()`, `saslBufferAppend()`, `saslBufferCleanup()`, `saslBufferClear()`

###### 6.1.1.1.3 依赖关系

- 内部依赖: SSaslConn 包含三个 SSaslBuffer 成员（in, out, authInfo）
- 外部依赖:
  - `sasl/sasl.h`: 底层 SASL 库（条件编译）
  - `taoserror.h`: 错误码定义
  - `transComm.h`: 传输层通用定义
  - `transLog.h`: 日志记录
  - `tversion.h`: 版本信息

###### 6.1.1.1.4 设计模式

- 封装: 将底层 SASL 库的复杂性封装在 SSaslConn 和 SSaslBuffer 中
- 资源管理: 明确的初始化/清理函数对
- 状态管理: 通过 state 字段管理连接状态机
- 缓冲区管理: 独立的缓冲区结构体支持数据的高效处理

##### 6.1.1.2 使用流程

1. 调用 `saslLibInit()` 初始化 SASL 库
2. 使用 `saslConnCreate()` 创建连接上下文
3. 调用 `saslConnInit()` 初始化连接
4. 使用 `saslConnHandleAuth()` 进行认证握手
5. 认证成功后，使用 `saslConnEncode()`/`saslConnDecode()` 进行数据加密/解密
6. 使用完成后调用 `saslConnCleanup()` 清理连接
7. 程序退出前调用 `saslLibCleanup()` 清理库资源

#### 6.1.2 连接管理模块

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TD\n    A[ESessionType 枚举] --\u003e B[SSessionError 结构体]\n    A --\u003e C[SSessMetric 结构体]\n    A --\u003e D[SSessParam 结构体]\n    \n    E[sessCheckFn 函数指针] --\u003e B\n    F[sessUpdateValueFn 函数指针] --\u003e B\n    G[sessUpdateLimitFn 函数指针] --\u003e B\n    \n    B --\u003e H[SSessionMgt 结构体]\n    C --\u003e H\n    \n    I[TdThreadRwlock] --\u003e C\n    I --\u003e H\n    \n    J[SHashObj*] --\u003e H\n    \n    K[SSessParam 结构体] --\u003e L[sessMgtUpdateUserMetric 函数]\n    A --\u003e L\n    \n    M[sessMgtUpdataLimit 函数] --\u003e A\n    N[sessMgtGet 函数] --\u003e A\n    O[sessMgtCheckUser 函数] --\u003e A\n    \n    P[sessMgtInit 函数] --\u003e H\n    Q[sessMgtRemoveUser 函数] --\u003e H\n    R[sessMgtDestroy 函数] --\u003e H\n","theme":"default","view":"chart"}"/>

##### 6.1.2.1 详细组件说明

###### 6.1.2.1.1 核心枚举类型

- ESessionType: 会话限制类型枚举
```c
typedef enum {
  SESSION_PER_USER = 0,           // 每个用户的最大会话数限制
  SESSION_CONN_TIME = 1,          // 会话连接的最大存活时间
  SESSION_CONN_IDLE_TIME = 2,     // 会话连接的最大空闲时间
  SESSION_MAX_CONCURRENCY = 3,    // 最大并发数限制
  SESSION_MAX_CALL_VNODE_NUM = 4, // 单次调用涉及的最大 VNode 数量限制
  SESSION_MAX_TYPE = 5            // 会话类型的最大值，用于边界检查
} ESessionType;
```

###### 6.1.2.1.2 函数指针类型

- sessCheckFn: 会话检查函数指针
```c
typedef int32_t (*sessCheckFn)(int64_t value, int64_t limit);
```

- sessUpdateValueFn: 会话值更新函数指针
```c
typedef int32_t (*sessUpdateValueFn)(int64_t* pValue, int64_t delta);
```

- sessUpdateLimitFn: 会话限制更新函数指针
```c
typedef int32_t (*sessUpdateLimitFn)(int64_t* pValue, int64_t limit);
```

###### 6.1.2.1.3 错误处理结构体

- SSessionError: 会话错误处理结构体
```c
typedef struct {
  ESessionType      type;     // 会话类型
  sessCheckFn       checkFn;  // 检查函数
  sessUpdateValueFn updateFn; // 更新函数
  sessUpdateLimitFn limitFn;  // 限制更新函数
} SSessionError;
```

###### 6.1.2.1.4 会话指标结构体

- SSessMetric: 会话指标结构体
```c
typedef struct SSessMetric {
  int32_t refCnt;                    // 引用计数
  int64_t accessTime;                // 当前访问时间戳
  int64_t lastAccessTime;            // 上一次访问时间戳
  int64_t value[SESSION_MAX_TYPE];   // 各类会话指标当前值
  int64_t limit[SESSION_MAX_TYPE];   // 各类会话指标上限值
  TdThreadRwlock lock;               // 读写锁
} SSessMetric;
```

###### 6.1.2.1.5 会话参数结构体

- SSessParam: 会话参数结构体
```c
typedef struct {
  ESessionType type;   // 参数类型
  int64_t      value;  // 参数值
} SSessParam;
```

###### 6.1.2.1.6 会话管理结构体

- SSessionMgt: 会话管理结构体
```c
typedef struct SSessionMgt {
  TdThreadRwlock lock;      // 读写锁
  SHashObj* pSessMetricMap; // 会话指标哈希表
} SSessionMgt;
```

### 6.2 图表说明

#### 6.2.1 数据流图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    A[客户端请求] --\u003e B[TCP连接建立]\n    B --\u003e C{TLS握手}\n    C --\u003e|成功| D[SASL认证]\n    C --\u003e|失败| E[连接关闭]\n    \n    D --\u003e F{认证结果}\n    F --\u003e|成功| G[访问控制检查]\n    F --\u003e|失败| H[记录失败日志]\n    H --\u003e E\n    \n    G --\u003e I{ACL检查}\n    I --\u003e|允许| J[资源限制检查]\n    I --\u003e|拒绝| K[返回访问拒绝]\n    K --\u003e E\n    \n    J --\u003e L{资源检查}\n    L --\u003e|通过| M[建立安全会话]\n    L --\u003e|超限| N[返回资源限制错误]\n    N --\u003e E\n    \n    M --\u003e O[正常数据通信]\n    O --\u003e P[连接监控]\n    P --\u003e Q{连接状态}\n    Q --\u003e|正常| O\n    Q --\u003e|超时/空闲| E\n","theme":"default","view":"chart"}"/>

#### 6.2.2 消息序列图（TLS+SASL连接建立）

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant Client as 客户端\n    participant TLS as TLS模块\n    participant SASL as SASL模块\n    participant ACL as 访问控制\n    participant CM as 连接管理\n    \n    Client-\u003e\u003eTLS: ClientHello\n    TLS-\u003e\u003eClient: ServerHello + Certificate\n    Client-\u003e\u003eTLS: ClientKeyExchange + Finished\n    TLS-\u003e\u003eClient: Finished\n    Note over Client,TLS: TLS握手完成\n    \n    Client-\u003e\u003eSASL: 认证请求(username, mechanism)\n    SASL-\u003e\u003eClient: 挑战(nonce, salt, iterations)\n    Client-\u003e\u003eSASL: 响应(client_proof)\n    SASL-\u003e\u003eClient: 认证成功(server_signature)\n    Note over Client,SASL: SASL认证完成\n    \n    Client-\u003e\u003eACL: 连接请求\n    ACL-\u003e\u003eCM: 检查用户限制\n    CM-\u003e\u003eACL: 限制检查结果\n    ACL-\u003e\u003eClient: 连接建立成功\n    \n    loop 数据通信\n        Client-\u003e\u003eTLS: 加密数据\n        TLS-\u003e\u003eSASL: 解码数据\n        SASL-\u003e\u003eClient: 处理响应\n    end\n","theme":"default","view":"chart"}"/>

#### 6.2.3 状态转换图（连接状态）

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    [*] --\u003e DISCONNECTED\n    \n    DISCONNECTED --\u003e CONNECTING : connect()\n    CONNECTING --\u003e TLS_HANDSHAKE : TCP连接成功\n    TLS_HANDSHAKE --\u003e SASL_AUTH : TLS握手成功\n    SASL_AUTH --\u003e ACL_CHECK : SASL认证成功\n    ACL_CHECK --\u003e ACTIVE : 访问控制通过\n    \n    ACTIVE --\u003e IDLE : 无活动超时\n    IDLE --\u003e ACTIVE : 新请求\n    IDLE --\u003e DISCONNECTING : 空闲超时\n    \n    ACTIVE --\u003e DISCONNECTING : 主动关闭/超时\n    DISCONNECTING --\u003e DISCONNECTED : 清理完成\n    \n    CONNECTING --\u003e DISCONNECTED : 连接失败\n    TLS_HANDSHAKE --\u003e DISCONNECTED : TLS握手失败\n    SASL_AUTH --\u003e DISCONNECTED : 认证失败\n    ACL_CHECK --\u003e DISCONNECTED : 访问拒绝\n    \n    note right of ACTIVE\n        正常数据通信状态\n        监控资源使用\n        检查连接限制\n    end note\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 API文档

#### 7.1.1 管理接口

##### 7.1.1.1 SQL接口

```sql
-- TLS证书管理
ALTER DNODES RELOAD TLS;
```

#### 7.1.2 编程接口（C API）

```c
DLL_EXPORT int32_t taos_connect_is_alive(TAOS *taos);
```

#### 7.1.3 配置接口

在 TDengine TSDB 的配置文件 `taos.cfg` 中添加以下配置选项：
```plaintext

## 8. taos.cfg

enableSasl              0
sessionPerUser         -1
sessionConnTime        -1
sessionConnIdleTime    -1
sessionMaxConcurrency  -1
sessionMaxCallVnodeNum -1
```

### 8.1 用户界面

无。

## 9. 安全考虑（如适用）

### 9.1 安全要求

#### 9.1.1 数据加密

- 传输加密：所有网络通信使用 TLS 1.3 加密，禁用不安全的协议版本和密码套件
- 密钥加密：TLS 私钥使用 CFG_KEY 加密存储，密钥材料在内存中加密
- 密码哈希：用户密码使用 PBKDF2 加盐哈希存储，迭代次数不低于 10000 次
- 敏感数据：认证令牌、会话密钥等敏感数据在内存中使用安全区域存储，使用后立即清除
- 第三方库加密：确保使用的加密库（如 OpenSSL）为最新安全版本，禁用已知不安全的算法

#### 9.1.2 用户认证

- 双重认证：TLS 证书认证 + SASL 用户认证，支持证书吊销列表检查
- 强密码策略：支持密码复杂度要求，密码长度不低于 12 字符
- 多因素认证：预留多因素认证接口，支持 TOTP、硬件令牌等
- 会话管理：安全的会话创建、维护和销毁，会话令牌使用密码学安全随机数生成
- 库认证机制：验证第三方认证库（如 libsasl2）的安全配置和机制选择

#### 9.1.3 访问控制

- 最小权限：遵循最小权限原则，所有组件以最低必要权限运行
- 角色分离：SYSSSO 角色与其他角色权限分离，关键操作需双人复核
- 细粒度控制：支持 IP、主机名、时间、协议版本等多维度访问控制
- 审计跟踪：所有安全操作完整审计，审计日志防篡改
- 库权限控制：限制第三方库的文件系统、网络访问权限，使用沙箱机制

### 9.2 第三方库安全管理

#### 9.2.1 库选择与评估

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart LR\n    A[需求分析] --\u003e B[候选库筛选]\n    B --\u003e C[安全评估]\n    C --\u003e D{评估结果}\n    D --\u003e|通过| E[批准使用]\n    D --\u003e|不通过| F[重新筛选或自研]\n    \n    subgraph C[安全评估]\n        C1[代码质量审计]\n        C2[漏洞历史分析]\n        C3[许可证合规检查]\n        C4[维护活跃度评估]\n        C5[依赖关系分析]\n    end\n    \n    E --\u003e G[集成与配置]\n    G --\u003e H[持续监控]\n","theme":"default","view":"chart"}"/>

##### 9.2.1.1 评估标准

1. 安全历史记录：
  - 分析库的 CVE 漏洞历史
  - 评估漏洞修复响应时间
  - 检查安全公告发布频率
1. 代码质量指标：
  - 代码覆盖率（不低于 80%）
  - 静态分析结果
  - 内存安全实践（对 C/C++ 库）
  - 输入验证完整性
1. 维护状态：
  - 最后更新时间（不超过 6 个月）
  - 维护团队规模和经验
  - 社区活跃度（Issue 响应时间）
1. 许可证合规：
  - 许可证兼容性检查
  - 商业使用限制
  - 专利风险评估

#### 9.2.2 库集成安全

- [x]  库版本锁定（使用固定版本号）
- [x]  编译时安全选项启用（如栈保护、地址随机化）
- [x]  运行时安全限制（能力降级、资源限制）
- [x]  输入验证包装层
- [x]  错误处理安全包装
- [x]  内存安全边界检查

## 10. 性能和可扩展性（如适用）

### 10.1 性能要求

#### 10.1.1 性能指标

| 指标 | 要求 | 测量方法 |
| --- | --- | --- |
| TLS 握手时间 | 95% < 500ms | 连接建立时间统计 |
| SASL 认证时间 | 95% < 300ms | 认证过程时间统计 |
| 加密吞吐量 | ≥ 500MB/s | 大数据传输测试 |
| 连接建立速率 | ≥ 1000 conn/s | 高并发连接测试 |
| 内存占用 | ≤ 64KB/连接 | 内存使用监控 |
| CPU 开销 | ≤ 80% 增加 | 性能对比测试 |

#### 10.1.2 性能优化策略

1. 连接复用：TLS 会话复用，减少握手开销
2. 批量处理：多个小消息合并传输
3. 异步操作：证书加载、日志写入等异步执行
4. 缓存优化：
  - 证书和密钥缓存
  - 用户限制信息缓存
  - 访问控制规则缓存
1. 硬件加速：支持 AES-NI 等硬件加速指令

## 11. 部署和配置

本功能随 TDengine TSDB 发布，无需特殊部署。新增配置选项参考配置接口章节。

## 12. 监控和维护

无。

## 13. 参考资料

1. [传输安全模块-Requirement Spec](https://taosdata.feishu.cn/wiki/Dwxvww5zoidGu6kQxFdc8Vp0ngi)
2. [传输安全模块-Function Spec](https://taosdata.feishu.cn/wiki/PgL7wlFpmi4qpWkHk0tcmnW4nze)
