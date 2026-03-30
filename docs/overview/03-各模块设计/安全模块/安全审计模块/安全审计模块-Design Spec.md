# 安全审计模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-22 | 2025-12-24 | 1.0 | 程洪泽 | 新建 |

## 2. 引言

### 2.1 目的

本文档旨在定义 TDengine TSDB 安全审计模块的设计实现方案，基于需求规格说明书（RS）和功能规格说明书（FS），提供详细的技术架构、组件设计、接口规范和安全考虑，指导开发团队实现安全审计功能。

### 2.2 范围

本文档涵盖安全审计模块的以下方面：
- 五级审计体系设计与实现（系统级、集群级、数据库级、子表级、数据级）
- 审计权限管理（SYSAUDITOR 角色、审计用户、权限控制）
- 审计数据库设计与安全存储
- 审计信息传输机制（taosd 与 taosKeeper 通信）
- 审计日志内容规范与敏感信息脱敏
- 性能优化与安全合规性设计

### 2.3 受众

- 系统架构师：了解整体架构设计和技术选型
- 开发工程师：实现具体功能模块
- 测试工程师：设计测试用例和验证方案
- 运维工程师：部署、配置和监控审计功能
- 安全审计人员：评估审计合规性和安全性

## 3. 术语

| 术语/缩写 | 全称/解释 |
| --- | --- |
| DDL | Data Definition Language，数据定义语言 |
| DML | Data Manipulation Language，数据操作语言 |
| SQL | Structured Query Language，结构化查询语言 |
| WAL | Write-Ahead Logging，预写式日志 |
| SYSAUDITOR | 系统审计员角色，具有审计相关权限 |
| taosKeeper | TDengine的守护进程，负责监控和审计功能 |
| 审计数据库 | 专门用于存储审计日志的数据库类型 |
| 审计级别 | 审计操作的粒度级别，分为1-5级 |
| auditLevel | 审计级别控制参数（1-5） |
| AuditHttps | 审计信息传输协议控制参数（true=HTTPS，false=HTTP） |

## 4. 概述

### 4.1 架构

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    %% 客户端层\n    Client[\"客户端应用\"] --\u003e|SQL 请求| MNode[\"MNode 管理节点\"]\n    \n    %% MNode 层\n    subgraph MNode_Group [MNode 管理节点]\n        MNode_API[\"MNode API 层\"]\n        MNode_Processor[\"请求处理器\"]\n        MNode_Audit[\"MNode 审计调用点\"]\n        \n        MNode_API --\u003e MNode_Processor\n        MNode_Processor --\u003e MNode_Audit\n    end\n    \n    %% VNode 层\n    subgraph VNode_Group [VNode 虚拟节点集群]\n        VNode1[\"VNode 1\"]\n        VNode2[\"VNode 2\"]\n        VNode3[\"VNode 3\"]\n    end\n    \n    %% 审计插件层\n    subgraph AuditPlugin_Group [审计插件]\n        Audit_API[\"审计API接口\"]\n        Audit_Processor[\"审计记录处理器\"]\n        Audit_Sender[\"审计发送器\"]\n        Audit_Buffer[\"批量缓冲区\"]\n        \n        Audit_API --\u003e Audit_Processor\n        Audit_Processor --\u003e Audit_Buffer\n        Audit_Processor --\u003e Audit_Sender\n        Audit_Buffer --\u003e Audit_Sender\n    end\n    \n    %% 监控服务器层\n    subgraph MonitorServer_Group [监控服务器]\n        Audit_Receiver[\"审计接收器\"]\n        Audit_Storage[\"审计存储\"]\n        Audit_Analytics[\"审计分析\"]\n        \n        Audit_Receiver --\u003e Audit_Storage\n        Audit_Storage --\u003e Audit_Analytics\n    end\n    \n    %% 数据流\n    MNode_Audit --\u003e|\"调用 auditRecord()\"| Audit_API\n    MNode_Group --\u003e|\"分发请求\"| VNode_Group\n    \n    %% 审计发送路径\n    Audit_Sender --\u003e|\"HTTP/HTTPS\"| Audit_Receiver\n    \n    %% 配置和状态\n    Config[\"全局配置\"] --\u003e MNode_Group\n    Config --\u003e AuditPlugin_Group\n    \n    %% 样式定义\n    classDef client fill:#e1f5fe,stroke:#01579b,stroke-width:2px\n    classDef mnode fill:#f3e5f5,stroke:#4a148c,stroke-width:2px\n    classDef vnode fill:#e8f5e8,stroke:#1b5e20,stroke-width:2px\n    classDef audit fill:#fff3e0,stroke:#e65100,stroke-width:2px\n    classDef monitor fill:#fce4ec,stroke:#880e4f,stroke-width:2px\n    classDef config fill:#f5f5f5,stroke:#616161,stroke-width:2px\n    \n    class Client client\n    class MNode_Group mnode\n    class VNode_Group vnode\n    class AuditPlugin_Group audit\n    class MonitorServer_Group monitor\n    class Config config\n","theme":"default","view":"chart"}"/>

- **MNode（管理节点）**：负责集群管理、元数据控制和 DDL 操作，是审计的主要触发点。
- **VNode（虚拟节点）**：负责数据存储、查询处理和 DML 操作，通过内部接口触发审计。
- **审计插件**：核心处理模块，包含 API 接口、记录处理器、发送器和批量缓冲区。
- **监控服务器**：接收、存储和分析审计记录的外部系统。

### 4.2 技术

- **网络传输**：HTTP/HTTPS 协议、cURL 库、TLS 加密
- **数据存储**：TDengine 时序数据库
- **安全机制**：数字签名、哈希链、完整性校验
- **性能优化**：异步处理、批量传输、内存缓冲

### 4.3 依赖项

- **第三方库**：cURL 库（HTTP/HTTPS通信）、JSON 解析库
- **内部依赖**：TDengine 核心引擎、访问控制模块、加密算法模块
- **工具依赖**：taosKeeper 守护进程、监控告警系统

## 5. 设计考虑

### 5.1 假设和限制

#### 5.1.1 假设

1. 审计功能主要在企业版中完整支持，社区版仅支持基础功能
2. 审计数据库使用 TDengine 时序数据库存储，支持高效的时间序列查询
3. 审计信息传输依赖 taosKeeper 组件，需要确保 taosKeeper 服务正常运行
4. 开启高级别审计会对系统性能产生一定影响，需在安全性和性能间平衡

#### 5.1.2 限制

1. 系统内只能存在一个审计数据库，防止审计日志分散存储
2. 审计数据库必须使用 CBC 模式的对称加密算法（SM4-CBC 或 AES-128-CBC）
3. 审计记录保存时间必须大于 5 年，且不可修改为更短时间
4. 数据级别审计（5 级）仅在企业版中支持，社区版不支持
5. 审计格式不向后兼容，使用新版本后不能退回到旧版本

### 5.2 设计模式和原则

1. **分层架构**：系统采用清晰的分层设计，包括客户端层、MNode 层、VNode 层、审计插件层和监控服务器层，各层职责明确，耦合度低。
2. **插件化设计**：审计功能以插件形式实现，可以独立部署和升级，不影响核心数据库功能。
3. **异步处理**：支持实时和批量两种发送模式，平衡性能与实时性要求。
4. **可配置性**：通过全局配置变量控制审计级别、传输协议、目标服务器等参数。

### 5.3 风险和缓解措施

#### 5.3.1 技术风险

1. **性能风险**：审计操作影响数据库性能
  - **缓解措施**：异步处理、批量传输、内存缓冲、性能优化
1. **安全风险**：审计日志被篡改或泄露
  - **缓解措施**：加密存储、数字签名、权限控制、完整性校验
1. **可靠性风险**：审计信息丢失或传输失败
  - **缓解措施**：断点续传、重试机制、本地缓存、监控告警

#### 5.3.2 实施风险

1. **复杂度风险**：五级审计体系实现复杂
  - **缓解措施**：模块化设计、分阶段实施、充分测试
1. **兼容性风险**：新旧版本审计格式不兼容
  - **缓解措施**：版本标识、迁移工具、明确兼容性说明
1. **运维风险**：审计系统维护复杂
  - **缓解措施**：自动化工具、详细文档、监控告警

## 6. 详细设计

### 6.1 关键数据结构

#### 6.1.1 审计响应结构 (SAuditResp)

```c
typedef struct {
  char   *data;      // 响应数据指针
  int64_t dataLen;   // 数据长度（字节）
} SAuditResp;
```

#### 6.1.2 审计记录结构 (SAuditRecord)

```cpp
typedef struct SAuditRecord {
  char     user[TSDB_USER_LEN];           // 操作用户
  int64_t  curTime;                       // 时间戳（纳秒）
  char     strClusterId[TSDB_CLUSTER_ID_LEN]; // 集群ID字符串
  char     clientAddress[256];            // 客户端地址
  char     operation[64];                 // 操作类型
  char     target1[TSDB_DB_FNAME_LEN];    // 目标1（数据库）
  char     target2[TSDB_TABLE_FNAME_LEN]; // 目标2（资源）
  char    *detail;                        // 操作详情
  double   duration;                      // 操作耗时（秒）
  int64_t  affectedRows;                  // 影响行数
} SAuditRecord;
```

#### 6.1.3 全局审计结构 (SAudit)

```c
typedef struct SAudit {
  SAuditCfg    cfg;           // 配置信息
  int32_t      dnodeId;       // 数据节点ID
  char         auditDB[TSDB_DB_FNAME_LEN];    // 审计数据库名
  char         auditToken[AUDIT_TOKEN_LEN];   // 审计令牌
  TdRwLock     infoLock;      // 配置信息读写锁
  TdMutex      recordLock;    // 记录数组互斥锁
  SArray      *records;       // 批量记录数组
} SAudit;
```

#### 6.1.4 全局配置变量

```c
// 审计功能开关
extern bool tsEnableAudit;

// 监控服务器配置
extern char tsMonitorFqdn[];
extern int32_t tsMonitorPort;

// 传输协议选择
extern bool tsAuditHttps;

// 审计级别控制
extern int32_t tsAuditLevel;

// URI路径配置
extern char *tsAuditUri;       // 单条记录URI: "/audit_v2"
extern char *tsAuditBatchUri;  // 批量记录URI: "/audit-batch"

// 全局审计实例
extern SAudit tsAudit;
```

#### 6.1.5 审计级别定义

```c
// 审计级别枚举
typedef enum {
  AUDIT_LEVEL_NONE = 0,      // 禁用审计
  AUDIT_LEVEL_DATABASE = 1,  // 数据库级别操作
  AUDIT_LEVEL_TABLE = 2,     // 表级别操作
  AUDIT_LEVEL_QUERY = 3,     // 查询级别操作
  AUDIT_LEVEL_ALL = 4        // 所有操作
} EAuditLevel;
```

#### 6.1.6 配置结构体

```c
// 审计配置结构
typedef struct SAuditCfg {
  char    server[TSDB_FQDN_LEN];  // 监控服务器地址
  int32_t port;                   // 监控服务器端口
  bool    comp;                   // 是否启用压缩
  // 其他配置字段...
} SAuditCfg;
```

### 6.2 设计图表

#### 6.2.1 MNode 中的审计调用

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant Client as 客户端\n    participant MNode as MNode\n    participant Audit as 审计插件\n    participant Monitor as 监控服务器\n    \n    Client-\u003e\u003eMNode: SQL 操作请求（如 CREATE DATABASE）\n    MNode-\u003e\u003eMNode: 处理请求并执行操作\n    MNode-\u003e\u003eAudit: 调用 auditRecord() 函数\n    Note over MNode,Audit: 参数：操作类型、目标、用户、详情等\n    \n    alt 实时发送模式\n        Audit-\u003e\u003eAudit: 构建 JSON 审计记录\n        Audit-\u003e\u003eMonitor: 立即发送 HTTP/HTTPS 请求\n        Monitor--\u003e\u003eAudit: 返回响应\n    else 批量发送模式\n        Audit-\u003e\u003eAudit: 将记录添加到批量缓冲区\n        Audit-\u003e\u003eAudit: 定时或手动触发批量发送\n        Audit-\u003e\u003eMonitor: 发送批量审计记录\n        Monitor--\u003e\u003eAudit: 返回响应\n    end\n    \n    Audit--\u003e\u003eMNode: 返回执行状态\n    MNode--\u003e\u003eClient: 返回操作结果\n","theme":"default","view":"chart"}"/>

#### 6.2.2 审计插件内部处理流程

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[收到审计请求] --\u003e CheckConfig{检查配置}\n    \n    CheckConfig --\u003e|审计未启用| Skip[跳过审计]\n    CheckConfig --\u003e|审计已启用| Validate{验证参数}\n    \n    Validate --\u003e|参数无效| LogError[记录错误日志]\n    Validate --\u003e|参数有效| ProcessDetail[处理详情数据]\n    \n    ProcessDetail --\u003e CheckLength{检查详情长度}\n    CheckLength --\u003e|超过限制| Truncate[截断详情]\n    CheckLength --\u003e|未超限制| KeepOriginal[保持原样]\n    \n    Truncate --\u003e BuildJSON\n    KeepOriginal --\u003e BuildJSON\n    \n    subgraph BuildJSON [构建JSON记录]\n        B1[添加时间戳]\n        B2[添加集群ID]\n        B3[添加用户信息]\n        B4[添加操作类型]\n        B5[添加客户端地址]\n        B6[添加目标数据库]\n        B7[添加目标资源]\n        B8[添加操作详情]\n        B9[添加影响行数]\n        B10[添加操作耗时]\n    end\n    \n    BuildJSON --\u003e SendMode{选择发送模式}\n    \n    SendMode --\u003e|实时发送| SendNow[立即发送]\n    SendMode --\u003e|批量发送| AddToBuffer[添加到缓冲区]\n    \n    SendNow --\u003e HTTP{选择传输协议}\n    HTTP --\u003e|HTTP| SendHTTP[发送HTTP请求]\n    HTTP --\u003e|HTTPS| SendHTTPS[发送HTTPS请求]\n    \n    AddToBuffer --\u003e WaitTrigger[等待触发条件]\n    WaitTrigger --\u003e|定时触发| SendBatch\n    WaitTrigger --\u003e|手动触发| SendBatch\n    \n    subgraph SendBatch [发送批量记录]\n        SB1[锁定缓冲区]\n        SB2[构建批量JSON]\n        SB3[发送批量请求]\n        SB4[释放内存]\n        SB5[解锁缓冲区]\n    end\n    \n    SendHTTP --\u003e End[完成]\n    SendHTTPS --\u003e End\n    SendBatch --\u003e End\n    Skip --\u003e End\n    LogError --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.2.3 MNode 和 VNode 的审计职责划分

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph LR\n    %% 职责划分\n    subgraph MNodeResponsibilities [MNode 审计职责]\n        M1[管理操作审计]\n        M2[DDL 操作审计]\n        M3[权限操作审计]\n        M4[集群管理审计]\n        M5[配置变更审计]\n    end\n    \n    subgraph VNodeResponsibilities [VNode 审计职责]\n        V1[DML 操作审计]\n        V2[数据查询审计]\n        V3[数据压缩审计]\n        V4[数据迁移审计]\n    end\n    \n    %% 实际调用示例\n    subgraph MNodeExamples [MNode 审计调用示例]\n        ME1[compactDB - 数据库压缩]\n        ME2[restore - 数据恢复]\n        ME3[balanceVgroupLead - VGroup 负载均衡]\n    end\n    \n    %% 连接关系\n    MNodeResponsibilities --\u003e AuditPlugin\n    VNodeResponsibilities --\u003e AuditPlugin\n    MNodeExamples --\u003e MNodeResponsibilities\n    \n    %% 样式\n    classDef mnodeResp fill:#f3e5f5,stroke:#4a148c\n    classDef vnodeResp fill:#e8f5e8,stroke:#1b5e20\n    classDef examples fill:#fff3e0,stroke:#e65100\n    \n    class MNodeResponsibilities mnodeResp\n    class VNodeResponsibilities vnodeResp\n    class MNodeExamples examples\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 公共API接口

#### 7.1.1 审计记录接口

```c
// 实时发送审计记录
void auditRecord(
    SRpcMsg *pReq,           // RPC请求上下文
    int64_t clusterId,       // 集群ID
    char *operation,         // 操作类型
    char *target1,           // 目标1（数据库）
    char *target2,           // 目标2（资源）
    char *detail,            // 操作详情
    int32_t len,             // 详情长度
    double duration,         // 操作耗时（秒）
    int64_t affectedRows     // 影响行数
);

// 批量模式添加记录
void auditAddRecord(
    SRpcMsg *pReq,
    int64_t clusterId,
    char *operation,
    char *target1,
    char *target2,
    char *detail,
    int32_t len,
    double duration,
    int64_t affectedRows
);

// 发送批量记录
void auditSendRecordsInBatch();
```

#### 7.1.2 配置管理接口

```c
// 初始化审计插件
int32_t auditInit(const SAuditCfg *pCfg);

// 设置数据节点ID
void auditSetDnodeId(int32_t dnodeId);

// 清理审计插件
void auditCleanup();

// 获取/设置数据库和令牌
void getAuditDbNameToken(char *pDb, char *pToken);
void setAuditDbNameToken(char *pDb, char *pToken);
```

### 7.2 内部实现接口

#### 7.2.1 核心处理函数

```c
// 审计记录实现（实时发送）
void auditRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation, 
                   char *target1, char *target2, char *detail,
                   int32_t len, double duration, int64_t affectedRows);

// 批量添加记录实现
void auditAddRecordImp(SRpcMsg *pReq, int64_t clusterId, char *operation,
                      char *target1, char *target2, char *detail,
                      int32_t len, double duration, int64_t affectedRows);

// 批量发送实现
void auditSendRecordsInBatchImp();
```

#### 7.2.2 HTTP传输函数（非Windows平台）

```c
// CURL响应回调
static size_t taosAuditWriteData(char *pCont, size_t contLen, 
                                size_t nmemb, void *userdata);

// POST请求发送
static int32_t taosAuditPostRequest(const char *url, SAuditResp *pRsp,
                                   const char *buf, int32_t bufLen,
                                   int32_t timeout, char *qid);

// CURL请求封装
static int32_t taosAuditSendReqByCurl(const char *url, char *pCont,
                                     int64_t contentLen, int64_t timeout,
                                     char *qid);
```

## 8. 安全可控考虑

### 8.1 身份验证机制

```c
// 令牌验证（通过URL参数）
char httpPath[1000] = {0};
tsnprintf(httpPath, 1000, "%s?db=%s&token=%s", tsAuditUri, db, token);

// 硬编码令牌（安全考虑）
tstrncpy(pToken, "xxxxxxxxxx", AUDIT_TOKEN_LEN);
```

### 8.2 传输安全

```c
// HTTPS支持（企业版功能）
if (tsAuditHttps) {
#ifndef WINDOWS
  char path[1000] = {0};
  (void)tsnprintf(path, 1000, "https://%s:%d/%s", 
                  tsAudit.cfg.server, tsAudit.cfg.port, httpPath);
  // 使用libcurl进行HTTPS传输
#endif
}
```

### 8.3 数据完整性

```c
// 详情长度限制防止攻击
if(len > AUDIT_DETAIL_MAX){
  uWarn("can't record total audit since detail is too long, len:%d", len);
}

// 缓冲区清理
memset(buf, 0, min);  // 清零初始化
```

## 9. 性能和可扩展性

### 9.1 批量处理优化

```c
// 批量发送减少网络开销
void auditSendRecordsInBatchImp() {
  // 1. 锁定记录数组
  // 2. 批量构建JSON（减少序列化次数）
  // 3. 单次HTTP请求发送所有记录
  // 4. 批量释放内存
  // 5. 解锁记录数组
}
```

### 9.2 内存复用机制

```c
// CURL响应缓冲区复用
if (pRsp->data == NULL) {
  pRsp->data = taosMemoryMalloc(size + 1);
} else {
  char *p = taosMemoryRealloc(pRsp->data, size + 1);  // 复用原有内存
  pRsp->data = p;
}
```

### 9.3 网络传输优化

```c
// HTTP压缩支持
EHttpCompFlag flag = tsAudit.cfg.comp ? HTTP_GZIP : HTTP_FLAT;
if (taosSendHttpReportWithQID(tsAudit.cfg.server, httpPath, 
                             tsAudit.cfg.port, pCont, strlen(pCont), 
                             flag, qid) != 0) {
  // 错误处理
}

// CURL连接优化
if ((code = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L)) != CURLE_OK) goto _OVER;
if ((code = curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout)) != CURLE_OK) goto _OVER;
```

## 10. 部署和配置

无。

## 11. 监控和维护

### 11.1 审计相关日志函数

```c
// 错误日志 - 操作失败
uError("failed to send audit msg, cont:%s, since %s", pCont, terrstr(code));

// 警告日志 - 参数问题
uWarn("can't record total audit since detail is too long, len:%d", len);

// 调试日志 - 发送详情
uDebug("audit record with QID:%s cont:%s\n", qid, pCont);

// 跟踪日志 - 配置检查
uTrace("auditDB or auditToken is empty, can't send audit record");

// 详细调试 - CURL响应
uDebugL("curl response is received, len:%" PRId64 ", content:%s", size, pRsp->data);
```

### 11.2 日志内容规范

- **时间戳**：自动添加
- **函数名**：使用 `__func__` 宏
- **文件名和行号**：`__FILE__`, `__LINE__`
- **错误码**：使用 `tstrerror(code)` 转换
- **关键参数**：操作类型、目标、用户等

## 12. 参考资料

1. [安全审计模块-Requirement Spec](https://taosdata.feishu.cn/wiki/Gji4wOzfTiEirMkzDV2cbY1unHb)
2. [安全审计模块-Function Spec](https://taosdata.feishu.cn/wiki/GUHHwbSmzigseCkpC9lcH8WNnrd)
