# 通信模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-19 | 2025-01-19 | 1.0 | 邓怡豪 | 第一次安可送测 |
| 2025-12-17 | 2025-12-17 | 1.1 | 廖浩均 | 重构文档 |

## 2. 背景

本文档旨在定义和描述一个高性能通信模块功能定义。该系统主要用于分布式环境下的并发访问数据、数据库集群内跨节点的数据传输，客户端与数据库服务器之间的通信工作。系统需要确保高效、可靠、安全的数据传输，并能够支持大规模数据传输和实时数据同步的功能需求。

## 3. 定义

1. **TDengine**：一种高性能、支持分布式架构的时间序列数据库。
2. **FQDN**: 即完全限定域名，是互联网中用于唯一标识一台特定主机或服务的完整、绝对域名。它能够明确指出主机在DNS层次结构中的确切位置，确保信息准确无误地送达。
3. **IP**:  指网际互连协议，Internet Protocol的缩写，是TCP/IP体系中的网络层协议。
4. **Latency**：从发出请求到接收到响应所花费的时间。
5. **EpSet**：集群内各个节点（主副本）地址所组成的集合。
6. **TCP**：是一种面向连接的、可靠的、基于字节流的传输层通信协议。

## 4. 行为说明

 内部接口，和用户无关

### 4.1 函数接口说明

```c
// 模块初始化
int32_t rpcInit();

// 模块清理
void rpcCleanup();

// 打开 rpc 对象实例
void *rpcOpen(const SRpcInit *pRpc);

// 关闭 rpc 对象实例
void rpcClose(void *);

// 分配内存
void *rpcMallocCont(int64_t contLen);

// 释放内存
void  rpcFreeCont(void *pCont);

// 重新分配内存
void *rpcReallocCont(void *ptr, int64_t contLen);

// 异步发送数据
int32_t rpcSendRequest(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid);

// 异步返回响应消息
int32_t rpcSendResponse(const SRpcMsg *pMsg);

// 注册断开时刻的回调函数
int32_t rpcRegisterBrokenLinkArg(SRpcMsg *msg);

//  发送消息时候带上下文
int32_t rpcSendRequestWithCtx(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid, SRpcCtx *ctx);

// 同步收发消息
int32_t rpcSendRecv(void *shandle, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);

// 带超时时间的同步收发消息
int32_t rpcSendRecvWithTimeout(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp, int8_t *epUpdated,
                               int32_t timeoutMs);
                               

// 释放ID极其上下文信息。 
int32_t rpcFreeConnById(void *shandle, int64_t connId);

//  设置 fqdn到ip的转换。
int32_t rpcSetDefaultAddr(void *thandle, const char *ip, const char *fqdn);

//  预先设置Handle
int32_t rpcAllocHandle(int64_t *refId);

// 设置白名单 
int32_t rpcSetIpWhite(void *thandle, void *arg);
// 设置 time ip white
int32_t rpcSetTimeIpWhite(void *thandle, void *arg);

// 重新加载TLS 证书
int32_t rpcReloadTlsConfig(void* handle, int8_t type);


int32_t rpcCvtErrCode(int32_t code);

```

### 4.2 配置参数说明

| 名称 | 含义 | 备注 |
| --- | --- | --- |
| numOfRpcThreads | 传输层收发数据的线程个数 | 客户端/服务端参数，默认是核数的一半 |
| numOfRpcSessions | 传输层控制单个进程可以建立的conn 个数 | 客户端/服务端参数，默认是10000 |
| timeToGetAvailableConn | 传输层获取可用链接的最大等待时间 | 客户端/服务端参数，默认是50s |
| readTimeout | 单个请求的超时时长 | 客户端/服务端参数，默认是900s |
| maxRetryWaitTime | 重试的最大超时时间 | 客户端参数，默认是20s, 从重试开始计算 |
| enableIpv6 | 使用ipv6 | 客户端/服务端参数，默认是false. |
| enableTLS | 是否启用ssl/TLS | 客户端/服务端参数，默认不启用 |
| tlsCaPath | ca 证书路径 | 客户端/服务端参数， |
| tlsSvrCertPath | cert 证书路径 | 服务端参数 |
| tlsSvrKeyPath | Key 证书路径 | 服务端参数 |
| tlsCliCertPath | 客户端 cert 证书路径 | 客户端/服务端参数 |
| tlsCliKeyPath | 客户端 key 证书路径 | 客户端/服务端参数 |

## 5. 性能

本需求对系统的写入、查询、以及启动等关键方面的性能提出了明确的要求，旨在确保系统具备高吞吐量和低延迟特性。
- 在数据传输能力方面，系统必须能够在多种不同的数据包大小（package size）情景下，充分消耗掉千兆以太网的全部可用带宽，以确保数据传输的高效性。
- 吞吐量默认不小于 100,000 QPS，上限取决于线程数目配置和硬件配置。
- 为了维持高效能，传输协议本身引入的系统开销必须严格控制在小于 5% 的范围内。 CPU 占用率 ≤ 20%，内存占用 ≤ 1 GB。
- 在系统可用性方面，传输模块的启动时间必须小于 300 毫秒，传输延迟 ≤ 1 ms（局域网内）以保证快速上线和故障恢复。
- 此外，系统必须具备强大的并发连接支撑能力，能够稳定地支撑大规模（例如 50,000 个）并发连接请求。

## 6. 兼容性

 不涉及

## 7. 安全

通信机密性：所有传输中的数据对未经授权的第三方保持机密。强制传输层加密，强制加密套件支持。
确保通信双方（客户端、服务器、节点）的身份真实性。客户端连接服务器时，通过 TLS 证书验证服务器的身份。
数据完整性：确保数据在传输过程中不被篡改。
可用性与韧性：实现连接数限制、连接速率限制以及会话超时机制，防止资源耗尽型的拒绝服务 (DoS) 攻击。
配置与管理：启用最安全的设置，提供安全的接口来加载、更新和管理 TLS 证书和密钥。

## 8. 运维

无

## 9. 使用场景

  本功能属于TDengine的基础功能，跨进程访问数据都需用到，主要是两个方面： 
1. 客户端访问集群
2. 集群之间数据同步

## 10. 约束和限制

 无 

## 11. 常见错误和排查

| 错误码 | 错误码字符串 | 错误码含义说明 |
| --- | --- | --- |
| TSDB_CODE_RPC_NETWORK_UNAVAIL | Unable to establish connection | 无法建立链接 |
| TSDB_CODE_RPC_FQDN_ERROR | Unable to resolve FQDN | 无法解析FQDN |
| TSDB_CODE_RPC_PORT_EADDRINUSE | Port already in use | 端口被占用 |
| TSDB_CODE_RPC_BROKEN_LINK | Conn is broken | 链接断开 |
| TSDB_CODE_RPC_TIMEOUT | Conn read timeout | 链接上读数据超时 |
| TSDB_CODE_RPC_MAX_SESSIONS | rpc open too many session | 打开过多session |
| TSDB_CODE_RPC_NETWORK_ERROR | rpc network error | 集群内部通信出现网络问题 |
| TSDB_CODE_RPC_NETWORK_BUSY | rpc network busy | socket资源紧张 |
| TSDB_CODE_RPC_MODULE_QUIT, | rpc module already quit | 传输模块已经退出 |

## 12. 可观测性

通信模块本身不提供直接可观测的机制。可以通过第三方工具监测通信模块的运行状态，例如：netstat、iftop等工具。
消息收发的缓冲区满了以后，可以在系统日志中检查到相关的警告信息。

## 13. 安装和卸载

无。

## 14. 文档

不涉及

## 15. 参考文档

《[通信模块 - Requirement Spec](https://taosdata.feishu.cn/wiki/D4gLwz0MjiwfrMk7JvncVkYenyh)》

## 16. 附录

无。
