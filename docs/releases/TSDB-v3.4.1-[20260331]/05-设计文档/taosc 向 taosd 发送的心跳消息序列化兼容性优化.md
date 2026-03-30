# taosc 向 taosd 发送的心跳消息序列化兼容性优化

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-20 | 2026-04-01 | 1.0 | 王明明 | 初稿 |

## 2. 背景

1. SClientHbBatchReq 是 client 端往 server 发送心跳的结构体，该结构体是批量发送的，内部嵌套了心跳消息结构体 SClientHbReq，如下图所展示。
2. 每次心跳变更时，都在 SClientHbReq 结构体序列化/反序列化消息里修改逻辑，由于不知道外面还套了一层SClientHbBatchReq，并且 SClientHbReq 结构体的序列化/反序列化缺少 start/end 处理，不支持嵌套。导致在 SClientHbReq 里面增加字段大概率出现 coredump。
```cpp
┌─────────────────────────────────────────────────────────────┐
  │                    SClientHbBatchReq                        │
  ├─────────────────────────────────────────────────────────────┤
  │  int64_t  reqId                                             │
  │  int64_t  ipWhiteListVer                                    │
  │  SArray*  reqs  ──────────────────────────┐                 │
  └───────────────────────────────────────────┼─────────────────┘
                             SArray<SClientHbReq>
                                              │  [0..N]
                              ┌───────────────▼──────────────────────────────┐
                              │              SClientHbReq                    │
                              ├──────────────────────────────────────────────┤
                              │  ┌─────────────────────┐                     │
                              │  │    SClientHbKey      │ connKey             │
                              │  ├─────────────────────┤                     │
                              │  │ int64_t  tscRid      │                     │
                              │  │ int8_t   connType    │                     │
                              │  └─────────────────────┘                     │
                              │  int64_t   clusterId  ⚠️  未序列化             │
                              │  uint32_t  userIp                             │
                              │  char      user[]                             │
                              │  char      tokenName[]  ← 最新追加            │
                              │  char      userApp[]                          │
                              │  char      sVer[]                             │
                              │  char      cInfo[]                            │
                              │                                               │
                              │  ┌─────────────────────────────────┐          │
                              │  │         SAppHbReq               │ app      │
                              │  ├─────────────────────────────────┤          │
                              │  │ int64_t  appId                  │          │
                              │  │ int32_t  pid                    │          │
                              │  │ char     name[]                 │          │
                              │  │ int64_t  startTime              │          │
                              │  │ ┌───────────────────────────┐   │          │
                              │  │ │    SAppClusterSummary     │   │ summary  │
                              │  │ ├───────────────────────────┤   │          │
                              │  │ │ uint64_t numOfInsertsReq  │   │          │
                              │  │ │ uint64_t numOfInsertRows  │   │          │
                              │  │ │ uint64_t insertElapsedTime│   │          │
                              │  │ │ uint64_t insertBytes      │   │          │
                              │  │ │ uint64_t fetchBytes       │   │          │
                              │  │ │ uint64_t numOfQueryReq    │   │          │
                              │  │ │ uint64_t queryElapsedTime │   │          │
                              │  │ │ uint64_t numOfSlowQueries │   │          │
                              │  │ │ uint64_t totalRequests    │   │          │
                              │  │ │ uint64_t currentRequests  │   │          │
                              │  │ └───────────────────────────┘   │          │
                              │  └─────────────────────────────────┘          │
                              │                                               │
                              │  ┌─────────────────────────────────┐          │
                              │  │      SQueryHbReqBasic*          │ query    │
                              │  ├─────────────────────────────────┤          │
                              │  │ uint32_t connId                 │          │
                              │  │ SArray*  queryDesc ─────────────┼──┐       │
                              │  └─────────────────────────────────┘  │       │
                              │           SArray<SQueryDesc>           │       │
                              │                            ┌───────────▼────┐  │
                              │                            │   SQueryDesc   │  │
                              │                            ├────────────────┤  │
                              │                            │ char  sql[]    │  │
                              │                            │ uint64 queryId │  │
                              │                            │ int64  useconds│  │
                              │                            │ int64  stime   │  │
                              │                            │ int64  reqRid  │  │
                              │                            │ bool stableQuery│  │
                              │                            │ bool isSubQuery│  │
                              │                            │ char  fqdn[]   │  │
                              │                            │ int32 subPlanNum│  │
                              │                            │ SArray* subDesc─┼──┐
                              │                            └────────────────┘  │
                              │                      SArray<SQuerySubDesc>     │
                              │                                      ┌─────────▼──┐
                              │                                      │SQuerySubDesc│
                              │                                      ├────────────┤
                              │                                      │int64  tid  │
                              │                                      │char status[]│
                              │                                      └────────────┘
                              │  ┌─────────────────────────────────┐          │
                              │  │         SHashObj*               │ info     │
                              │  ├─────────────────────────────────┤          │
                              │  │      hash<key, SKv>             │          │
                              │  │  SKv { int16_t key,             │          │
                              │  │        int32_t valueLen,        │          │
                              │  │        void*   value }          │          │
                              │  └─────────────────────────────────┘          │
                              │                                               │
                              │  ┌─────────────────────────────────┐          │
                              │  │          SIpRange               │userDualIp│
                              │  ├─────────────────────────────────┤          │
                              │  │ int8_t type  (0:IPv4 / 1:IPv6)  │          │
                              │  │ int8_t neg                      │          │
                              │  │ union {                         │          │
                              │  │   SIpV4Range { ip, mask }       │          │
                              │  │   SIpV6Range { addr[2], mask }  │          │
                              │  │ }                               │          │
                              │  └─────────────────────────────────┘          │
                              └──────────────────────────────────────────────┘
```

## 3. 行为说明

增加SClientHbReq 结构编解码的前后兼容，避免后续出现因不知道外层还有嵌套，导致的潜在 coredump。主要修改包括如下两点：
1. 为 SClientHbReq 结构的序列化/反序列化逻辑前后加上 start/end encode 和 decode 的逻辑即可。编解码器 encoder/decoder 包含自动记录结构体的长度做到前后兼容的功能。
2. *另外，把之前在外层的 SClientHbReq 的 userDualIp 字段移到 SClientHbReq 内部编解码，保证代码优雅和一致性。*

## 4. 性能

无。

## 5. 安全

不涉及。

## 6. 兼容性

在 SClientHbReq 里面增加字段后，序列化/反序列化 函数可以做到前后相互兼容。

## 7. 运维

无

## 8. 约束和限制

无

## 9. 常见错误和排查

无新增错误代码。

## 10. 可观测性

不涉及。

## 11. 安装和卸载

不涉及。

## 12. 文档

需要修改官网使用手册。

## 13. 参考文档

无

## 14. 附录

无。
