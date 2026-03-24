# 引擎支持 IPv6 RS

## 1. 引言

### 1.1 术语与缩写名词

1. 地址长度
   - IPv4：使用 32 位地址
   - IPv6：使用 128 位地址
2. 地址格式
   - IPv4：点分十进制，如 `192.168.1.1`
   - IPv6：冒号分隔的十六进制，如 `2001:0db8:85a3:0000:0000:8a2e:0370:7334`。可以压缩表示，如 `2001:db8:85a3::8a2e:370:7334`

### 1.2 相关文档资料

JIRA [TS-6415](https://jira.taosdata.com:18080/browse/TS-6415)

### 1.3 优先级要求

高

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/04/28 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

1. 支持在 IPv6 环境下运行，但是不要求 IPv4 和 IPv6 同时支持
2. IPv4 和 IPv6 的数据存储不需要保持兼容

## 4. 功能需求

1. 配置参数
   - enableIPv6：新增配置参数，默认值为 0，取值为 1 时表示启用 IPv6
   - firstEp、secondEp、fqdn：支持配置为 IPv6 地址，或者可以解析为 IPv6 地址的 FQDN
2. 网络连接（enableIPv6=1 的集群）
   - taosc 从 taosd 获得 IPv6 地址列表，taosc 随后使用这些信息与 taosd 通信
   - taosd 之间通信时（例如心跳、sync），采用 IPv6 地址
   - taosd 向 telemetry 上报消息时，不支持 IPv6
   - taosd 向 taoskeeper 上报消息时，例如审计、监控等，采用 IPv6 地址（第二期）
3. 其他功能
   - Create Dnode/Mnode/Qnode 等使用 IP 的语法，支持 IPv6
   - taos_connect，支持使用 IPv6 或者 FQDN
   - taos.h 中的 TSDB_OPTION_CONNECTION，支持 IPv6 （第二期）
   - IP 白名单，包括配置及存储，支持 IPv6（第二期）
   - 慢查询，记录 IPv6 地址（第二期）

## 5. 性能需求

和 IPv4 的典型写入及查询相比，性能应该无明显偏差

## 6. 安全需求

本期不考虑安全传输

## 7. 其他需求

无
