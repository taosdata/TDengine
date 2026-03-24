# 引擎支持 IPv6 FS

## 1. 背景

JIRA [TS-6415](https://jira.taosdata.com:18080/browse/TS-6415)
需求 [taosX 支持 IPv6 - RS](https://taosdata.feishu.cn/wiki/Q5YMwHNGLi0RA5kuEwRcd5KJnQF) 

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/25 | 1.0 | 邓怡豪 | 初稿 |
|  |  |  |  |

## 3. 定义

## 4. 行为说明

### 4.1 增加配置参数

enableIpv6
1. 为 0 时，所有的通信相关都走 ipv4
2. 为 1 时，所有的通信都走 ipv6， 参数不支持动态修改   

### 4.2 具体行为说明

1. firstEp、secondEp、fqdn：支持配置为 IPv6 地址，或者可以解析为 IPv6 地址的 FQDN， 如果 firseEP、sencodeEp、fqdn 设置的不一致，即不全为 ipv4，或者不全为ipv6，则无法正常通信。
2. 网络连接（enableIPv6=1 的集群）
   - taosc 从 taosd 获得 IPv6 地址列表，taosc 随后使用这些信息与 taosd 通信
   - taosd 之间通信时（例如心跳、sync），采用 IPv6 地址
   - taosd 向 telemetry 上报消息时, **暂不支持向**** IP****v6**** ****地址进行上报**。 
   - taosd 向 taoskeeper 上报消息时，例如审计、监控等，采用 IPv6 地址
3. Create Dnode/Mnode/Qnode 等使用 IP 的语法
4. taos_connect，支持使用 IPv6 或者 FQDN
5. taos.h 中的 TSDB_OPTION_CONNECTION，支持 IPv6 
6. IP 白名单，包括配置及存储都支持 ipv6. 
7. 慢查询，记录 IPv6 地址。 

## 5. 性能

不涉及性能

## 6. 兼容性

1. 可以直接升级，但不能支持回退,   原因：ipv4和ipv6 要存储的长度不一样，为了同时支持ipv4/ipv6 白名单， 需要升级IP白名单的数据版本， 新版本的数据可以存储ipv4或者ipv6。 如果需要回退： 先备份mnode 的数据文件，升级后发现有问题， 停下所有taosd,  用备份的数据文件替换mnode的数据文件， 不过要注意这种回退方式会导致升级-回退期间mnode 的其他更新丢失。 

## 7. 运维

## 8. 使用场景

## 9. 约束和限制

1. firstEp/secondeEp/fqdn 需要设置为一致，都是 ipv4 或者都是 ipv6
2. 一个集群里不能 ipv4  和ipv6 混合部署，默认为 ipv4

## 10. 常见错误和排查

## 11. 可观测性

如果出现问题，需要打开trace日志。 

## 12. 安装和卸载

不涉及

## 13. 文档

## 14. 参考文档

## 15. 附录
