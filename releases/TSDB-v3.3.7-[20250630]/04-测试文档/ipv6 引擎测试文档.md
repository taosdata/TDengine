# ipv6 引擎测试文档

### 1. 测试目标

本文旨在编写测试用例用于引taosd引擎是否支持IPV6。具体目标：
1. 测试新增的CFG是否生效。
2. 测试引擎是否支持IPV6的链接  
-  taosc 从 taosd 获得 IPv6 地址列表，taosc 随后使用这些信息与 taosd 通信
- taosd 之间通信时（例如心跳、sync），采用 IPv6 地址
- taosd 向 telemetry 上报消息时, **暂不支持向 IPv6 地址进行上报**。 
- taosd 向 taoskeeper 上报消息时，例如审计、监控等，采用 IPv6 地址
1. 测试引擎是否支持ipv6的白名单
2. 测试新增对外接口(taos.h)的是否生效
3. 测试兼容性，主要是支持ipv4/ipv6双栈存储

### 2. 参考文档

[引擎支持 IPv6 FS](https://taosdata.feishu.cn/wiki/HoCIwj8hHiJLcokkDs7cukLMnpf)

### 3. 测试项目

| 测试项目 | 测试用例 | 测试方法 | 测试结果 |
| --- | --- | --- | --- |
| CFG 是否生效 |  |  |  |
| 引擎对IPV6的支持--taosc从taosd 获取IPV6的列表 |  |  |  |
| 引擎IPV6的支持--taosd 直接的通信 |  |  |  |
| 引擎对IPV6的支持---taosd 向 telemetry 上报消息时 |  |  | 不支持 |
| 引擎对IPV6的支持--向 taoskeeper 上报消息时 |  |  |  |
| 引擎是否支持ipv6的白名单 |  |  |  |
| 测试新增的对外接口(taos.h)的是否生效 1. taos_fetch_whitelist_dual_stack_a 1. taos_options_connection | tests/script/api/whiteListTest.c | 1. 测试接口，返回默认的两个whiteList, 分别为ipv4和ipv6的默认 1. 测试设置option 是否正常，能否清空clientIp | 通过 |
| 测试兼容性--支持ipv4和ipv6的双栈存储 | compatibility.py | 用之前的版本的启动且写入数据，关闭集群 ， 用新版本taosd 启动 | 通过 |


## 1. 易用性测试（可选）

无

## 2. 长期稳定性测试（可选）

无， 之后需要让平台组支持, 启动测试的用例的时候，用ipv6的方式启动docker 

## 3. 性能测试

 无

## 4. 安全测试

无

## 5. 兼容性测试

无

## 6. 已知问题和限制（可选）
  







###
