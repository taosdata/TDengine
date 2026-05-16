# taosX支持IPv6-Test Spec

## 1. 测试目标

本文旨在编写测试用例用于测试 taosx 支持 IPv6 的连通性。具体目标：
1. 测试 REST API 是否正常提供 IPv6 地址访问
2. 测试 gRPC API 是否提供正常 IPv6 地址访问
3. 测试 REST API 和 gRPC API 的 IPv4 是否访问正常

## 2. 参考文档

JIRA： [TS-6469](https://jira.taosdata.com:18080/browse/TS-6469)

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-5-23 | 0.1 | 张贵川 | 文档撰写 |

## 4. 测试结论

1. REST API 和 gRPC API 均可通过 IPv6 访问
2. 兼容历史版本，同时 REST API 和 gRPC API 也均可通过 IPv4 访问

## 5. 测试环境

- OS:  Linux x64,  ubuntu:22.04, DeepinOS 23.1
- 环境：

| **硬件环境** | **IP** | 用途 | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| 本地开发机 | 192.168.100.85 | taosd, taosX,taos-explorer | Intel(R) Core(TM) U7-155H CPU @1945GHz 22核 | 32G | SKHynix_HFS001TEJ9X164N SSD nvme0n1 953.9G |

## 6. 功能测试

### 6.1 REST API 功能

#### 6.1.1 测试要点

1. 测试 REST API 是否能够通过 IPv6 地址访问
2. 测试 REST API 是否能够通过 IPv4 地址访问

#### 6.1.2 用例列表

| # | 测试功能 | 测试用例 | 测试描述 | 预期行为 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| 1 | 启动 taosx serve 并使用默认端口，验证 IPv6 地址访问 | 可以通过 telnet 命令和通过发送 Get 请求验证是否有效 | ipv6地址可访问 | 通过 |
|  | 启动 taosX serve 并指定 --listen 为 IPv6 地址，验证 IPv6 地址访问 | 可以通过 telnet 命令和通过发送 Get 请求验证是否有效 | ipv6地址可访问 | 通过 |
|  | 配置 /etc/taos/taosx.toml listen参数，使用不同的 taosx 启动方式启动 serve，验证 IPv6 地址访问。 这里启动方式使用：taosx serve -c 指定配置文件 | 配置 /etc/taos/taosx.toml 参数，验证 IPv6 地址访问 ```plaintext {wrap} listen = "172.18.0.5:6050,[2001:db9:2::6]:6050" ``` 启动方式： ```plaintext {wrap} ./taosx serve -c /etc/taos/taosx.toml -v ``` | ipv6地址可访问配置的6050端口 | 通过 |
|  | 配置 /etc/taos/taosx.toml listen参数，使用不同的 taosx 启动方式启动serve，验证 IPv6 地址访问。 这里启动方式使用：taosx serve 使用默认配置文件 | 配置 /etc/taos/taosx.toml 参数，验证 IPv6 地址访问 ```plaintext {wrap} listen = "172.18.0.5:6050,[2001:db9:2::6]:6050" ``` 启动方式： ```plaintext {wrap} ./taosx serve -v ``` | ipv6地址可访问配置的6050端口 | 通过 |
|  | 配置 /etc/taos/taosx.toml listen参数，使用不同的 taosx 启动方式启动serve，验证 IPv6 地址访问。 这里启动方式使用：taosx 使用默认参数 | 配置 /etc/taos/taosx.toml 参数，验证 IPv6 地址访问 ```plaintext {wrap} listen = "172.18.0.5:6050,[2001:db9:2::6]:6050" ``` 启动方式： ```plaintext {wrap} ./taosx -v ``` | 配置文件端口不生效，因为没有指定serve命令，使用的是内置默认参数 | 通过 |
|  | 配置 /etc/taos/taosx.toml listen参数，为保持兼容性，配置使用域名，验证 IPv6 地址访问。 | 配置 /etc/taos/taosx.toml 参数，验证 IPv6 地址访问 ```plaintext {wrap} listen = "taosx:6050,[::1]:6050" ``` 启动方式： ```plaintext {wrap} ./taosx -v ``` | 配置域名也可以使用ipv6地址可访问配置的6050端口 | 通过 |
|  | 启动 taosx serve 并使用默认端口，验证 IPv4 地址访问 | 可以通过 telnet 命令和通过发送 Get 请求验证是否有效 | ipv4地址可访问 | 通过 |
|  | 启动 taosX serve 并指定 --listen 为 IPv4 地址，验证 IPv4 地址访问 | 可以通过 telnet 命令和通过发送 Get 请求验证是否有效 | ipv4地址可访问 | 通过 |

### 6.2 gRPC 功能

#### 6.2.1 测试要点

1. 测试 gRPC API 是否能够通过 IPv6 地址访问
2. 测试 gRPC API 是否能够通过 IPv4 地址访问

#### 6.2.2 用例列表

| # | 测试功能 | 测试用例 | 测试描述 |  | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| 1 | 启动 taosx serve 并使用默认端口，验证 IPv6 地址访问 | 可以通过 telnet 命令和通过启动 agent 请求验证是否有效 | ipv6地址可访问 | 通过 |
|  | 启动 taosX serve 并指定 --grpc 为 IPv6 地址，验证 IPv6 地址访问 | 可以通过 telnet 命令和通过启动 agent 请求验证是否有效 | ipv6地址可访问 | 通过 |
|  | 配置 /etc/taos/taosx.toml grpc参数，使用不同的 taosx 启动方式启动serve，验证 IPv6 地址访问。 这里启动方式使用：taosx serve -c 指定配置文件 | 配置 /etc/taos/taosx.toml grpc参数，验证 IPv6 地址访问 ```plaintext {wrap} grpc = "172.18.0.5:6055,[2001:db9:2::6]:6055" ``` 启动方式： ```plaintext {wrap} ./taosx serve -c /etc/taos/taosx.toml -v ``` | ipv6地址可访问配置的6055端口 | 通过 |
|  | 配置 /etc/taos/taosx.toml grpc参数，使用不同的 taosx 启动方式启动serve，验证 IPv6 地址访问。 这里启动方式使用：taosx serve 使用默认配置文件/etc/taos/taosx.toml | 配置 /etc/taos/taosx.toml grpc参数，验证 IPv6 地址访问 ```plaintext {wrap} grpc = "172.18.0.5:6055,[2001:db9:2::6]:6055" ``` 启动方式： ```plaintext {wrap} ./taosx serve -v ``` | ipv6地址可访问配置的6055端口 | 通过 |
|  | 配置 /etc/taos/taosx.toml grpc参数，使用不同的 taosx 启动方式启动serve，验证 IPv6 地址访问。 这里启动方式使用：taosx 使用默认配置文件/etc/taos/taosx.toml | 配置 /etc/taos/taosx.toml grpc参数，验证 IPv6 地址访问 ```plaintext {wrap} grpc = "172.18.0.5:6055,[2001:db9:2::6]:6055,[::1]:6055" ``` 启动方式： ```plaintext {wrap} ./taosx -v ``` | 配置文件端口不生效，因为没有指定serve命令，使用的是内置默认参数 | 通过 |
|  | 配置 /etc/taos/taosx.toml grpc参数，为保持兼容性，配置使用域名，验证 IPv6 地址访问。 | 配置 /etc/taos/taosx.toml grpc参数，验证 IPv6 地址访问 ```plaintext {wrap} grpc = "taosx:6055,[::1]:6055" ``` 启动方式： ```plaintext {wrap} ./taosx serve -v ``` | 配置域名也可以使用ipv6地址可访问配置的6055端口 | 通过 |
|  | 启动 taosx serve 并使用默认端口，验证 IPv4 地址访问 | 可以通过 telnet 命令和通过启动 agent 请求验证是否有效 | ipv4地址可访问 | 通过 |
|  | 启动 taosX serve 并指定 --grpc 为 IPv4 地址，验证 IPv4 地址访问 | 可以通过 telnet 命令和通过启动 agent 请求验证是否有效 | ipv4地址可访问 | 通过 |


## 7. 易用性测试（可选）

无。

## 8. 长期稳定性测试（可选）

无。

## 9. 性能测试

无。

## 10. 安全测试

无。

## 11. 兼容性测试

| # | 测试功能 | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- | --- |
| 1 | REST API 提供 IPv4 地址访问 | 启动 taosx serve 并使用默认端口，验证 IPv4 地址访问 | 可以通过 telnet 命令和通过发送 Get 请求验证是否有效 | 通过 |
| 2 | gRPC API 提供 IPv4 地址访问 | 启动 taosx serve 并使用默认端口，验证 IPv4 地址访问 | 可以通过 telnet 命令和通过启动 agent 请求验证是否有效 | 通过 |


## 12. 已知问题和限制（可选）

无。
