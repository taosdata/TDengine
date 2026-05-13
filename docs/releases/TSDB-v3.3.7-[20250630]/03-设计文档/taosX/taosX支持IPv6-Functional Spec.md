# taosX支持IPv6-Functional Spec

## 1. 背景

IPv6 网络协议栈当前大部分网络设备和操作系统都已经支持，甚至部分客户的内网环境已经强制要求项目支持 IPv6，而不再提供 IPv4 的服务器、网络资源。
为满足客户的场景需求以及丰富 taosX 的服务能力，支持 IPv6 目前已成为必要功能。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/22 | 0.1 | 张贵川 | 文档撰写 |

## 3. 定义

**IP 协议**：是为网络上的计算机提供识别和定位系统，并在互联网上传输流量的通信协议。
**IPv6**：新一代互联网地址协议，用于替代 IPv4。
**REST API：**一种基于 HTTP 协议强调资源操作的 API 设计风格。
**端口**：网络通信中用于区分同一设备上不同服务的逻辑编号（范围 0-65535）。
**gRPC**：由 Google 开发的高性能、开源、通用的远程过程调用（RPC）框架，专为分布式系统和微服务架构设计。

## 4. 行为说明

taosX 服务启动方式没有变化，参数使用方式没有变化。在 taosX 服务模式下，不传入任何 IP 地址相关参数时候，taosX 会自动检测是否支持 IPv6，如果不支持，会切换回 IPv4 协议。
主要变化内容有两处：
1. 在用户需要将 taosX 服务绑定用户指定的 IPv6 地址时，参数内容需要写为 IPv6 地址形式，示例如下。
REST API 服务使用用例：
```plaintext {wrap}
./taosx serve -l [::]:6050
```

gRPC 服务使用用例：
```plaintext {wrap}
./taosx serve -g [::]:6055
```

1. 支持多地址形式，分隔符号使用英文逗号,分割
   - 命令行方式支持多地址形式
  ```plaintext {wrap}
  ./taosx serve -l 172.18.0.5:6050,[2001:db9:2::6]:6050
  ./taosx serve -g 172.18.0.5:6055,[2001:db9:2::6]:6055
  ./taosx serve -l 172.18.0.5:6050,[2001:db9:2::6]:6050 -g 172.18.0.5:6055,[2001:db9:2::6]:6055
  ```

   - 配置文件
```plaintext {wrap}
listen = "172.18.0.5:6050,[2001:db9:2::6]:6050"

grpc = "172.18.0.5:6055,[2001:db9:2::6]:6055"
```

## 5. 性能

无。

## 6. 兼容性

- REST API 服务：兼容历史所有版本 IPv4 访问方式
- gRPC 服务：兼容历史所有版本 IPv4 访问方式

## 7. 运维

无。

## 8. 使用场景

1. taosExplorer 请求 taosX 相关REST API
2. taosx-agent 与 taosX 通信
3. 相关 plugins 与 taosX 通信

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

该组件随 TDengine 产品安装包一同发布，随 TDengine 安装和卸载。

## 13. 文档

需要输出企业版帮助文档以下内容：
1. 参考手册 - 产品组件 - taosX 组件介绍、配置、使用等。

## 14. 参考文档

- IPv6协议：https://en.wikipedia.org/wiki/IPv6
- Arrow Flight: https://arrow.apache.org/docs/format/Flight.html#rpc-methods-and-request-patterns
- tonic: https://github.com/hyperium/tonic?tab=readme-ov-file

## 15. 附录

无。
