---
name: tsdb-dev-stmt2
description: "生成 TDengine STMT2 参数绑定代码（C/C++）。适用场景：用户需要使用 taos_stmt2_* API 进行高性能批量写入或参数化查询，涉及超级表、子表、多表批量绑定、同步/异步执行、交织写入等。触发关键词：stmt2、参数绑定、批量写入、交织写入、interlace、taos_stmt2_bind_param、TAOS_STMT2_BIND、TAOS_STMT2_BINDV、parameter binding、batch insert。"
metadata:
  author: Mario Peng
  version: 1.0.0
  owner_team: engine
---

# TDengine STMT2 参数绑定

## When to Use

- 用户需要使用 TDengine C/C++ STMT2 API（`taos_stmt2_*`）进行**参数化 INSERT 或 SELECT**
- 需要对**超级表多子表批量写入**（一次 bind 多张子表）
- 需要**异步执行**（`asyncExecFn` 回调）
- 用户提到 `taos_stmt2_bind_param`、`TAOS_STMT2_BIND`、`TAOS_STMT2_BINDV` 等关键词
- 需要在写入前通过 `taos_stmt2_get_fields` 获取字段元信息

## Prerequisites

- 已安装 TDengine 客户端库（`libtaos.so` / `libtaos.dylib`）并可链接 `-ltaos`
- 头文件 `taos.h` 可用
- TDengine 服务端已启动，连接信息（host/user/password/port）已知

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-stmt2 version=1.0.0 author=Mario Peng`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
