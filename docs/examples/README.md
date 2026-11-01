# TDengine 示例程序

本目录是 TDengine 的官方示例程序，覆盖各语言连接器和常用的建库、写入、查询、订阅等场景。
官网文档中引用的示例源码都来自本目录。

## 目录说明

| 目录 | 语言 / 连接方式 | 内容 |
| --- | --- | --- |
| `c/` | C（原生连接，libtaos） | 建库、写入、查询、参数绑定、无模式写入、订阅等 |
| `c-ws/`、`c-ws-new/` | C（WebSocket） | 通过 taosAdapter 的 WebSocket 接口，`c-ws-new/` 为较新的一版 |
| `csharp/` | C# / .NET | 基于 `TDengine.Connector` 包，含原生 / WebSocket / 无模式写入 / 订阅 |
| `go/` | Go | 基于 driver-go，含原生 / WebSocket / REST |
| `JDBC/` | Java / JDBC | Maven 工程，含基础 JDBC、连接池、MyBatis-Plus、Spring Boot、多线程写入 |
| `matlab/` | MATLAB | 通过 JDBC 驱动连接 |
| `node/` | Node.js | 原生 / REST / WebSocket 三种连接方式 |
| `php/` | PHP | 通过 REST 接口 |
| `python/` | Python | 基于 taospy，含原生 / WebSocket / REST、参数绑定、无模式写入、订阅 |
| `R/` | R | 通过 RJDBC |
| `rust/` | Rust | 原生 / REST 两种连接方式 |
| `flink/` | Flink | Source / Sink 连接器示例 |
| `udf/` | C / Python | 用户自定义函数（UDF）示例 |
| `taosgen/` | — | taosgen 配置文件示例 |
| `perspective/`、`other/` | — | 与第三方工具集成的示例 |

## 运行前提

1. 已安装并启动 TDengine。
   - 原生连接默认连本机 `6030` 端口；
   - REST / WebSocket 连接需要 `taosAdapter`（默认 `6041` 端口）。
2. 按所用语言安装对应连接器：
   - Python：`pip3 install taospy`，WebSocket 连接另需 `pip3 install taos-ws-py`
   - C#：工程中引用 `TDengine.Connector` 包
   - Java：见 `JDBC/` 下各工程的 `pom.xml`
   - Go、Rust、Node.js、PHP、R：见对应目录内的工程文件
3. C 示例需要本机已安装 TDengine 客户端（提供 `taos.h` 与 `libtaos`）。在 `c/` 目录执行
   `make` 可编译其中列出的 9 个示例，其余 `.c` 文件为独立示例，可单独编译。也可以使用
   CMake：`cmake -S c -B c/build && cmake --build c/build`，若头文件或库不在默认位置，
   通过 `-DTAOS_INCLUDE_DIR=<含 taos.h 的目录> -DTAOS_LIBRARY=<libtaos 路径>` 指定。

多数示例默认以 `root` / `taosdata` 连接本机，运行前请按实际环境修改连接参数。

各接口的完整说明请参考 TDengine 官方文档的开发者指南章节。
