# taosgen 支持优先读取程序目录下的 libtaos - FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-26 | YYYY-MM-DD | 0.1 | 裴亚明 | 初始版本 |

## 2. 背景

在实际使用中，安装 IDMP 的服务器可能已经安装了 TDengine 服务端，而安装 IDMP 会安装 taosgen 和其依赖的 libtaos 库，该库可能与原有的 TDengine libtaos 库产生冲突或不兼容。
taosgen 需要支持在同一服务器加载不同的 libtaos 库，优先加载程序目录下的 libtaos 库，支持多版本共存。

## 3. 定义

libtaos 库：指 TDengine C连接器客户端库文件

## 4. 行为说明

1. taosgen 程序在创建 TDengine 客户端时，优先加载程序所在路径下子文件夹 lib 中的 libtaos 库，指 libtaos 的全路径指明位置。
2. 如果上述加载失败回退到根据文件名称依赖系统环境搜索路径加载 libtaos 库。
3. 上述加载库的操作仅在首次创建 TDengine 客户端实例时执行。

## 5. 性能

无

## 6. 安全

不涉及

## 7. 兼容性

兼容旧加载行为，保持一致。

## 8. 运维

在集成 taosgen 产品时，根据需要可选择为其准备程序所在路径的子目录 `lib`下 libtaos 库，运行时支持优先加载该库，而非系统环境中的 libtaos 库。

## 9. 使用场景

IDMP 产品中部署 taosgen 程序，假设安装路径为 TAOSGEN_INSTALL_PATH
1. 将 taosgen 程序复制到 TAOSGEN_INSTALL_PATH；
2. 将 libtaos 库复制到 TAOSGEN_INSTALL_PATH/lib/；
3. 可选的，将 taosgen 样例配置文件复制到 TAOSGEN_INSTALL_PATH/conf/；
4. 运行 taosgen，将加载程序子目录 lib 中的 libtaos 库；
5. 卸载时，删除 TAOSGEN_INSTALL_PATH 目录即可；

## 10. 约束和限制

约束：由于 libtaos 库的限制，不支持混合使用 Native 和 WebSocket
限制：不支持运行时热切换 libtaos 库，加载后无法在进程生命周期内更换库

## 11. 常见错误和排查

提示 "Failed to load libtaos shared library"：确保将 libtaos 库及其依赖库在系统环境中或程序子目录 lib 中。

## 12. 可观测性

通过打印日志查看 libtaos 库加载情况：
1. 加载程序目录 libtaos 库：`Loaded libtaos from program directory:xxx `
2. 加载系统环境 libtaos 库：`Loaded libtaos from system path: xxx`

## 13. 安装和卸载

跟随 TDengine TSDB Server/Client 或 IDMP 安装或卸载。

## 14. 文档

为 IDMP 定制开发的需求，无需修改官网文档。

## 15. 参考文档

## 16. 附录

无
