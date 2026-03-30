# taosgen 支持优先读取程序目录下的 libtaos - RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-26 | YYYY-MM-DD | 0.1 | 裴亚明 | 初始版本 |

## 2. 引言

### 2.1 术语与缩写名词

libtaos 库：指 TDengine C连接器客户端文件

### 2.2 相关文档资料

[IDMP 安装包调整方案](https://taosdata.feishu.cn/wiki/CUYYwII1gi6Qwuk2268cmdgxnsi)

### 2.3 优先级要求

优先级：高

### 2.4 版本要求

企业版和社区版都支持

## 3. 需求目标

在实际使用中，安装 IDMP 的服务器可能已经安装了 TDengine 服务端，而安装 IDMP 会安装 taosgen 和其依赖的 libtaos 库，该库可能与原有的 TDengine libtaos 库产生冲突或不兼容。
taosgen 需要支持在同一服务器加载不同的 libtaos 库，优先加载程序目录下的 libtaos 库，支持多版本共存。

## 4. 功能需求

1. taosgen 程序在创建 TDengine 客户端时，优先加载程序所在路径下子文件夹 lib 中的 libtaos 库，指 libtaos 的全路径指明位置。
2. 如果上述加载失败回退到根据文件名称依赖系统环境搜索路径加载 libtaos 库。

## 5. 性能需求

无

## 6. 安全需求

不涉及

## 7. 其他需求

无
