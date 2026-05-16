<!-- omit in toc -->
# TDengine ODBC Connector

![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-odbc)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-odbc)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/taos-connector-odbc?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

简体中文 | [English](./README.md)

<!-- omit in toc -->
## 目录
- [1. 简介](#1-简介)
- [2. 文档](#2-文档)
- [3. 前置条件](#3-前置条件)
  - [3.1 Windows](#31-windows)
  - [3.2 Linux](#32-linux)
  - [3.3 macOS](#33-macos)
- [4. 构建和安装](#4-构建和安装)
  - [4.1 Windows](#41-windows)
  - [4.2 Linux/macOS](#42-linuxmacos)
- [5. 测试](#5-测试)
  - [5.1 运行测试](#51-运行测试)
    - [5.1.1 Windows](#511-windows)
    - [5.1.2 Linux/macOS](#512-linuxmacos)
  - [5.2 添加用例](#52-添加用例)
  - [5.3 性能测试](#53-性能测试)
- [6. CI/CD](#6-cicd)
- [7. 提交 Issue](#7-提交-issue)
  - [7.1 必要信息](#71-必要信息)
  - [7.2 额外信息](#72-额外信息)
- [8. 提交 PR](#8-提交-pr)
- [9. 引用](#9-引用)
- [10. 附录](#10-附录)
- [11. 许可证](#11-许可证)


## 1. 简介
`taos-connector-odbc` 是 TDengine 的官方 ODBC 连接器，使得用各种编程语言如 C/C++、C#、Go、Rust、Python、Node.js 等编写的应用程序能够与 TDengine 数据库进行交互。通过利用标准化的 ODBC 接口，`taos-connector-odbc` 促进了在不同平台和环境间无缝地进行数据写入、查询和参数绑定。

`taos-connector-odbc` 支持多个操作系统，包括 Windows、Linux 和 macOS。

## 2. 文档
- 使用 TDengine ODBC 连接器，请查阅[参考手册](https://docs.tdengine.com/tdengine-reference/client-libraries/odbc/)，其中包含版本历史、数据类型、示例程序、API 描述和常见问题解答。
- 本快速指南主要面向那些喜欢自己贡献、构建和测试 ODBC 连接器的开发者。要了解更多关于 TDengine 的信息，您可以访问[官方文档](https://docs.taosdata.com/)。
- TDengine ODBC 连接器遵循 ODBC 标准，确保在各种数据库系统之间的兼容性和互操作性。有关 ODBC 标准和规范的更详细信息，请参阅[Microsoft Open Database Connectivity (ODBC)](https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc)文档。此资源提供了对 ODBC 接口、方法及其他相关内容的全面见解，有助于加深您对 TDengine ODBC 连接器如何在更广泛的 ODBC 应用程序中运作的理解。

## 3. 前置条件
首先，确保 TDengine 已本地部署。有关详细的部署步骤，请参阅[部署TDengine](https://docs.tdengine.com/get-started/deploy-from-package/)。确保 taosd 和 taosAdapter 服务均已启动并运行。

之后，在安装和使用 `taos-connector-odbc` 之前，请确保您已满足特定平台的以下前置条件。

- cmake，3.0 或以上版本，请参阅 [cmake](https://cmake.org/)。
- flex，2.6.4 或以上版本。注意：在 Windows 平台上需要安装 win_flex_bison。
- bison，3.5.1 或以上版本。注意：在 Windows 平台上需要安装 win_flex_bison。
- odbc 驱动管理器，例如 Linux 中的 unixodbc（2.3.6 或以上版本）。注意：ODBC 驱动管理器在 Windows 平台上预装。
- iconv，通常已包含在 libc 中。注意：构建此项目时会自动下载 win_iconv。

如果您希望启用 valgrind 内存检查（在 cmake 配置期间启用 `ENABLE_VALGRIND_TEST` 选项），或者其他编程语言的测试用例，请参考以下内容。

| 工具或语言         | 版本要求                                   | 相关库和版本                                                                                             | 参考链接                                                                       |
|-------------------|-------------------------------------------|---------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| valgrind          | v3.20 或以上                              |                                                                                                          | [valgrind](https://valgrind.org/)                                             |
| node.js           | v12.0 或以上                              | node-odbc ，2.4.4 或以上                                                                                  | [node-odbc](https://www.npmjs.com/package/odbc/)                              |
| rust              | v1.63 或以上                              | odbc，0.17.0 或以上<br>env_logger，0.8.2 或以上<br>json                                                    | [rust-odbc](https://docs.rs/odbc/latest/odbc/)<br>[env_logger](https://docs.rs/env_logger/latest/env_logger/)<br>[json](https://docs.rs/json/latest/json/) |
| python            | v3.10 或以上                              | pyodbc ，4.0.39 或以上                                                                                    | [python-odbc](https://pypi.org/project/pyodbc/)                               |
| go                | v1.17 或以上                              | odbc                                                                                                     | [go-odbc](https://github.com/alexbrainman/odbc)                               |
| erlang            | v12.2 或以上                              | odbc                                                                                                     | [erlang-odbc](https://www.erlang.org/doc/apps/odbc/getting_started.html)      |
| haskell           | cabal v3.6 或以上，ghc v9.2 或以上         | hsql-odbc                                                                                                | [haskell-odbc](https://hackage.haskell.org/package/hsql-odbc)                 |
| common lisp       | sbcl v2.1.11 或以上                       | plain-odbc                                                                                               | [common-lisp-odbc](https://plain-odbc.common-lisp.dev/)                       |
| R                 | v4.3 或以上                               | odbc, DBI, assert                                                                                        | [R-odbc](https://cran.r-project.org/web/packages/odbc/index.html)             |

在本指南的以下部分中，我们将使用以下版本作为示例：Windows 11（用于 Windows 平台），Ubuntu 20.04（用于 Linux 平台），以及 macOS Big Sur（用于 macOS 平台）。

### 3.1 Windows
- 安装 win_flex_bison ：
  - 下载地址： [win_flex_bison](https://github.com/lexxmark/winflexbison/releases/) 。
  - 解压文件并将目录添加到系统的 PATH 环境变量中。
- 验证安装：
  ```
  win_flex --version
  ```
- 安装 ODBC Driver Manager：
确保系统上已安装 Microsoft ODBC Driver Manager 。它通常在 Windows 平台上预装。

### 3.2 Linux
- 安装所需的依赖项，包括 ODBC 驱动管理器：
  ```
  sudo apt install flex bison unixodbc unixodbc-dev odbcinst && echo -=Done=-
  ```

### 3.3 macOS
- 安装所需的依赖项，包括 ODBC 驱动管理器：
  ```
  brew install flex bison unixodbc && echo -=Done=-
  ```

## 4. 构建和安装
本节提供了在不同平台上构建和安装 ODBC 连接器的详细说明。
在继续之前，请确保您位于该项目的根目录中。

### 4.1 Windows
- 可选地，设置构建环境：如果您在 64 位 Windows 平台上安装了 Visual Studio Community 2022 ，请运行以下命令来设置构建环境：
  ```
  "\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
  ```
- 生成 make 文件：
  ```
  cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64
  ```
  **故障排除**：如果在以下步骤中发生编译器错误，例如 <path_to_winbase.h> 警告 C5105：宏扩展产生 'defined' 具有未定义行为，请使用以下命令重试：
  ```
  cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64 -DDISABLE_C5105:BOOL=ON
  ```
- 构建项目：
  ```
  cmake --build build --config Debug -j 4
  ```
- 安装连接器：
  这将默认把 taos_odbc.dll 安装到 `C:\TDengine\taos_odbc\x64\`。
  ```
  cmake --install build --config Debug
  cmake --build build --config Debug --target install_templates
  ```
- 验证安装:
  检查 Windows 注册表中是否已设置新的 TAOS_ODBC_DSN 注册表项：
  ```
  HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\TDengine
  HKEY_CURRENT_USER\Software\ODBC\Odbc.ini\TAOS_ODBC_DSN
  ```
### 4.2 Linux/macOS
- 生成 make 文件：
  ```
  cmake -B debug -DCMAKE_BUILD_TYPE=Debug
  ```
- 构建项目：
  ```
  cmake --build debug
  ```
- 安装连接器：
  ```
  sudo cmake --install debug
  cmake --build debug --target install_templates
  ```
- 验证安装:
  检查 ODBC DSN 配置文件（例如， `/etc/odbc.ini` 或 `~/.odbc.ini` ）是否包含 TAOS_ODBC_DSN 条目。

## 5. 测试
### 5.1 运行测试
ODBC 连接器测试框架使用 ctest 来运行测试用例。测试用例位于项目根目录的 tests 目录中。

#### 5.1.1 Windows
- 设置测试环境变量：
  设置以下环境变量以配置日志级别和目标：
  ```
  set TAOS_ODBC_LOG_LEVEL=ERROR
  set TAOS_ODBC_LOGGER=stderr
  ```
  可用的日志级别有：VERBOSE、DEBUG、INFO、WARN、ERROR、FATAL。较低的级别提供更详细的日志。
  可用的日志记录器有：
  - stderr：记录到标准错误。
  - temp：记录到临时目录中的文件（`taos_odbc.log`）。

- 运行测试:
  ```
  ctest --test-dir build --output-on-failure -C Debug
  ```

#### 5.1.2 Linux/macOS
- 设置测试环境变量：
  设置以下环境变量来配置日志级别和目标：
  ```
  export TAOS_ODBC_LOG_LEVEL=ERROR
  export TAOS_ODBC_LOGGER=stderr
  ```
  可用的日志级别有：VERBOSE、DEBUG、INFO、WARN、ERROR、FATAL。较低级别提供更详细的日志。

  可用的日志记录器有：
  - stderr：记录到标准错误。
  - temp：记录到临时目录中的文件（`taos_odbc.log`）。
  - syslog：记录到系统日志（仅在 Linux 平台上可用；在 macOS 上不支持）。

- 运行测试:
  ```
  pushd debug >/dev/null && ctest --output-on-failure && echo -=Done=-; popd >/dev/null
  ```

### 5.2 添加用例
所有测试位于项目 `./tests/` 目录中。此目录包含各种语言和类型测试的子目录。基本功能验证测试用例是用 C 语言编写的，位于 `c` 和 `taos` 文件夹中。
您可以添加新的测试文件或在现有的测试文件中添加符合测试框架标准的测试用例。
对于 C/C++ 测试，确保它们遵循 CTest 框架规范。
将新的测试文件放置在 tests 目录中的适当子目录内。


### 5.3 性能测试
性能测试还在开发中。

## 6. CI/CD
- [Build Workflow] -TODO
- [Code Coverage] -TODO

## 7. 提交 Issue
我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taos-connector-odbc/issues/new?template=Blank+issue) 。提交时，请提供以下信息以帮助我们更高效地诊断和解决问题：


### 7.1 必要信息
- 问题描述：
  提供您遇到的问题的清晰和详细描述。
  指出问题是持续发生还是间歇性发生。
  如果可能，请包括详细的调用栈或错误消息，以帮助诊断问题。

- ODBC 连接器版本：
  指定您使用的 ODBC 连接器的版本。

- ODBC 连接参数：
  提供您的 ODBC 连接字符串或 DSN（数据源名称）配置详情。请勿包含敏感信息，如用户名和密码。
  示例：
  ```
  DSN=TAOS_ODBC_DSN;UID=<your_username>;PWD=<your_password>
  ```
  出于安全原因，请将 <your_username> 和 <your_password> 替换为占位符或省略它们。
- TDengine 服务器版本：
  指定您连接到的 TDengine 服务器的版本。

### 7.2 额外信息
- 操作系统：指定操作系统及其版本。
- 重现步骤：提供说明如何重现问题。这有助于我们复现和验证问题。
- 环境配置：包括任何相关的环境配置，例如 odbc.ini 、 odbcinst.ini 或其他配置文件中的特定设置。
- 日志：附加任何可能有助于诊断问题的相关日志。日志可以在您的日志配置指定的位置找到（例如 stderr 、 temp 或 syslog）。

## 8. 提交 PR
我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：
1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
2. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。注意不要直接在 main 分支上修改。
3. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
4. 提交修改到远端分支 (`git push origin my_branch`)。
5. 在 GitHub 上创建一个 Pull Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
6. 提交 PR 后，可以通过 [Pull Request](https://github.com/taosdata/taos-connector-odbc/pulls) 找到自己的 PR，点击对应链接进去可以看到自己 PR CI 是否通过，如果通过会显示 “All checks have passed”。无论 CI 是否通过，都可以点击 “Show all checks” -> “Details” 来查看详细用例日志。
7. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taos-connector-odbc/pulls) 页面找到自己 PR，看单测覆盖率。

## 9. 引用
- [TDengine Official Website](https://www.tdengine.com/) 
- [TDengine GitHub](https://github.com/taosdata/TDengine) 
- [Microsoft Introduction to ODBC](https://learn.microsoft.com/en-us/sql/odbc/reference/introduction-to-odbc)
- [Microsoft ODBC API Reference](https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference)

## 10. 附录
项目源代码布局，仅目录：
```
<root>
├── benchmark
├── cmake
├── common
├── inc
├── samples
│   └── c
├── sh
├── src
│   ├── core
│   ├── inc
│   ├── os_port
│   ├── parser
│   ├── tests
│   └── utils
├── templates
├── tests
│   ├── c
│   ├── cpp
│   ├── cs
│   ├── erl
│   ├── go
│   ├── hs
│   │   └── app
│   ├── lisp
│   ├── node
│   ├── python
│   ├── R
│   ├── rust
│   │   └── main
│   │       └── src
│   ├── sh
│   └── taos
└── valgrind
```

## 11. 许可证
[MIT License](./LICENSE)

<!-- omit in toc -->
## 由 freemine@yeah.net 初创
