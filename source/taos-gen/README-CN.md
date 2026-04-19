<!-- omit in toc -->
# taosgen

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taosgen/build.yml)](https://github.com/taosdata/taosgen/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taosgen/branch/main/graph/badge.svg)](https://app.codecov.io/github/taosdata/taosgen)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taosgen)
![GitHub License](https://img.shields.io/github/license/taosdata/taosgen)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/taosgen?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

<!-- omit in toc -->
## 目录
- [1. 简介](#1-简介)
- [2. 架构](#2-架构)
- [3. 文档](#3-文档)
- [4. AI Agent 集成](#4-ai-agent-集成)
- [5. 前置条件](#5-前置条件)
  - [平台特定要求](#平台特定要求)
    - [Linux / macOS](#linux--macos)
    - [Windows](#windows)
- [6. 构建](#6-构建)
  - [Linux / macOS](#linux--macos-1)
  - [Windows](#windows-1)
    - [方式一：使用 Visual Studio 开发者命令提示符](#方式一使用-visual-studio-开发者命令提示符)
    - [方式二：使用 vcvarsall.bat](#方式二使用-vcvarsallbat)
- [7. 测试](#7-测试)
  - [7.1 运行测试](#71-运行测试)
  - [7.2 添加用例](#72-添加用例)
- [8. CI/CD](#8-cicd)
- [9. 提交 Issue](#9-提交-issue)
  - [9.1 必要信息](#91-必要信息)
  - [9.2 额外信息](#92-额外信息)
- [10. 提交 PR](#10-提交-pr)
- [11. 引用](#11-引用)
- [12. 附录](#12-附录)
  - [12.1 性能测试](#121-性能测试)
- [13. 许可证](#13-许可证)

## 1. 简介
`taosgen` 是时序数据领域产品的性能基准测试工具，支持数据生成、写入性能测试等功能。`taosgen` 以“作业”为基础单元，作业是由用户定义，用于完成特定任务的一组操作集合。每个作业包含一个或多个步骤，并可通过依赖关系与其他作业连接，形成有向无环图（DAG）式的执行流程，实现灵活高效的任务编排。

`taosgen` 目前支持 Linux、macOS 和 Windows 系统。

## 2. 架构

详细的架构内容请查阅设计文档：

- [架构设计](docs/architecture-CN.md)

快速摘要：

- `taosgen` 采用配置驱动模式：将 CLI/ENV/YAML 合并为运行时作业定义。
- 执行模型基于 DAG 作业调度，由 worker 驱动步骤执行。
- `ActionFactory` 根据步骤 `uses` + 配置映射到具体动作（DDL / insert / query / subscribe）。
- Insert 负载采用生产者-消费者流水线，结合有界队列与可插拔 sink。

如需了解设计哲学、权衡取舍、模块职责、核心时序图以及可选生命周期细节，请阅读 `docs/architecture-CN.md`。

## 3. 文档
- 使用 `taosgen` 工具，请查阅[参考手册](https://docs.taosdata.com/reference/tools/taosgen/)，其中包含运行、命令行参数、配置文件参数、配置文件示例等内容。
- 本快速指南主要面向那些喜欢自己贡献、构建和测试 `taosgen` 工具的开发者。要了解更多关于 TDengine 的信息，您可以访问[官方文档](https://docs.taosdata.com/)。

## 4. AI Agent 集成

`taosgen` 提供 AI Skill 配置，帮助 AI 智能体（如 Claude、Claude Code、Cursor 等）通过自然语言对话协助用户完成配置生成、构建编译和开发工作流。

**Skills 位置：** `.agent/skills/`

**可用 Skills：**

1. **taosgen-config** - 生成基准测试配置
   - 通过自然语言描述生成 TDengine、MQTT 和 Kafka 的 taosgen 配置文件
   - 自动验证配置并提供优化建议
   - 支持多种数据生成方式（随机、表达式、CSV 导入）
   - 配置具有依赖关系的复杂作业工作流

2. **taosgen-build** - 构建编译辅助
   - 指导用户使用 cmake 和 conan 完成构建流程
   - 诊断和解决不同平台的常见构建问题
   - 提供 IDE 集成说明（VSCode、CLion）
   - 协助测试和安装

**使用方法（以 Claude Code 为例）：**

**方式一：复制到 Claude Code 技能目录（推荐）**
```bash
mkdir -p ~/.claude/skills/
cp -r .agent/skills/taosgen-* ~/.claude/skills/

# 然后在项目目录中启动 Claude Code
claude
```

**方式二：项目本地软链接**
Claude Code 从 `.claude/skills/` 目录识别 Skill。要在本项目本地使用该 Skill：
```bash
# 在项目的 .claude 目录创建软链接
mkdir -p .claude/
ln -s ../.agent/skills .claude/

# 启动 Claude Code
claude
```


**与 Claude Code 的对话示例：**

```
"创建一个 taosgen 配置，用于测试 TDengine，模拟 10000 个设备，
 每个设备每秒上报温度和湿度，持续 1 小时"

"生成一个 MQTT 基准测试配置，模拟 1000 个 IoT 设备
 以 QoS 1 发布消息到不同主题"

"帮我创建一个 Kafka 负载测试配置，发送 500 万条消息并启用批量处理"
```

**Skill 文档：**
- [taosgen-config/SKILL.md](.agent/skills/taosgen-config/SKILL.md) - 配置生成器
- [taosgen-config/references/](.agent/skills/taosgen-config/references/) - 配置参考文档
- [taosgen-build/SKILL.md](.agent/skills/taosgen-build/SKILL.md) - 构建助手

## 5. 前置条件
首先，确保 TDengine 已本地部署。有关详细的部署步骤，请参阅[部署TDengine](https://docs.tdengine.com/get-started/deploy-from-package/)。确保 taosd 和 taosAdapter 服务均已启动并运行。

在安装和使用 `taosgen` 之前，请确保您已满足特定平台的以下前置条件。

- cmake，3.19 或以上版本，请参阅 [cmake](https://cmake.org)。
- conan，2.19 或以上版本，请参阅 [conan](https://conan.io/)。

### 平台特定要求

#### Linux / macOS
- 支持 C++17 的 GCC/Clang 编译器

#### Windows
- Visual Studio 2019 或以上版本（推荐 Visual Studio 2022）

## 6. 构建
本节提供了在 Linux、macOS 或 Windows 平台构建 `taosgen` 的详细说明。
在继续之前，请确保您位于该项目的根目录中。

>**注意：本项目使用 C++17 标准进行开发和编译。请确保您的编译器支持 C++17。**

### Linux / macOS

```shell
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

在 macOS 平台中，如果您使用的编译器无法自行选择合适的默认 SDK，那么您需要在配置构建时明确指定 CMAKE_OSX_SYSROOT，例如：
```shell
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path) -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
```

### Windows

#### 方式一：使用 Visual Studio 开发者命令提示符

从开始菜单打开 **x64 Native Tools Command Prompt for VS 2022**（或 VS 2019），然后运行：

```cmd
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release --settings=compiler=msvc --settings=compiler.version=193 --settings=compiler.cppstd=17 --settings=compiler.runtime=dynamic
cmake .. -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
cmake --build . --config Release
```

如果使用 Visual Studio 2019，请将生成器改为 `"Visual Studio 16 2019"`。

#### 方式二：使用 vcvarsall.bat

如果您更喜欢使用普通命令提示符，可以使用 `vcvarsall.bat` 脚本设置环境：

```cmd
"<VS安装路径>\VC\Auxiliary\Build\vcvarsall.bat" x64
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release --settings=compiler=msvc --settings=compiler.version=193 --settings=compiler.cppstd=17 --settings=compiler.runtime=dynamic
cmake .. -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
cmake --build . --config Release
```

请将 `<VS安装路径>` 替换为实际的 Visual Studio 安装路径，例如：
- `"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64`
- `"C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" x64`

如果使用 Visual Studio 2019，还需将生成器改为 `"Visual Studio 16 2019"`，`compiler.version` 改为 `192`。

## 7. 测试

### 7.1 运行测试
`taosgen` 测试框架使用 ctest 来运行测试用例，在构建目录中运行 `ctest` 命令将运行所有测试用例。

Linux / macOS：
```shell
cd build
ctest --output-on-failure
```

Windows（MSVC 多配置生成器需指定 `--build-config`）：
```cmd
cd build
ctest --build-config Release --output-on-failure
```

### 7.2 添加用例
测试用例位于各子模块的 test 目录中。
- 在现有测试文件中添加测试用例：测试用例函数名称以 `test_` 开头，并在 `main` 函数中调用。
- 新增测试文件：在文件内编写测试用例和 `main` 函数，并在同目录下的 `CMakeLists.txt` 文件中，添加编译控制相关配置。

## 8. CI/CD
- [Build Workflow](https://github.com/taosdata/taosgen/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/github/taosdata/taosgen)

## 9. 提交 Issue
我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taosgen/issues/new?template=Blank+issue) 。提交时，请提供以下信息以帮助我们更高效地诊断和解决问题：

### 9.1 必要信息
- 问题描述：
  提供您遇到的问题的清晰和详细描述。
  指出问题是持续发生还是间歇性发生。
  如果可能，请包括详细的调用栈或错误消息，以帮助诊断问题。

- taosgen 版本或 Commit ID
- taosgen 配置参数
- TDengine 服务器版本

### 9.2 额外信息
- 操作系统：指定操作系统及其版本。
- 重现步骤：提供说明如何重现问题，这有助于我们复现和验证问题。
- 环境配置：包括任何相关的环境配置。
- 日志：附加任何可能有助于诊断问题的相关日志。

## 10. 提交 PR
我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：
1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
2. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。注意不要直接在 main 分支上修改。
3. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
4. 提交修改到远端分支 (`git push origin my_branch`)。
5. 在 GitHub 上创建一个 Pull Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
6. 提交 PR 后，可以通过 [Pull Request](https://github.com/taosdata/taosgen/pulls) 找到自己的 PR，点击对应链接进去可以看到自己 PR CI 是否通过，如果通过会显示 “All checks have passed”。无论 CI 是否通过，都可以点击 “Show all checks” -> “Details” 来查看详细用例日志。
7. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taosgen/pulls) 页面找到自己 PR，看单测覆盖率。

## 11. 引用
- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 12. 附录
项目源代码布局，仅目录：
```
<root>
├── cmake
├── conf
└── src
    ├── actions
    │   ├── base
    │   ├── components
    │   │   ├── compressor
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── connector
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── encoding
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── expression
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── formatter
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── garbage_collector
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── generator
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── memory_pool
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── metrics
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   └── reader
    │   │       └── csv
    │   │           ├── inc
    │   │           ├── src
    │   │           └── test
    │   ├── config
    │   │   ├── inc
    │   │   ├── src
    │   │   └── test
    │   └── core
    │       ├── checkpoint
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── create
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── insert
    │       │   ├── inc
    │       │   ├── src
    │       │   │   ├── generator
    │       │   │   │   ├── inc
    │       │   │   │   ├── src
    │       │   │   │   └── test
    │       │   │   ├── pipeline
    │       │   │   │   ├── inc
    │       │   │   │   ├── src
    │       │   │   │   └── test
    │       │   │   └── writer
    │       │   │       ├── inc
    │       │   │       ├── src
    │       │   │       └── test
    │       │   └── test
    │       ├── query
    │       │   └── inc
    │       └── subscribe
    │           ├── inc
    │           └── src
    ├── engine
    │   ├── inc
    │   ├── src
    │   └── test
    ├── parameter
    │   ├── conf
    │   ├── inc
    │   ├── src
    │   └── test
    ├── plugins
    │   ├── inc
    │   └── src
    │       ├── kafka
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── mqtt
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       └── tdengine
    │           ├── inc
    │           ├── src
    │           └── test
    ├── utils
    │   ├── inc
    │   ├── src
    │   └── test
    └── workflow
        ├── inc
        └── src
```

### 12.1 性能测试

- 环境（客户端与服务端一致）：

  | 组件 | 规格 |
  |---|---|
  | 操作系统 | Ubuntu 20.04.6 LTS |
  | CPU | Intel Xeon E5-2650 v3 @ 2.30GHz（Haswell-EP），双路 |
  | 核心/线程 | 20C/40T（每路 10C/20T，超线程） |
  | 缓存 | L3 25MB（cache size: 25600 KB） |
  | 内存 | 251 GB |
  | 存储 | 447 GB SSD × 2，1.76 TB SSD |
  | 软件 | TDengine Enterprise 3.3.8.9（默认）；FlashMQ v1.24.0（默认）；Kafka 2.13-4.1.0（默认） |

- 数据模型：100万子表 meters，电流/电压/相位三列；按 interlace=1 交错写入。
- 结果为示范性数据，实际性能受网络/服务器配置/消息大小/并发度影响。
- 单位：K=千条/秒，M=百万条/秒。

| 目标 | 场景 | 基线 | taosgen | 配置摘要 | 提升 |
|---|---|---:|---:|---|---:|
| TDengine | 1亿行、20线程 | 3.168M rps（taosBenchmark） | 3.534M rps | vgroups=32、stmt2、batch=10k | +11.58% |
| MQTT | 200万行、20线程、单条/消息 | — | 15.15K rps | qos=0、records_per_message=1 | — |
| MQTT | 1亿行、20线程、打包500条/消息 | — | 3.127M rps | qos=0、records_per_message=500 | 显著提升 |
| Kafka（单线程） | 1亿行、官方脚本 | 912.70K rps | 968.93K rps | acks=0、batch优化 | +6.16% |
| Kafka（20并发） | 官方脚本 20 进程 | 2.772M rps | 4.577M rps | taosgen 20 线程 | +65.14% |

说明：
- MQTT 在 QoS0 下，打包发送可显著提高吞吐；Broker 配置与消息大小对结果影响极大。
- TDengine 对标 taosBenchmark，taosgen 在等价模型下具备更高吞吐与更低框架开销。
- Kafka 对标官方脚本，单线程与多并发场景下 taosgen 均有优势。

## 13. 许可证
[MIT License](./LICENSE)
