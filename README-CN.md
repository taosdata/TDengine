<p align="center">
  <a href="https://www.taosdata.com" target="_blank">
  <img
    src="docs/assets/tdengine.svg"
    alt="TDengine"
    width="500"
  />
  </a>
</p>

简体中文 | [English](README.md) | [TDengine 云服务](https://cloud.taosdata.com/?utm_medium=cn&utm_source=github) | 很多职位正在热招中，请看 [这里](https://www.taosdata.com/careers/)

# 目录

1. [TDengine 简介](#1-tdengine-简介)
1. [文档](#2-文档)
1. [必备工具](#3-必备工具)
    - [3.1 Linux 预备](#31-Linux系统)
    - [3.2 macOS 预备](#32-macOS系统)
    - [3.3 Windows 预备](#3.3-Windows系统) 
    - [3.4 克隆仓库](#34-克隆仓库) 
1. [构建](#4-构建)
    - [4.1 Linux 系统上构建](#41-Linux系统上构建)
    - [4.2 macOS 系统上构建](#42-macOS系统上构建)
    - [4.3 Windows 系统上构建](#43-Windows系统上构建) 
1. [打包](#5-打包)
1. [安装](#6-安装)
    - [6.1 Linux 系统上安装](#61-Linux系统上安装)
    - [6.2 macOS 系统上安装](#62-macOS系统上安装)
    - [6.3 Windows 系统上安装](#63-Windows系统上安装)
1. [快速运行](#7-快速运行)
    - [7.1 Linux 系统上运行](#71-Linux系统上运行)
    - [7.2 macOS 系统上运行](#72-macOS系统上运行)
    - [7.3 Windows 系统上运行](#73-Windows系统上运行)
1. [测试](#8-测试)
1. [版本发布](#9-版本发布)
1. [工作流](#10-工作流)
1. [覆盖率](#11-覆盖率)
1. [成为社区贡献者](#12-成为社区贡献者)


# 1. 简介

TDengine 是一款开源、高性能、云原生、AI 驱动的时序数据库 (Time-Series Database, TSDB)。TDengine 能被广泛运用于物联网、工业互联网、车联网、IT 运维、金融等领域。除核心的时序数据库功能外，TDengine 还提供缓存、数据订阅、流式计算、AI 智能体等功能，是一极简的时序数据处理平台，最大程度的减小系统设计的复杂度，降低研发和运营成本。与其他时序数据库相比，TDengine 的主要优势如下：

- **高性能**：通过创新的存储引擎设计，无论是数据写入还是查询，TDengine 的性能比通用数据库快 10 倍以上，也远超其他时序数据库，存储空间不及通用数据库的 1/10。

- **云原生**：通过原生分布式的设计，充分利用云平台的优势，TDengine 提供了水平扩展能力，具备弹性、韧性和可观测性，支持 k8s 部署，可运行在公有云、私有云和混合云上。

- **极简时序数据平台**：TDengine 内建消息队列、缓存、流式计算、AI 智能体等功能，应用无需再集成 Kafka/Redis/HBase/Spark 等软件，大幅降低系统的复杂度，降低应用开发和运营成本。

- **分析能力**：支持 SQL，同时为时序数据特有的分析提供 SQL 扩展。通过超级表、存储计算分离、分区分片、预计算、自定义函数以及 AI Agent 等技术，TDengine 具备强大的分析能力。

- **AI智能体**：内置时序数据智能体 TDgpt, 无缝连接时序数据基础模型、大语言模型、机器学习、传统统计算法等，提供时序数据预测、异常检测、数据补全和数据分类的功能。

- **简单易用**：无任何依赖，安装、集群几秒搞定；提供 REST 以及各种语言连接器，与众多第三方工具无缝集成；提供命令行程序，便于管理和即席查询；提供各种运维工具。

- **核心开源**：TDengine 的核心代码包括集群功能全部开源，截止到 2022 年 8 月 1 日，全球超过 135.9k 个运行实例，GitHub Star 18.7k，Fork 4.4k，社区活跃。

了解TDengine高级功能的完整列表，请 [点击](https://tdengine.com/tdengine/)。体验 TDengine 最简单的方式是通过 [TDengine云平台](https://cloud.tdengine.com)。对最新发布的 TDengine 组件 TDgpt，请访问 [TDgpt README](./tools/tdgpt/README.md) 了解细节。

# 2. 文档

关于完整的使用手册，系统架构和更多细节，请参考 [TDengine](https://www.taosdata.com/) 或者 [TDengine 官方文档](https://docs.taosdata.com)。

用户可根据需求选择通过 [容器](https://docs.taosdata.com/get-started/docker/)、[安装包](https://docs.taosdata.com/get-started/package/)、[Kubernetes](https://docs.taosdata.com/deployment/k8s/) 来安装或直接使用无需安装部署的 [云服务](https://cloud.taosdata.com/)。本快速指南是面向想自己编译、打包、测试的开发者的。
  
如果想编译或测试 TDengine 连接器，请访问以下仓库：[JDBC连接器](https://github.com/taosdata/taos-connector-jdbc)、[Go连接器](https://github.com/taosdata/driver-go)、[Python连接器](https://github.com/taosdata/taos-connector-python)、[Node.js连接器](https://github.com/taosdata/taos-connector-node)、[C#连接器](https://github.com/taosdata/taos-connector-dotnet)、[Rust连接器](https://github.com/taosdata/taos-connector-rust)。

# 3. 前置条件

TDengine 目前可以在 Linux 和 macOS 平台上安装和运行 (企业版支持 Windows)。任何 OS 的应用也可以选择 taosAdapter 的 RESTful 接口连接服务端 taosd。CPU 支持 X64、ARM64，后续会支持 MIPS64、Alpha64、ARM32、RISC-V 等 CPU 架构。目前不支持使用交叉编译器构建。

如果你想要编译 taosAdapter 或者 taosKeeper，需要安装 Go 1.23 及以上版本。

## 3.1 Linux 系统

<details>

<summary>安装 Linux 必备工具</summary>

### Ubuntu 18.04、20.04、22.04

```bash
sudo apt-get update
sudo apt-get install -y gcc cmake build-essential git libjansson-dev \
  libsnappy-dev liblzma-dev zlib1g-dev pkg-config
```

### CentOS 8

```bash
sudo yum update
yum install -y epel-release gcc gcc-c++ make cmake git perl dnf-plugins-core 
yum config-manager --set-enabled powertools
yum install -y zlib-static xz-devel snappy-devel jansson-devel pkgconfig libatomic-static libstdc++-static 
```

</details>

## 3.2 macOS 系统

<details>

<summary>安装 macOS 必备工具</summary>

根据提示安装依赖工具 [brew](https://brew.sh/)

```bash
brew install argp-standalone gflags pkgconfig
```

</details>

## 3.3 Windows 系统

<details>

<summary>安装 Windows 必备工具</summary>

进行中。

</details>

## 3.4 克隆仓库

通过如下命令将 TDengine 仓库克隆到指定计算机：

```bash
git clone https://github.com/taosdata/TDengine.git
cd TDengine
```

# 4. 构建

TDengine 还提供一组辅助工具软件 taosTools，目前它包含 taosBenchmark（曾命名为 taosdemo）和 taosdump 两个软件。默认 TDengine 编译不包含 taosTools，您可以在编译 TDengine 时使用 `cmake .. -DBUILD_TOOLS=true` 来同时编译 taosTools。

为了构建 TDengine，请使用 [CMake](https://cmake.org/) 3.13.0 或者更高版本。

## 4.1 Linux 系统上构建

<details>

<summary>Linux 系统上构建步骤</summary>

可以通过以下命令使用脚本 `build.sh` 编译 TDengine 和 taosTools，包括 taosBenchmark 和 taosdump。

```bash
./build.sh
```

也可以通过以下命令进行构建：

```bash
mkdir debug && cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_CONTRIB=true
make
```

如果你想要编译 taosAdapter，需要添加 `-DBUILD_HTTP=false` 选项。

如果你想要编译 taosKeeper，需要添加 `-DBUILD_KEEPER=true` 选项。

可以使用 Jemalloc 作为内存分配器，而不是使用 glibc：

```bash
cmake .. -DJEMALLOC_ENABLED=ON
```
TDengine 构建脚本可以自动检测 x86、x86-64、arm64 平台上主机的体系结构。
您也可以通过 CPUTYPE 选项手动指定架构：

```bash
cmake .. -DCPUTYPE=aarch64 && cmake --build .
```

</details>

## 4.2 macOS 系统上构建

<details>

<summary>macOS 系统上构建步骤</summary>

请安装 XCode 命令行工具和 cmake。使用 XCode 11.4+ 在 Catalina 和 Big Sur 上完成验证。

```shell
mkdir debug && cd debug
cmake .. && cmake --build .
```

如果你想要编译 taosAdapter，需要添加 `-DBUILD_HTTP=false` 选项。

如果你想要编译 taosKeeper，需要添加 `-DBUILD_KEEPER=true` 选项。

</details>

## 4.3 Windows 系统上构建

<details>

<summary>Windows 系统上构建步骤</summary>

如果您使用的是 Visual Studio 2013，请执行 “cmd.exe” 打开命令窗口执行如下命令。
执行 vcvarsall.bat 时，64 位的 Windows 请指定 “amd64”，32 位的 Windows 请指定 “x86”。

```cmd
mkdir debug && cd debug
"C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" < amd64 | x86 >
cmake .. -G "NMake Makefiles"
nmake
```

如果您使用 Visual Studio 2019 或 2017：

请执行 “cmd.exe” 打开命令窗口执行如下命令。
执行 vcvarsall.bat 时，64 位的 Windows 请指定 “x64”，32 位的 Windows 请指定 “x86”。

```cmd
mkdir debug && cd debug
"C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" < x64 | x86 >
cmake .. -G "NMake Makefiles"
nmake
```

或者，您可以通过点击 Windows 开始菜单打开命令窗口 -> `Visual Studio < 2019 | 2017 >` 文件夹 -> `x64 原生工具命令提示符 VS < 2019 | 2017 >` 或 `x86 原生工具命令提示符  < 2019 | 2017 >` 取决于你的 Windows 是什么架构，然后执行命令如下：

```cmd
mkdir debug && cd debug
cmake .. -G "NMake Makefiles"
nmake
```
</details>

# 5. 打包

由于一些组件依赖关系，TDengine 社区安装程序不能仅由该存储库创建。我们仍在努力改进。

# 6. 安装


## 6.1 Linux 系统上安装

<details>

<summary>Linux 系统上安装详细步骤</summary>

构建成功后，TDengine 可以通过以下命令进行安装：

```bash
sudo make install
```
从源代码安装还将为 TDengine 配置服务管理。用户也可以使用 [TDengine安装包](https://docs.taosdata.com/get-started/package/)进行安装。 

</details>

## 6.2 macOS 系统上安装

<details>

<summary>macOS 系统上安装详细步骤</summary>

构建成功后，TDengine可以通过以下命令进行安装：

```bash
sudo make install
```

</details>

## 6.3 Windows 系统上安装

<details>

<summary>Windows 系统上安装详细步骤</summary>

构建成功后，TDengine 可以通过以下命令进行安装：

```cmd
nmake install
```

</details>

# 7. 快速运行

## 7.1 Linux 系统上运行

<details>

<summary>Linux 系统上运行详细步骤</summary>

在Linux 系统上安装 TDengine 完成后，在终端运行如下命令启动服务：

```bash
sudo systemctl start taosd
```
然后用户可以通过如下命令使用 TDengine 命令行连接 TDengine 服务：

```bash
taos
```

如果 TDengine 命令行连接服务器成功，系统将打印欢迎信息和版本信息。否则，将显示连接错误信息。

如果您不想将 TDengine 作为服务运行，您可以在当前终端中运行它。例如，要在构建完成后快速启动 TDengine 服务器，在终端中运行以下命令：（以 Linux 为例，Windows 上的命令为 `taosd.exe`）

```bash
./build/bin/taosd -c test/cfg
```

在另一个终端上，使用 TDengine 命令行连接服务器：

```bash
./build/bin/taos -c test/cfg
```

选项 `-c test/cfg` 指定系统配置文件的目录。

</details>

## 7.2 macOS 系统上运行

<details>

<summary>macOS 系统上运行详细步骤</summary>

在 macOS 上安装完成后启动服务，双击 `/applications/TDengine` 启动程序，或者在终端中执行如下命令：

```bash
sudo launchctl start com.tdengine.taosd
```

然后在终端中使用如下命令通过 TDengine 命令行连接 TDengine 服务器：

```bash
taos
```

如果 TDengine 命令行连接服务器成功，系统将打印欢迎信息和版本信息。否则，将显示错误信息。

</details>


## 7.3 Windows 系统上运行

<details>

<summary>Windows 系统上运行详细步骤</summary>

您可以使用以下命令在 Windows 平台上启动 TDengine 服务器：

```cmd
.\build\bin\taosd.exe -c test\cfg
```

在另一个终端上，使用 TDengine 命令行连接服务器：

```cmd
.\build\bin\taos.exe -c test\cfg
```

选项 `-c test/cfg` 指定系统配置文件的目录。

</details>

# 8. 测试

有关如何在 TDengine 上运行不同类型的测试，请参考 [TDengine测试](./tests/README-CN.md)

# 9. 版本发布

TDengine 发布版本的完整列表，请参考 [版本列表](https://github.com/taosdata/TDengine/releases)

# 10. 工作流

TDengine 构建检查工作流可以在参考 [Github Action](https://github.com/taosdata/TDengine/actions/workflows/taosd-ci-build.yml)，更多的工作流正在创建中，将很快可用。

# 11. 覆盖率

最新的 TDengine 测试覆盖率报告可参考 [coveralls.io](https://coveralls.io/github/taosdata/TDengine)

<details>

<summary>如何在本地运行测试覆盖率报告？</summary>

在本地创建测试覆盖率报告（HTML 格式），请运行以下命令：

```bash
cd tests
bash setup-lcov.sh -v 1.16 && ./run_local_coverage.sh -b main -c task 
# on main branch and run cases in longtimeruning_cases.task 
# for more information about options please refer to ./run_local_coverage.sh -h
```
> **注意**：
> 请注意，-b 和 -i 选项将使用 -DCOVER=true 选项重新编译 TDengine，这可能需要花费一些时间。

</details>

# 12. 成为社区贡献者

点击 [这里](https://www.taosdata.com/contributor)，了解如何成为 TDengine 的贡献者。
