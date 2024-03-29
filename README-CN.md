<p>
<p align="center">
  <a href="https://tdengine.com" target="_blank">
  <img
    src="docs/assets/tdengine.svg"
    alt="TDengine"
    width="500"
  />
  </a>
</p>
<p>

[![Build Status](https://travis-ci.org/taosdata/TDengine.svg?branch=master)](https://travis-ci.org/taosdata/TDengine)
[![Build status](https://ci.appveyor.com/api/projects/status/kf3pwh2or5afsgl9/branch/master?svg=true)](https://ci.appveyor.com/project/sangshuduo/tdengine-2n8ge/branch/master)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/TDengine/badge.svg?branch=3.0)](https://coveralls.io/github/taosdata/TDengine?branch=3.0)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4201/badge)](https://bestpractices.coreinfrastructure.org/projects/4201)

简体中文 | [English](README.md) | [TDengine 云服务](https://cloud.taosdata.com/?utm_medium=cn&utm_source=github) | 很多职位正在热招中，请看[这里](https://www.taosdata.com/cn/careers/)

# TDengine 简介

TDengine 是一款开源、高性能、云原生的时序数据库 (Time-Series Database, TSDB)。TDengine 能被广泛运用于物联网、工业互联网、车联网、IT 运维、金融等领域。除核心的时序数据库功能外，TDengine 还提供缓存、数据订阅、流式计算等功能，是一极简的时序数据处理平台，最大程度的减小系统设计的复杂度，降低研发和运营成本。与其他时序数据库相比，TDengine 的主要优势如下：

- **高性能**：通过创新的存储引擎设计，无论是数据写入还是查询，TDengine 的性能比通用数据库快 10 倍以上，也远超其他时序数据库，存储空间不及通用数据库的1/10。

- **云原生**：通过原生分布式的设计，充分利用云平台的优势，TDengine 提供了水平扩展能力，具备弹性、韧性和可观测性，支持k8s部署，可运行在公有云、私有云和混合云上。

- **极简时序数据平台**：TDengine 内建消息队列、缓存、流式计算等功能，应用无需再集成 Kafka/Redis/HBase/Spark 等软件，大幅降低系统的复杂度，降低应用开发和运营成本。

- **分析能力**：支持 SQL，同时为时序数据特有的分析提供SQL扩展。通过超级表、存储计算分离、分区分片、预计算、自定义函数等技术，TDengine 具备强大的分析能力。

- **简单易用**：无任何依赖，安装、集群几秒搞定；提供REST以及各种语言连接器，与众多第三方工具无缝集成；提供命令行程序，便于管理和即席查询；提供各种运维工具。

- **核心开源**：TDengine 的核心代码包括集群功能全部开源，截止到2022年8月1日，全球超过 135.9k 个运行实例，GitHub Star 18.7k，Fork 4.4k，社区活跃。

# 文档

关于完整的使用手册，系统架构和更多细节，请参考 [TDengine 文档](https://docs.taosdata.com) 或者  [TDengine Documentation](https://docs.tdengine.com)。

# 构建

TDengine 目前可以在 Linux、 Windows、macOS 等平台上安装和运行。任何 OS 的应用也可以选择 taosAdapter 的 RESTful 接口连接服务端 taosd。CPU 支持 X64/ARM64，后续会支持 MIPS64、Alpha64、ARM32、RISC-V 等 CPU 架构。目前不支持使用交叉编译器构建。

用户可根据需求选择通过源码、[容器](https://docs.taosdata.com/get-started/docker/)、[安装包](https://docs.taosdata.com/get-started/package/)或[Kubernetes](https://docs.taosdata.com/deployment/k8s/)来安装。本快速指南仅适用于通过源码安装。
  
TDengine 还提供一组辅助工具软件 taosTools，目前它包含 taosBenchmark（曾命名为 taosdemo）和 taosdump 两个软件。默认 TDengine 编译不包含 taosTools, 您可以在编译 TDengine 时使用`cmake .. -DBUILD_TOOLS=true` 来同时编译 taosTools。

为了构建TDengine, 请使用 [CMake](https://cmake.org/) 3.13.0 或者更高版本。

## 安装工具

### Ubuntu 18.04 及以上版本 & Debian：

```bash
sudo apt-get install -y gcc cmake build-essential git libssl-dev libgflags2.2 libgflags-dev
```

#### 为 taos-tools 安装编译需要的软件

为了在 Ubuntu/Debian 系统上编译 [taos-tools](https://github.com/taosdata/taos-tools) 需要安装如下软件：

```bash
sudo apt install build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config
```

### CentOS 7.9

```bash
sudo yum install epel-release
sudo yum update
sudo yum install -y gcc gcc-c++ make cmake3 gflags git openssl-devel
sudo ln -sf /usr/bin/cmake3 /usr/bin/cmake
```

### CentOS 8/Fedora/Rocky Linux

```bash
sudo dnf install -y gcc gcc-c++ gflags make cmake epel-release git openssl-devel
```

#### 在 CentOS 上构建 taosTools 安装依赖软件


#### CentOS 7.9


```
sudo yum install -y zlib-devel zlib-static xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libatomic-static libstdc++-static openssl-devel
```

#### CentOS 8/Fedora/Rocky Linux 

```
sudo yum install -y epel-release
sudo yum install -y dnf-plugins-core
sudo yum config-manager --set-enabled powertools
sudo yum install -y zlib-devel zlib-static xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libatomic-static libstdc++-static openssl-devel
```

注意：由于 snappy 缺乏 pkg-config 支持（参考 [链接](https://github.com/google/snappy/pull/86)），会导致 cmake 提示无法发现 libsnappy，实际上工作正常。

若 powertools 安装失败，可以尝试改用：
```
sudo yum config-manager --set-enabled powertools
```

#### CentOS + devtoolset

除上述编译依赖包，需要执行以下命令：

```
sudo yum install centos-release-scl
sudo yum install devtoolset-9 devtoolset-9-libatomic-devel
scl enable devtoolset-9 -- bash
```

### macOS

```
brew install argp-standalone gflags pkgconfig
```

### 设置 golang 开发环境

TDengine 包含数个使用 Go 语言开发的组件，比如taosAdapter, 请参考 golang.org 官方文档设置 go 开发环境。

请使用 1.20 及以上版本。对于中国用户，我们建议使用代理来加速软件包下载。

```
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

缺省是不会构建 taosAdapter, 但您可以使用以下命令选择构建 taosAdapter 作为 RESTful 接口的服务。

```
cmake .. -DBUILD_HTTP=false
```

### 设置 rust 开发环境

TDengine 包含数个使用 Rust 语言开发的组件. 请参考 rust-lang.org 官方文档设置 rust 开发环境。

## 获取源码

首先，你需要从 GitHub 克隆源码：

```bash
git clone https://github.com/taosdata/TDengine.git
cd TDengine
```
如果使用 https 协议下载比较慢，可以通过修改 ~/.gitconfig 文件添加以下两行设置使用 ssh 协议下载。需要首先上传 ssh 密钥到 GitHub，详细方法请参考 GitHub 官方文档。

```
[url "git@github.com:"]
    insteadOf = https://github.com/
```
## 特别说明

[JDBC 连接器](https://github.com/taosdata/taos-connector-jdbc)， [Go 连接器](https://github.com/taosdata/driver-go)，[Python 连接器](https://github.com/taosdata/taos-connector-python)，[Node.js 连接器](https://github.com/taosdata/taos-connector-node)，[C# 连接器](https://github.com/taosdata/taos-connector-dotnet) ，[Rust 连接器](https://github.com/taosdata/taos-connector-rust) 和 [Grafana 插件](https://github.com/taosdata/grafanaplugin)已移到独立仓库。


## 构建 TDengine

### Linux 系统

可以运行代码仓库中的 `build.sh` 脚本编译出 TDengine 和 taosTools（包含 taosBenchmark 和 taosdump）。

```bash
./build.sh
```

这个脚本等价于执行如下命令：

```bash
mkdir debug
cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_CONTRIB=true
make
```

您也可以选择使用 jemalloc 作为内存分配器，替代默认的 glibc：

```bash
apt install autoconf
cmake .. -DJEMALLOC_ENABLED=true
```

在 X86-64、X86、arm64 平台上，TDengine 生成脚本可以自动检测机器架构。也可以手动配置 CPUTYPE 参数来指定 CPU 类型，如 aarch64 等。

aarch64：

```bash
cmake .. -DCPUTYPE=aarch64 && cmake --build .
```

### Windows 系统

如果你使用的是 Visual Studio 2013 版本：

打开 cmd.exe，执行 vcvarsall.bat 时，为 64 位操作系统指定“x86_amd64”，为 32 位操作系统指定“x86”。

```bash
mkdir debug && cd debug
"C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" < x86_amd64 | x86 >
cmake .. -G "NMake Makefiles"
nmake
```

如果你使用的是 Visual Studio 2019 或 2017 版本：

打开 cmd.exe，执行 vcvarsall.bat 时，为 64 位操作系统指定“x64”，为 32 位操作系统指定“x86”。

```bash
mkdir debug && cd debug
"c:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" < x64 | x86 >
cmake .. -G "NMake Makefiles"
nmake
```

你也可以从开始菜单中找到"Visual Studio < 2019 | 2017 >"菜单项，根据你的系统选择"x64 Native Tools Command Prompt for VS < 2019 | 2017 >"或"x86 Native Tools Command Prompt for VS < 2019 | 2017 >"，打开命令行窗口，执行：

```bash
mkdir debug && cd debug
cmake .. -G "NMake Makefiles"
nmake
```

### macOS 系统

安装 XCode 命令行工具和 cmake. 在 Catalina 和 Big Sur 操作系统上，需要安装 XCode 11.4+ 版本。

```bash
mkdir debug && cd debug
cmake .. && cmake --build .
```

# 安装

## Linux 系统

生成完成后，安装 TDengine：

```bash
sudo make install
```

用户可以在[文件目录结构](https://docs.taosdata.com/reference/directory/)中了解更多在操作系统中生成的目录或文件。

从源代码安装也会为 TDengine 配置服务管理 ，用户也可以选择[从安装包中安装](https://docs.taosdata.com/get-started/package/)。

安装成功后，在终端中启动 TDengine 服务：

```bash
sudo systemctl start taosd
```

用户可以使用 TDengine CLI 来连接 TDengine 服务，在终端中，输入：

```bash
taos
```

如果 TDengine CLI 连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印出错误消息。

## Windows 系统

生成完成后，安装 TDengine：

```cmd
nmake install
```

## macOS 系统

生成完成后，安装 TDengine：

```bash
sudo make install
```

用户可以在[文件目录结构](https://docs.taosdata.com/reference/directory/)中了解更多在操作系统中生成的目录或文件。

从源代码安装也会为 TDengine 配置服务管理 ，用户也可以选择[从安装包中安装](https://docs.taosdata.com/get-started/package/)。

安装成功后，可以在应用程序中双击 TDengine 图标启动服务，或者在终端中启动 TDengine 服务：

```bash
sudo launchctl start com.tdengine.taosd
```

用户可以使用 TDengine CLI 来连接 TDengine 服务，在终端中，输入：

```bash
taos
```

如果 TDengine CLI 连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印出错误消息。

## 快速运行

如果不希望以服务方式运行 TDengine，也可以在终端中直接运行它。也即在生成完成后，执行以下命令（在 Windows 下，生成的可执行文件会带有 .exe 后缀，例如会名为 taosd.exe ）：

```bash
./build/bin/taosd -c test/cfg
```

在另一个终端，使用 TDengine CLI 连接服务器：

```bash
./build/bin/taos -c test/cfg
```

"-c test/cfg"指定系统配置文件所在目录。

# 体验 TDengine

在 TDengine 终端中，用户可以通过 SQL 命令来创建/删除数据库、表等，并进行插入查询操作。

```sql
CREATE DATABASE demo;
USE demo;
CREATE TABLE t (ts TIMESTAMP, speed INT);
INSERT INTO t VALUES('2019-07-15 00:00:00', 10);
INSERT INTO t VALUES('2019-07-15 01:00:00', 20);
SELECT * FROM t;
          ts          |   speed   |
===================================
 19-07-15 00:00:00.000|         10|
 19-07-15 01:00:00.000|         20|
Query OK, 2 row(s) in set (0.001700s)
```

# 应用开发

## 官方连接器

TDengine 提供了丰富的应用程序开发接口，其中包括 C/C++、Java、Python、Go、Node.js、C# 、RESTful 等，便于用户快速开发应用：

- [Java](https://docs.taosdata.com/connector/java/)
- [C/C++](https://docs.taosdata.com/connector/cpp/)
- [Python](https://docs.taosdata.com/connector/python/)
- [Go](https://docs.taosdata.com/connector/go/)
- [Node.js](https://docs.taosdata.com/connector/node/)
- [Rust](https://docs.taosdata.com/connector/rust/)
- [C#](https://docs.taosdata.com/connector/csharp/)
- [RESTful API](https://docs.taosdata.com/connector/rest-api/)

# 成为社区贡献者

点击 [这里](https://www.taosdata.com/cn/contributor/)，了解如何成为 TDengine 的贡献者。

# 加入技术交流群

TDengine 官方社群「物联网大数据群」对外开放，欢迎您加入讨论。搜索微信号 "tdengine"，加小 T 为好友，即可入群。
