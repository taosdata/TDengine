[![Build Status](https://travis-ci.org/taosdata/TDengine.svg?branch=master)](https://travis-ci.org/taosdata/TDengine)
[![Build status](https://ci.appveyor.com/api/projects/status/kf3pwh2or5afsgl9/branch/master?svg=true)](https://ci.appveyor.com/project/sangshuduo/tdengine-2n8ge/branch/master)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/TDengine/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/TDengine?branch=develop)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4201/badge)](https://bestpractices.coreinfrastructure.org/projects/4201)
[![tdengine](https://snapcraft.io//tdengine/badge.svg)](https://snapcraft.io/tdengine)

[![TDengine](TDenginelogo.png)](https://www.taosdata.com)

简体中文 | [English](./README.md)
很多职位正在热招中，请看[这里](https://www.taosdata.com/cn/careers/)

# TDengine 简介

TDengine 是一款高性能、分布式、支持 SQL 的时序数据库（Time-Series Database）。而且除时序数据库功能外，它还提供缓存、数据订阅、流式计算等功能，最大程度减少研发和运维的复杂度，且核心代码，包括集群功能全部开源（开源协议，AGPL v3.0）。与其他时序数据数据库相比，TDengine 有以下特点：

- **高性能**：通过创新的存储引擎设计，无论是数据写入还是查询，TDengine 的性能比通用数据库快 10 倍以上，也远超其他时序数据库，而且存储空间也大为节省。

- **分布式**：通过原生分布式的设计，TDengine 提供了水平扩展的能力，只需要增加节点就能获得更强的数据处理能力，同时通过多副本机制保证了系统的高可用。

- **支持 SQL**：TDengine 采用 SQL 作为数据查询语言，减少学习和迁移成本，同时提供 SQL 扩展来处理时序数据特有的分析，而且支持方便灵活的 schemaless 数据写入。

- **All in One**：将数据库、消息队列、缓存、流式计算等功能融合一起，应用无需再集成 Kafka/Redis/HBase/Spark 等软件，大幅降低应用开发和维护成本。

- **零管理**：安装、集群几秒搞定，无任何依赖，不用分库分表，系统运行状态监测能与 Grafana 或其他运维工具无缝集成。

- **零学习成本**：采用 SQL 查询语言，支持 Python、Java、C/C++、Go、Rust、Node.js 等多种编程语言，与 MySQL 相似，零学习成本。

- **无缝集成**：不用一行代码，即可与 Telegraf、Grafana、EMQX、Prometheus、StatsD、collectd、Matlab、R 等第三方工具无缝集成。

- **互动 Console**: 通过命令行 console，不用编程，执行 SQL 语句就能做即席查询、各种数据库的操作、管理以及集群的维护.

TDengine 可以广泛应用于物联网、工业互联网、车联网、IT 运维、能源、金融等领域，让大量设备、数据采集器每天产生的高达 TB 甚至 PB 级的数据能得到高效实时的处理，对业务的运行状态进行实时的监测、预警，从大数据中挖掘出商业价值。

# 文档

TDengine 采用传统的关系数据库模型，您可以像使用关系型数据库 MySQL 一样来使用它。但由于引入了超级表，一个采集点一张表的概念，建议您在使用前仔细阅读一遍下面的文档，特别是 [数据模型](https://www.taosdata.com/cn/documentation/architecture) 与 [数据建模](https://www.taosdata.com/cn/documentation/model)。除本文档之外，欢迎 [下载产品白皮书](https://www.taosdata.com/downloads/TDengine%20White%20Paper.pdf)。

# 构建

TDengine 目前 2.0 版服务器仅能在 Linux 系统上安装和运行，后续会支持 Windows、macOS 等系统。客户端可以在 Windows 或 Linux 上安装和运行。任何 OS 的应用也可以选择 RESTful 接口连接服务器 taosd。CPU 支持 X64/ARM64/MIPS64/Alpha64，后续会支持 ARM32、RISC-V 等 CPU 架构。用户可根据需求选择通过[源码](https://www.taosdata.com/cn/getting-started/#通过源码安装)或者[安装包](https://www.taosdata.com/cn/getting-started/#通过安装包安装)来安装。本快速指南仅适用于通过源码安装。

## 安装工具

### Ubuntu 16.04 及以上版本 & Debian：

```bash
sudo apt-get install -y gcc cmake build-essential git
```

### Ubuntu 14.04：

```bash
sudo apt-get install -y gcc cmake3 build-essential git binutils-2.26
export PATH=/usr/lib/binutils-2.26/bin:$PATH
```

编译或打包 JDBC 驱动源码，需安装 Java JDK 8 或以上版本和 Apache Maven 2.7 或以上版本。

安装 OpenJDK 8：

```bash
sudo apt-get install -y openjdk-8-jdk
```

安装 Apache Maven：

```bash
sudo apt-get install -y  maven
```

#### 为 taos-tools 安装编译需要的软件

taosTools 是用于 TDengine 的辅助工具软件集合。目前它包含 taosBenchmark（曾命名为 taosdemo）和 taosdump 两个软件。

默认 TDengine 编译不包含 taosTools。您可以在编译 TDengine 时使用`cmake .. -DBUILD_TOOLS=true` 来同时编译 taosTools。

为了在 Ubuntu/Debian 系统上编译 [taos-tools](https://github.com/taosdata/taos-tools) 需要安装如下软件：

```bash
sudo apt install build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config
```

### CentOS 7：

```bash
sudo yum install -y gcc gcc-c++ make cmake git
```

安装 OpenJDK 8：

```bash
sudo yum install -y java-1.8.0-openjdk
```

安装 Apache Maven：

```bash
sudo yum install -y maven
```

### CentOS 8 & Fedora

```bash
sudo dnf install -y gcc gcc-c++ make cmake epel-release git
```

安装 OpenJDK 8：

```bash
sudo dnf install -y java-1.8.0-openjdk
```

安装 Apache Maven：

```bash
sudo dnf install -y maven
```

#### 在 CentOS 上构建 taosTools 安装依赖软件

为了在 CentOS 上构建 [taosTools](https://github.com/taosdata/taos-tools) 需要安装如下依赖软件

```bash
sudo yum install zlib-devel xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libstdc++-static
```

注意：由于 snappy 缺乏 pkg-config 支持
（参考 [链接](https://github.com/google/snappy/pull/86)），会导致
cmake 提示无法发现 libsnappy，实际上工作正常。

## 获取源码

首先，你需要从 GitHub 克隆源码：

```bash
git clone https://github.com/taosdata/TDengine.git
cd TDengine
```

Go 连接器和 Grafana 插件在其他独立仓库，如果安装它们的话，需要在 TDengine 目录下通过此命令安装：

```bash
git submodule update --init --recursive
```

如果使用 https 协议下载比较慢，可以通过修改 ~/.gitconfig 文件添加以下两行设置使用 ssh 协议下载。需要首先上传 ssh 密钥到 GitHub，详细方法请参考 GitHub 官方文档。

```
[url "git@github.com:"]
    insteadOf = https://github.com/
```

## 构建 TDengine

### Linux 系统

可以运行代码仓库中的 `build.sh` 脚本编译出 TDengine 和 taosTools（包含 taosBenchmark 和 taosdump）。

```bash
./build.sh
```

这个脚本等价于执行如下命令：

```bash
git submodule update --init --recursive
mkdir debug
cd debug
cmake .. -DBUILD_TOOLS=true
make
```

您也可以选择使用 jemalloc 作为内存分配器，替代默认的 glibc：

```bash
apt install autoconf
cmake .. -DJEMALLOC_ENABLED=true
```

在 X86-64、X86、arm64、arm32 和 mips64 平台上，TDengine 生成脚本可以自动检测机器架构。也可以手动配置 CPUTYPE 参数来指定 CPU 类型，如 aarch64 或 aarch32 等。

aarch64：

```bash
cmake .. -DCPUTYPE=aarch64 && cmake --build .
```

aarch32：

```bash
cmake .. -DCPUTYPE=aarch32 && cmake --build .
```

mips64：

```bash
cmake .. -DCPUTYPE=mips64 && cmake --build .
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

安装 Xcode 命令行工具和 cmake. 在 Catalina 和 Big Sur 操作系统上，需要安装 XCode 11.4+ 版本。

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

用户可以在[文件目录结构](https://www.taosdata.com/cn/documentation/administrator#directories)中了解更多在操作系统中生成的目录或文件。
从 2.0 版本开始, 从源代码安装也会为 TDengine 配置服务管理。
用户也可以选择[从安装包中安装](https://www.taosdata.com/en/getting-started/#Install-from-Package)。

安装成功后，在终端中启动 TDengine 服务：

```bash
sudo systemctl start taosd
```

用户可以使用 TDengine Shell 来连接 TDengine 服务，在终端中，输入：

```bash
taos
```

如果 TDengine Shell 连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印出错误消息。

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

安装成功后，如果想以服务形式启动，先配置 `.plist` 文件，在终端中执行：

```bash
sudo cp ../packaging/macOS/com.taosdata.tdengine.plist /Library/LaunchDaemons
```

在终端中启动 TDengine 服务：

```bash
sudo launchctl load /Library/LaunchDaemons/com.taosdata.tdengine.plist
```

在终端中停止 TDengine 服务：

```bash
sudo launchctl unload /Library/LaunchDaemons/com.taosdata.tdengine.plist
```

## 快速运行

如果不希望以服务方式运行 TDengine，也可以在终端中直接运行它。也即在生成完成后，执行以下命令（在 Windows 下，生成的可执行文件会带有 .exe 后缀，例如会名为 taosd.exe ）：

```bash
./build/bin/taosd -c test/cfg
```

在另一个终端，使用 TDengine shell 连接服务器：

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

- [Java](https://www.taosdata.com/cn/documentation/connector/java)

- [C/C++](https://www.taosdata.com/cn/documentation/connector#c-cpp)

- [Python](https://www.taosdata.com/cn/documentation/connector#python)

- [Go](https://www.taosdata.com/cn/documentation/connector#go)

- [RESTful API](https://www.taosdata.com/cn/documentation/connector#restful)

- [Node.js](https://www.taosdata.com/cn/documentation/connector#nodejs)

- [Rust](https://www.taosdata.com/cn/documentation/connector/rust)

## 第三方连接器

TDengine 社区生态中也有一些非常友好的第三方连接器，可以通过以下链接访问它们的源码。

- [Rust Bindings](https://github.com/songtianyi/tdengine-rust-bindings/tree/master/examples)
- [.Net Core Connector](https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos)
- [Lua Connector](https://github.com/taosdata/TDengine/tree/develop/examples/lua)

# 运行和添加测试例

TDengine 的测试框架和所有测试例全部开源。

点击 [这里](https://github.com/taosdata/TDengine/blob/develop/tests/How-To-Run-Test-And-How-To-Add-New-Test-Case.md)，了解如何运行测试例和添加新的测试例。

# 成为社区贡献者

点击 [这里](https://www.taosdata.com/cn/contributor/)，了解如何成为 TDengine 的贡献者。

# 加入技术交流群

TDengine 官方社群「物联网大数据群」对外开放，欢迎您加入讨论。搜索微信号 "tdengine"，加小 T 为好友，即可入群。

# [谁在使用 TDengine](https://github.com/taosdata/TDengine/issues/2432)

欢迎所有 TDengine 用户及贡献者在 [这里](https://github.com/taosdata/TDengine/issues/2432) 分享您在当前工作中开发/使用 TDengine 的故事。
