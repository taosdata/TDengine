[![Build Status](https://travis-ci.org/taosdata/TDengine.svg?branch=master)](https://travis-ci.org/taosdata/TDengine)
[![Build status](https://ci.appveyor.com/api/projects/status/kf3pwh2or5afsgl9/branch/master?svg=true)](https://ci.appveyor.com/project/sangshuduo/tdengine-2n8ge/branch/master)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/TDengine/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/TDengine?branch=develop)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4201/badge)](https://bestpractices.coreinfrastructure.org/projects/4201)
[![tdengine](https://snapcraft.io//tdengine/badge.svg)](https://snapcraft.io/tdengine)

[![TDengine](TDenginelogo.png)](https://www.taosdata.com)

简体中文 | [English](./README.md) 

# TDengine 简介

TDengine是涛思数据专为物联网、车联网、工业互联网、IT运维等设计和优化的大数据平台。除核心的快10倍以上的时序数据库功能外，还提供缓存、数据订阅、流式计算等功能，最大程度减少研发和运维的复杂度，且核心代码，包括集群功能全部开源（开源协议，AGPL v3.0）。

- 10 倍以上性能提升。定义了创新的数据存储结构，单核每秒就能处理至少2万次请求，插入数百万个数据点，读出一千万以上数据点，比现有通用数据库快了十倍以上。
- 硬件或云服务成本降至1/5。由于超强性能，计算资源不到通用大数据方案的1/5；通过列式存储和先进的压缩算法，存储空间不到通用数据库的1/10。
- 全栈时序数据处理引擎。将数据库、消息队列、缓存、流式计算等功能融合一起，应用无需再集成Kafka/Redis/HBase/Spark等软件，大幅降低应用开发和维护成本。
- 强大的分析功能。无论是十年前还是一秒钟前的数据，指定时间范围即可查询。数据可在时间轴上或多个设备上进行聚合。即席查询可通过Shell/Python/R/Matlab随时进行。
- 与第三方工具无缝连接。不用一行代码，即可与Telegraf, Grafana, EMQ X, Prometheus, Matlab, R集成。后续还将支持MQTT, OPC, Hadoop，Spark等, BI工具也将无缝连接。
- 零运维成本、零学习成本。安装、集群一秒搞定，无需分库分表，实时备份。标准SQL，支持JDBC,RESTful，支持Python/Java/C/C++/Go/Node.JS, 与MySQL相似，零学习成本。

# 文档

TDengine是一个高效的存储、查询、分析时序大数据的平台，专为物联网、车联网、工业互联网、运维监测等优化而设计。您可以像使用关系型数据库MySQL一样来使用它，但建议您在使用前仔细阅读一遍下面的文档，特别是 [数据模型](https://www.taosdata.com/cn/documentation/architecture) 与 [数据建模](https://www.taosdata.com/cn/documentation/model)。除本文档之外，欢迎 [下载产品白皮书](https://www.taosdata.com/downloads/TDengine%20White%20Paper.pdf)。

# 生成

TDengine目前2.0版服务器仅能在Linux系统上安装和运行，后续会支持Windows、macOS等系统。客户端可以在Windows或Linux上安装和运行。任何OS的应用也可以选择RESTful接口连接服务器taosd。CPU支持X64/ARM64/MIPS64/Alpha64，后续会支持ARM32、RISC-V等CPU架构。用户可根据需求选择通过[源码](https://www.taosdata.com/cn/getting-started/#通过源码安装)或者[安装包](https://www.taosdata.com/cn/getting-started/#通过安装包安装)来安装。本快速指南仅适用于通过源码安装。

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

### CentOS 8 & Fedora:

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

## 生成 TDengine

### Linux 系统

```bash
mkdir debug && cd debug
cmake .. && cmake --build .
```

在X86-64、X86、arm64 和 arm32 平台上，TDengine 生成脚本可以自动检测机器架构。也可以手动配置 CPUTYPE 参数来指定 CPU 类型，如 aarch64 或 aarch32 等。

aarch64：

```bash
cmake .. -DCPUTYPE=aarch64 && cmake --build .
```

aarch32：

```bash
cmake .. -DCPUTYPE=aarch32 && cmake --build .
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

打开cmd.exe，执行 vcvarsall.bat 时，为 64 位操作系统指定“x64”，为 32 位操作系统指定“x86”。

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

### Mac OS X 系统

安装 Xcode 命令行工具和 cmake. 在 Catalina 和 Big Sur 操作系统上，需要安装 XCode 11.4+ 版本。

```bash
mkdir debug && cd debug
cmake .. && cmake --build .
```

# 安装

如果你不想安装，可以直接在shell中运行。生成完成后，安装 TDengine：
```bash
make install
```

用户可以在[文件目录结构](https://www.taosdata.com/cn/documentation/administrator#directories)中了解更多在操作系统中生成的目录或文件。

安装成功后，在终端中启动 TDengine 服务：

```bash
taosd
```

用户可以使用 TDengine Shell 来连接 TDengine 服务，在终端中，输入：

```bash
taos
```

如果 TDengine Shell 连接服务成功，将会打印出欢迎消息和版本信息。如果失败，则会打印出错误消息。

## 快速运行

TDengine 生成后，在终端执行以下命令：

```bash
./build/bin/taosd -c test/cfg
```

在另一个终端，使用 TDengine shell 连接服务器：

```bash
./build/bin/taos -c test/cfg
```

"-c test/cfg"指定系统配置文件所在目录。

# 体验 TDengine

在TDengine终端中，用户可以通过SQL命令来创建/删除数据库、表等，并进行插入查询操作。

```bash
create database demo;
use demo;
create table t (ts timestamp, speed int);
insert into t values ('2019-07-15 00:00:00', 10);
insert into t values ('2019-07-15 01:00:00', 20);
select * from t;
          ts          |   speed   |
===================================
 19-07-15 00:00:00.000|         10|
 19-07-15 01:00:00.000|         20|
Query OK, 2 row(s) in set (0.001700s)
```

# 应用开发

## 官方连接器

TDengine 提供了丰富的应用程序开发接口，其中包括C/C++、Java、Python、Go、Node.js、C# 、RESTful 等，便于用户快速开发应用：

- Java

- C/C++

- Python

- Go

- RESTful API

- Node.js

## 第三方连接器

TDengine 社区生态中也有一些非常友好的第三方连接器，可以通过以下链接访问它们的源码。

- [Rust Connector](https://github.com/taosdata/TDengine/tree/master/tests/examples/rust)
- [.Net Core Connector](https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos)
- [Lua Connector](https://github.com/taosdata/TDengine/tree/develop/tests/examples/lua)

# 运行和添加测试例

TDengine 的测试框架和所有测试例全部开源。

点击 [这里](tests/How-To-Run-Test-And-How-To-Add-New-Test-Case.md)，了解如何运行测试例和添加新的测试例。

# 成为社区贡献者

点击 [这里](https://www.taosdata.com/cn/contributor/)，了解如何成为 TDengine 的贡献者。

# 加入技术交流群

TDengine 官方社群「物联网大数据群」对外开放，欢迎您加入讨论。搜索微信号 "tdengine"，加小T为好友，即可入群。

# [谁在使用TDengine](https://github.com/taosdata/TDengine/issues/2432)

欢迎所有 TDengine 用户及贡献者在 [这里](https://github.com/taosdata/TDengine/issues/2432) 分享您在当前工作中开发/使用 TDengine 的故事。 
