[![Build Status](https://cloud.drone.io/api/badges/taosdata/TDengine/status.svg?ref=refs/heads/master)](https://cloud.drone.io/taosdata/TDengine)
[![Build status](https://ci.appveyor.com/api/projects/status/kf3pwh2or5afsgl9/branch/master?svg=true)](https://ci.appveyor.com/project/sangshuduo/tdengine-2n8ge/branch/master)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/TDengine/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/TDengine?branch=develop)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4201/badge)](https://bestpractices.coreinfrastructure.org/projects/4201)
[![tdengine](https://snapcraft.io//tdengine/badge.svg)](https://snapcraft.io/tdengine)

[![TDengine](TDenginelogo.png)](https://www.taosdata.com)

English | [简体中文](./README-CN.md)
We are hiring, check [here](https://www.taosdata.com/en/careers/)

# What is TDengine？

TDengine is a high-performance, scalable time-series database with SQL support. Its code including cluster feature is open source under [GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html). Besides the database, it provides caching, stream processing, data data subscription and other functionalities to reduce the complexity and cost of development and operation. TDengine differentiates itself from other TSDBs with the following advantages.

- **High Performance**: TDengine outperforms other time series databases in data ingestion and querying while significantly reducing storage cost and compute costs, with an innovatively designed and purpose-built storage engine.

- **Scalable**: TDengine provides out-of-box scalability and high-availability through its native distributed design. Nodes can be added through simple configuration to achieve greater data processing power. In addition, this feature is open source.

- **SQL Support**: TDengine uses SQL as the query language, thereby reducing learning and migration costs, while adding SQL extensions to handle time-series data better, and supporting convenient and flexible schemaless data ingestion.

- **All in One**: TDengine has built-in caching, stream processing and data subscription functions, it is no longer necessary to integrate Kafka/Redis/HBase/Spark or other software in some scenarios. It makes the system architecture much simpler and easy to maintain.

- **Seamless Integration**: Without a single line of code, TDengine provide seamless integration with third-party tools such as Telegraf, Grafana, EMQX, Prometheus, StatsD, collectd, etc. More will be integrated.

- **Zero Management**: Installation and cluster setup can be done in seconds. Data partitioning and sharding are executed automatically. TDengine’s running status can be monitored via Grafana or other DevOps tools.

- **Zero Learning Cost**: With SQL as the query language, support for ubiquitous tools like Python, Java, C/C++, Go, Rust, Node.js connectors, there is zero learning cost.

- **Interactive Console**: TDengine provides convenient console access to the database to run ad hoc queries, maintain the database, or manage the cluster without any programming.

TDengine can be widely applied to Internet of Things (IoT), Connected Vehicles, Industrial IoT, DevOps, energy, finance and many other scenarios.

# Documentation

For user manual, system design and architecture, engineering blogs, refer to [TDengine Documentation](https://www.taosdata.com/en/documentation/)(中文版请点击[这里](https://www.taosdata.com/cn/documentation20/))
for details. The documentation from our website can also be downloaded locally from _documentation/tdenginedocs-en_ or _documentation/tdenginedocs-cn_.

# Building

At the moment, TDengine server only supports running on Linux systems. You can choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) or build it from the source code. This quick guide is for installation from the source only.

To build TDengine, use [CMake](https://cmake.org/) 3.0.2 or higher versions in the project directory.

## Install build dependencies

### Ubuntu 16.04 and above or Debian

```bash
sudo apt-get install -y gcc cmake build-essential git
```

### Ubuntu 14.04

```bash
sudo apt-get install -y gcc cmake3 build-essential git binutils-2.26
export PATH=/usr/lib/binutils-2.26/bin:$PATH
```

To compile and package the JDBC driver source code, you should have a Java jdk-8 or higher and Apache Maven 2.7 or higher installed.

To install openjdk-8:

```bash
sudo apt-get install -y openjdk-8-jdk
```

To install Apache Maven:

```bash
sudo apt-get install -y maven
```

#### Install build dependencies for taosTools

We provide a few useful tools such as taosBenchmark (was named taosdemo) and taosdump. They were part of TDengine. From TDengine 2.4.0.0, taosBenchmark and taosdump were not released together with TDengine.
By default, TDengine compiling does not include taosTools. You can use 'cmake .. -DBUILD_TOOLS=true' to make them be compiled with TDengine.

To build the [taosTools](https://github.com/taosdata/taos-tools) on Ubuntu/Debian, the following packages need to be installed.

```bash
sudo apt install build-essential libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config
```

### CentOS 7

```bash
sudo yum install epel-release
sudo yum update
sudo yum install -y gcc gcc-c++ make cmake3 git
sudo ln -sf /usr/bin/cmake3 /usr/bin/cmake
```

To install openjdk-8:

```bash
sudo yum install -y java-1.8.0-openjdk
```

To install Apache Maven:

```bash
sudo yum install -y maven
```

### CentOS 8 & Fedora

```bash
sudo dnf install -y gcc gcc-c++ make cmake epel-release git
```

To install openjdk-8:

```bash
sudo dnf install -y java-1.8.0-openjdk
```

To install Apache Maven:

```bash
sudo dnf install -y maven
```

#### Install build dependencies for taosTools on CentOS

To build the [taosTools](https://github.com/taosdata/taos-tools) on CentOS, the following packages need to be installed.

```bash
sudo yum install zlib-devel xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libstdc++-static
```

Note: Since snappy lacks pkg-config support (refer to [link](https://github.com/google/snappy/pull/86)), it lead a cmake prompt libsnappy not found. But snappy will works well.

### Setup golang environment

TDengine includes few components developed by Go language. Please refer to golang.org official documentation for golang environment setup.

Please use version 1.14+. For the user in China, we recommend using a proxy to accelerate package downloading.

```
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

## Get the source codes

First of all, you may clone the source codes from github:

```bash
git clone https://github.com/taosdata/TDengine.git
cd TDengine
```

The connectors for go & Grafana and some tools have been moved to separated repositories,
so you should run this command in the TDengine directory to install them:

```bash
git submodule update --init --recursive
```

You can modify the file ~/.gitconfig to use ssh protocol instead of https for better download speed. You need to upload ssh public key to GitHub first. Please refer to GitHub official documentation for detail.

```
[url "git@github.com:"]
    insteadOf = https://github.com/
```

## Build TDengine

### On Linux platform

You can run the bash script `build.sh` to build both TDengine and taosTools including taosBenchmark and taosdump as below:

```bash
./build.sh
```

It equals to execute following commands:

```bash
git submodule update --init --recursive
mkdir debug
cd debug
cmake .. -DBUILD_TOOLS=true
make
```

Note TDengine 2.3.x.0 and later use a component named 'taosAdapter' to play http daemon role by default instead of the http daemon embedded in the early version of TDengine. The taosAdapter is programmed by go language. If you pull TDengine source code to the latest from an existing codebase, please execute 'git submodule update --init --recursive' to pull taosAdapter source code. Please install go language version 1.14 or above for compiling taosAdapter. If you meet difficulties regarding 'go mod', especially you are from China, you can use a proxy to solve the problem.

```
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

The embedded http daemon still be built from TDengine source code by default. Or you can use the following command to choose to build taosAdapter.

```
cmake .. -DBUILD_HTTP=false
```

You can use Jemalloc as memory allocator instead of glibc:

```
apt install autoconf
cmake .. -DJEMALLOC_ENABLED=true
```

TDengine build script can detect the host machine's architecture on X86-64, X86, arm64, arm32 and mips64 platform.
You can also specify CPUTYPE option like aarch64 or aarch32 too if the detection result is not correct:

aarch64:

```bash
cmake .. -DCPUTYPE=aarch64 && cmake --build .
```

aarch32:

```bash
cmake .. -DCPUTYPE=aarch32 && cmake --build .
```

mips64:

```bash
cmake .. -DCPUTYPE=mips64 && cmake --build .
```

### On Windows platform

If you use the Visual Studio 2013, please open a command window by executing "cmd.exe".
Please specify "amd64" for 64 bits Windows or specify "x86" is for 32 bits Windows when you execute vcvarsall.bat.

```cmd
mkdir debug && cd debug
"C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" < amd64 | x86 >
cmake .. -G "NMake Makefiles"
nmake
```

If you use the Visual Studio 2019 or 2017:

please open a command window by executing "cmd.exe".
Please specify "x64" for 64 bits Windows or specify "x86" is for 32 bits Windows when you execute vcvarsall.bat.

```cmd
mkdir debug && cd debug
"c:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" < x64 | x86 >
cmake .. -G "NMake Makefiles"
nmake
```

Or, you can simply open a command window by clicking Windows Start -> "Visual Studio < 2019 | 2017 >" folder -> "x64 Native Tools Command Prompt for VS < 2019 | 2017 >" or "x86 Native Tools Command Prompt for VS < 2019 | 2017 >" depends what architecture your Windows is, then execute commands as follows:

```cmd
mkdir debug && cd debug
cmake .. -G "NMake Makefiles"
nmake
```

### On macOS platform

Please install XCode command line tools and cmake. Verified with XCode 11.4+ on Catalina and Big Sur.

```shell
mkdir debug && cd debug
cmake .. && cmake --build .
```

# Installing

## On Linux platform

After building successfully, TDengine can be installed by

```bash
sudo make install
```

Users can find more information about directories installed on the system in the [directory and files](https://www.taosdata.com/en/documentation/administrator/#Directory-and-Files) section. Since version 2.0, installing from source code will also configure service management for TDengine.
Users can also choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) for it.

To start the service after installation, in a terminal, use:

```bash
sudo systemctl start taosd
```

Then users can use the [TDengine shell](https://www.taosdata.com/en/getting-started/#TDengine-Shell) to connect the TDengine server. In a terminal, use:

```bash
taos
```

If TDengine shell connects the server successfully, welcome messages and version info are printed. Otherwise, an error message is shown.

### Install TDengine by apt-get

If you use Debian or Ubuntu system, you can use 'apt-get' command to install TDengine from official repository. Please use following commands to setup:

```
wget -qO - http://repos.taosdata.com/tdengine.key | sudo apt-key add -
echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-stable stable main" | sudo tee /etc/apt/sources.list.d/tdengine-stable.list
[Optional] echo "deb [arch=amd64] http://repos.taosdata.com/tdengine-beta beta main" | sudo tee /etc/apt/sources.list.d/tdengine-beta.list
sudo apt-get update
apt-cache policy tdengine
sudo apt-get install tdengine
```

## On Windows platform

After building successfully, TDengine can be installed by:

```cmd
nmake install
```

## On macOS platform

After building successfully, TDengine can be installed by:

```bash
sudo make install
```

To start the service after installation, config `.plist` file first, in a terminal, use:

```bash
sudo cp ../packaging/macOS/com.taosdata.tdengine.plist /Library/LaunchDaemons
```

To start the service, in a terminal, use:

```bash
sudo launchctl load /Library/LaunchDaemons/com.taosdata.tdengine.plist
```

To stop the service, in a terminal, use:

```bash
sudo launchctl unload /Library/LaunchDaemons/com.taosdata.tdengine.plist
```

## Quick Run

If you don't want to run TDengine as a service, you can run it in current shell. For example, to quickly start a TDengine server after building, run the command below in terminal: (We take Linux as an example, command on Windows will be `taosd.exe`)

```bash
./build/bin/taosd -c test/cfg
```

In another terminal, use the TDengine shell to connect the server:

```bash
./build/bin/taos -c test/cfg
```

option "-c test/cfg" specifies the system configuration file directory.

# Try TDengine

It is easy to run SQL commands from TDengine shell which is the same as other SQL databases.

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

# Developing with TDengine

## Official Connectors

TDengine provides abundant developing tools for users to develop on TDengine. Follow the links below to find your desired connectors and relevant documentation.

- [Java](https://www.taosdata.com/en/documentation/connector/java)
- [C/C++](https://www.taosdata.com/en/documentation/connector#c-cpp)
- [Python](https://www.taosdata.com/en/documentation/connector#python)
- [Go](https://www.taosdata.com/en/documentation/connector#go)
- [RESTful API](https://www.taosdata.com/en/documentation/connector#restful)
- [Node.js](https://www.taosdata.com/en/documentation/connector#nodejs)
- [Rust](https://www.taosdata.com/en/documentation/connector/rust)

## Third Party Connectors

The TDengine community has also kindly built some of their own connectors! Follow the links below to find the source code for them.

- [Rust Bindings](https://github.com/songtianyi/tdengine-rust-bindings/tree/master/examples)
- [.Net Core Connector](https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos)
- [Lua Connector](https://github.com/taosdata/TDengine/tree/develop/tests/examples/lua)

# How to run the test cases and how to add a new test case

TDengine's test framework and all test cases are fully open source.
Please refer to [this document](https://github.com/taosdata/TDengine/blob/develop/tests/How-To-Run-Test-And-How-To-Add-New-Test-Case.md) for how to run test and develop new test case.

# TDengine Roadmap

- Support event-driven stream computing
- Support user defined functions
- Support MQTT connection
- Support OPC connection
- Support Hadoop, Spark connections
- Support Tableau and other BI tools

# Contribute to TDengine

Please follow the [contribution guidelines](CONTRIBUTING.md) to contribute to the project.

# Join TDengine WeChat Group

Add WeChat “tdengine” to join the group，you can communicate with other users.

# [User List](https://github.com/taosdata/TDengine/issues/2432)

If you are using TDengine and feel it helps or you'd like to do some contributions, please add your company to [user list](https://github.com/taosdata/TDengine/issues/2432) and let us know your needs.
