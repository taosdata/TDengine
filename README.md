[![Build Status](https://travis-ci.org/taosdata/TDengine.svg?branch=master)](https://travis-ci.org/taosdata/TDengine)
[![Build status](https://ci.appveyor.com/api/projects/status/kf3pwh2or5afsgl9/branch/master?svg=true)](https://ci.appveyor.com/project/sangshuduo/tdengine-2n8ge/branch/master)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/TDengine/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/TDengine?branch=develop)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/4201/badge)](https://bestpractices.coreinfrastructure.org/projects/4201)
[![tdengine](https://snapcraft.io//tdengine/badge.svg)](https://snapcraft.io/tdengine)

[![TDengine](TDenginelogo.png)](https://www.taosdata.com)

# What is TDengine？

TDengine is an open-sourced big data platform under [GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html), designed and optimized for the Internet of Things (IoT), Connected Cars, Industrial IoT, and IT Infrastructure and Application Monitoring. Besides the 10x faster time-series database, it provides caching, stream computing, message queuing and other functionalities to reduce the complexity and cost of development and operation.

- **10x Faster on Insert/Query Speeds**: Through the innovative design on storage, on a single-core machine, over 20K requests can be processed, millions of data points can be ingested, and over 10 million data points can be retrieved in a second. It is 10 times faster than other databases.

- **1/5 Hardware/Cloud Service Costs**: Compared with typical big data solutions, less than 1/5 of computing resources are required. Via column-based storage and tuned compression algorithms for different data types, less than 1/10 of storage space is needed.

- **Full Stack for Time-Series Data**: By integrating a database with message queuing, caching, and stream computing features together, it is no longer necessary to integrate Kafka/Redis/HBase/Spark or other software. It makes the system architecture much simpler and more robust.

- **Powerful Data Analysis**: Whether it is 10 years or one minute ago, data can be queried just by specifying the time range. Data can be aggregated over time, multiple time streams or both. Ad Hoc queries or analyses can be executed via TDengine shell, Python, R or Matlab.

- **Seamless Integration with Other Tools**: Telegraf, Grafana, Matlab, R, and other tools can be integrated with TDengine without a line of code. MQTT, OPC, Hadoop, Spark, and many others will be integrated soon.

- **Zero Management, No Learning Curve**: It takes only seconds to download, install, and run it successfully; there are no other dependencies. Automatic partitioning on tables or DBs. Standard SQL is used, with C/C++, Python, JDBC, Go and RESTful connectors.

# Documentation
For user manual, system design and architecture, engineering blogs, refer to [TDengine Documentation](https://www.taosdata.com/en/documentation/)(中文版请点击[这里](https://www.taosdata.com/cn/documentation20/))
 for details. The documentation from our website can also be downloaded locally from *documentation/tdenginedocs-en* or *documentation/tdenginedocs-cn*.

# Building
At the moment, TDengine only supports building and running on Linux systems. You can choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) or from the source code. This quick guide is for installation from the source only.

To build TDengine, use [CMake](https://cmake.org/) 3.5 or higher versions in the project directory. 

## Install tools

### Ubuntu 16.04 and above & Debian:
```bash
sudo apt-get install -y gcc cmake build-essential git
```

### Ubuntu 14.04:
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
sudo apt-get install -y  maven
```

### Centos 7:
```bash
sudo yum install -y gcc gcc-c++ make cmake3 epel-release git
sudo yum remove -y cmake
sudo ln -s /usr/bin/cmake3 /usr/bin/cmake
```

To install openjdk-8:
```bash
sudo yum install -y java-1.8.0-openjdk
```

To install Apache Maven:
```bash
sudo yum install -y maven
```

### Centos 8 & Fedora:
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

## Get the source codes

First of all, you may clone the source codes from github:
```bash
git clone https://github.com/taosdata/TDengine.git
cd TDengine
```

The connectors for go & grafana have been moved to separated repositories,
so you should run this command in the TDengine directory to install them:
```bash
git submodule update --init --recursive
```

## Build TDengine

### On Linux platform

```bash
mkdir debug && cd debug
cmake .. && cmake --build .
```

To compile on an ARM processor (aarch64 or aarch32), please add option CPUTYPE as below:

aarch64:
```bash
cmake .. -DCPUTYPE=aarch64 && cmake --build .
```

aarch32:
```bash
cmake .. -DCPUTYPE=aarch32 && cmake --build .
```

### On Windows platform

If you use the Visual Studio 2013, please open a command window by executing "cmd.exe".
Please specify "x86_amd64" for 64 bits Windows or specify "x86" is for 32 bits Windows when you execute vcvarsall.bat.
```cmd
mkdir debug && cd debug
"C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" < x86_amd64 | x86 >
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

### On Mac OS X platform

Please install XCode command line tools and cmake. Verified with XCode 11.4+ on Catalina and Big Sur.

```shell
mkdir debug && cd debug
cmake .. && cmake --build .
```

# Quick Run

# Quick Run
To quickly start a TDengine server after building, run the command below in terminal:
```bash
./build/bin/taosd -c test/cfg
```
In another terminal, use the TDengine shell to connect the server:
```bash
./build/bin/taos -c test/cfg
```
option "-c test/cfg" specifies the system configuration file directory. 

# Installing
After building successfully, TDengine can be installed by:
```bash
make install
```
Users can find more information about directories installed on the system in the [directory and files](https://www.taosdata.com/en/documentation/administrator/#Directory-and-Files) section. It should be noted that installing from source code does not configure service management for TDengine.
Users can also choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) for it.

To start the service after installation, in a terminal, use:
```cmd
taosd
```

Then users can use the [TDengine shell](https://www.taosdata.com/en/getting-started/#TDengine-Shell) to connect the TDengine server. In a terminal, use:
```cmd
taos
```

If TDengine shell connects the server successfully, welcome messages and version info are printed. Otherwise, an error message is shown.

# Try TDengine
It is easy to run SQL commands from TDengine shell which is the same as other SQL databases.
```sql
create database db;
use db;
create table t (ts timestamp, a int);
insert into t values ('2019-07-15 00:00:00', 1);
insert into t values ('2019-07-15 01:00:00', 2);
select * from t;
drop database db;
```

# Developing with TDengine
### Official Connectors

TDengine provides abundant developing tools for users to develop on TDengine. Follow the links below to find your desired connectors and relevant documentation.

- [Java](https://www.taosdata.com/en/documentation/connector/#Java-Connector)
- [C/C++](https://www.taosdata.com/en/documentation/connector/#C/C++-Connector)
- [Python](https://www.taosdata.com/en/documentation/connector/#Python-Connector)
- [Go](https://www.taosdata.com/en/documentation/connector/#Go-Connector)
- [RESTful API](https://www.taosdata.com/en/documentation/connector/#RESTful-Connector)
- [Node.js](https://www.taosdata.com/en/documentation/connector/#Node.js-Connector)

### Third Party Connectors

The TDengine community has also kindly built some of their own connectors! Follow the links below to find the source code for them.

- [Rust Connector](https://github.com/taosdata/TDengine/tree/master/tests/examples/rust)
- [.Net Core Connector](https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos)
- [Lua Connector](https://github.com/taosdata/TDengine/tree/develop/tests/examples/lua)

# How to run the test cases and how to add a new test case?
  TDengine's test framework and all test cases are fully open source.
  Please refer to [this document](tests/How-To-Run-Test-And-How-To-Add-New-Test-Case.md) for how to run test and develop new test case.

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

