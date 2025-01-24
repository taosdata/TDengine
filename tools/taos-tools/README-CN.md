# taosTools

[![CI](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/taos-tools/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/taos-tools?branch=develop)

taosTools 是用于 TDengine 的辅助工具软件集合。

taosBenchmark （曾命名为 taosdemo）可以用于对 TDengine 进行全功能的写入、查询、订阅等功能的压力测试。taosBenchmark 在 TDengine 2.4.0.7 和之前发布版本在 taosTools 安装包中发布提供，在后续版本中 taosBenchmark 将在 TDengine 标准安装包中发布。详细使用方法请参考[taosBenchmark 用户手册](https://docs.taosdata.com/reference/taosbenchmark)。

taosdump 是用于备份 TDengine 数据到本地目录和从本地目录恢复数据到 TDengine 的工具。详细使用方法请参考[taosdump 用户手册](https://docs.taosdata.com/reference/taosdump)。

## 安装 taosTools

## 如何通过源代码构建

### 安装依赖软件包

#### 对于 Ubuntu/Debian 系统

```shell
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config libssl-dev
```

#### 对于 CentOS 7/RHEL 系统

```shell
sudo yum install -y zlib-devel zlib-static xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libatomic-static libstdc++-static openssl-devel
```

#### 对于 CentOS 8/Rocky Linux 系统

```shell
sudo yum install -y epel-release
sudo yum install -y dnf-plugins-core
sudo yum config-manager --set-enabled powertools
sudo yum install -y zlib-devel zlib-static xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libatomic-static libstdc++-static openssl-devel
```

注意：由于 snappy 缺乏 pkg-config 支持
（参考 [链接](https://github.com/google/snappy/pull/86)），会导致
cmake 提示无法发现 libsnappy，实际上工作正常。

如果有些包由于 CentOS 8 EOL 的问题无法下载，可以尝试先执行如下命令：

```
sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
```

#### 对于 CentOS + devtoolset 系统

除上述编译依赖包，需要执行以下命令：

```
sudo yum install centos-release-scl
sudo yum install devtoolset-9 devtoolset-9-libatomic-devel
scl enable devtoolset-9 -- bash
```

#### 对于 macOS 系统（目前仅支持 taosBenchmark）

```shell
brew install argp-standalone
```

### 安装 TDengine 客户端软件

请从 [taosdata.com](https://www.taosdata.com/cn/all-downloads/) 下载
TDengine 客户端安装或参考 [GitHub](github.com/taosdata/TDengine)
编译安装 TDengine 到您的系统中。

### 克隆源码并编译

```shell
git clone https://github.com/taosdata/taos-tools
cd taos-tools
git submodule update --init --recursive
mkdir build
cd build
cmake ..
make
```

### 安装

```shell
sudo make install
```
