# taosTools

<div align="center">
<p>

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/7fb6f1cb61ab453580b69e48050dc9be)](https://app.codacy.com/gh/taosdata/taos-tools?utm_source=github.com&utm_medium=referral&utm_content=taosdata/taos-tools&utm_campaign=Badge_Grade_Settings) [![CppCheck action](https://github.com/taosdata/taos-tools/actions/workflows/cppcheck.yml/badge.svg?branch=develop)](https://github.com/taosdata/taos-tools/actions/workflows/cppcheck.yml) [![CodeQL](https://github.com/taosdata/taos-tools/actions/workflows/codeql.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/codeql.yml) [![Coverage Status](https://coveralls.io/repos/github/taosdata/taos-tools/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/taos-tools?branch=develop)
<br />
[![3.0 taosbenchmark release](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosbenchmark-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosBenchmark-release.yml) [![3.0 taosdump](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosdump-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosdump-release.yml) [![Windows (3.0 build)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-windows-build.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-windows-build.yml)
<br />
[![2.x taosBenchmark native release](https://github.com/taosdata/taos-tools/actions/workflows/2.x-taosbenchmark-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/2.x-taosbenchmark-release.yml) [![2.x taosdump Release](https://github.com/taosdata/taos-tools/actions/workflows/2.x-taosdump-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/2.x-taosdump-release.yml) [![Windows (2.x build)](https://github.com/taosdata/taos-tools/actions/workflows/2.x-windows-build.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/2.x-windows-build.yml)
</p>
</div>

taosTools are some useful tool collections for TDengine.

taosBenchmark (once named taosdemo) can be used to stress test TDengine
for full-featured writes, queries, subscriptions, etc. In 2.4.0.7 and early release, taosBenchmark is distributed within taosTools package. In later release, taosBenchmark will be included within TDengine again. Please refer to
the [taosBenchmark User Manual](https://docs.tdengine.com/reference/taosbenchmark)
for details on how to use it.

taosdump is a tool for backing up and restoring TDengine data to/from local directory.
Please refer to the [taosdump User Manual](https://docs.tdengine.com/reference/taosdump)
for details on how to use it.

## Install taosTools

## How to build from source

### Install dependencies

#### For Ubuntu/Debian system

```shell
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config libssl-dev gawk
```

#### For CentOS 7/RHEL

```shell
sudo yum install -y zlib-devel zlib-static xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libatomic-static libstdc++-static openssl-devel gawk
```

#### For CentOS 8/Rocky Linux

```shell
sudo yum install -y epel-release
sudo yum install -y dnf-plugins-core
sudo yum config-manager --set-enabled powertools
sudo yum install -y zlib-devel zlib-static xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libatomic-static libstdc++-static openssl-devel gawk
```

Note: Since snappy lacks pkg-config support (refer to [link](https://github.com/google/snappy/pull/86)),
it lead a cmake prompt libsnappy not found. But snappy will works well.

In case you encounter the issue some packages are not found due to CentOS 8 EOL, you can try following instructions first.

```
sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
```

#### For CentOS + devtoolset

Besides above dependencies, please run following commands:

```
sudo yum install centos-release-scl
sudo yum install devtoolset-9 devtoolset-9-libatomic-devel
scl enable devtoolset-9 -- bash
```

#### For macOS (only taosBenchmark for now)

```shell
brew install argp-standalone gawk
```

### Install TDengine client

Please [download the TDengine client package](https://docs.tdengine.com/releases/tdengine/)
or compile TDengine source from [GitHub](https://github.com/taosdata/TDengine)
and install to your system.

### Clone source code and build

```shell
git clone https://github.com/taosdata/taos-tools
cd taos-tools
mkdir build
cd build
cmake ..
make
```

#### build taos-tools for TDengine 2.x

```shell
...
cmake .. -DTD_VER_COMPATIBLE=2.0.0.0
make
```

### Install

```shell
sudo make install
```
