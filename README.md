# Table of Contents

1. [Introduction](#Introduction)
2. [Documentation](#Documentation)
3. [Building](#Building)
4. [Packaging](#Packaging)
5. [Contributing](#Contributing)

# Introduction

TDengine Enterprise includes all features of TDengine OSS and further helps industrial customers.

- Benefit from improved performance in large-scale industrial scenarios
- Retain affordable access to all your data with tiered storage, including S3
- Mount multiple storage media to each tier for faster data ingestion
- Merge and split vnodes to balance load efficiently

For a full list of TDengine competitive advantages, please [check here](https://tdengine.com/enterprise/). The easiest way to experience TDengine is through [TDengine Cloud](https://cloud.tdengine.com). 

# Documentation

For user manual, system design and architecture, please refer to [TDengine Documentation](https://docs.tdengine.com) ([TDengine 文档](https://docs.taosdata.com)).

# Building

At the moment, TDengine server supports running on Linux/Windows/macOS systems. Any application can also choose the WebSocket interface provided by taosAdapter to connect the taosd service . TDengine supports X64/ARM64 CPU, and it will support MIPS64, Alpha64, ARM32, RISC-V and other CPU architectures in the future. Right now we don't support build with cross-compiling environment.

You can choose to install through source code or [installation package](http://192.168.1.252:5000/). This quick guide only applies to installing from source.

## Prerequisites

List the software and tools required to work on the project.

- go 1.20+ (for taosadapter and taosx)
- cargo 1.82.0+ (for taosx)
- jdk 11~17, maven 3.8.0+ (for taosx plugin influxDB & openTSDB)
- node 16.20.2 (for taos-explorer)
- python 3.10.12+ (for test)

Step-by-step instructions to set up the prerequisites software.

### Install the required package

```bash
apt-get install -y llvm gcc make cmake libssl-dev pkg-config perl g++ lzma curl locales psmisc sudo tree libgeos-dev libgflags2.2 libgflags-dev libgoogle-glog-dev libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g build-essential valgrind rsync vim libjemalloc-dev openssh-server screen sshpass net-tools dirmngr gnupg apt-transport-https ca-certificates software-properties-common  r-base iputils-ping 
```

### Install Go

Update the installation package to version 1.23.3.
```bash
cd /usr/local/ 
wget https://golang.google.cn/dl/go1.23.3.linux-amd64.tar.gz 
rm -rf /usr/local/go && tar -C /usr/local -xzf go1.23.3.linux-amd64.tar.gz
```

Set up environment variables, first add the following content to the end of the `~/.bashrc` file.
```bash
export GO_HOME=/usr/local/go
export PATH=$GO_HOME/bin:$PATH
export CGO_ENABLED=1
```

Then make the environment variables take effect.
```bash
source ~/.bashrc
```

Configure proxy to accelerate the download of Go dependencies.
```bash
go env -w GOPROXY=https://goproxy.cn,direct
go env -w GO111MODULE=on
```

Check if the environment variables have taken effect and if the version is the installed version.
```bash
go env
go version
```

### Install Cargo

Better start it from [rustup](https://rustup.rs/)(the installer for Rust).
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Set up environment variables, first add the following content to the end of the `~/.bashrc` file.
```bash
export RUSTUP_DIST_SERVER="https://rsproxy.cn"
export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
export PATH=$HOME/.cargo/bin:$PATH
```

Then make the environment variables take effect.
```bash
source ~/.bashrc
```

Modify the cargo configuration source by creating a `~/.cargo/config` file and adding the following content.
```yaml
[source.crates-io]
#registry = "GitHub - rust-lang/crates.io-index: Registry index for crates.io"

#replace-with = 'ustc'
#replace-with = 'sjtu'
replace-with = 'rsproxy-sparse'
[source.rsproxy]
registry = "https://rsproxy.cn/crates.io-index"
[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"

[registries.rsproxy]
index = "https://rsproxy.cn/crates.io-index"

[net]
git-fetch-with-cli = true

[source.tuna]
registry = "https://mirrors.tuna.tsinghua.edu.cn/git/crates.io-index.git"

[source.ustc]
registry = "git://mirrors.ustc.edu.cn/crates.io-index"

[source.sjtu]
registry = "https://mirrors.sjtug.sjtu.edu.cn/git/crates.io-index"

[source.rustcc]
registry = "git://crates.rustcc.cn/crates.io-index"
```

Install the cargo-make component.
```bash
cargo install cargo-make
```

### Install Jdk & maven

Install JDK & maven

```bash
apt install openjdk-17-jdk
wget https://dlcdn.apache.org/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz
tar -C /usr/local -xzf apache-maven-3.8.8-bin.tar.gz
```

Set up environment variables, first add the following content to the end of the `~/.bashrc` file.

```bash
export PATH=$PATH:/usr/local/apache-maven-3.8.8/bin
```

Then make the environment variables take effect.

```bash
source ~/.bashrc
```

### Install node

Recommend install node using nvm.

``` bash
curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.33.8/install.sh | bash
```

Set up environment variables, add the following content to the end of the `~/.bashrc` file.

```bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
```

Then make the environment variables take effect.

```bash
source ~/.bashrc
```

Finally, Install node and yarn.

```bash
nvm install 16.20.2
npm config set registry=https://registry.npmmirror.com
npm install -g yarn
```

### Install Python-connector

Install Python3.
```bash
apt install python3
apt install python3-pip
```

Install the dependent Python components.
```bash
pip3 install pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog
```

Install the Python connector for TDengine.
```bash
pip3 install taospy taos-ws-py
```

## Building the Project

Clone TDinternal repository to a local directory (for example, /root).
```bash
cd /root
git clone git@github.com:taosdata/TDinternal.git 
```

Execute the cmake command to download the community and other repositories (this may take about twenty minutes).
```bash
cd /root/TDinternal && git checkout main
mkdir /root/TDinternal/debug
cd /root/TDinternal/debug
cmake .. -DBUILD_TEST=true
```

Select the current branch of community repositorie.
```bash
cd /root/TDinternal/community && git checkout main
```

Compile
```bash
cd /root/TDinternal/debug
cmake .. -DBUILD_TEST=true
make -j4
```

Install
```bash
make install
```

# Packaging

Using the following script to package the enterprise edition.
```bash
cd /root/TDinternal/enterprise/packaging
./new_ver_release.sh -n <version_number>
```

After the packaging is complete, you can see the following files
```bash
ll /root/TDinternal/community/release
```

Using the following script to package doc zip if you want to build docs zip
```bash
cd /root/TDinternal/enterprise/packaging
pip3 install GitPython
python3 build.py -b 'main' -ez -nu 
```

After the packaging is complete, you can see the following files
```bash
ll /root/TDinternal/enterprise/packaging/docs-zh.zip
ll /root/TDinternal/enterprise/packaging/docs-en.zip
```

# Testing

## Run the TSIM test script

```bash
cd /root/TDinternal/community/tests/script
./test.sh -f tsim/db/basic1.sim
```

## Run the Python test script

```bash
cd /root/TDinternal/community/tests/system-test
python3 ./test.py -f 2-query/floor.py
```

## Run unittest

```bash
cd /root/TDinternal/debug
ctest
```

## Continuous Integration

Temporarily unable to work, adjustments are being made.

```bash
cd /root/TDinternal/community/tests
./run_all_ci_cases.sh
```

## Smoke Testing

```bash
cd /root/TDinternal/community/packaging/smokeTest
./test_smoking_selfhost.sh
```

# Contributing

Guidelines for contributing to the project:
- Fork the repository
- Create a feature branch
- Submit a pull request
