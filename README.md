# Table of Contents

1. [Introduction](#1-introduction)
1. [Documentation](#2-documentation)
1. [Prerequisites](#3-prerequisites)
1. [Building](#4-building)
1. [Packaging](#5-packaging)
1. [Installation](#6-installing)
1. [Running](#7-running)
1. [Testing](#8-testing)
1. [Releasing](#9-releasing)
1. [CI/CD](#10-cicd)
1. [Coverage](#11-coverage)
1. [Contributing](#12-contributing)

# 1. Introduction

TDengine Enterprise includes all features of TDengine OSS and further helps industrial customers.

- Benefit from improved performance in large-scale industrial scenarios
- Retain affordable access to all your data with tiered storage, including S3
- Mount multiple storage media to each tier for faster data ingestion
- Merge and split vnodes to balance load efficiently

For a full list of TDengine competitive advantages, please [check here](https://tdengine.com/enterprise/). The easiest way to experience TDengine is through [TDengine Cloud](https://cloud.tdengine.com).

# 2. Documentation

For user manual, system design and architecture, please refer to [TDengine Documentation](https://docs.tdengine.com/next) ([TDengine 文档](https://docs.taosdata.com/next)).

# 3. Prerequisites

List the software and tools required to work on the project.

- go 1.20+ (for taosadapter and taosx)
- cargo 1.82.0+ (for taosx)
- jdk 11~17, maven 3.8.0+ (for taosx plugin influxDB & openTSDB)
- node 16.20.2 (for taos-explorer)
- python 3.10.12+ (for test)

Step-by-step instructions to set up the prerequisites software.

## 3.1 Install the required package

```bash
apt-get install -y llvm gcc make cmake libssl-dev pkg-config perl g++ lzma curl locales psmisc sudo tree libgeos-dev libgflags2.2 libgflags-dev libgoogle-glog-dev libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g build-essential valgrind rsync vim libjemalloc-dev openssh-server screen sshpass net-tools dirmngr gnupg apt-transport-https ca-certificates software-properties-common  r-base iputils-ping
```

## 3.2 Install Go

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

## 3.3 Install Cargo

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

## 3.4 Install Jdk & maven

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

## 3.5 Install node

Recommend install node using nvm.

```bash
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

## 3.6 Install Python-connector

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

# 4. Building

At the moment, TDengine server supports running on Linux/Windows/macOS systems. Any application can also choose the WebSocket interface provided by taosAdapter to connect the taosd service . TDengine supports X64/ARM64 CPU, and it will support MIPS64, Alpha64, ARM32, RISC-V and other CPU architectures in the future. Right now we don't support build with cross-compiling environment.

You can choose to install through source code or [installation package](http://192.168.1.252:5000/). This quick guide only applies to installing from source.

## 4.1 Building the Project

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

# 5. Packaging

Using the following script to package the enterprise edition.

```bash
cd /root/TDinternal/enterprise/packaging
./new_ver_release.sh -n <version_number>
```

After the packaging is complete, you can see the following files.

```bash
ll /root/TDinternal/community/release
```

# 6. Installing

```bash
tar -xvzf TDengine-enterprise-<version_number>-Linux-x64.tar.gz
cd TDengine-enterprise-<version_number>-Linux-x64
./install.sh
```

# 7. Running

```bash
cd TDengine-enterprise-<version_number>-Linux-x64
./start-all.sh
./stop-all.sh
```

# 8. Testing

## 8.1 Run the TSIM test script

```bash
cd /root/TDinternal/community/tests/script
./test.sh -f tsim/db/basic1.sim
```

## 8.2 Run the Python test script

```bash
cd /root/TDinternal/community/tests/system-test
python3 ./test.py -f 2-query/avg.py
```

## 8.3 Run unittest

```bash
cd /root/TDinternal/community/tests/unit-test/
bash test.sh
```

## 8.4 Smoke Testing

```bash
cd /root/TDinternal/community/packaging/smokeTest
./test_smoking_selfhost.sh
```

## 8.5 TSBS Test

1. Clone the code
```bash
cd /root && git clone https://github.com/taosdata/tsbs.git && cd tsbs/scripts/tsdbComp
```
2. Modify IP and host of client and server in `test.ini`
```ini
clientIP="192.168.0.203"   # client ip
clientHost="trd03"         # client hostname
serverIP="192.168.0.204"   # server ip
serverHost="trd04"         # server hostname
```
3. Set up passwordless login between the client and server; otherwise, you'll need to configure the server password:
```ini
serverPass="taosdata123"   # server root password
```
4. Run the following command to start the test:
 ```bash
nohup bash tsdbComparison.sh > test.log &
```
5. When the test is done, the result can be found in `/data2/` directory, which can also be configured in `test.ini`.

## 8.6 Crash_gen Test

```bash
cd /root/TDinternal/community/tests/pytest/ && ./crash_gen.sh
```

## 8.7 TestNG Test

1. Clone the code:
```bash
cd /root && \
  git clone -b master https://github.com/taosdata/taos-test-framework && \
  git clone -b master https://github.com/taosdata/TestNG
```
2. Build taostest:
```bash
apt install -y python3-pip && \
  pip3 install poetry && \
  cd /root/taos-test-framework && \
  yes | bash reinstall.sh && \
  pip3 install --upgrade numpy pandas
```
3. Configure passwdless login:
```bash
[ ! -f "$HOME/.ssh/id_rsa" ] && yes | ssh-keygen -t rsa -b 2048 -N "" -f $HOME/.ssh/id_rsa
[ -f "$HOME/.ssh/id_rsa.pub" ] && \
  ! grep -q -F "$(cat $HOME/.ssh/id_rsa.pub)" "$HOME/.ssh/authorized_keys" && \
  cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
```
4. How to add test case:

You can add python test case under TestNG/cases. When the case passes in the test branch, add the case to the testng_cases.txt file under TestNG/scripts, and then merge the pr into master branch .

5. Run test script:
```bash
/root/TestNG/scripts/run.sh \
  -m /root/TestNG/scripts/testng.json \
  -t /root/TestNG/scripts/testng_cases.txt \
  -l /root/TestNG/testlog_$(date +"%Y-%m-%d_%H-%M-%S") \
  -d debug -o 12000 -f False -a True
```
6. When the test is done, the result can be found in `/root/TestNG/testlog_$(date +"%Y-%m-%d_%H-%M-%S")` directory.

# 9 Releasing

TDengine Enterprise installers can be found on the corporate NAS server:

    NAS Server URL： http://192.168.1.252:5000/
    Directory: /Release/TDengine/

NAS server write permission is enabled on `192.168.1.131`. To release, please follow steps below, take v3.3.4.0 for example:

```bash
# create the release directory first
ssh root@192.168.1.131
mkdir -p /pkgs/TDengine/3.3/v3.3.4.0/enterprise

# copy the installer to release directory
scp <installer> root@192.168.1.131:/pkgs/TDengine/3.3/v3.3.4.0/enterprise/
```

# 10 CI/CD

Now, Jenkins is mainly used to build CI/CD pipeline for TDengine. To run the tests in the CI/CD pipeline, please run following commands:

```bash
cd tests
./run_all_ci_cases.sh -b main # on main branch
```

TDengine build check workflow can be found in this [Github Action](https://github.com/taosdata/TDengine/actions/workflows/taosd-ci-build.yml).

# 11 Coverage

Latest TDengine test coverage report can be found on [coveralls.io](https://coveralls.io/github/taosdata/TDengine). To create the test coverage report (in HTML format) locally, please run following commands:

```bash
cd tests
bash setup-lcov.sh -v 1.16 && ./run_local_coverage.sh -b main -c task 
# on main branch and run cases in longtimeruning_cases.task 
# for more infomation about options please refer to ./run_local_coverage.sh -h
```
> [!NOTE]
> Please note that the -b and -i options will recompile TDengine with the -DCOVER=true option, which may take a amount of time.

# 12 Contributing

Guidelines for contributing to the project:

- Fork the repository
- Create a feature branch
- Submit a pull request
