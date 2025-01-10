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

Run the script to set up the prerequisite software:

```bash
wget https://raw.githubusercontent.com/taosdata/TDengine/main/packaging/setup_env.sh
chmod +x setup_env.sh
./setup_env.sh TDinternal && ./setup_env.sh install_packages && source ~/.bashrc
```

You can also set up the prerequisite software by following the step-by-step instructions.

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

Ensure that the following content is added to your `~/.bashrc` file:

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
pip3 install pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog toml
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

Execute the cmake command to download the community and other repositories (this may take some minutes).

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

Run the following commands to compile and obtain the executables in debug/build/bin after a successful compilation.

```bash
cd /root/TDinternal/debug
cmake .. -DBUILD_TEST=true 
make -j4
```

Install
Run the following command to install the executables to /usr/bin and perform some additional configurations.

```bash
make install
```

# 5. Packaging

Using the following script to package the enterprise edition.

```bash
cd /root/TDinternal/enterprise/packaging
./new_ver_release.sh -n <version_number>   # version_number should be in the format x.x.x.x[.x], e.g., 3.3.5.0 or 3.3.5.0.1234
```

Once the packaging process is complete, you can find the installation package files listed below by executing the command:

```bash
ll /root/TDinternal/community/release
```

# 6. Installing

Get the installation package from [installation package](http://192.168.1.252:5000/) or from the packaging step and run following commands to install:

```bash
tar -xvzf TDengine-enterprise-<version_number>-Linux-x64.tar.gz
cd TDengine-enterprise-<version_number>-Linux-x64
./install.sh
```

# 7. Running

Run the following scripts to start/stop TDengine services.

```bash
start-all.sh
stop-all.sh
```

# 8. Testing

## 8.1 Run the TSIM test script

```bash
cd /root/TDinternal/community/tests/script
./test.sh -f path/to/tsimfile     #e.g. ./test.sh -f tsim/db/basic1.sim
```

## 8.2 Run the Python test script

```bash
cd /root/TDinternal/community/tests/system-test
python3 ./test.py -f path/to/pythonfile  #e.g. python3 ./test.py -f 2-query/join.py
```

## 8.3 Run unit test

```bash
cd /root/TDinternal/community/tests/unit-test
bash test.sh
```

## 8.4 Run smoke test

```bash
cd /root/TDinternal/community/packaging/smokeTest
./test_smoking_selfhost.sh
```

## 8.5 Run TSBS test

1. Clone the code and  run the tests locally on your machine. Ensure that your virtual machine supports the AVX instruction set:
```bash
cd /usr/local/src && git clone https://github.com/taosdata/tsbs-internal.git tsbs && cd tsbs &&  git checkout enh/chr-td-33357 && cd scripts/tsdbComp && ./testTsbs.sh  
```
2. When testing the client and server on separate machines, you should set up your environment as outlined in the steps below:

    2.1. Modify IP and host of client and server in `test.ini`
    ```ini
    clientIP="192.168.0.203"   # client ip
    clientHost="trd03"         # client hostname
    serverIP="192.168.0.204"   # server ip
    serverHost="trd04"         # server hostname
    ```
    2.2. Set up passwordless login between the client and server; otherwise, you'll need to configure the server password:
    ```ini
    serverPass="taosdata123"   # server root password
    ```
    2.3. Run the following command to start the test:
     ```bash
    ./testTsbs.sh  
    ```
3. When the test is done, the result can be found in `/data2/` directory, which can also be configured in `test.ini`.

## 8.6 Run Crash_gen Test

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
4. Run test script:
```bash
/root/TestNG/scripts/run.sh \
  -m /root/TestNG/scripts/testng.json \
  -t /root/TestNG/scripts/testng_cases.txt \
  -l /root/TestNG/testlog_$(date +"%Y-%m-%d_%H-%M-%S") \
  -d debug -o 12000 -f False -a True
```
5. When the test is done, the result can be found in `/root/TestNG/testlog_$(date +"%Y-%m-%d_%H-%M-%S")` directory.

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

We use jenkins for CI/CD workflow configuration. See http://ci.bl.taosdata.com:8080/job/NewTest/view/change-requests/ (need login first)
We can also run ci script locally.

```bash
cd /root/TDinternal/community/tests
chmod +x run_all_ci_cases.sh && ./run_all_ci_cases.sh   # use -d $TDENGINE_DIR option if not use default /root/TDinternal/community
```

# 11 Coverage

We can see coverage result in https://coveralls.io/github/taosdata/TDengine
We can also run coverage script locally with following commands. Please note that the -b and -i options will recompile TDengine with the -DCOVER=true option, which may take a considerable amount of time.

```bash
cd /root/TDinternal/community/tests
bash setup-lcov.sh -v 1.16 && ./run_local_coverage.sh -d [TDengine dir] -b [Test branch] -i [Build test branch] -f [TDengine gcda dir] -c [Test single case/all cases] -u [Unit test case] -l [Lcov dir]     # for more infomation about options please refer to ./run_local_coverage.sh -h
```

# 12 Contributing

Guidelines for contributing to the project:

- Fork the repository
- Create a feature branch
- Submit a pull request
