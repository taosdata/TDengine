[![TDinternal Build](https://github.com/taosdata/TDinternal/actions/workflows/tdinternal-ci-build.yml/badge.svg)](https://github.com/taosdata/TDinternal/actions/workflows/tdinternal-ci-build.yml)
[![TDinternal Test](https://github.com/taosdata/TDinternal/actions/workflows/tdinternal-test.yml/badge.svg)](https://github.com/taosdata/TDinternal/actions/workflows/tdinternal-test.yml)


# Table of Contents

1. [Introduction](#1-introduction)
1. [Documentation](#2-documentation)
1. [Prerequisites](#3-prerequisites)
    - [3.1 Prerequisites On Linux](#31-on-linux)
    - [3.2 Prerequisites On macOS](#32-on-macos)
    - [3.3 Prerequisites On Windows](#33-on-windows) 
1. [Building](#4-building)
    - [4.1 Build on Linux](#41-build-on-linux)
    - [4.2 Build on macOS](#42-build-on-macos)
    - [4.3 Build On Windows](#43-build-on-windows) 
1. [Packaging](#5-packaging)
1. [Installation](#6-installing)
    - [6.1 Install on Linux](#61-install-on-linux)
    - [6.2 Install on macOS](#62-install-on-macos)
    - [6.3 Install on Windows](#63-install-on-windows)
1. [Running](#7-running)
    - [7.1 Run TDengine on Linux](#71-run-tdengine-on-linux)
    - [7.2 Run TDengine on macOS](#72-run-tdengine-on-macos)
    - [7.3 Run TDengine on Windows](#73-run-tdengine-on-windows)
1. [Testing](#8-testing)
    - [8.1 Introduction](#81-introduction)
    - [8.2 Prerequisites](#82-prerequisites)
    - [8.3 Testing Guide](#83-testing-guide)
1. [Releasing](#9-releasing)
1. [Workflow](#10-workflow)
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

## 3.1 On Linux

<details>

<summary>Install required tools on Linux</summary>

List the software and tools required to work on the project.

- go 1.23+ (for taosadapter and taosx)
- cargo 1.82.0+ (for taosx)
- jdk 11~17, maven 3.8.0+ (for taosx plugin influxDB & openTSDB)
- node 22.14.0 (for taos-explorer)
- python 3.10.12+ (for test)

Run the script to set up the prerequisite software:

```bash
wget https://raw.githubusercontent.com/taosdata/TDengine/main/packaging/setup_env.sh
chmod +x setup_env.sh
./setup_env.sh TDinternal && ./setup_env.sh install_packages && source ~/.bashrc
```

You can also set up the prerequisite software by following the step-by-step instructions.

### 3.1.1 Install the required package

```bash
apt-get install -y llvm gcc make cmake libssl-dev pkg-config perl g++ lzma curl locales psmisc sudo tree libgeos-dev libgflags2.2 libgflags-dev libgoogle-glog-dev libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g build-essential valgrind rsync vim libjemalloc-dev openssh-server screen sshpass net-tools dirmngr gnupg apt-transport-https ca-certificates software-properties-common  r-base iputils-ping
```

### 3.1.2 Install Go

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

### 3.1.3 Install Cargo

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

### 3.1.4 Install Jdk & maven

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

### 3.1.5 Install node

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

### 3.1.6 Install Python-connector

Install Python3.

```bash
apt install python3
apt install python3-pip
```

Install the dependent Python components.

```bash
pip3 install pandas psutil fabric2 requests faker simplejson toml \
     pexpect tzlocal distro decorator loguru hyperloglog toml
```

Install the Python connector for TDengine.

```bash
pip3 install taospy taos-ws-py
```


</details>


## 3.2 On macOS

<details>

<summary>Install required tools on macOS</summary>

Work in Progress.

</details>

## 3.3 On Windows

<details>

<summary>Install required tools on Windows</summary>

List the software and tools required to work on the project.

- go 1.20+ (for taosadapter and taosx)
- cargo 1.82.0+ (for taosx)
- jdk 11~17, maven 3.8.0+ (for taosx plugin influxDB & openTSDB)
- node 22.3.0 (for taos-explorer)
- python 3.8~3.10 (for test)

Please follow the step-by-step instructions below to install the prerequisites.

### 3.3.1 Install the required package

Download Visual Studio from the following link:

```cmd
https://visualstudio.microsoft.com/zh-hans/downloads

```

Download Microsoft Visual C++ 2015-2022 Redistributable (x64) from the following link:

```cmd
https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist?view=msvc-170
```

Download and install msys2 from the following link, then set path for msys2(like C:\msys64\usr\bin):

```cmd
https://mirrors.tuna.tsinghua.edu.cn/msys2/distrib/x86_64/
```

Configure msys2 path:

```cmd
setx PATH "%PATH%;C:\msys64\usr\bin" /M
```

Download and unzip jom from the following link, then set path for jom:

```cmd
https://mirror.aarnet.edu.au/pub/qtproject/official_releases/jom/jom.zip
```

Unzip the downloaded file to the target directory,(such as C:\jom-1.1.3),then configuring Jom environment variables:

```cmd
setx PATH "%PATH%;C:\jom-1.1.3" /M
```

### 3.1.2 Install Go

Update the installation package to version 1.23.3 from the following link:

```cmd
https://golang.google.cn/dl/

```

Enter in PowerShell to check if the installation is correct:

```cmd
go version
```

Configure Go environment variables:

```cmd
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

### 3.1.3 Install Cargo

Download rustup-init.exe from the following link:

```cmd
https://win.rustup.rs/

```

Download and install open-ssl:

```cmd
git clone https://github.com/Microsoft/vcpkg.git
.\vcpkg\bootstrap-vcpkg.bat
cd vcpkg
.\vcpkg.exe install openssl:x64-windows-static-md
```
### 3.1.4 Install Jdk & maven

Visit the following link to download openjdk17 and maven, then follow the installation wizard to operate the installation.


```cmd
https://adoptium.net/temurin/releases/
https://maven.apache.org/download.cgi
```

Set environment variables of maven.

```cmd
setx MAVEN_HOME "C:\apache-maven-3.9.5" /M
setx PATH "%PATH%;%MAVEN_HOME%\bin" /M
```



### 3.1.5 Install node

Visit the following link to download node 22, then follow the installation wizard to operate the installation

```cmd
https://nodejs.org/en/download
```


### 3.1.6 Install Python-connector

Taking python 3.8.10 as an example, install Python3 from the following link:

```cmd
https://www.python.org/ftp/python/3.8.10/python-3.8.10-amd64.exe
```

Install Pip3 

```cmd
python3 -m pip install
```

Install the dependent Python components.

```cmd
pip3 install pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog toml
```

Install the Python connector for TDengine.

```cmd
pip3 install taospy taos-ws-py
```
</details>

# 4. Building

At the moment, TDengine server supports running on Linux/Windows/macOS systems. Any application can also choose the WebSocket interface provided by taosAdapter to connect the taosd service . TDengine supports X64/ARM64 CPU, and it will support MIPS64, Alpha64, ARM32, RISC-V and other CPU architectures in the future. Right now we don't support build with cross-compiling environment.

You can choose to install through source code or [installation package](http://192.168.1.252:5000/). This quick guide only applies to installing from source.

TDengine provide a few useful tools such as taosBenchmark (was named taosdemo) and taosdump. They were part of TDengine. By default, TDengine compiling does not include taosTools. You can use `cmake .. -DBUILD_TOOLS=true` to make them be compiled with TDengine.

To build TDengine, use [CMake](https://cmake.org/) 3.13.0 or higher versions in the project directory.


## 4.1 Build on Linux

<details>

<summary>Detailed steps to build on Linux</summary>

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

</details>

## 4.2 Build On macOS

<details>

<summary>Detailed steps to build on macOS</summary>


Please install XCode command line tools and cmake. Verified with XCode 11.4+ on macOS Catalina and Big Sur.
Clone TDinternal repository to a local directory (for example, /root).

```shell
cd /root
git clone git@github.com:taosdata/TDinternal.git
```

Execute the cmake command to download the community and other repositories (this may take about twenty minutes).

```shell
cd /root/TDinternal && git checkout main
mkdir /root/TDinternal/debug
cd /root/TDinternal/debug
cmake .. 
```

Select the current branch of community repositorie.

```shell
cd /root/TDinternal/community && git checkout main
```

Compile


```shell
cd ../debug
cmake .. && cmake --build .
```

</details>

## 4.3 Build On Windows

<details>

<summary>Detailed steps to build on Windows</summary>

If you use the Visual Studio 2013, please open a command window by executing "cmd.exe".
Please specify "amd64" for 64 bits Windows or specify "x86" for 32 bits Windows when you execute vcvarsall.bat.

```cmd
mkdir debug && cd debug
"C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" < amd64 | x86 >
cmake .. -G "NMake Makefiles"
jom -j 4
```

If you use the Visual Studio 2019 or 2017 or 2022:

please open a command window by executing "cmd.exe".
Please specify "x64" for 64 bits Windows or specify "x86" for 32 bits Windows when you execute vcvarsall.bat.

```cmd
mkdir debug && cd debug
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat"  < x64 | x86 >
cmake .. -G "NMake Makefiles JOM" -DBUILD_TEST=true -DBUILD_TOOLS=true
jom -j 4
```

</details>

# 5. Packaging

## 5.1 Package on Linux
<details>

<summary>Detailed steps to package on Linux</summary>

Using the following script to package the enterprise edition.

```bash
cd /root/TDinternal/enterprise/packaging
# version_number should be in the format x.x.x.x[.x], e.g., 3.3.5.0 or 3.3.5.0.1234
# if you use option "-b <branch_name>" and branch_name is not main or 3.0,
# please ensure that both TDinternal and TDengine repo have this branch.
./new_ver_release.sh -n <version_number>  
```

Once the packaging process is complete, you can find the installation package files listed below by executing the command:

```bash
ll /root/TDinternal/community/release
```
</details>

## 5.2 Package on macOS
<details>

<summary>Detailed steps to package on macOS</summary>
Using the following script to package the enterprise edition.

```bash
cd /root/TDinternal/enterprise/packaging
# version_number should be in the format x.x.x.x[.x], e.g., 3.3.5.0 or 3.3.5.0.1234
# if you use option "-b <branch_name>" and branch_name is not main or 3.0,
# please ensure that both TDinternal and TDengine repo have this branch.
./new_ver_release.sh -n <version_number>  
```

Once the packaging process is complete, you can find the installation package files listed below by executing the command:

```bash
ll /root/TDinternal/community/release
```
</details>

## 5.3 Package on Windows
<details>

<summary>Detailed steps to package on Windows</summary>
Work in Progress.
</details>

# 6. Installing

## 6.1 Install on Linux

<details>

<summary>Detailed steps to install on Linux</summary>

After building successfully, TDengine can be installed by:

```bash
sudo make install
```

Installing from source code will also configure service management for TDengine. Users can also choose to [install from packages](https://docs.tdengine.com/get-started/deploy-from-package/) for it.

```bash
tar -xvzf TDengine-enterprise-<version_number>-Linux-x64.tar.gz
cd TDengine-enterprise-<version_number>-Linux-x64
./install.sh
```

</details>

## 6.2 Install on macOS

<details>

<summary>Detailed steps to install on macOS</summary>

After building successfully, TDengine can be installed by:

```bash
sudo make install
```

</details>

## 6.3 Install on Windows

<details>

<summary>Detailed steps to install on Windows</summary>

After building successfully, TDengine can be installed by:

```cmd
jom install
```

</details>


# 7. Running


## 7.1 Run TDengine on Linux

<details>

<summary>Detailed steps to run on Linux</summary>

To start the service after installation on linux, in a terminal, use:

```bash
sudo systemctl start taosd
```

Then users can use the TDengine CLI to connect the TDengine server. In a terminal, use:

```bash
taos
```

If TDengine CLI connects the server successfully, welcome messages and version info are printed. Otherwise, an error message is shown.

If you don't want to run TDengine as a service, you can run it in current shell. For example, to quickly start a TDengine server after building, run the command below in terminal: 

```bash
./build/bin/taosd -c test/cfg
```

In another terminal, use the TDengine CLI to connect the server:

```bash
./build/bin/taos -c test/cfg
```

Option `-c test/cfg` specifies the system configuration file directory.


Running from source code will also configure service management for TDengine. Users can also choose to [install from packages](https://docs.tdengine.com/get-started/deploy-from-package/) for it.


```bash
start-all.sh
stop-all.sh
```

</details>

## 7.2 Run TDengine on macOS

<details>

<summary>Detailed steps to run on macOS</summary>

To start the service after installation on macOS, double-click the /applications/TDengine to start the program, or in a terminal, use:

```bash
sudo launchctl start com.tdengine.taosd
```

Then users can use the TDengine CLI to connect the TDengine server. In a terminal, use:

```bash
taos
```

If TDengine CLI connects the server successfully, welcome messages and version info are printed. Otherwise, an error message is shown.

</details>


## 7.3 Run TDengine on Windows

<details>

<summary>Detailed steps to run on Windows</summary>

You can start TDengine server on Windows platform with below commands:

```cmd
.\TDinternal\debug\build\bin\taosd.exe -c .\TDinternal\debug\test\cfg
```

In another terminal, use the TDengine CLI to connect the server:

```cmd
.\TDinternal\debug\build\bin\taos.exe -c .\TDinternal\debug\test\cfg
```

option "-c test/cfg" specifies the system configuration file directory.

</details>

# 8. Testing

## 8.1 Introduction

This manual is intended to give developers a comprehensive guidance to test TDengine efficiently. It is divided into three main sections: introduction, prerequisites and testing guide.

> [!NOTE]
> - The commands and scripts below are verified on Linux (Ubuntu 18.04/20.04/22.04).
> - The commands and steps described below are to run the tests on a single host.

## 8.2 Prerequisites

<details>

<summary>Detailed prerequisites on Linux</summary>

- Install Python3

```bash
apt install python3
apt install python3-pip
```

- Install Python dependencies

```bash
pip3 install pandas psutil fabric2 requests faker simplejson \
  toml pexpect tzlocal distro decorator loguru hyperloglog
```

- Install Python connector for TDengine

```bash
pip3 install taospy taos-ws-py
```

- Building

Before testing, please make sure the building operation with option `-DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true` has been done, otherwise execute commands below:

```bash
cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true
make && make install
```

</details>

## 8.3 Testing Guide

<details>

<summary>Detailed testing guide on Linux</summary>

In `tests` directory, there are different types of tests for TDengine. Below is a brief introduction about how to run them and how to add new cases.

### 8.3.1 Unit Test

Unit tests are the smallest testable units, which are used to test functions, methods or classes in TDengine code.

#### 8.3.1.1 How to run single test case?

```bash
cd debug/build/bin
./osTimeTests
```

#### 8.3.1.2 How to run all unit test cases?

```bash
cd tests/unit-test/
bash test.sh 
```

#### 8.3.1.3 How to add new cases? 

The Google test framework is used for unit testing to specific function module, please refer to steps below to add a new test case:

##### a. Create test case file and develop the test scripts

In the test directory corresponding to the target function module, create test files in CPP format and write corresponding test cases.

##### b. Update build configuration

Modify the CMakeLists.txt file in this directory to ensure that the new test files are properly included in the compilation process. See the `source/os/test/CMakeLists.txt` file for configuration examples.

##### c. Compile test code

In the root directory of the project, create a compilation directory (e.g., debug), switch to the directory and run CMake commands (e.g., `cmake .. -DBUILD_TEST=1`) to generate a compilation file,

and then run a compilation command (e.g. make) to complete the compilation of the test code. 

##### d. Execute the test program

Find the executable file in the compiled directory(e.g. `TDengine/debug/build/bin/`) and run it.

##### e. Integrate into CI tests

Use the add_test command to add new compiled test cases into CI test collection, ensure that the new added test cases can be run for every build.


### 8.3.2 System Test

System tests are end-to-end test cases written in Python from a system point of view. Some of them are designed to test features only in enterprise ediiton, so when running on community edition, they may fail. We'll fix this issue by separating the cases into different groups in the future.

#### 8.3.2.1 How to run a single test case?

Take test file `system-test/2-query/avg.py` for example:

```bash
cd tests/system-test
python3 ./test.py -f 2-query/avg.py
```

#### 8.3.2.2 How to run all system test cases?

```bash
cd tests
./run_all_ci_cases.sh -t python # all python cases

```

#### 8.3.2.3 How to add new case?


The Python test framework is developed by TDengine team, and test.py is the test case execution and monitoring of the entry program, Use `python3 ./test.py -h` to view more features.

Please refer to steps below for how to add a new test case:

##### a. Create a test case file and develop the test cases

Create a file in `tests/system-test` containing each functional directory and refer to the use case template `tests/system-test/0-others/test_case_template.py` to add a new test case. 

##### b. Execute the test case 

Ensure the test case execution is successful.

``` bash
cd tests/system-test && python3 ./test.py -f 0-others/test_case_template.py 
```

##### c. Integrate into CI tests

Edit `tests/parallel_test/cases.task` and add the testcase path and executions in the specified format. The third column indicates whether to use Address Sanitizer mode for testing.

```bash
#caseID,rerunTimes,Run with Sanitizer,casePath,caseCommand
,,n,system-test, python3 ./test.py  -f 0-others/test_case_template.py 
```


### 8.3.3 Legacy Test

In the early stage of TDengine development, test cases are run by an internal test framework called TSIM, which is developed in C++.

#### 8.3.3.1 How to run single test case?

To run the legacy test cases, please execute the following commands:

```bash
cd tests/script
./test.sh -f tsim/db/basic1.sim
```

#### 8.3.3.2 How to run all legacy test cases?

```bash
cd tests
./run_all_ci_cases.sh -t legacy # all legacy cases
```

#### 8.3.3.3 How to add new cases?

> **NOTE:**
> TSIM test framework is deprecated by system test now, it is encouraged to add new test cases in system test, please refer to [System Test](#832-system-test) for details.


### 8.3.4 Smoke Test

Smoke test is a group of test cases selected from system test, which is also known as sanity test to ensure the critical functionalities of TDengine.

#### 8.3.4.1 How to run test?

```bash
cd /root/TDinternal/community/packaging/smokeTest
./test_smoking_selfhost.sh
```

#### 8.3.4.2 How to add new cases?

New cases can be added by updating the value of `commands` variable in `test_smoking_selfhost.sh`.

### 8.3.5 Chaos Test

A simple tool to execute various functions of the system in a randomized way, hoping to expose potential problems without a pre-defined test scenario.

#### 8.3.5.1 How to run test?

```bash
cd tests/pytest
python3 crash_gen_bootstrap.py \
    --max-dbs=2 \
    --connector-type=native \
    --larger-data \
    --dynamic-db-table-names \
    --per-thread-db-connection \
    --max-steps=50 \
    --num-threads=2 \
    --continue-on-exception \
    --run-with-pkg \
    -g 0x32c,0x32d,0x3d3,0x18,0x2501,0x369,0x388,0x061a,0x2550,0x0203,0x4012
```

#### 8.3.5.2 How to add new cases?

1. Add a function, such as `TaskCreateNewFunction` in `pytest/crash_gen/crash_gen_main.py`.
2. Integrate `TaskCreateNewFunction` into the `balance_pickTaskType` function in `crash_gen_main.py`.

### 8.3.6 CI Test

CI testing (Continuous Integration testing), is an important practice in software development that aims to automate frequent integration of code into a shared codebase, build and test it to ensure code quality and stability.

TDengine CI testing will run all the test cases from the following three types of tests: unit test, system test and legacy test.

#### 8.3.6.1 How to run all CI test cases?

If this is the first time to run all the CI test cases, it is recommended to add the test branch, please run it with following commands:

```bash
cd tests
./run_all_ci_cases.sh -b main # on main branch
```

#### 8.3.6.2 How to add new cases?

Please refer to the [Unit Test](#831-unit-test)、[System Test](#832-system-test) and [Legacy Test](#833-legacy-test) sections for detailed steps to add new test cases, when new cases are added in above tests, they will be run automatically by CI test.

### 8.3.7 TSBS Test

Please refer to [TSBS Test](https://github.com/taosdata/TDengine/blob/main/tests/README.md#37-tsbs-test) in TDengine repo for details.

### 8.3.8 TestNG Test

TestNG Test is another test framework which developed by python, functionally speaking, it's a supplement for system test, and also run longer time than system test for stability testing purposes.

#### 8.3.8.1 How to run tests?

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

#### 8.3.8.2 How to add new cases?

You can add python test case under `TestNG/cases`. When the case passes in the test branch, add the case to the `testng_cases.txt` file under `TestNG/scripts`, and then merge the pr into master branch .

</details>

# 9. Releasing
<details>

<summary>Detailed releasing information</summary>

TDengine Enterprise installers can be found on the corporate NAS server:

    NAS Server URL: http://192.168.1.252:5000/
    Directory: /Release/TDengine/

NAS server write permission is enabled on `192.168.1.131`. To release, please follow steps below, take v3.3.4.0 for example:

```bash
# create the release directory first
ssh root@192.168.1.131
mkdir -p /pkgs/TDengine/3.3/v3.3.4.0/enterprise

# copy the installer to release directory
scp <installer> root@192.168.1.131:/pkgs/TDengine/3.3/v3.3.4.0/enterprise/
```
</details>

# 10. Workflow

TDengine build check workflow can be found in this [Github Action](https://github.com/taosdata/TDengine/actions/workflows/taosd-ci-build.yml). More workflows will be available soon.

# 11. Coverage

Latest TDengine test coverage report can be found on [coveralls.io](https://coveralls.io/github/taosdata/TDengine). 

<details>

<summary>How to run the coverage report locally?</summary>
To create the test coverage report (in HTML format) locally, please run following commands:

```bash
cd tests
bash setup-lcov.sh -v 1.16 && ./run_local_coverage.sh -b main -c task 
# on main branch and run cases in longtimeruning_cases.task 

```
> **NOTE:**
> Please note that the -b and -i options will recompile TDengine with the -DCOVER=true option, which may take a amount of time.

</details>

# 12. Contributing

Guidelines for contributing to the project:

- Fork the repository
- Create a feature branch
- Submit a pull request
