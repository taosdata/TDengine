# Table of Contents

1. [Introduction](#1-introduction)
1. [Prerequisites](#2-prerequisites)
1. [Testing Guide](#3-testing-guide)
    - [3.1 Unit Test](#31-unit-test)
    - [3.2 System Test](#32-system-test)
    - [3.3 Legacy Test](#33-legacy-test)
    - [3.4 Smoke Test](#34-smoke-test)
    - [3.5 Chaos Test](#35-chaos-test)
    - [3.6 CI Test](#36-ci-test)
    - [3.7 TSBS Test](#37-tsbs-test)

# 1. Introduction

This manual is intended to give developers a comprehensive guidance to test TDengine efficiently. It is divided into three main sections: introduction, prerequisites and testing guide.

> [!NOTE]
> - The commands and scripts below are verified on Linux (Ubuntu 18.04/20.04/22.04).
> - [taos-connector-python](https://github.com/taosdata/taos-connector-python) is used by tests written in Python, which requires Python 3.7+.
> - The commands and steps described below are to run the tests on a single host.
> - The testing framework is currently compatible with Python versions 3.8 through 3.10.
> - Vitural Environment is advised when setting up the environment, please refer to [venv](https://docs.python.org/3/library/venv.html) for details.


# 2. Prerequisites

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

# 3. Testing Guide

In `tests` directory, there are different types of tests for TDengine. Below is a brief introduction about how to run them and how to add new cases.

### 3.1 Unit Test

Unit tests are the smallest testable units, which are used to test functions, methods or classes in TDengine code.

### 3.1.1 How to run single test case?

```bash
cd debug/build/bin
./osTimeTests
```

### 3.1.2 How to run all unit test cases?

```bash
cd tests/unit-test/
bash test.sh -e 0
```

### 3.1.3 How to add new cases? 

<details>

<summary>Detailed steps to add new unit test case</summary>

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

</details>

## 3.2 System Test

System tests are end-to-end test cases written in Python from a system point of view. Some of them are designed to test features only in enterprise ediiton, so when running on community edition, they may fail. We'll fix this issue by separating the cases into different groups in the future.

### 3.2.1 How to run a single test case?

Take test file `system-test/2-query/avg.py` for example:

```bash
cd tests/system-test
python3 ./test.py -f 2-query/avg.py
```

### 3.2.2 How to run all system test cases?

```bash
cd tests
./run_all_ci_cases.sh -t python # all python cases
```

### 3.2.3 How to add new case?

<details>

<summary>Detailed steps to add new system test case</summary>

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

</details>

## 3.3 Legacy Test

In the early stage of TDengine development, test cases are run by an internal test framework called TSIM, which is developed in C++.

### 3.3.1 How to run single test case?

To run the legacy test cases, please execute the following commands:

```bash
cd tests/script
./test.sh -f tsim/db/basic1.sim
```

### 3.3.2 How to run all legacy test cases?

```bash
cd tests
./run_all_ci_cases.sh -t legacy # all legacy cases
```

### 3.3.3 How to add new cases?

> [!NOTE] 
> TSIM test framework is deprecated by system test now, it is encouraged to add new test cases in system test, please refer to [System Test](#32-system-test) for details.

## 3.4 Smoke Test

Smoke test is a group of test cases selected from system test, which is also known as sanity test to ensure the critical functionalities of TDengine.

### 3.4.1 How to run test?

```bash
cd /root/TDengine/packaging/smokeTest
./test_smoking_selfhost.sh
```

### 3.4.2 How to add new cases?

New cases can be added by updating the value of `commands` variable in `test_smoking_selfhost.sh`.

## 3.5 Chaos Test

A simple tool to execute various functions of the system in a randomized way, hoping to expose potential problems without a pre-defined test scenario.

### 3.5.1 How to run test?

```bash
cd tests/pytest
python3 auto_crash_gen.py
```

### 3.5.2 How to add new cases?

1. Add a function, such as `TaskCreateNewFunction` in `pytest/crash_gen/crash_gen_main.py`.
2. Integrate `TaskCreateNewFunction` into the `balance_pickTaskType` function in `crash_gen_main.py`.

## 3.6 CI Test

CI testing (Continuous Integration testing), is an important practice in software development that aims to automate frequent integration of code into a shared codebase, build and test it to ensure code quality and stability.

TDengine CI testing will run all the test cases from the following three types of tests: unit test, system test and legacy test.

### 3.6.1 How to run all CI test cases?

If this is the first time to run all the CI test cases, it is recommended to add the test branch, please run it with following commands:

```bash
cd tests
./run_all_ci_cases.sh -b main # on main branch
```

### 3.6.2 How to add new cases?

Please refer to the [Unit Test](#31-unit-test)、[System Test](#32-system-test) and [Legacy Test](#33-legacy-test) sections for detailed steps to add new test cases, when new cases are added in above tests, they will be run automatically by CI test.


## 3.7 TSBS Test

[Time Series Benchmark Suite (TSBS)](https://github.com/timescale/tsbs) is an open-source performance benchmarking platform specifically designed for time-series data processing systems, such as databases. It provides a standardized approach to evaluating the performance of various databases by simulating typical use cases such as IoT and DevOps.

### 3.7.1 How to run tests?


TSBS test can be started locally by running command below. Ensure that your virtual machine supports the AVX instruction set. 
You need to use sudo -s to start a new shell session as the superuser (root) in order to begin the testing:

```bash
cd /usr/local/src && \
git clone https://github.com/taosdata/tsbs.git && \
cd tsbs && \
git checkout enh/add-influxdb3.0 && \
cd scripts/tsdbComp && \
./tsbs_test.sh -s scenario4
```

> [!NOTE]
> 1. TSBS test is written in Golang. If you are unable to connect to the [international Go proxy](https://proxy.golang.org), the script will automatically set it to the [china Go proxy](https://goproxy.cn).
> 2. If you need to cancel this china Go proxy, you can execute the following command in your environment `go env -u GOPROXY`.
> 3. To check your current Go proxy setting, please run `go env | grep GOPROXY`.

### 3.7.2 How to start client and server on different hosts?

By default, both client and server will be started on the local host. To start the client and server on separate hosts, please follow steps below to configure `test.ini` before starting the test:

1. Modify IP and hostname of client and server in `test.ini`:

```ini
clientIP="192.168.0.203"   # client ip
clientHost="trd03"         # client hostname
serverIP="192.168.0.204"   # server ip
serverHost="trd04"         # server hostname
```

2. Passwordless ssh login between the client and server is required; otherwise, please set the ssh login password in `test.int`, for example:

```ini
serverPass="taosdata123"   # server root password
```

### 3.7.3 Check test results

When the test is done, the result can be found in `${installPath}/tsbs/scripts/tsdbComp/log/` directory, which ${installPath} can be configured in `test.ini`.

### 3.7.4 Test more scenario

Use `./tsbs_test.sh -h` to  get more test scenarios.

