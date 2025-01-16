# Table of Contents

1. [Introduction](#1-introduction)
1. [Prerequisites](#2-prerequisites)
1. [Testing Guide](#3-testing-guide)
    1. [Unit Test](#31-unit-test)
    1. [System Test](#32-system-test)
    1. [Smoke Test](#33-smoke-test)
    1. [Legacy Test](#34-legacy-test)
    1. [Chaos Test](#35-chaos-test)
    1. [TSBS Test](#36-tsbs-test)

# 1. Introduction

This manual is intended to provide users with comprehensive guidance to help them verify the TDengine function efficiently. The document is divided into three main sections: introduction, prerequisites and testing guide.

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

Please make sure building operation with option `-DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true` has been finished, otherwise execute commands below:

```bash
cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true
make && make install
```

# 3. Testing Guide

In `tests` directory, there are different types of tests for TDengine. Below is a brief introduction about how to run them and how to add new cases.

## 3.1. Unit Test

Unit test script is the smallest testable part and developed for some function, method or class of TDengine.

### How to run tests?

```bash
cd tests/unit-test/
bash test.sh -e 0
```

### How to add new cases?

Copy from the old version, need updates:
You can add sim test case under tests/script, python test case under tests/system-test or tests/army. When the case passes in the test branch, add the case to the cases.task file under tests/parallel_test, and then merge the pr into main branch to run in the future CI.

## 3.2. System Test

Python test script includes almost all of the functions of TDengine, so some test case maybe fail cause the function only
work for TDengine Enterprise Edition.

### How to run tests?

```bash
cd tests/system-test
python3 ./test.py -f 2-query/floor.py
```

### How to add new cases?

[Placeholder]

## 3.3. Smoke Test

Smoke test script is known as sanity testing to ensure that the critical functionalities of TDengine.

### How to run tests?

```bash
cd /root/TDengine/packaging/smokeTest
./test_smoking_selfhost.sh
```

### How to add new cases?

[Placeholder]

## 3.4. Legacy Test

In the early stage of TDengine development, test cases are run by an internal test framework called TSIM, which is developed in C++.

### How to run tests?

To run the legacy test cases, please execute the following commands:

```bash
cd tests/script
./test.sh -f tsim/db/basic1.sim
```

### How to add new cases?

[Placeholder]

## 3.5. Chaos Test

[Desciprtion]

### How to run tests?

[Placeholder]

### How to add new cases?

[Placeholder]

## 3.6. TSBS Test

[Time Series Benchmark Suite (TSBS)](https://github.com/timescale/tsbs) is an open-source performance benchmarking platform specifically designed for time-series data processing systems, such as databases. It provides a standardized approach to evaluating the performance of various databases by simulating typical use cases such as IoT and DevOps.

### How to run tests?

1. Clone the code and  run the tests locally on your machine. Ensure that your virtual machine supports the AVX instruction set:
```bash
  cd /usr/local/src && git clone https://github.com/taosdata/tsbs-internal.git tsbs && \
  cd tsbs &&  git checkout enh/chr-td-33357 && \
  cd scripts/tsdbComp && ./testTsbs.sh  
```
> [!NOTE]
> The default case type, CPU test, takes a long time to run.
> 
> If you want to run quick test to verify the test environment, please set `caseType=cputest` in test.ini .


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
