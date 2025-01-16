# Table of Contents

1. [Introduction](#1-introduction)
1. [Prerequisites](#2-prerequisites)
1. [Testing Guide](#3-testing-guide)
    - [3.1 CI Test](#31-ci-test)
      - [3.1.1 Unit Test](#311-unit-test)
      - [3.1.2 System Test](#312-system-test)
      - [3.1.3 Legacy Test](#313-legacy-test)
    - [3.2 Smoke Test](#32-smoke-test)
    - [3.3 Chaos Test](#33-chaos-test)


# 1. Introduction

This manual is intended to provide users with comprehensive guidance to help them verify the TDengine function efficiently. The document is divided into three main sections: introduction, prerequisites and testing guide.

> [!NOTE]
> The below commands and test scripts are verified on linux (Ubuntu 18.04、20.04、22.04) locally.

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


## 3.1 CI Test

[Desciprtion]

### How to run tests?

If this is the first time to run all the CI tests, it is recommended to add the test branch, please run like following commands:

```bash
cd tests
./run_all_ci_cases.sh -b main # on main branch
```

### How to add new cases?

[You can add sim test case under tests/script, python test case under tests/system-test or tests/army. When the case passes in the test branch, add the case to the cases.task file under tests/parallel_test, and then merge the pr into main branch to run in the future CI.]


### 3.1.1 Unit Test

Unit test script is the smallest testable part and developed for some function, method or class of TDengine.

### How to run single test case?

```bash
cd debug/build/bin
./osTimeTests 
```

### How to run all unit test cases?

```bash
cd tests/unit-test/
bash test.sh -e 0
```

#### How to add new cases?

Copy from the old version, need updates:


## 3.1.2 System Test

Python test script includes all of the functions of TDengine OSS, so some test case maybe fail cause the function only
work for TDengine Enterprise Edition.

### How to run single test case?

```bash
cd tests/system-test
python3 ./test.py -f 2-query/floor.py
```

### How to run all system test cases?

```bash
cd tests
./run_all_ci_cases.sh -t python # all python cases
```

### How to add new case?

[Placeholder]

## 3.1.3 Legacy Test

In the early stage of TDengine development, test cases are run by an internal test framework called TSIM, which is developed in C++.

### How to run single test case?

To run the legacy test cases, please execute the following commands:

```bash
cd tests/script
./test.sh -f tsim/db/basic1.sim
```

### How to run all legacy test cases?

```bash
cd tests
./run_all_ci_cases.sh -t legacy # all legacy cases
```

### How to add new case?

[Placeholder]


## 3.2 Smoke Test

Smoke test script is known as sanity testing to ensure that the critical functionalities of TDengine.

### How to run test?

```bash
cd /root/TDengine/packaging/smokeTest
./test_smoking_selfhost.sh
```

### How to add new case?

[Placeholder]

## 3.3 Chaos Test

[Desciprtion]

### How to run test?

[Placeholder]

### How to add new case?

[Placeholder]
