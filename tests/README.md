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

### 3.1 Unit Test

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

<details>

<summary>Detailed steps to add new unit test case</summary>

The Google test framwork is used for unit testing to specific function module, you can refer below steps to add one test case:

##### 1. Create test case file and develop the test scripts

In the test directory corresponding to the target function module, create test files in CPP format and write corresponding test cases.

##### 2. Update build configuration

Modify the CMakeLists.txt file in this directory to ensure that the new test files are properly included in the compilation process. See the `source/os/test/CMakeLists.txt` file for configuration examples.

##### 3. Compile test code

In the root directory of the project, create a compilation directory (e.g., debug), switch to the directory and run CMake commands (e.g., `cmake .. -DBUILD_TEST=1`) to generate a compilation file, and then run a compilation command (e.g. make) to complete the compilation of the test code. 

##### 4. Execute the test program

Find the executable file in the compiled directory(e.g. `TDengine/debug/build/bin/`) and run it.

##### 5. Integrate into CI tests

Use the add_test command to add new compiled test cases into CI test collection, ensure that the new added test cases can be run for every build.

</details>

## 3.2 System Test

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

<details>

<summary>Detailed steps to add new system test case</summary>

The Python test framework is developed by TDengine teams, and test.py is the test case execution and monitoring of the entry program, Use `python3  ./test.py -h` to view more features.
you can refer below steps to add one test case:

##### 1.Create a test case file and develop the test cases

Create a file in `tests/system-test` containing each functional directory and refer to the use case template `tests/system-test/0-others/test_case_template.py` to add a new test case. 

##### 2.Execute the test case 

cd tests/system-test & python3 ./test.py  -f 0-others/test_case_template.py 

##### 3.Integrate into CI tests

Edit `tests/parallel_test/cases.task` and add the testcase path and executions in the specified format. The third column indicates whether to use Address Sanitizer mode for testing.



```bash
#caseID,rerunTimes,Run with Sanitizer,casePath,caseCommand
,,n,system-test, python3 ./test.py  -f 0-others/test_case_template.py 
```

</details>

## 3.3 Legacy Test

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

> [!NOTE] 
> TSIM test framwork is replaced by system test currently, suggest to add new test scripts to system test, you can refer [System Test](#32-system-test) for detail steps.

## 3.4 Smoke Test

Smoke test script is from system test and known as sanity testing to ensure that the critical functionalities of TDengine.

### How to run test?

```bash
cd /root/TDengine/packaging/smokeTest
./test_smoking_selfhost.sh
```

### How to add new case?

You can update python commands part of test_smoking_selfhost.sh file to add any system test case into smoke test.

## 3.5 Chaos Test

A simple tool to exercise various functions of the system in a randomized fashion, hoping to expose maximum number of problems without a pre-determined scenario.

### How to run test?

```bash
cd tests/pytest
python3 auto_crash_gen.py
```

### How to add new case?

Add a function, such as TaskCreateNewFunction, to pytest/crash_gen/crash_gen_main.py.

Integrate TaskCreateNewFunction into the balance_pickTaskType function in crash_gen_main.py.

## 3.6 CI Test

CI testing (Continuous Integration testing), is an important practice in software development that aims to automate frequent integration of code into a shared codebase, build and test it to ensure code quality and stability. TDengine CI testing includes three part of test cases: unit test、system test and legacy test

### How to run all CI test cases?

If this is the first time to run all the CI test cases, it is recommended to add the test branch, please run it with  following commands:

```bash
cd tests
./run_all_ci_cases.sh -b main # on main branch
```

### How to add new cases?

You can refer the [Unit Test](#31-unit-test)、[System Test](#32-system-test) and [Legacy Test](#33-legacy-test) sections for detail steps to add new test cases for CI test.
