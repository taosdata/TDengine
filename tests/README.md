# Testing TDengine

## Install the required tools

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

> [!NOTE]
> Please make sure building operation with option '-DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true' has been finished, execute the below commands if not:

```bash
cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true
make && make install
```

## Unit Test

Unit test script is the smallest testable part and developed for some function, method or class of TDengine, you can run
the script with below command:

```bash
cd tests/unit-test/
bash test.sh -e 0
```

## System Test

Python test script includes almost all of the functions of TDengine, so some test case maybe fail cause the function only
work for TDengine Enterprise Edition, you can run the script with below command:

```bash
cd tests/system-test
python3 ./test.py -f 2-query/avg.py
```

## Smoke Test

Smoke test script is known as sanity testing to ensure that the critical functionalities of TDengine, you can run the 
script with commands below:

```bash
cd /root/TDengine/packaging/smokeTest
./test_smoking_selfhost.sh
```

## Legacy Test

In the early stage of TDengine development, test cases are run by an internal test framework called TSIM, which is developed in C++. To run the legacy test cases, please execute the following commands:

```bash
cd tests/script
./test.sh -f tsim/db/basic1.sim
```

## How TO Add Test Cases

You can add sim test case under tests/script, python test case under tests/system-test or tests/army. When the case passes in the test branch, add the case to the cases.task file under tests/parallel_test, and then merge the pr into main branch to run in the future CI.

## TSBS Test

Time Series Benchmark Suite (TSBS) is an open-source performance benchmarking platform specifically designed for time-series data processing systems, such as databases. It provides a standardized approach to evaluating the performance of various databases by simulating typical use cases such as IoT and DevOps.

TSBS Test is based on the TDengine Enterprise Edition and need private repositry privilege, you can refer the link for detail steps. [TSBS Test](https://github.com/taosdata/TDinternal/tree/main?tab=readme-ov-file#85-tsbs-test)

## TestNG Test

TestNG Test is another test framwork which developed by python, functionally speaking, it's a supplement for system test, and
also run longer time than system test for stability testing purposes. 

TestNG Test is based on the TDengine Enterprise Edition and need private repositry privilege, you can refer the link for detail steps. [TestNG Test](https://github.com/taosdata/TDinternal/tree/main?tab=readme-ov-file#87-testng-test)
