# Testing TDengine

## Unit Test

Unit test script is the smallest testable part and developed for some function, method or class of TDengine, you can run
the script with below command:

```bash
cd tests/unit-test/
bash test.sh
```

## System Test

Python test script includes almost all of the functions of TDengine, so some test case maybe fail cause the function only
work for TDengine enterprise version, you can run the script with below command:

```bash
cd tests/system-test
python3 ./test.py -f 2-query/floor.py
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



