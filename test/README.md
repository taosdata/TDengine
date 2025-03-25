# Table of Contents

1. [Introduction](#1-introduction)
1. [Prerequisites](#2-prerequisites)
1. [Project Structure](#3-Project-Structure)
1. [Run Test Cases](#4-Run-Test-Cases)
1. [Add New Case](#5-Add-New-Case)
1. [Add New Case to CI](#6-Add-New-Case-to-CI)


# 1. Introduction

This manual is intended to give developers a comprehensive guidance to test TDengine efficiently. It is divided into three main sections: introduction, prerequisites and testing guide.

> [!NOTE]
> - The commands and scripts below are verified on Linux (Ubuntu 18.04/20.04/22.04).
> - [taos-connector-python](https://github.com/taosdata/taos-connector-python) is used by tests written in Python, which requires Python 3.7+.

# 2. Prerequisites

- Install Python3

```bash
apt install python3
apt install python3-pip
```

- Install Python dependencies

```bash
cd test
pip3 install -r requirements.txt
```

- Building

Before testing, please make sure the building operation with option `-DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true` has been done, otherwise execute commands below:

```bash
cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true
make && make install
```

# 3. Project Structure

Outline the main directories and their purposes:
```
test/
│
├── cases/                # cases directory
│   ├── demo/             
│   │   ├── test_demo.py  # demo test case
│   └── ...
│
├── env/                  # TDengine deploy configuration yaml file directory
│   └── demo.yaml         # configuration example
│
├── utils/                # common functions utils directory
│   └── ...
│
├── requirements.txt      # dependencies
└── README.md             # project description
```

# 4. Run Test Cases

Run test cases command description:

```bash
cd test
pytest [options] [test_file_path] [test_file_path]
```

Options:

- `-N <num>`: start dnodes numbers in clusters
- `-M <num>`: create mnode numbers in clusters
- `-R`: restful realization form
- `-Q`: set queryPolicy in one dnode
- `-D <num>`: set disk number on each level. range 1 ~ 10
- `-L <num>`: set multiple level number. range 1 ~ 3
- `-C <num>`: create Dnode Numbers in one cluster
- `-I <num>`: independentMnode Mnode
- `--replica <num>`: set the number of replicas
- `--tsim <file>`: tsim test file (for compatibility with the original tsim framework; not typically used in normal circumstances)
- `--yaml_file <file>`: TDengine deploy configuration yaml file (default directory is `env`, no need to specify the `env` path) 
- `--skip_test`: only do deploy or install without running test
- `--skip_deploy`: Only run test without start TDengine

**Mutually Exclusive Options**:
- The `--yaml_file` option is mutually exclusive with the following options:
  - `-N`
  - `-R`
  - `-Q`
  - `-I`
  - `-D`
  - `-L`
  - `-C`

Some useful pytest common options:
- `-s`: disable output capturing
- `--log-level`: set log level
- `--alluredir`: generate allure report directory
- `-m`: run test by mark

test_file_path:
- test_file_path is optional, if not provided, all test cases(files start with test_) will be run
- if provided, only the test case in the file or path will be run

Here are some examples of using pytest to execute test cases:

```bash
# 1. Run all test cases
pytest

# 2. Run all test cases in a specific test file
pytest cases/data_write/sql_statement/test_insert_double.py

# 3. Run a specific test case
pytest cases/data_write/sql_statement/test_insert_double.py::TestInsertDouble::test_value

# 4. Run test cases with a specific marker
pytest -m ci

# 5. Set the log level for the tests
pytest --log-level=DEBUG

# 6. Generate Allure report
pytest --alluredir=allure-results

# 7. Run tests with a specific YAML configuration file
pytest --yaml_file=ci_default.yaml

```

# 5. Add New Case

To add a new test case, follow these steps:

1. **Create a New Test File**:
   - Navigate to the `test/cases` directory.
   - Create a new Python file for your test case. It is required to name the file starting with `test_`, for example, `test_new_feature.py`.

2. **Import Required Modules**:
   - At the top of your new test file, import the necessary modules, including `pytest`.

3. **Define Your Test Function**:
   - Define a function for your test case. The function name should start with `test_` to ensure that `pytest` recognizes it as a test case.

4. **Write Assertions**:
   - Inside your test function, use assertions to verify the expected behavior of the code you are testing.

5. **Describe case function in docstring**:
   - Describe the case function in docstring, including the case name, description, labels, jira, since, history, etc.

### Example of a New Test Case

Here is an example of how to write a new test case:

```python
# cases/demo/test_demo.py
```

# 6. Add New Case to CI

To add a new test case to the CI pipeline, include the case run command in the `test/parallel_test/cases.task` file. For example:

```text
# test/parallel_test/cases.task
,,y,.,pytest cases/demo/test_demo.py -N 3 -M 2
```

