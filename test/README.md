# Table of Contents

1. [Introduction](#1-introduction)
1. [Prerequisites](#2-prerequisites)
1. [Project Structure](#3-Project-Structure)
1. [Run Test Cases](#4-Run-Test-Cases)
1. [Add New Case](#5-Add-New-Case)
1. [Add New Case to CI](#6-Add-New-Case-to-CI)
1. [Workflows](#7-workflows)
1. [Test Report](#8-test-report)

# 1. Introduction

This is the new end-to-end testing framework for TDengine. It offers several advantages:

1. **Built on Pytest**: The new framework is based on Pytest, which is known for its simplicity and scalability. Pytest provides powerful features such as fixtures, parameterized testing, and easy integration with other plugins, making it a popular choice for testing in Python.

2. **Integration of Common Functions**: The framework integrates the capabilities of the original testing framework's common functions, allowing for seamless reuse of existing code and utilities.

3. **Enhanced Deployment and Testing Structure**: It supports a more structured approach to deployment and testing, enabling users to define their testing environments and configurations more effectively.

4. **Flexible Server Deployment via YAML**: Users can deploy servers in a more versatile manner using YAML files, allowing for customized configurations and easier management of different testing scenarios.

5. **Integration with Allure Report**: After the tests are done, [Allure Report](https://allurereport.org/) will be created automatically, which can be accessed by just one click.

6. **Integration with Github Action**: Including workflow to run the test cases in the new test framework, workflow to validate the docstring of test cases and workflow to publish case docs to Github Pages.

> [!NOTE]
> - The commands and scripts below are verified on Linux (Ubuntu 18.04/20.04/22.04).
> - [taos-connector-python](https://github.com/taosdata/taos-connector-python) is used by tests written in Python, which requires Python 3.8+.

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

- Building (Optional)

Tests can be run against TDengine either by installation or by build. When building TDengine, please make sure the building options `-DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true` has been used:

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
├── new_test_framework/   
│   └── utils             # common functions utils directory
│   │   ├── ...
│
├── requirements.txt      # dependencies
└── README.md             # project description
```

# 4. Run Test Cases

## 4.1 Set environment variables (Optional)

Please set the environment variables according to your scenarios. This is optional, the binaries in `debug/build/bin` will be used by default.

```bash
source ./setenv_build.sh    # run test with build binaries
source ./setenv_install.sh  # run test with installation
```

## 4.2 Basic Usage

```bash
cd test
pytest [options] [test_file_path]
```

Notes:
- options: described below 
- test_file_path: optional, if provided, only the test case in the file or path will be run; if not provided, all test cases (files start with test_) will be run

## 4.3 Run tests by command line arguments

The template configuration files for **taosd**, **taosadapter**, and **taoskeeper** are located in the `env` directory.  
You can update these YAML files to customize the deployment and test environment.

- **taos_config.yaml**:  
  - `port`: Base port for the first dnode (default: 6030, increments by 100 for each additional dnode)
  - `mqttPort`: Base MQTT port (default: 6083, increments by 100 for each additional dnode)

- **taosadapter_config.yaml**:  
  - Used to configure taosadapter service.  
  - You can set log level, port, log path, and other adapter-specific options.

- **taoskeeper_config.yaml**:  
  - Used to configure taoskeeper service.  
  - You can set database connection info, log options, metrics, etc.

Run test cases command description:

Options:

- `-N <num>`: start dnodes numbers in clusters
- `-M <num>`: create mnode numbers in clusters
- `-R`: restful realization form
- `-A`: address sanitizer mode
- `-Q`: set queryPolicy in one dnode
- `-D <num>`: set disk number on each level. range 1 ~ 10
- `-L <num>`: set multiple level number. range 1 ~ 3
- `-C <num>`: create Dnode Numbers in one cluster
- `-I <num>`: independentMnode Mnode
- `--replica <num>`: set the number of replicas
- `--clean`: Clean test env processe and workdir before deploy
- `--tsim <file>`: tsim test file (for compatibility with the original tsim framework; not typically used in normal circumstances)

- `--skip_test`: only do deploy or install without running test
- `--skip_deploy`: Only run test without start TDengine
- `--testlist`: Path to file containing list of test files to run. Each line should contain one Python test file path, and lines starting with # will be ignored.
- `--skip_stop`: Do not destroy/stop the TDengine cluster after test class execution (for debugging or keeping environment alive)
- `--only_deploy`: Only do deploy without starting TDengine cluster

## 4.4 Run tests by configuration file

- `--yaml_file <file>`: TDengine deploy configuration yaml file (default directory is `env`, no need to specify the `env` path) 

**Mutually Exclusive Options**:
- The `--yaml_file` option is mutually exclusive with the following options:
  - `-N`
  - `-R`
  - `-Q`
  - `-I`
  - `-D`
  - `-L`
  - `-C`

## 4.5 Common Options

Some useful pytest common options:
- `-s`: disable output capturing
- `--log-level`: set log level
- `--alluredir`: generate allure report directory
- `-m`: run test by mark

## 4.6 Examples

Here are some examples of using pytest to execute test cases:

```bash
# 1. Run a specific test file
pytest cases/data_write/sql_statement/test_insert_double.py

# 2. Run a specific test case
pytest cases/data_write/sql_statement/test_insert_double.py::TestInsertDouble::test_value

# 3. Run test cases with a specific marker
pytest -m cluster

# 4. Set the log level for the tests
pytest --log-level=DEBUG

# 5. Run test with a specific YAML configuration file
pytest --yaml_file=ci_default.yaml cases/data_write/sql_statement/test_insert_double.py

# 6. Only do deploy
pytest --clean --only_deploy 
```

## 4.7 Batch Run Test Cases

To run test cases in batch:

Run with default test list file:
```bash
./start_run_test_list.sh                           #run with default test list file(test_list.txt)
./start_run_test_list.sh path/to/case_list_file    #run with custom test list file
```

For the format of the test list file, please refer to `test_list.txt`.

To stop the test execution:
```bash
./stop_run_test_list.sh    #the script will stop after completing the current test case.
```

Batch test results can be found in the `test_logs` directory:
- `test_logs/case_result.txt`: Case execution results
- `test_logs/run_tests.log`: All cases outputs
- `test_logs/xxx.log`: Failed case outputs
- `test_logs/allure-report`: Allure report (if allure command is supported)

# 5. Add New Case

To add a new test case, follow these steps:

1. **Create a New Test File**:
   - Navigate to the `test/cases` directory.
   - Create a new Python file for your test case. It is required to name the file starting with `test_`, for example, `test_new_feature.py`.

2. **Import Required Modules**:
   - At the top of your new test file, import the necessary modules. For example:

   ```python
   from new_test_framework.utils import tdLog, tdSql, etool
   ```

3. **Define Your Test Function**:
   - Define a function for your test case. The function name should start with `test_` to ensure that `pytest` recognizes it as a test case.

4. **Write Assertions**:
   - Inside your test function, use assertions to verify the expected behavior of the code you are testing.

5. **Describe case function in docstring**:
   - Describe the case function in docstring, including the case name, description, labels, jira, since, history, etc.

Please refer to `cases/demo/test_demo.py` as an example.

# 6. Add New Case to CI

To add a new test case to the CI pipeline, include the case run command in the `test/ci/cases.task` file. For example:

```text
# test/ci/cases.task
,,y,.,./ci/pytest.sh pytest cases/storage/tsma/test_tsma.py
```

# 7. Workflows

Every time new code is submitted, the corresponding GitHub workflows are triggered as follows:

## 7.1 CI Test
A CI test is triggered whenever a pull request (PR) is submitted.

## 7.2 Docstring Check
A Docstring check is triggered for any PR submitted to the `test/cases` directory, ensuring the completeness of the case descriptions.

## 7.3 Cases Doc Publish
A cases documentation publish is triggered when a PR is merged into the `test/cases` directory, updating the case description documentation page.

Note:
- Please referto [Deploy Case Docs](https://github.com/taosdata/TDengine/actions/workflows/deploy-case-docs.yml) for details.
- Published cases doc can be found at [TDengine Case List](https://taosdata.github.io/TDengine/main/).

## 8. Test Report

The testing framework executes with the `--alluredir=allure-results` parameter by default, which generates an Allure report. In the Allure report, you can view the test execution results, logs for failed test cases, and case description information.

### 8.1 Local Execution Results

After execution, you can check the `allure-results` directory, which contains the Allure report files. Users can manually generate the report page using the Allure command:

```bash
allure generate allure-results -o $YOUR_REPORT_DIR --clean
```

### 8.2 CI Execution Results

You can find the test report link in the GitHub workflow, which redirects you to the Allure report page when clicked. Please refer to the execution log of [New Framework Test](https://github.com/taosdata/TDengine/actions/workflows/new-framework-test.yml) for details.
