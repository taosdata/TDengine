<!-- omit in toc -->
# TDengine Java Connector

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-jdbc/build.yml)](https://github.com/taosdata/taos-connector-jdbc/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taos-connector-jdbc/graph/badge.svg?token=GQRD9WCQ64)](https://codecov.io/gh/taosdata/taos-connector-jdbc)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-jdbc)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-jdbc)
[![Maven Central Version](https://img.shields.io/maven-central/v/com.taosdata.jdbc/taos-jdbcdriver?label=Maven%20Central)](https://central.sonatype.com/artifact/com.taosdata.jdbc/taos-jdbcdriver)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

English | [简体中文](./README-CN.md)

<!-- omit in toc -->
## Table of Contents

- [1. Introduction](#1-introduction)
- [2. Documentation](#2-documentation)
- [3. Prerequisites](#3-prerequisites)
- [4. Build](#4-build)
- [5. Testing](#5-testing)
  - [5.1 Test Execution](#51-test-execution)
  - [5.2 Test Case Addition](#52-test-case-addition)
  - [5.3 Performance Testing](#53-performance-testing)
- [6. CI/CD](#6-cicd)
- [7. Submitting Issues](#7-submitting-issues)
- [8. Submitting PRs](#8-submitting-prs)
- [9. References](#9-references)
- [10. License](#10-license)


## 1. Introduction
`taos-jdbcdriver` is the official Java connector for TDengine, allowing Java developers to develop applications that access the TDengine database. `taos-jdbcdriver` implements the standard interfaces of the JDBC driver and supports functions such as data writing, querying, subscription, schemaless writing, and parameter binding.

## 2. Documentation  
- To use JDBC connector, please check [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes how an application can introduce the `taos-jdbcdriver`, as well as examples of data writing, querying, schemaless writing, parameter binding, and data subscription.
- For other reference information, please check [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/java/), which includes version history, data types, example programs, API descriptions, and FAQs.
- This quick guide is mainly for developers who like to contribute/build/test the JDBC connector by themselves. To learn about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites
- Java 1.8 or above runtime environment and Maven 3.6 or above installed, with environment variables correctly set.
- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Please make sure taosd and taosAdapter have been started. If you are using a Mac system, please use the command `sudo ln -s /usr/local/lib/libtaos.dylib /Library/Java/Extensions/libtaos.dylib` to create a symbolic link for the `taos` dynamic library.

## 4. Build
Execute `mvn clean package` in the project directory to build the project.

## 5. Testing
### 5.1 Test Execution
Execute `mvn test` in the project directory to run the tests. The test cases will connect to the local TDengine server and taosAdapter for testing.
After running the tests, the result similar to the following will be printed eventually. If all test cases pass, both Failures and Errors will be 0.
```
[INFO] Results:
[INFO] 
[WARNING] Tests run: 2353, Failures: 0, Errors: 0, Skipped: 16
```

### 5.2 Test Case Addition
All tests are located in the `src/test/java/com/taosdata/jdbc` directory of the project. The directory is divided according to the functions being tested. You can add new test files or add test cases in existing test files.
The test cases use the JUnit framework. Generally, a connection is established and a database is created in the `before` method, and the database is droped and the connection is released in the `after` method.

### 5.3 Performance Testing
Performance testing is in progress.

## 6. CI/CD
- [Build Workflow](https://github.com/taosdata/taos-connector-jdbc/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-jdbc)

## 7. Submitting Issues
We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-jdbc/issues/new?template=Blank+issue). When submitting, please provide the following information:

- Problem description, whether it always occurs, and it's best to include a detailed call stack.
- JDBC driver version.
- JDBC connection parameters (username and password not required).
- TDengine server version.

## 8. Submitting PRs
We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
1. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
1. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
1. Push the changes to the remote branch (`git push origin my_branch`).
1. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
1. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/taos-connector-jdbc/pulls). Click on the corresponding link to see if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
1. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-jdbc/pulls) page to check the test coverage.

## 9. References
- [TDengine Official Website](https://www.tdengine.com/) 
- [TDengine GitHub](https://github.com/taosdata/TDengine) 

## 10. License
[MIT License](./LICENSE)
