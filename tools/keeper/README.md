<!-- omit in toc -->
# taosKeeper

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/TDengine/taoskeeper-build.yml)](https://github.com/taosdata/TDengine/actions/workflows/taoskeeper-build.yml)
[![codecov](https://codecov.io/gh/taosdata/taoskeeper/graph/badge.svg)](https://codecov.io/gh/taosdata/taoskeeper)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/TDengine)
![GitHub License](https://img.shields.io/github/license/taosdata/TDengine)
![GitHub Release](https://img.shields.io/github/v/release/taosdata/tdengine)
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
- [8. Submitting PR](#8-submitting-pr)
- [9. References](#9-references)
- [10. License](#10-license)

## 1. Introduction

taosKeeper is a new monitoring indicator export tool introduced in TDengine 3.0, which is designed to facilitate users to monitor the operating status and performance indicators of TDengine in real time. With simple configuration, TDengine can report its own operating status and various indicators to taosKeeper. After receiving the monitoring data, taosKeeper will use the RESTful interface provided by taosAdapter to store the data in TDengine.

An important value of taosKeeper is that it can store the monitoring data of multiple or even a batch of TDengine clusters in a unified platform. In this way, the monitoring software can easily obtain this data, and then realize comprehensive monitoring and real-time analysis of the TDengine cluster. Through taosKeeper, users can more easily understand the operation status of TDengine, discover and solve potential problems in a timely manner, and ensure the stability and efficiency of the system.

## 2. Documentation

- To use taosKeeper, please refer to the [taosKeeper Reference](https://docs.tdengine.com/tdengine-reference/components/taoskeeper/), which includes installation, configuration, startup, data collection and monitoring, and Prometheus integration.
- This README is mainly for developers who want to contribute code, compile and test taosKeeper. If you want to learn TDengine, you can browse the [official documentation](https://docs.tdengine.com/).

## 3. Prerequisites

1. Go 1.23 or above has been installed.
2. TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/), and taosd and taosAdapter have been started.

## 4. Build

Run the following command in the `TDengine/tools/keeper` directory to build the project:

```bash
go build
```

## 5. Testing

### 5.1 Test Execution

Run the test by executing the following command in the `TDengine/tools/keeper` directory:

```bash
sudo go test ./...
```

The test case will connect to the local TDengine server and taosAdapter for testing. After the test is completed, you will see a result summary similar to the following. If all test cases pass, there will be no `FAIL` in the output.

```text
ok      github.com/taosdata/taoskeeper/api      17.405s
ok      github.com/taosdata/taoskeeper/cmd      1.819s
ok      github.com/taosdata/taoskeeper/db       0.484s
ok      github.com/taosdata/taoskeeper/infrastructure/config    0.417s
ok      github.com/taosdata/taoskeeper/infrastructure/log       0.785s
ok      github.com/taosdata/taoskeeper/monitor  4.623s
ok      github.com/taosdata/taoskeeper/process  0.606s
ok      github.com/taosdata/taoskeeper/system   3.420s
ok      github.com/taosdata/taoskeeper/util     0.097s
ok      github.com/taosdata/taoskeeper/util/pool        0.146s
```

### 5.2 Test Case Addition

Add test cases in files ending with `_test.go` and make sure the new code is covered by the corresponding test cases.

### 5.3 Performance Testing

Performance testing is under development.

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/TDengine/actions/workflows/taoskeeper-ci-build.yml)
- Code Coverage - TODO

## 7. Submitting Issues

We welcome submissions of [GitHub Issues](https://github.com/taosdata/TDengine/issues). Please provide the following information when submitting so that the problem can be quickly located:

- Problem description: The specific problem manifestation and whether it must occur. It is recommended to attach detailed call stack or log information.
- taosKeeper version: You can get the version information through `taoskeeper -V`.
- TDengine server version: You can get the version information through `taos -V`.

If you have other relevant information (such as environment configuration, operating system version, etc.), please add it so that we can understand the problem more comprehensively.

## 8. Submitting PR

We welcome developers to participate in the development of this project. Please follow the steps below when submitting a PR:

1. Fork the repository: Please fork this repository first. For specific steps, please refer to [How to Fork a Repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo).
2. Create a new branch: Create a new branch based on the `main` branch and use a meaningful branch name (for example: `git checkout -b feature/my_feature`). Do not modify it directly on the main branch.
3. Development and testing: After completing the code modification, make sure that all unit tests pass, and add corresponding test cases for new features or fixed bugs.
4. Submit code: Submit the changes to the remote branch (for example: `git push origin feature/my_feature`).
5. Create a Pull Request: Initiate a [Pull Request](https://github.com/taosdata/TDengine/pulls) on GitHub. For specific steps, please refer to [How to Create a Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).
6. Check CI: After submitting the PR, you can find the PR you submitted in the Pull Request and click the corresponding link to check whether the CI of the PR has passed. If it has passed, it will show `All checks have passed`. Regardless of whether CI has passed or not, you can click `Show all checks/Details` to view detailed test case logs.

## 9. References

[TDengine Official Website](https://www.tdengine.com/)

## 10. License

[AGPL-3.0 License](../../LICENSE)
