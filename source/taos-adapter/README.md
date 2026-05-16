<!-- omit in toc -->
# taosAdapter

<!-- omit in toc -->
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taosadapter/build.yml)](https://github.com/taosdata/taosadapter/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taosadapter/graph/badge.svg?token=WCN19U180U)](https://codecov.io/gh/taosdata/taosadapter)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taosadapter)
![GitHub License](https://img.shields.io/github/license/taosdata/taosadapter)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/taosadapter?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

English | [简体中文](./README-CN.md)

<!-- omit in toc -->

## Table of Contents

<!-- omit in toc -->

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

taosAdapter is a companion tool for TDengine, serving as a bridge and adapter between the TDengine cluster and
applications. It provides an easy and efficient way to ingest data directly from data collection agents (such as
Telegraf, StatsD, collectd, etc.). It also offers InfluxDB/OpenTSDB compatible data ingestion interfaces, allowing
InfluxDB/OpenTSDB applications to be seamlessly ported to TDengine. The connectors of TDengine in various languages
communicate with TDengine through the WebSocket interface, hence the taosAdapter must be installed.

## 2. Documentation

- To use taosAdapter, please refer to
  the [taosAdapter Reference](https://docs.tdengine.com/tdengine-reference/components/taosadapter/).
- This quick guide is mainly for developers who like to contribute/build/test the taosAdapter by themselves. To learn
  about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites

- Go 1.23 or above is installed and CGO is enabled `export CGO_ENABLED=1`.
- TDengine has been deployed locally. For specific steps, please refer
  to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Please make sure taosd has been
  started.

## 4. Build

Execute `go build` in the project directory to build the project.

## 5. Testing

### 5.1 Test Execution

1. Before running tests, ensure that the TDengine server is installed and the `taosd` is running.
   The database should be empty.
2. In the project directory, run `go test ./...` to execute the tests. The tests will connect to the local TDengine
   server and taosAdapter for testing.
3. The output result `PASS` means the test passed, while `FAIL` means the test failed. For detailed information, run
   `go test -v ./...`.

### 5.2 Test Case Addition

Add test cases to the `*_test.go` file to ensure that the test cases cover the new code.

### 5.3 Performance Testing

Performance testing is in progress.

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/taosadapter/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taosadapter)

## 7. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/taosadapter/issues/new?template=Blank+issue).
When
submitting, please provide the following information:

- Description of the issue and whether it is consistently reproducible.
- taosAdapter version.
- TDengine version.

## 8. Submitting PRs

We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project. Please refer
   to [how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`).
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub. Please refer
   to [how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).
6. After submitting the PR, you can find your PR through
   the [Pull Request](https://github.com/taosdata/taosadapter/pulls). Click on the corresponding link to see if the CI
   for
   your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes
   or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
7. After submitting the PR, if the CI passes, you can find your PR on
   the [codecov](https://app.codecov.io/gh/taosdata/taosadapter/pulls) page to check the coverage.

## 9. References

- [TDengine Official Website](https://tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. License

[MIT License](./LICENSE)