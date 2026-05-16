<!-- omit in toc -->
# TDengine Go Connector

<!-- omit in toc -->
[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/driver-go/build.yml)](https://github.com/taosdata/driver-go/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/driver-go/graph/badge.svg?token=70E8APPMKR)](https://codecov.io/gh/taosdata/driver-go)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/driver-go)
![GitHub License](https://img.shields.io/github/license/taosdata/driver-go)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/driver-go?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

English | [简体中文](README-CN.md)

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

`driver-go` is the official Go language connector for TDengine. It implements the Go language `database/sql` interface,
allowing Go developers to create applications that interact with TDengine clusters.

## 2. Documentation

- To use Go connector, please check [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes how an
  application can introduce the `driver-go`, as well as examples of data writing, querying, schemaless writing,
  parameter binding, and data subscription.
- For other reference information, please
  check [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/go/), which includes version
  history, data types, example programs, API descriptions, and FAQs.
- This quick guide is mainly for developers who like to contribute/build/test the Go connector by themselves. To learn
  about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites

- Go 1.14 or above and enable CGO with `export CGO_ENABLED=1`.
- TDengine has been deployed locally. For specific steps, please refer
  to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Please make sure taosd and
  taosAdapter have been started.

## 4. Build

No need to build.

## 5. Testing

### 5.1 Test Execution

1. Before running tests, ensure that the TDengine server is installed and that `taosd` and `taosAdapter` are running.
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

- [Build Workflow](https://github.com/taosdata/driver-go/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/driver-go)

## 7. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/driver-go/issues/new?template=Blank+issue). When
submitting, please provide the following information:

- Description of the issue and whether it is consistently reproducible
- Driver version
- Connection parameters (excluding server address, username, and password)
- TDengine version

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
   the [Pull Request](https://github.com/taosdata/driver-go/pulls). Click on the corresponding link to see if the CI for
   your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes
   or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
7. After submitting the PR, if the CI passes, you can find your PR on
   the [codecov](https://app.codecov.io/gh/taosdata/driver-go/pulls) page to check the coverage.

## 9. References

- [TDengine Official Website](https://tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. License

[MIT License](./LICENSE)
