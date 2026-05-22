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
- [4. Building](#4-building)
- [5. Testing](#5-testing)
    - [5.1 Test Execution](#51-test-execution)
    - [5.2 Test Case Addition](#52-test-case-addition)
    - [5.3 Performance Testing](#53-performance-testing)
- [6. Packaging](#6-packaging)
- [7. CI/CD](#7-cicd)
- [8. Submitting Issues](#8-submitting-issues)
- [9. Submitting PRs](#9-submitting-prs)
- [10. References](#10-references)
- [11. License](#11-license)

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

### System Requirements
- Go >= 1.14 (1.23+ recommended)
- For native connector: TDengine client library (`libtaos.so`)
- For REST/WebSocket connector: no native library needed

### Installing Go
**Ubuntu/Debian & CentOS/RHEL:**
```bash
wget https://go.dev/dl/go1.23.4.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.23.4.linux-amd64.tar.gz
export PATH=/usr/local/go/bin:$PATH
```

### Native Connector Notes
- Enable CGO with `export CGO_ENABLED=1` when building or testing the native connector.
- Ensure `libtaos.so` is installed in a standard library search path, or set `LD_LIBRARY_PATH` accordingly.

### Local Test Environment
- TDengine has been deployed locally. For specific steps, please refer
  to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Please make sure taosd and
  taosAdapter have been started.

## 4. Building

```bash
git clone https://github.com/taosdata/driver-go.git
cd driver-go
go build ./...   # verify compilation
```

`driver-go` is a Go library, so `go build ./...` is the standard way to verify that all packages compile successfully.

## 5. Testing

### 5.1 Test Execution

1. Before running tests, ensure that the TDengine server is installed and that `taosd` and `taosAdapter` are running.
   The database should be empty.
2. In the project directory, run `go test ./...` to execute the tests. The tests will connect to the local TDengine
   server and taosAdapter for testing.
3. The output result `PASS` means the test passed, while `FAIL` means the test failed. For detailed information, run
   `go test -v ./...`.

```bash
# run the full test suite
go test ./...

# run with verbose output
go test -v ./...
```

- Native connector tests require CGO enabled and a working `libtaos.so` installation.
- REST/WebSocket tests do not require the TDengine native client library, but still require running `taosd` and `taosAdapter`.

### 5.2 Test Case Addition

Add test cases to the `*_test.go` file to ensure that the test cases cover the new code.

### 5.3 Performance Testing

Performance testing is in progress.

## 6. Packaging

`driver-go` is a Go module — no separate packaging step needed. Users import it via:

```go
go get github.com/taosdata/driver-go/v3@latest
```

## 7. CI/CD

- [Build Workflow](https://github.com/taosdata/driver-go/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/driver-go)

## 8. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/driver-go/issues/new?template=Blank+issue). When
submitting, please provide the following information:

- Description of the issue and whether it is consistently reproducible
- Driver version
- Connection parameters (excluding server address, username, and password)
- TDengine version

## 9. Submitting PRs

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

## 10. References

- [TDengine Official Website](https://tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 11. License

[MIT License](./LICENSE)
