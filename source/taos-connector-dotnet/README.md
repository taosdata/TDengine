<!-- omit in toc -->
# TDengine C# Connector
<!-- omit in toc -->

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-dotnet/build.yml)](https://github.com/taosdata/taos-connector-dotnet/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taos-connector-dotnet/graph/badge.svg?token=U30JZYDGMS)](https://codecov.io/gh/taosdata/taos-connector-dotnet)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-dotnet)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-dotnet)
[![NuGet Version](https://img.shields.io/nuget/v/TDengine.Connector)](https://www.nuget.org/packages/TDengine.Connector)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

English | [简体中文](README-CN.md)

<!-- omit in toc -->
## Table of Contents

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

`TDengine.Connector` is the C# language connector provided by TDengine. C# developers can use it to develop C# application software that accesses TDengine cluster data.

## 2. Documentation

- To use C# connector, please check [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes how an
  application can introduce the `TDengine.Connector`, as well as examples of data writing, querying, schemaless writing,
  parameter binding, and data subscription.
- For other reference information, please
  check [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/csharp/), which includes
  version history, data types, example programs, API descriptions, and FAQs.
- This quick guide is mainly for developers who like to contribute/build/test the C# connector by themselves. To learn
  about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites

### System Requirements

- .NET SDK 6.0+ (8.0 recommended)
- For native connection: TDengine client library (`libtaos.so`)
- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Please make sure taosd and taosAdapter have been started.

### Installing .NET SDK

**Ubuntu/Debian:**

```bash
wget https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update && sudo apt-get install -y dotnet-sdk-8.0
```

**CentOS/RHEL:**

```bash
sudo yum install -y dotnet-sdk-8.0
```

## 4. Building

```bash
git clone https://github.com/taosdata/taos-connector-dotnet.git
cd taos-connector-dotnet
dotnet restore
dotnet build --no-restore
```

## 5. Testing

### 5.1 Test Execution

1. Before running tests, ensure that the TDengine server is installed and that `taosd` and `taosAdapter` are running. The database should be empty.
2. In the project directory, run `dotnet test` to execute the tests. The tests will connect to the local TDengine server and taosAdapter for testing.

```bash
dotnet test
```

You can also run the main test projects individually:

```bash
dotnet test test/Data.Tests/Data.Tests.csproj
dotnet test test/Driver.Test/Driver.Test.csproj
```

3. If the tests pass, `Test Run Successful` will be printed. If the tests fail, the failure information `Test Run Failed` will be printed.

### 5.2 Test Case Addition

Add test cases in the `test` directory. Add ADO.NET test cases to `test/Data.Tests` and client driver test cases to `test/Driver.Test/Client`.
The test cases use the xunit framework.

### 5.3 Performance Testing

Performance testing is in progress.

## 6. Packaging

```bash
dotnet pack -c Release
# Output: bin/Release/TDengine.<version>.nupkg
```

## 7. CI/CD

- [Build Workflow](https://github.com/taosdata/taos-connector-dotnet/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-dotnet)

## 8. Submitting Issues

We welcome the submission
of [GitHub Issue](https://github.com/taosdata/taos-connector-dotnet/issues/new?template=Blank+issue). When
submitting, please provide the following information:

- Description of the issue and whether it is consistently reproducible
- Driver version
- Connection parameters (excluding server address, username, and password)
- TDengine version

## 9. Submitting PRs

We welcome developers to contribute to this project. Please follow the steps below to submit a PR:

1. Fork this project. Please refer
   to [how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`).
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub. Please refer
   to [how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request).
6. After submitting the PR, you can find your PR through
   the [Pull Request](https://github.com/taosdata/taos-connector-dotnet/pulls). Click on the corresponding link to see
   if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether
   the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
7. After submitting the PR, if the CI passes, you can find your PR on
   the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-dotnet/pulls) page to check the coverage.

## 10. References

- [TDengine Official Website](https://tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 11. License

[MIT License](./LICENSE)
