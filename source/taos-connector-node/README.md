<!-- omit in toc -->
# TDengine Node.js Connector

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-node/build.yml)](https://github.com/taosdata/taos-connector-node/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taos-connector-node/graph/badge.svg?token=5379a80b-063f-48c2-ab56-09564e7ca777)](https://codecov.io/gh/taosdata/taos-connector-node)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-node)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-node)
[![NPM Version](https://shields.io/npm/v/@tdengine/websocket)](https://www.npmjs.com/package/@tdengine/websocket)
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

@tdengine/websocket is an efficient connector specially designed by TDengine for Node.js developers. It uses the WebSocket API provided by the taosAdapter component to establish a connection with TDengine, eliminating the dependence on TDengine client drivers and opening up a convenient development path for developers. With this powerful tool, developers can easily build applications for TDengine clusters. Whether it is performing complex SQL write and query tasks, implementing flexible schemaless write operations, or achieving highly real-time subscription functionality, this connector can easily and perfectly meet diverse data interaction needs in all aspects.

## 2. Documentation

- To use Node.js connector, please refer to the [Developer Guide](https://docs.tdengine.com/developer-guide/), which includes instructions on how to integrate `@tdengine/websocket` into an application, along with examples of data writing, querying, schemaless writing, parameter binding, and data subscription.
- For other reference information, please check [Reference Manual](https://docs.tdengine.com/tdengine-reference/client-libraries/node/), which includes version history, data types, example programs, API descriptions, and FAQs.
- This quick guide is primarily for developers who wish to contribute, build, and test the Node.js connector on their own. To learn about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites

- Install the Node.js development environment, using version 14 or above. Download link: [https://nodejs.org/en/download/](https://nodejs.org/en/download/)
- Install the Node.js connector dependencies using npm, execute the 'npm install' command in the `nodejs` directory of the project for installation.
- Install TypeScript 5.3.3 and above using npm.
- TDengine has been deployed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Please make sure `taosd` and `taosAdapter` have been started.

## 4. Build

Execute `tsc` to build the project in the 'nodejs' directory.

## 5. Testing

### 5.1 Test Execution

In the `nodejs` directory of your project, execute the command `npm run test` to run all test cases in the `test` directory. The test cases will connect to the local TDengine server and taosAdapter for testing.
After running the tests, the result similar to the following will be printed eventually. If all test cases pass, without any failures or errors.

```text
Test Suites: 8 passed, 8 total
Tests:       1 skipped, 44 passed, 45 total
Snapshots:   0 total
Time:        20.373 s
Ran all test suites.
```

### 5.2 Test Case Addition

All tests are organized by functional domain in the `nodejs/test` directory. You can add new test files in the corresponding directory or add test cases to existing test files.
The test cases use the jest framework. Generally, a connection is established and a database is created in the `beforeAll` method, and the database is droped and the connection is released in the `afterAll` method.

### 5.3 Performance Testing

Performance testing is in progress.

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/taos-connector-node/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-node)

## 7. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-node/issues/new?template=Blank+issue). When submitting, please provide the following information:

- Problem description, is it necessary to present it.
- Nodejs version.
- @tdengine/websocket version.
- Connection parameters (no username or password required).
- TDengine Server Version.

## 8. Submitting PRs

We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
1. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
1. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
1. Push the changes to the remote branch (`git push origin my_branch`).
1. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
1. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/taos-connector-node/pulls). Click on the corresponding link to see if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
1. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-node/pulls) page to check the test coverage.

## 9. References

- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. License

[MIT License](./LICENSE)
