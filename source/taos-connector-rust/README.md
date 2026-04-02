<!-- omit in toc -->
# TDengine Rust Connector

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-rust/build.yml)](https://github.com/taosdata/taos-connector-rust/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taos-connector-rust/branch/main/graph/badge.svg?token=P11UKNLTVO)](https://codecov.io/gh/taosdata/taos-connector-rust)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-rust)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-rust)
[![Crates.io](https://img.shields.io/crates/v/taos)](https://crates.io/crates/taos)
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

`taos` is the official Rust language connector of TDengine, through which Rust developers can develop applications that access TDengine databases. It supports data writing, data query, data subscription, schemaless writing, parameter binding and other functions.

## 2. Documentation

- To use Rust Connector, please check [Developer Guide](https://docs.taosdata.com/develop/), which includes examples of data writing, data querying, data subscription, modeless writing, and parameter binding.
- For other reference information, please refer to the [Reference Manual](https://docs.taosdata.com/reference/connector/rust/), which includes version history, data type mapping, sample program summary, API reference, and FAQ.
- This quick guide is mainly for developers who like to contribute/build/test the Rust connector by themselves. To learn about TDengine, you can visit the [official documentation](https://docs.tdengine.com).

## 3. Prerequisites

1. Rust 1.78 or above has been installed. The latest version is recommended.
2. TDengine has been installed locally. For specific steps, please refer to [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/).
3. Modify the `/etc/taos/taos.cfg` configuration file and add the following configuration:
   ```text
   supportVnodes 256
   ```
4. Start taosd and taosAdapter.

## 4. Build

Run the following command in the project directory to build the project:

```bash
cargo build
```

## 5. Testing

### 5.1 Test Execution

Run the test by executing the following command in the project directory:

```bash
cargo test
```

The test case will connect to the local TDengine server and taosAdapter for testing. After the test is completed, you will see a result summary similar to the following. If all test cases pass, the `failed` item should be 0:

```text
test result: ok. 101 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.21s
```

### 5.2 Test Case Addition

1. Create a test module: In the `.rs` file that needs to be tested, add a module with the `#[cfg(test)]` attribute. This attribute ensures that the test code is only compiled when the test is executed.

   ```rust
   #[cfg(test)]
   mod tests {
       // Write your test cases here
   }
   ```

2. Import the contents of the module under test: In the test module, use `use super::*;` to import all the contents of the external module into the scope of the test module so that you can access the functions and structures that need to be tested.

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;

       // Write your test cases here
   }
   ```

3. Write test functions: In the test module, define functions with the `#[test]` attribute. Each test function should contain the following steps:

   - Setup: Prepare the data or state required for the test.
   - Execution: Call the function or method that needs to be tested.
   - Assertions: Use assertion macros to verify that the results are as expected.

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;

       #[test]
       fn test_add() {
           let result = add(2, 3);
           assert_eq!(result, 5);
       }
   }
   ```

   In the above example, the `assert_eq!` macro is used to check if `result` is equal to the expected value `5`. If not, the test will fail and panic.

4. Asynchronous function testing: For asynchronous functions, you can use the `#[tokio::test]` attribute macro to mark the test function and provide it with the Tokio asynchronous runtime.

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;
       use tokio;

       #[tokio::test]
       async fn test_async_function() {
           let result = async_function().await;
           assert_eq!(result, expected_value);
       }
   }
   ```

   To enable asynchronous testing support, make sure to include the Tokio dependency in your `Cargo.toml`. You can choose the appropriate asynchronous runtime and corresponding test property macros based on your project needs.

5. Test panic cases: For functions that are expected to panic, you can use the `#[should_panic]` attribute. This attribute optionally accepts an `expected` parameter to specify the expected panic message.

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;

       #[test]
       #[should_panic(expected = "Divide by zero error")]
       fn test_divide_by_zero() {
           divide(1, 0);
       }
   }
   ```

   In this example, the `divide` function should panic when the denominator is zero, and the message should be "Divide by zero error".

6. Ignore specific tests: For tests that take a long time or are not run often, you can use the `#[ignore]` attribute to mark them. By default, these tests will not be run unless explicitly run with the `cargo test -- --ignored` command.

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;

       #[test]
       #[ignore]
       fn test_long_running() {
           // Long-running test code
       }
   }
   ```

### 5.3 Performance Testing

Performance testing is under development.

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/taos-connector-rust/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-rust)

## 7. Submitting Issues

We welcome the submission of [GitHub Issue](https://github.com/taosdata/taos-connector-rust/issues/new?template=Blank+issue). When submitting, please provide the following information:

- Description of the problem, whether it must occur, preferably with detailed call stack.
- Rust connector version.
- Connection parameters (no username or password required).
- TDengine server version.

## 8. Submitting PRs

We welcome developers to contribute to this project. When submitting PRs, please follow these steps:

1. Fork this project, refer to ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
2. Create a new branch from the main branch with a meaningful branch name (`git checkout -b my_branch`). Do not modify the main branch directly.
3. Modify the code, ensure all unit tests pass, and add new unit tests to verify the changes.
4. Push the changes to the remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
6. After submitting the PR, you can find your PR through the [Pull Request](https://github.com/taosdata/taos-connector-rust/pulls). Click on the corresponding link to see if the CI for your PR has passed. If it has passed, it will display "All checks have passed". Regardless of whether the CI passes or not, you can click "Show all checks" -> "Details" to view the detailed test case logs.
7. After submitting the PR, if CI passes, you can find your PR on the [codecov](https://app.codecov.io/gh/taosdata/taos-connector-rust/pulls) page to check the test coverage.

## 9. References

- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. License

[MIT License](./LICENSE)
