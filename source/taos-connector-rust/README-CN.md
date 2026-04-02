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

简体中文 | [English](./README.md)

<!-- omit in toc -->
## 目录

- [1. 简介](#1-简介)
- [2. 文档](#2-文档)
- [3. 前置条件](#3-前置条件)
- [4. 构建](#4-构建)
- [5. 测试](#5-测试)
  - [5.1 运行测试](#51-运行测试)
  - [5.2 添加用例](#52-添加用例)
  - [5.3 性能测试](#53-性能测试)
- [6. CI/CD](#6-cicd)
- [7. 提交 Issue](#7-提交-issue)
- [8. 提交 PR](#8-提交-pr)
- [9. 引用](#9-引用)
- [10. 许可证](#10-许可证)

## 1. 简介

`taos` 是 TDengine 的官方 Rust 语言连接器，Rust 开发人员可以通过它开发存取 TDengine 数据库的应用软件。它支持数据写入、数据查询、数据订阅、无模式写入以及参数绑定等功能。

## 2. 文档

- 使用 Rust Connector，请参考 [开发指南](https://docs.taosdata.com/develop/)，其中包括数据写入、数据查询、数据订阅、无模式写入以及参数绑定等示例。
- 其它参考信息请看 [参考手册](https://docs.taosdata.com/reference/connector/rust/)，其中包括版本历史、数据类型映射、示例程序汇总、API 参考以及常见问题等内容。
- 本 README 主要是为想自己贡献、编译、测试 Rust Connector 的开发者写的。如果要学习 TDengine，可以浏览 [官方文档](https://docs.taosdata.com/)。

## 3. 前置条件

1. 已安装 Rust 1.78 及以上版本，建议使用最新版本。
2. 本地已安装 TDengine，具体步骤请参考 [部署服务端](https://docs.taosdata.com/get-started/package/)。
3. 修改 `/etc/taos/taos.cfg` 配置文件，添加以下配置：
   ```text
   supportVnodes 256
   ```
4. 启动 taosd 与 taosAdapter。

## 4. 构建

在项目目录下运行以下命令以构建项目：

```bash
cargo build
```

## 5. 测试

### 5.1 运行测试

在项目目录下执行以下命令运行测试：

```bash
cargo test
```

测试用例将连接到本地的 TDengine 服务器和 taosAdapter 进行测试。测试完成后，将看到类似以下的结果摘要，若所有测试用例通过，`failed` 项应为 0：

```text
test result: ok. 101 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.21s
```

### 5.2 添加用例

1. 创建测试模块：在需要测试的 `.rs` 文件中，添加一个带有 `#[cfg(test)]` 属性的模块。该属性确保测试代码仅在执行测试时编译。

   ```rust
   #[cfg(test)]
   mod tests {
       // 在此处编写测试用例
   }
   ```

2. 引入被测模块的内容：在测试模块中，使用 `use super::*;` 将外部模块的所有内容引入测试模块的作用域，以便访问需要测试的函数和结构体。

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;

       // 在此处编写测试用例
   }
   ```

3. 编写测试函数：测试模块中，定义带有 `#[test]` 属性的函数。每个测试函数应包含以下步骤：

   - 设置：准备测试所需的数据或状态。
   - 执行：调用需要测试的函数或方法。
   - 断言：使用断言宏验证结果是否符合预期。

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

   上述示例中，`assert_eq!` 宏用于检查 `result` 是否等于预期值 `5`。如果不相等，测试将失败并引发 panic。

4. 异步函数测试：对于异步函数，可以使用 `#[tokio::test]` 属性宏来标记测试函数，并为其提供 Tokio 异步运行时。

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

   要启用异步测试支持，需确保在 `Cargo.toml` 中包含 Tokio 依赖。你可以依据项目需求选择适合的异步运行时和相应的测试属性宏。

5. 测试引发 panic 的情况：对于预期会引发 panic 的函数，可以使用 `#[should_panic]` 属性。该属性可选地接受 `expected` 参数，用于指定预期的 panic 消息。

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

   此示例中，`divide` 函数在分母为零时应引发 panic，且消息应为 "Divide by zero error"。

6. 忽略特定测试：对于耗时较长或不常运行的测试，可以使用 `#[ignore]` 属性标记。默认情况下，这些测试不会运行，除非使用 `cargo test -- --ignored` 命令显式运行。

   ```rust
   #[cfg(test)]
   mod tests {
       use super::*;

       #[test]
       #[ignore]
       fn test_long_running() {
           // 耗时较长的测试代码
       }
   }
   ```

### 5.3 性能测试

性能测试正在开发中。

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/taos-connector-rust/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-rust)

## 7. 提交 Issue

我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taos-connector-rust/issues/new?template=Blank+issue)。提交时请说明下面信息：

- 问题描述，是否必现，最好能包含详细调用堆栈。
- Rust 连接器版本。
- 连接参数（不需要用户名密码）。
- TDengine 服务端版本。

## 8. 提交 PR

我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：

1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
2. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。注意不要直接在 main 分支上修改。
3. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
4. 提交修改到远端分支 (`git push origin my_branch`)。
5. 在 GitHub 上创建一个 Pull Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
6. 提交 PR 后，可以通过 [Pull Request](https://github.com/taosdata/taos-connector-rust/pulls) 找到自己的 PR，点击对应链接进去可以看到自己 PR CI 是否通过，如果通过会显示 “All checks have passed”。无论 CI 是否通过，都可以点击 “Show all checks” -> “Details” 来查看详细用例日志。
7. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taos-connector-rust/pulls) 页面找到自己 PR，看单测覆盖率。

## 9. 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. 许可证

[MIT License](./LICENSE)
