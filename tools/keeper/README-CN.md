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
- [7. 提交 Issues](#7-提交-issues)
- [8. 提交 PR](#8-提交-pr)
- [9. 引用](#9-引用)
- [10. 许可证](#10-许可证)

## 1. 简介

taosKeeper 是 TDengine 3.0 版本全新引入的监控指标导出工具，旨在方便用户对 TDengine 的运行状态和性能指标进行实时监控。只需进行简单配置，TDengine 就能将自身的运行状态和各项指标等信息上报给 taosKeeper。taosKeeper 在接收到监控数据后，会利用 taosAdapter 提供的 RESTful 接口，将这些数据存储到 TDengine 中。

taosKeeper 的一个重要价值在于，它能够将多个甚至一批 TDengine 集群的监控数据集中存储到一个统一的平台。如此一来，监控软件便能轻松获取这些数据，进而实现对 TDengine 集群的全面监控与实时分析。通过 taosKeeper，用户可以更加便捷地了解 TDengine 的运行状况，及时发现并解决潜在问题，确保系统的稳定性和高效性。

## 2. 文档

- 使用 taosKeeper，请参考 [taosKeeper 参考手册](https://docs.taosdata.com/reference/components/taoskeeper/)，其中包括安装、配置、启动、数据收集与监控，以及集成 Prometheus 等方面的内容。
- 本 README 主要面向希望自行贡献代码、编译和测试 taosKeeper 的开发者。如果想要学习 TDengine，可以浏览 [官方文档](https://docs.taosdata.com/)。

## 3. 前置条件

1. 已安装 Go 1.23 及以上版本。
2. 本地已部署 TDengine，具体步骤请参考 [部署服务端](https://docs.taosdata.com/get-started/package/)，且已启动 taosd 与 taosAdapter。

## 4. 构建

在 `TDengine/tools/keeper` 目录下运行以下命令以构建项目：

```bash
go build
```

## 5. 测试

### 5.1 运行测试

在 `TDengine/tools/keeper` 目录下执行以下命令运行测试：

```bash
sudo go test ./...
```

测试用例将连接到本地的 TDengine 服务器和 taosAdapter 进行测试。测试完成后，你将看到类似如下的结果摘要。如果所有测试用例均通过，输出中将不会出现 `FAIL` 字样。

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

### 5.2 添加用例

在以 `_test.go` 结尾的文件中添加测试用例，并且确保新增代码都有对应的测试用例覆盖。

### 5.3 性能测试

性能测试正在开发中。

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/TDengine/actions/workflows/taoskeeper-ci-build.yml)
- Code Coverage - TODO

## 7. 提交 Issues

我们欢迎提交 [GitHub Issue](https://github.com/taosdata/TDengine/issues)。提交时请尽量提供以下信息，以便快速定位问题：

- 问题描述：具体问题表现及是否必现，建议附上详细调用堆栈或日志信息。
- taosKeeper 版本：可通过 `taoskeeper -V` 获取版本信息。
- TDengine 服务端版本：可通过 `taos -V` 获取版本信息。

如有其它相关信息（如环境配置、操作系统版本等），请一并补充，以便我们更全面地了解问题。

## 8. 提交 PR

我们欢迎开发者共同参与本项目开发，提交 PR 时请按照以下步骤操作：

1. Fork 仓库：请先 Fork 本仓库，具体步骤请参考 [如何 Fork 仓库](https://docs.github.com/en/get-started/quickstart/fork-a-repo)。
2. 创建新分支：基于 `main` 分支创建一个新分支，并使用有意义的分支名称（例如：`git checkout -b feature/my_feature`）。请勿直接在 main 分支上进行修改。
3. 开发与测试：完成代码修改后，确保所有单元测试都能通过，并为新增功能或修复的 Bug 添加相应的测试用例。
4. 提交代码：将修改提交到远程分支（例如：`git push origin feature/my_feature`）。
5. 创建 Pull Request：在 GitHub 上发起 [Pull Request](https://github.com/taosdata/TDengine/pulls)，具体步骤请参考 [如何创建 Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)。
6. 检查 CI：提交 PR 后，可在 Pull Request 中找到自己提交的 PR，点击对应的链接，即可查看该 PR 的 CI 是否通过。若通过，会显示 `All checks have passed`。无论 CI 是否通过，均可点击 `Show all checks -> Details` 查看详细的测试用例日志。

## 9. 引用

[TDengine 官网](https://www.taosdata.com/)

## 10. 许可证

[AGPL-3.0 License](../../LICENSE)
