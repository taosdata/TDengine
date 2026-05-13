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

[English](README.md) | 简体中文

<!-- omit in toc -->

## 目录

<!-- omit in toc -->

- [1.简介](#1简介)
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

## 1.简介

taosAdapter 是一个 TDengine 的配套工具，是 TDengine 集群和应用程序之间的桥梁和适配器。它提供了一种易于使用和高效的方式来直接从数据收集代理软件（如
Telegraf、StatsD、collectd 等）摄取数据。它还提供了 InfluxDB/OpenTSDB 兼容的数据摄取接口，允许 InfluxDB/OpenTSDB 应用程序无缝移植到
TDengine。 TDengine 的各语言连接器通过 WebSocket 接口与 TDengine 进行通信，因此必须安装 taosAdapter。

## 2. 文档

- 使用 taosAdapter，请参考 [taosAdapter 参考手册](https://docs.taosdata.com/reference/components/taosadapter/)。
- 本 README 主要是为想自己贡献、编译、测试 taosAdapter 的开发者写的。如果要学习
  TDengine，可以浏览 [官方文档](https://docs.taosdata.com/)。

## 3. 前置条件

- 已安装 Go 1.23 及以上版本，并允许 CGO `export CGO_ENABLED=1`。
- 本地已经部署 TDengine，具体步骤请参考 [部署服务端](https://docs.taosdata.com/get-started/package/)，且已经启动 taosd。

## 4. 构建

项目目录下执行 `go build` 构建项目。

## 5. 测试

### 5.1 运行测试

1. 执行测试前确保已经安装 TDengine 服务端，并且已经启动 taosd，数据库干净无数据。
2. 项目目录下执行 `go test ./...` 运行测试，测试会连接到本地的 TDengine 服务器进行测试。
3. 输出结果 `PASS` 为测试通过，`FAIL` 为测试失败，查看详细信息需要执行 `go test -v ./...`。

### 5.2 添加用例

在 `*_test.go` 文件中添加测试用例，确保测试用例覆盖到新增的代码。

### 5.3 性能测试

性能测试还在开发中。

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/taosadapter/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taosadapter)

## 7. 提交 Issue

我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taosadapter/issues/new?template=Blank+issue)。 提交时请说明下面信息：

- 问题描述，是否必现。
- taosAdapter 版本。
- TDengine 版本。

## 8. 提交 PR

我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：

1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
2. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。
3. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
4. 提交修改到远端分支 (`git push origin my_branch`)。
5. 在 GitHub 上创建一个 Pull
   Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
6. 提交 PR 后，可以通过 [Pull Request](https://github.com/taosdata/taosadapter/pulls) 找到自己的 PR，点击对应链接进去可以看到自己
   PR CI 是否通过，如果通过会显示 “All checks have passed”。无论 CI 是否通过，都可以点击 “Show all checks” -> “Details”
   来查看详细用例日志。
7. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taosadapter/pulls) 页面找到自己 PR 查看覆盖率。

## 9. 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. 许可证

[MIT License](./LICENSE)
