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

[English](README.md) | 简体中文

## 目录
<!-- omit in toc -->

- [目录](#目录)
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

@tdengine/websocket 是 TDengine 官方专为 Node.js 开发人员精心设计的一款高效连接器，它借助 taosAdapter 组件提供的 WebSocket API 与 TDengine 建立连接，摆脱了对 TDengine 客户端驱动的依赖 ，为开发者开辟了一条便捷的开发路径。凭借这一强大工具，开发人员能够轻松构建面向 TDengine 集群的应用程序。不管是执行复杂的 SQL 写入与查询任务，还是实现灵活的无模式写入操作，亦或是达成对实时性要求极高的订阅功能，这款连接器都能轻松胜任、完美实现，全方位满足多样化的数据交互需求。

## 2. 文档

- 使用 Node.js Connector, 请参考 [开发指南](https://docs.taosdata.com/develop/)，包含了应用如何引入 @tdengine/websocket 和数据写入、查询、无模式写入、参数绑定和数据订阅等示例。
- 其他参考信息请看 [参考手册](https://docs.taosdata.com/reference/connector/node/)，包含了版本历史、数据类型、示例程序汇总、API 说明和常见问题等。
- 本 README 主要是为想自己贡献、编译、测试 Node.js Connector 的开发者写的。如果要学习 TDengine，可以浏览 [官方文档](https://docs.taosdata.com/)。

## 3. 前置条件

- 安装 Node.js 开发环境, 使用 14 以上版本，[下载 Node.js](https://nodejs.org/en/download/)。
- 使用 npm 安装 TypeScript 5.3.3 以上版本。
- 使用 npm 安装 Node.js 连接器依赖, 在项目的 nodejs 目录下执行 `npm install` 命令进行安装。
- 本地已经部署 TDengine，具体步骤请参考 [部署服务端](https://docs.taosdata.com/get-started/package/)，且已经启动 `taosd` 与 `taosAdapter`。

## 4. 构建

在项目 `nodejs` 目录下执行 `tsc` 构建项目。

## 5. 测试

### 5.1 运行测试

在项目的 `nodejs` 目录下，执行 `npm run test` 命令，即可运行 `test` 目录下的所有测试用例。这些测试用例将连接本地的 `TDengine` 服务器与 `taosAdapter` 进行相关测试。在终端内将实时输出各测试用例的执行结果。待整个测试流程结束，如果所有用例通过，没有 Failures 和 Errors, 并且还会给出相应的覆盖率数据。

```text
Test Suites: 8 passed, 8 total
Tests:       1 skipped, 44 passed, 45 total
Snapshots:   0 total
Time:        20.373 s
Ran all test suites.
```

### 5.2 添加用例

所有测试按功能域组织在 `nodejs/test` 目录下，可以在对应目录新增测试文件或者在已有测试文件中添加用例。用例使用 jest 框架，一般在 `beforeAll` 方法中建立连接和创建数据库，在 `afterAll` 方法中删除数据库和释放连接。

### 5.3 性能测试

性能测试还在开发中。

## 6. CI/CD

- [Build Workflow](https://github.com/taosdata/taos-connector-node/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-node)

## 7. 提交 Issue

我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taos-connector-node/issues/new?template=Blank+issue)。 提交时请说明下面信息：

- 问题描述，是否必现。
- Nodejs 版本。
- @tdengine/websocket 版本。
- 连接参数（不需要用户名密码）。
- TDengine 服务端版本。

## 8. 提交 PR

我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：

1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
1. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。注意不要直接在 main 分支上修改。
1. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
1. 提交修改到远端分支 (`git push origin my_branch`)。
1. 在 GitHub 上创建一个 Pull Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
1. 提交 PR 后，可以通过 [Pull Request](https://github.com/taosdata/taos-connector-node/pulls) 找到自己的 PR，点击对应链接进去可以看到自己 PR CI 是否通过，如果通过会显示 “All checks have passed”。无论 CI 是否通过，都可以点击 “Show all checks” -> “Details” 来查看详细用例日志。
1. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taos-connector-node/pulls) 页面找到自己 PR，看单测覆盖率。

## 9. 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. 许可证

[MIT License](./LICENSE)
