<!-- omit in toc -->
# TDengine Python Connector


[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taos-connector-python/build.yml)](https://github.com/taosdata/taos-connector-python/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taos-connector-python/branch/main/graph/badge.svg?token=BDANN3DBXS)](https://codecov.io/gh/taosdata/taos-connector-python)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taos-connector-python)
![PyPI](https://img.shields.io/pypi/dm/taospy)
![GitHub License](https://img.shields.io/github/license/taosdata/taos-connector-python)
[![PyPI](https://img.shields.io/pypi/v/taospy)](https://pypi.org/project/taospy/)
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

`taospy` 是 TDengine 官方 Python 连接器，允许 Python 开发人员访问 TDengine 数据库的应用程序, 支持数据普通写入、无模式写入、参数绑定写入、查询及订阅等功能。  
`taospy` 的 API 符合 Python DB API 2.0（PEP-249） 规范，主要由两个模块组成：
1. `taos` 模块, 它使用 TDengine C 客户端库进行客户端-服务器间通信。
2. `taorsest` 模块, 它使用 TDengine RESTful API ，API 符合 Python DB API 2.0（PEP-249）规范, 使用此模块，您不需要再安装 TDengine C 客户端库。

## 2. 文档
- 使用 `Python 连接器`, 请参考 [开发指南](https://docs.taosdata.com/develop/)，包含了应用如何引入 `Python 连接器` 及数据写入、查询及数据订阅等示例。
- 其他参考信息请看 [参考手册](https://docs.taosdata.com/reference/connector/python/)，包含了版本历史、数据类型、示例程序汇总、API 说明和常见问题等。
- 本 README 主要是为想自己贡献、编译、测试 `Python 连接器` 的开发者写的。如果要学习 TDengine，可以浏览 [官方文档](https://docs.taosdata.com/)。

## 3. 前置条件

- Python 运行环境 (taospy: Python >= 3.6.2, taos-ws-py: Python >= 3.7)。
- 本地已部署 TDengine，具体步骤请参考 [部署服务端](https://docs.taosdata.com/get-started/package/)，且已经启动 taosd 与 taosAdapter。

## 4. 构建

下载代码库并在根目录中执行以下操作以构建开发环境：
``` bash
pip3 install -e ./ 
```

## 5. 测试
### 5.1 运行测试
Python 连接器测试用例使用 `pytest` 测试框架。  
组件 `taospy` 测试用例目录是: tests/  
组件 `taos-ws-py` 测试用例目录是: taos-ws-py/tests/  

搭建本地测试环境及运行测试用例的代码已封装在一个脚本文件中，执行以下命令即可轻松搭建及执行测试用例：
``` bash
# for taospy
bash ./test_taospy.sh

# for taos-ws-py
bash ./test_taos-ws-py.sh
```

### 5.2 添加用例
您可以添加新测试文件或在现有测试文件中添加符合 `pytest` 标准的测试用例。

### 5.3 性能测试
性能测试还在开发中。

## 6. CI/CD
- [Build Workflow](https://github.com/taosdata/taos-connector-python/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/gh/taosdata/taos-connector-python)

## 7. 提交 Issue
我们欢迎提交 [GitHub Issue](https://github.com/taosdata/taos-connector-python/issues/new?template=Blank+issue)。 提交时请说明下面信息：
- 问题描述，是否必现，最好能包含详细调用堆栈。
- Python 连接器版本。
- Python 连接器连接参数（不需要用户名密码）。
- TDengine 服务端版本。

## 8. 提交 PR
我们欢迎开发者一起开发本项目，提交 PR 时请参考下面步骤：
1. Fork 本项目，请参考 ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo))。
2. 从 main 分支创建一个新分支，请使用有意义的分支名称 (`git checkout -b my_branch`)。注意不要直接在 main 分支上修改。
3. 修改代码，保证所有单元测试通过，并增加新的单元测试验证修改。
4. 提交修改到远端分支 (`git push origin my_branch`)。
5. 在 GitHub 上创建一个 Pull Request ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request))。
6. 提交 PR 后，可以通过 [Pull Request](https://github.com/taosdata/taos-connector-python/pulls) 找到自己的 PR，点击对应链接进去可以看到自己 PR CI 是否通过，如果通过会显示 “All checks have passed”。无论 CI 是否通过，都可以点击 “Show all checks” -> “Details” 来查看详细用例日志。
7. 提交 PR 后，如果 CI 通过，可以在 [codecov](https://app.codecov.io/gh/taosdata/taos-connector-python/pulls) 页面找到自己 PR，看单测覆盖率。

## 9. 引用

- [TDengine 官网](https://www.taosdata.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 10. 许可证

[MIT License](./LICENSE)