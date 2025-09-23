# 目录

1. [简介](#1-简介)
2. [必备工具](#2-必备工具)
3. [测试指南](#3-测试指南)
    - [3.1 单元测试](#31-单元测试)
    - [3.2 系统测试](#32-系统测试)
    - [3.3 TSIM测试](#33-tsim测试)
    - [3.4 冒烟测试](#34-冒烟测试)
    - [3.5 混沌测试](#35-混沌测试)
    - [3.6 CI测试](#36-ci测试)

# 1. 简介

本手册旨在为开发人员提供有效测试TDengine的全面指导。它分为三个主要部分：简介，必备工具和测试指南。

> [!NOTE]
> - 本文档所有的命令和脚本在Linux（Ubuntu 18.04/20.04/22.04）上进行了验证。
> - 本文档所有的命令和脚本用于在单个主机上运行测试。

# 2. 必备工具

- 安装Python3

```bash
apt install python3
apt install python3-pip
```

- 安装Python依赖工具包

```bash
pip3 install pandas psutil fabric2 requests faker simplejson \
  toml pexpect tzlocal distro decorator loguru hyperloglog
```

- 安装TDengine的Python连接器

```bash
pip3 install taospy taos-ws-py
```

- 构建

在测试之前，请确保选项“-DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true”的构建操作已经完成，如果没有，请执行如下命令:

```bash
cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true
make && make install
```

# 3. 测试指南

在 `tests` 目录中，TDengine有不同类型的测试。下面是关于如何运行它们以及如何添加新测试用例的简要介绍。

### 3.1 单元测试

单元测试是最小的可测试单元，用于测试TDengine代码中的函数、方法或类。

### 3.1.1 如何运行单个测试用例?

```bash
cd debug/build/bin
./osTimeTests
```

### 3.1.2 如何运行所有测试用例?

```bash
cd tests/unit-test/
bash test.sh -e 0
```

### 3.1.3 如何添加测试用例? 

<details>

<summary>添加新单元测试用例的详细步骤</summary>

Google测试框架用于对特定功能模块进行单元测试，请参考以下步骤添加新的测试用例:

##### a. 创建测试用例文件并开发测试脚本

在目标功能模块对应的测试目录下，创建CPP格式的测试文件，编写相应的测试用例。

##### b. 更新构建配置

修改此目录中的CMakeLists.txt文件, 以确保新的测试文件被包含在编译过程中。配置示例可参考 `source/os/test/CMakeLists.txt`

##### c. 编译测试代码

在项目的根目录下，创建一个编译目录 (例如 debug), 切换到该目录并运行cmake命令 (如 `cmake .. -DBUILD_TEST=1` ) 生成编译文件，
然后运行make命令（如 make）来完成测试代码的编译。

##### d. 执行测试

在编译目录中找到可执行文件并运行它 (如：`TDengine/debug/build/bin/`)。

##### e. 集成用例到CI测试

使用add_test命令将新编译的测试用例添加到CI测试集合中，确保新添加的测试用例可以在每次构建运行。

</details>

## 3.2 系统测试

系统测试是用Python编写的端到端测试用例。其中一些特性仅在企业版中支持和测试，因此在社区版上运行时，它们可能会失败。我们将逐渐通过将用例分成不同的组来解决这个问题。

### 3.2.1 如何运行单个测试用例?

以测试文件 `system-test/2-query/avg.py` 举例，可以使用如下命令运行单个测试用例:

```bash
cd tests/system-test
python3 ./test.py -f 2-query/avg.py
```

### 3.2.2 如何运行所有测试用例?

```bash
cd tests
./run_all_ci_cases.sh -t python # all python cases
```

### 3.2.3 如何添加测试用例?

<details>

<summary>添加新系统测试用例的详细步骤</summary>

Python测试框架由TDengine团队开发, test.py是测试用例执行和监控的入口程序，使用 `python3 ./test.py -h` 查看更多功能。

请参考下面的步骤来添加一个新的测试用例:

##### a. 创建一个测试用例文件并开发测试用例

在目录 `tests/system-test` 下的某个功能目录创建一个测试用例文件, 并参考用例模板 `tests/system-test/0-others/test_case_template.py` 来添加一个新的测试用例。

##### b. 执行测试用例

使用如下命令执行测试用例, 并确保用例执行成功。

``` bash
cd tests/system-test && python3 ./test.py -f 0-others/test_case_template.py 
```

##### c. 集成用例到CI测试

编辑 `tests/parallel_test/cases.task`, 以指定的格式添加测试用例路径。文件的第三列表示是否使用 Address Sanitizer 模式进行测试。

```bash
#caseID,rerunTimes,Run with Sanitizer,casePath,caseCommand
,,n,system-test, python3 ./test.py  -f 0-others/test_case_template.py 
```

</details>

## 3.3 TSIM测试

在TDengine开发的早期阶段, TDengine团队用C++开发的内部测试框架 TSIM。

### 3.3.1 如何运行单个测试用例?

要运行TSIM测试用例，请执行如下命令:

```bash
cd tests/script
./test.sh -f tsim/db/basic1.sim
```

### 3.3.2 如何运行所有TSIM测试用例?

```bash
cd tests
./run_all_ci_cases.sh -t legacy # all legacy cases
```

### 3.3.3 如何添加TSIM测试用例?

> [!NOTE] 
> TSIM测试框架现已被系统测试弃用，建议在系统测试中增加新的测试用例，请参考 [系统测试](#32-系统测试)。 

## 3.4 冒烟测试

冒烟测试是从系统测试中选择的一组测试用例，也称为基本功能测试，以确保TDengine的关键功能。

### 3.4.1 如何运行冒烟测试?

```bash
cd /root/TDengine/packaging/smokeTest
./test_smoking_selfhost.sh
```

### 3.4.2 如何添加冒烟测试用例?

可以通过更新 `test_smoking_selfhost.sh` 中的 `commands` 变量的值来添加新的case。

## 3.5 混沌测试

一个简单的工具，以随机的方式执行系统的各种功能测试，期望在没有预定义测试场景的情况下暴露潜在的问题。

### 3.5.1 如何运行混沌测试?

```bash
cd tests/pytest
python3 auto_crash_gen.py
```

### 3.5.2 如何增加混沌测试用例?

1. 添加一个函数，如 `pytest/crash_gen/crash_gen_main.py` 中的 `TaskCreateNewFunction`。
2. 将 `TaskCreateNewFunction` 集成到 `crash_gen_main.py` 中的 `balance_pickTaskType` 函数中。

## 3.6 CI测试

CI测试（持续集成测试）是软件开发中的一项重要实践，旨在将代码频繁地自动集成到共享代码库的过程中，构建和测试它以确保代码的质量和稳定性。

TDengine CI测试将运行以下三种测试类型中的所有测试用例：单元测试、系统测试和TSIM测试。

### 3.6.1 如何运行所有CI测试用例?

如果这是第一次运行所有CI测试用例，建议添加测试分支，使用如下命令运行:

```bash
cd tests
./run_all_ci_cases.sh -b main # on main branch
```

### 3.6.2 如何添加新的CI测试用例?

请参考[单元测试](#31-单元测试)、[系统测试](#32-系统测试)和[TSIM测试](#33-tsim测试)部分，了解添加新测试用例的详细步骤，当在上述测试中添加新用例时，它们将在CI测试自动运行。
