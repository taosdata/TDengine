---
title: 开发者指南
sidebar_label: 开发者指南
description: 如何将统计分析、机器学习、时序大模型添加到 TDgpt
---

TDgpt 是一个可扩展的时序数据高级分析智能体，用户遵循简易的步骤就能将自己开发的分析算法添加到分析平台，各种应用就可以通过 SQL 语句直接调用，让高级分析算法的使用门槛降到几乎为零。目前 TDpgt 平台只支持使用 Python 语言开发的分析算法。
anode 采用类动态加载模式，在启动的时候扫描特定目录内满足约定条件的所有代码文件，并将其加载到系统中。因此，开发者只需要遵循以下几步就能完成新算法的添加工作。

1. 开发完成符合要求的分析算法类
2. 将代码文件放入对应目录，然后重启 anode
3. 使用 SQL 命令 `CREATE ANODE`，将 anode 添加到 TDengine

完成新算法添加工作后，就可以直接使用 SQL 语句调用新算法。得益于 TDgpt 与 TDengine 主进程 `taosd` 的松散耦合，anode 算法升级对 `taosd` 完全没有影响。应用系统只需要调整对应的 SQL 语句调用新（升级的）算法，就能够快速完成分析功能和分析算法的升级。

这种方式能够按需扩展分析算法，极大地拓展 TDgpt 的适应范围，用户可以按需将更契合业务场景的、更准确的（预测、异常检测）分析算法动态嵌入到 TDgpt，并通过 SQL 语句调用。在基本不用更改应用系统代码的前提下，就能够快速完成分析功能的平滑升级。

以下内容将说明如何将分析算法添加到 anode 中并能够通过 SQL 语句进行调用。

## 环境准备

建议进行进行分析模型开发的研发人员首先从 github 上克隆 [TDengine 社区版本](https://github.com/taosdata/tdengine) 源代码。
在克隆到本地的源代码目录中，TDgpt 的源代码位于 `./tools/tdgpt` 。[PyCharm 社区版](https://www.jetbrains.com/pycharm/download)
直接打开该目录即可以进行开发。

## 目录结构

anode 的主要目录结构如下图所示

```text
.
├── bin
├── cfg
├── lib
│   └── taosanalytics
│       ├── algo
│       │   ├── ad
│       │   └── fc
│       ├── misc
│       └── test
├── log -> /var/log/taos/taosanode
├── model -> /var/lib/taos/taosanode/model
└── venv -> /var/lib/taos/taosanode/venv
```

| 目录            | 说明                                                                                    |
|---------------|---------------------------------------------------------------------------------------|
| taosanalytics | 代码目录，包含模型代码保存目录 algo 和放置杂项目录 misc  <br/> 单元测试和集成测试目录 test <br/> 其下的 ad 目录存放异常检测算法代码，fc 目录存放预测算法代码 |
| venv          | Python 虚拟环境                                                                           |
| model         | 放置针对数据集完成的训练模型                                                                        |
| cfg           | 配置文件目录                                                                                |

## 约定与限制

- 异常检测算法 Python 代码文件需放在 `./taos/algo/ad` 目录中
- 预测分析算法 Python 代码文件需放在 `./taos/algo/fc` 目录中

### 类命名规范

anode 采用算法自动加载模式，因此只识别符合命名约定的 Python 类。需要加载的算法类名称需要以下划线 `_` 开始并以 `Service` 结尾。例如：`_KsigmaService` 是 KSigma 异常检测算法类。

### 类继承约定

- 异常检测算法需要从 `AbstractAnomalyDetectionService` 继承，并实现其核心抽象方法 `execute`
- 预测分析算法需要从 `AbstractForecastService` 继承，同样需要实现其核心抽象方法 `execute`

### 类属性初始化

实现的类需要初始化以下两个类属性：

- `name`：识别该算法的关键词，全小写英文字母。通过 `SHOW` 命令查看可用算法显示的名称即为该名称。
- `desc`：算法的基础描述信息

```SQL
--- algo 后面的参数 name 即为类属性 `name`
SELECT COUNT(*)
FROM foo ANOMALY_WINDOW(col_name, 'algo=name')
```
