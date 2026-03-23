# 代码仓库改造方案

## 1. 概述

### 1.1 改造背景

现有代码仓库结构无法满足安全管控和知识沉淀的需求。

1. **文档管理**：技术文档散落在各处，缺乏版本控制，AI Agent 读取需要做很多配置
2. **构建流程**：多仓库依赖，编译、打包步骤繁琐且代码混乱
3. **安全风险**：敏感代码缺乏有效隔离和权限管控（安可）

### 1.2 改造目标

1. 建立统一的代码仓库管理机制，创建内部 Gitlab 仓库，实现 GitHub 与 GitLab 协同
2. 提升构建、测试、部署自动化水平
3. 保障代码安全和知识产权保护，不同安全级别代码对应不同权限管控
4. 完善文档管理和知识传承机制，所有技术文档纳入版本管理

## 2. 仓库规划

### 2.1 代码主仓库

1. **TDengine-TSDB**：时序数据库核心代码仓库
2. **TDengine-IDMP**：工业数据管理平台代码仓库

### 2.2 文档主仓库

1. **TDengine-TSDB-docs**：时序数据库专属文档仓库
2. **TDengine-IDMP-docs**：工业数据平台专属文档仓库
3. **TDengine-Platform-docs**：平台级公共文档仓库

### 2.3 秘密文档仓库

1. **TDengine-Internal-docs**：敏感技术文档和内部资料

### 2.4 代码子仓库（Github）

**1. 开源代码**

- [https://github.com/taosdata/TDengine](https://github.com/taosdata/TDengine)
- [https://github.com/taosdata/taosadapter](https://github.com/taosdata/taosadapter)
- [https://github.com/taosdata/grafanaplugin](https://github.com/taosdata/grafanaplugin)
- [https://github.com/taosdata/taos-connector-jdbc](https://github.com/taosdata/taos-connector-jdbc)
- [https://github.com/taosdata/taos-connector-odbc](https://github.com/taosdata/taos-connector-odbc)
- [https://github.com/taosdata/taos-connector-python](https://github.com/taosdata/taos-connector-python)
- [https://github.com/taosdata/taos-connector-node](https://github.com/taosdata/taos-connector-node)
- [https://github.com/taosdata/taos-connector-rust](https://github.com/taosdata/taos-connector-rust)
- [https://github.com/taosdata/taos-connector-dotnet](https://github.com/taosdata/taos-connector-dotnet)
- [https://github.com/taosdata/driver-go](https://github.com/taosdata/driver-go)
- [https://github.com/taosdata/taosgen](https://github.com/taosdata/taosgen)

**2. 内部代码**

- [https://github.com/taosdata/TDinternal](https://github.com/taosdata/TDinternal)
- [https://github.com/taosdata/taosx](https://github.com/taosdata/taosx)
- [https://github.com/taosdata/grant-lib](https://github.com/taosdata/grant-lib)
- [https://github.com/taosdata/grant](https://github.com/taosdata/grant)
- [https://github.com/taosdata/TestNG](https://github.com/taosdata/TestNG)

**3. IDMP 代码：待 TSDB 改造完成后再实施**

### 2.5 同步策略（安可要求）

1. **开源代码仓库**：继续在 Github 工作，向 Gitlab 单向同步
2. **内部代码仓库**：Github 归档后，向 Gitlab 单向同步，后续在 Gitlab 开展工作

### 2.6 安全策略

1. **权限**
  1. GitHub 代码仓库：公开可读
  2. GitLab 代码及文档仓库：根据项目权限配置
  3. 合并权限：各组 Leader 合并，其他人员提 PR
2. **基线**
  1. 参考安可已编写的制度执行

## 3. TDengine-TSDB 仓库结构

以 TDengine-TSDB 为例，TDengine-IDMP 可以参考实现。

### 3.1 目录

```
仓库根目录/
├── source/               # 源码目录（submodule）
├── packaging/            # 打包配置和脚本
├── tools/                # 工具和构建脚本
├── tests/                # 测试相关
│   ├── stability/        # 常稳测试（submodule -> testNG）
│   ├── integration/      # 集成测试
│   ├── performance/      # 性能测试
│   └── ci/               # CI 测试脚本
├── docs/                 # 文档（submodule）
├── README.md             # 项目说明
├── .github/              # Github CI/CD 配置（后续废弃）
├── .gitlab/              # GitLab CI/CD 配置
└── .gitignore            # Git 忽略配置
```

### 3.2 source submodules

```
source/
├── taos-adapter/         (submodule -> taosadapter)
├── taos-community/       (submodule -> TDengine)
├── taos-internal/        (submodule -> TDinternal)
├── taos-grant-lib/       (submodule -> grant-lib)
├── taos-xservice/        (submodule -> taosX)
├── taos-insight/         (submodule -> grafanaplugin)
├── taos-gen/             (submodule -> taosgen)
├── taos-connector-jdbc/  (submodule -> taos-connector-jdbc)
├── taos-connector-odbc/  (submodule -> taos-connector-odbc)
├── taos-connector-python/(submodule -> taos-connector-python)
├── taos-connector-node/  (submodule -> taos-connector-node)
├── taos-connector-rust/  (submodule -> taos-connector-rust)
├── taos-connector-dotnet/(submodule -> taos-connector-dotnet)
└── taos-connector-go/    (submodule -> driver-go)
```

### 3.3 docs submodules

```
docs/
├── internal/         (submodule -> TDengine-Internal-docs) # 秘密文档
└── public/           (submodule -> TDengine-TSDB-docs)     # 公司内部公开文档
```

### 3.4 tools 目录

```
tools/
├── sync-github-to-gitlab/  # GitHub 到 GitLab 同步工具
├── code-quality/           # 代码质量检查工具
├── dependency-management/  # 依赖管理工具
└── security-scan/          # 安全扫描工具
```

## 4. TDengine-TSDB-docs 仓库结构

```
文档仓库根目录/
├── overview/
│   ├── 01-产品路线图/
│   ├── 02-总体设计/
│   └── 03-各模块设计/
├── releases/
│   ├── TSDB-v3.4.1-[20260331]/
│   └── TSDB-v3.4.2-[20260630]/
│       ├── 01-项目管理/
│       ├── 02-安全管理/
│       ├── 03-质量管理/
│       ├── 04-需求文档/
│       ├── 05-设计文档/
│       ├── 06-功能测试/
│       ├── 07-系统测试/
│       ├── 08-发布文档/
│       ├── 09-会议纪要和评审记录/
│       └── 10-其他文档/
├── reports/
│   ├── 2026Q1/
│   │   ├── agile-group/
│   │   ├── analysis-group/
│   │   ├── connector-group/
│   │   ├── query-group/
│   │   │   └── 王明明.md
│   │   └── taosx-group/
│   └── 2026Q2/
└── unplanned/
   ├── connector/
   ├── engine/
   └── taosX/
```

## 5. 时间安排

### 5.1 主要工作项

1. **代码仓库**：在 tools 目录提供脚本，根据选项仅初始化指定的仓库，改造不合要求的仓库（例如 TDinternal）
2. **代码编译**：在 tools 目录提供脚本，根据选项仅编译指定的仓库
3. **打包脚本**：在 packaging 目录提供脚本，根据选项编译完整、裁剪、OEM 等版本，各个仓库内部提供独立脚本（例如 TDengine 仓库可以打包社区版）
4. **CI/CD**
  1. TDengine、taos-connector-* 等社区仓库的 PR，依然采用现有 CI/CD 方式
  2. CI/CD 的构建方法按照新的打包脚本、编译脚本改造
5. **文档迁移**：现有文档迁移至文档仓库
6. **测试迁移**
  1. 单元测试、功能测试：保持现状
  2. 常稳测试、客户场景测试、性能测试、兼容性测试：进行少量改造，确保其正常运行即可

### 5.2 计划安排


| 时间                 | 工作内容                        | 负责人       |
| ------------------ | --------------------------- | --------- |
| 2026-03-23 ~ 03-29 | 完成代码仓库与代码编译调试               | @关胜亮 @霍琳贺 |
| 2026-03-30 ~ 04-10 | 完成打包脚本、文档迁移、CI/CD 改造、测试迁移工作 | @王旭 @陈浩然  |
| 2026-04-11 ~ 04-15 | 完成上线与宣贯                     | @肖波       |


