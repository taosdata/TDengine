# 代码仓库改造方案

> **状态**：已完成（2026-04 上线）

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

### 1.3 实施结果

改造已完成，最终采用**单一仓库（monorepo）** 方案，所有源码和文档以普通目录形式内联管理，未使用 git submodule。主要变更：

1. 源码目录 `source/` 下各组件为普通目录，不再是独立 submodule
2. 文档目录 `docs/` 直接内嵌在同一仓库中，未拆分为独立文档仓库
3. 顶层新增 `cmake/` 目录和 `CMakeLists.txt`，通过 CMake option 按需编译各组件
4. `.github/` 目录集成了 AI Agent skills 和 MCP 配置

## 2. 仓库规划

### 2.1 代码主仓库

1. **tsdb**：时序数据库核心代码仓库
2. **idmp**：工业数据管理平台代码仓库
2. **platform**：平台组代码仓库


### 2.3 秘密代码仓库

1. **tsdb-internal-docs**：敏感技术文档和内部资料

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
2. **内部代码仓库**：Github 归档后，向 Gitlab 单向同步，后续在 Gitlab 开展工作（择机而定）

### 2.6 安全策略

1. **权限**
  1. GitHub 代码仓库：公开可读
  2. GitLab 代码及文档仓库：根据项目权限配置
  3. 合并权限：各组 Leader 合并，其他人员提 PR
2. **基线**
  1. 参考安可已编写的制度执行

## 3. tsdb 仓库实际结构

以 tsdb 为例，idmp 可以参考实现。最终采用 monorepo 方案，所有组件以普通目录内联，不使用 git submodule。

### 3.1 根目录

```
仓库根目录/
├── CMakeLists.txt        # 顶层 CMake 构建入口，按 option 选择编译组件
├── cmake/                # CMake 构建配置
│   ├── build-config.cmake
│   ├── taos-adapter.cmake
│   ├── taos-connector-*.cmake
│   ├── taos-gen.cmake
│   ├── taos-insight.cmake
│   ├── taos-keeper.cmake
│   ├── taos-xservice.cmake
│   └── toolchains/       # 工具链补丁
├── source/               # 源码目录（普通目录，非 submodule）
├── packaging/            # 打包配置和脚本
├── tools/                # 工具和构建脚本
├── tests/                # 测试相关
├── docs/                 # 文档（内嵌，非 submodule）
├── downloads/            # 下载产物暂存
├── .github/              # GitHub CI/CD 配置 & AI Agent 配置
│   ├── agents/           # AI Agent 定义
│   ├── skills/           # AI Agent 技能库
│   └── mcp.json          # MCP 服务配置
└── .gitignore            # Git 忽略配置
```

### 3.2 source 目录

```
source/
├── taos-adapter/           # taosadapter（Go）
├── taos-community/         # TDengine 社区版（C/C++）
├── taos-internal/          # TDinternal 企业版增量
├── taos-grant-lib/         # 授权库
├── taos-xservice/          # taosX 数据接入服务
├── taos-insight/           # Grafana 插件
├── taos-gen/               # taosgen 数据生成工具
├── taos-connector-jdbc/    # JDBC 连接器
├── taos-connector-odbc/    # ODBC 连接器
├── taos-connector-python/  # Python 连接器
├── taos-connector-node/    # Node.js 连接器
├── taos-connector-rust/    # Rust 连接器
├── taos-connector-dotnet/  # .NET 连接器
└── taos-connector-go/      # Go 连接器
```

### 3.3 tests 目录

```
tests/
├── customer-scenario-tests/  # 客户场景测试
├── performance-tests/        # 性能测试
├── security-tests/           # 安全测试
└── stability-tests/          # 常稳测试
```

### 3.4 tools 目录

```
tools/
├── ci/                     # CI 相关脚本
├── deps/                   # 依赖管理（含 install_deps.sh、windows/）
└── scripts/                # 通用工具脚本
```

### 3.5 CMake 构建选项

顶层 `CMakeLists.txt` 提供以下 option，可按需开关各组件编译：

| Option | 默认值 | 说明 |
| --- | --- | --- |
| BUILD_ENTERPRISE | ON | 构建企业版 |
| BUILD_ENGINE | ON | 构建引擎（TDengine） |
| BUILD_ADAPTER | ON | 构建 taosadapter |
| BUILD_KEEPER | ON | 构建 taoskeeper |
| BUILD_TOOLS | ON | 构建工具组件 |
| BUILD_GEN | ON | 构建 taos-gen |
| BUILD_TAOSX | ON | 构建 taos-xservice |
| BUILD_INSIGHT | ON | 构建 taos-insight |
| BUILD_DOTNET / GO / JDBC / NODE / ODBC / PYTHON / RUST | ON | 构建各连接器 |
| BUILD_TEST | OFF | 构建单元测试 |
| BUILD_SANITIZER | OFF | 启用 Sanitizer |
| BUILD_COVERAGE | OFF | 启用代码覆盖率 |

## 4. docs 目录结构（内嵌）

文档直接内嵌在仓库 `docs/` 目录下，未拆分为独立仓库。

```
docs/
├── README.md
├── overview/
│   ├── 01-产品路线图/
│   ├── 02-总体设计/
│   ├── 03-各模块设计/
│   └── 04-行为变更/
├── releases/
│   ├── TSDB-v3.0.3-[20230228]/
│   ├── ...                          # 历史版本
│   ├── TSDB-v3.4.1-[20260331]/
│   ├── TSDB-v3.4.2-[20260630]/
│   │   ├── 01-项目管理/
│   │   ├── 02-安全管理/
│   │   ├── 03-质量管理/
│   │   ├── 04-需求文档/
│   │   ├── 05-设计文档/
│   │   ├── 06-功能测试/
│   │   ├── 07-系统测试/
│   │   ├── 08-发布文档/
│   │   ├── 09-会议纪要和评审记录/
│   │   └── 10-其他文档/
│   └── TSDB-v3.4.3-[20260930]/
├── templates/
│   ├── 01-项目管理模版/
│   ├── 02-需求文档模版/
│   ├── 03-设计文档模版/
│   ├── 04-测试文档模版/
│   ├── 05-发布文档模版/
│   ├── 06-安全文档模版/
│   ├── 07-质量文档模版/
│   └── 08-其他模版/
├── reports/
│   ├── 2026Q1/
│   └── 2026Q2/
└── unplanned/
    ├── connector/
    ├── engine/
    └── tools/
```

## 5. 时间安排

### 5.1 主要工作项

1. **代码仓库**：采用 monorepo 方案，所有组件以普通目录内联管理 ✅
2. **代码编译**：顶层 CMakeLists.txt 通过 option 按需编译各组件 ✅
3. **打包脚本**：packaging 目录（待完善） 🔧
4. **CI/CD**
   1. TDengine、taos-connector-* 等社区仓库的 PR，依然采用现有 CI/CD 方式
   2. CI/CD 的构建方法按照新的 CMake 选项改造
5. **文档迁移**：技术文档已迁入 docs/ 目录，含模版、版本发布文档、周报等 ✅
6. **测试迁移**
   1. 单元测试、功能测试：保持现状
   2. 常稳测试、客户场景测试、性能测试、安全测试：已迁入 tests/ 目录 ✅

### 5.2 计划安排

| 时间                 | 工作内容                        | 负责人       | 状态 |
| ------------------ | --------------------------- | --------- | --- |
| 2026-03-23 ~ 04-01 | 完成代码仓库与代码编译调试               | @关胜亮 @霍琳贺 | ✅ 已完成 |
| 2026-04-01 ~ 04-10 | 完成打包脚本、文档迁移、CI/CD 改造、测试迁移工作 | @王旭 @陈浩然  | ✅ 已完成 |
| 2026-04-11 ~ 04-15 | 完成上线与宣贯                     | @肖波       | ✅ 已完成 |

### 5.3 方案调整说明

实施过程中对原方案做了以下调整：

1. **放弃 submodule 方案**：原计划各源码目录作为 git submodule 引用，实际改为普通目录内联，简化了日常开发流程
2. **文档仓库内嵌**：原计划拆分为独立的 `taos-tsdb-docs`、`taos-internal-docs` 仓库，实际内嵌在同一仓库的 `docs/` 目录下
3. **新增 cmake 目录**：提供统一的 CMake 构建体系，每个组件对应一个 `.cmake` 文件
4. **新增 templates 目录**：`docs/templates/` 提供各类文档模版，规范文档编写
5. **新增 AI Agent 配置**：`.github/` 下集成 agents、skills、mcp.json，支持 AI 辅助开发


