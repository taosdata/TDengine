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

## 6. CI/CD 编译基础设施（tsdb-builder 概要设计）

> 来源：[TSDB GitLab CI/CD 迁移之 tsdb-builder 概要设计](https://taosdata.feishu.cn/wiki/N8o2wMXm1ibVoykVWYacFPsRnhe)

### 6.1 背景与目标

TDengine TSDB 的 CI/CD 流水线正在从 GitHub Actions 迁移到自托管 GitLab CI/CD。当前编译构建依赖 GitHub-hosted Runner 或物理构建机预装的工具链，**缺少统一的、可复现的容器化编译环境**。

> 核心目标：**全新设计 tsdb-builder** —— 一套容器化编译环境基础设施，为 TDengine 全组件提供标准化的 Docker 编译镜像和构建脚本，统一 MR/CI 验证、发版构建、开发者本地编译三大场景，作为 GitLab CI/CD 迁移的编译层基石。

#### 6.1.1 tsdb-builder 在 CI/CD 中的定位

tsdb-builder 是本次 CI/CD 迁移中**全新设计的编译基础设施仓库**，负责构建和维护编译环境 Docker 镜像及配套构建脚本。它将取代当前分散、不统一的编译方式，成为三种场景的统一编译入口：

1. **GitLab CI MR 流水线** — Build Runner 通过 `docker run` 使用镜像编译
2. **GitLab CD 发版流水线** — 发版脚本使用镜像作为编译基础，替代物理机直接编译
3. **开发者本地编译** — 开发者通过 `build.sh` 脚本使用同一套镜像，获得与 CI 完全一致的编译环境

### 6.2 现状分析

#### 6.2.1 TDengine 仓库 build.sh（开发者本地编译）

TDengine 仓库自带 `build.sh`，是对 cmake 的轻量封装，**直接在宿主机运行**：

```bash
# 典型开发流程
./build.sh gen          # cmake -B debug -DCMAKE_BUILD_TYPE=Debug ...
./build.sh bld          # cmake --build debug -j$(nproc)
./build.sh install      # cmake --install debug
./build.sh test         # ctest --test-dir debug
```

关键特征：

- 构建目录固定为 `debug/`（或 Conan 模式下的 `build/conan-<type>/`）
- 通过 `TD_CONFIG=Debug|Release` 环境变量控制构建类型
- 默认开启 `BUILD_TOOLS`、`BUILD_KEEPER`、`WEBSOCKET`
- 依赖管理通过 cmake `ExternalProject` 机制，缓存在 `.externals/` 目录
- 新增 Conan 构建路径（`conan-install` → `conan-gen` → `conan-bld`），但尚未在 CI 中使用
- **主要用途**：开发人员日常编译调试，不用于发版构建

> 注意：此处 `build.sh` 是 TDengine 仓库自带的开发者脚本，与本文设计的 tsdb-builder 构建脚本无关。TDengine 的 build.sh 直接在宿主机运行 cmake，不涉及任何容器化。

#### 6.2.2 发版构建系统（build_installer.py）

正式发版由 `platform-group/versionRelease/new_release.sh`（约 1440 行）驱动，运行在 **Jenkins 自托管构建机** 上：

```text
Jenkins Pipeline (Jenkinsfile-version-release)
  ├── Enterprise Linux x64  ─ build_installer.py → new_release.sh (x64 构建机)
  ├── Enterprise Linux arm64 ─ build_installer.py → new_release.sh (arm64 构建机)
  ├── Enterprise Mac x64/arm64 ─ build_installer.py → new_release.sh (Mac 构建机)
  ├── Enterprise Win64 ─ build_installer.py → new_win_release.py (Windows 构建机)
  ├── Community Linux/Mac/Win ─ 同上（仅 mainRelease 触发）
  └── Smoking Test ─ get_and_install.py → pytest
```

`new_release.sh` 的核心流程：

1. **拉代码** — `git_pull()` 拉取 TDinternal、community、taosadapter、grant-lib 到 `/data/release/<branch>/`
2. **cmake + make** — 直接在宿主机调用 cmake，传入大量 `-D` 参数（版本号、git 信息、功能开关等）
3. **构建 taosx** — 通过 `build_taosx()` 或 `copy_taosx()`（从预构建目录拷贝）
4. **打包** — `preparepkg()` + `makepkg()` 生成 tar.gz / deb / rpm / macOS pkg
5. **上传** — 通过 `upload_to_nas.py` SCP 到 NAS（`192.168.1.131:/nas/TDengine/v${version}/`）
6. **冒烟测试** — 安装后运行 pytest 验证

关键特征：

| 特征 | 说明 |
| --- | --- |
| 编译环境 | 直接依赖构建机预装的 GCC、Go、Rust、cmake 等，**无容器化隔离** |
| 并行构建 | `build_TDengine` 与 `copy_taosx`/`copy_common_files` 并行执行（bash `&` + `wait`） |
| 产物分发 | SCP 到内网 NAS 服务器（`192.168.1.131`），connector 等预构建件也从 NAS 下载 |
| 多版本支持 | enterprise / community / industry / oem / lite 等变体通过 cmake `-D` 开关组合控制 |
| CI 编排 | Jenkins Pipeline 调用 `build_installer.py` → `new_release.sh`/`new_win_release.py`，各架构/平台在不同 label 的构建机上并行执行 |

#### 6.2.3 当前 GitHub Actions CI（PR 验证）

TDengine 的 PR CI 目前运行在 **GitHub Actions 托管 Runner** 上：

| 工作流 | 触发条件 | Runner | 构建方式 |
| --- | --- | --- | --- |
| `tdengine-build.yml` | PR → main/3.0/3.3.8 | ubuntu-22.04/24.04, macos-14/15, windows-2022 | apt/brew 装依赖 + `./build.sh gen/bld` |
| `tdengine-release-build.yml` | push main / 定时 / 手动 | ubuntu-22.04, macos-14 | 直接调用 cmake（不经 build.sh 封装） |
| `tdengine-test.yml` | PR → 3.1/3.3.6 | 委托 `taosdata/.github` 可复用工作流 | 自托管 Runner + Jenkins 风格容器构建 |

PR 编译 Job 的典型流程（以 `tdengine-build.yml` 为例）：

```bash
# 1. GitHub Actions 安装系统依赖
sudo apt install -y build-essential cmake libgeos-dev libjansson-dev ...

# 2. 缓存 .externals（ExternalProject 产物）
actions/cache@v4 → ${{ github.workspace }}/.externals

# 3. 首次构建 externals（缓存未命中时）
./build.sh gen -DTAOSADAPTER_GIT_TAG:STRING=3.3.8
./build.sh bld --target build_externals

# 4. 正式构建（复用 externals 缓存）
./build.sh gen -DTD_EXTERNALS_USE_ONLY:BOOL=ON
./build.sh bld && ./build.sh install
```

#### 6.2.4 遗留 Jenkins CI 测试基础设施

`tests/ci/` 目录下存在一套**遗留的 Jenkins CI 测试基础设施**，与 tsdb-builder 无关：

- `Dockerfile` / `dockerfile_ci`：基于 python:3.9-bookworm 的测试镜像，内置 Go 1.17、JDK 8、Maven、.NET 5/6、Node.js 12 等（**版本严重滞后**）
- `tests/parallel_test/container_build.sh`：Jenkins 时代的容器编译脚本，硬编码 `/home/TDengine` 路径
- 构建产物通过 SCP 分发到测试机

#### 6.2.5 现状总结与痛点

| 痛点 | 说明 |
| --- | --- |
| **编译环境不统一** | 发版依赖构建机预装工具链（版本靠人工维护）；PR CI 用 GitHub-hosted Runner apt 安装；Jenkins 用过时 Docker 镜像——**三套环境各自独立，无统一的编译容器** |
| **glibc 兼容性无保障** | GitHub-hosted Runner（ubuntu-22.04）的 glibc 2.35 远高于生产目标 2.17，PR CI 编译产物无法在低版本系统运行 |
| **多套构建工具共存** | TDengine build.sh（开发）、new_release.sh（发版）、container_build.sh（Jenkins 测试）三套脚本并行，**缺少统一的容器化编译入口** |
| **产物分发依赖内网 SCP** | 发版产物通过 SCP 上传到 NAS（192.168.1.131），无版本化管理，不适合 CI/CD 自动化流水线 |
| **CI 缓存效率低** | GitHub Actions cache 有 10GB 上限且跨 PR 不共享；.externals 缓存 key 粒度粗，频繁失效 |
| **缺少 Linux ARM CI** | GitHub-hosted Runner 无 Linux ARM64，PR CI 仅在 macOS ARM 上验证；发版 ARM 编译靠单台构建机 |

### 6.3 设计方案

> tsdb-builder 是本次概要设计的核心产出——一个全新的编译基础设施仓库。它通过"标准化 Docker 镜像 + 统一构建脚本"的方式，从根本上解决编译环境碎片化问题，为 GitLab CI/CD 提供可靠的编译层。

#### 6.3.1 整体架构

tsdb-builder 包含三部分核心设计：

```text
tsdb-builder（全新设计）
  ├── 编译镜像 ─── Dockerfile.core / Dockerfile.others / Dockerfile.core-riscv64
  │                 统一工具链版本、glibc 版本、中国镜像加速
  ├── 镜像构建脚本 ─ build-core-image.sh / build-others-image.sh
  │                 多架构构建、Harbor 推送、版本管理
  └── 统一构建入口 ─ build.sh
                    docker run 容器化编译、缓存管理、组件路由
```

三大消费场景通过**同一套镜像 + 同一个 build.sh** 获得一致的编译环境：

```text
                    ┌─────────────────────────────────┐
                    │     tsdb-builder 镜像仓库        │
                    │  harbor.tdengine.net/tsdb-builder │
                    │    core:<ver>-<arch>              │
                    │    others:<ver>-<arch>            │
                    └──────────┬──────────────────────┘
                               │ docker pull
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                     ▼
   GitLab CI MR Job     GitLab CI 发版 Job     开发者本地
   (Build Runner)       (物理机/K8s)          (任意 Mac/Linux)
          │                    │                     │
          └────────────────────┼─────────────────────┘
                               │
                    build.sh --image core engine
                    (统一的 docker run 编译入口)
```

#### 6.3.2 双镜像体系

tsdb-builder 设计两套编译镜像，按 glibc 兼容性和组件分类划分：

**core 镜像**（核心组件编译）：

- 基础：manylinux2014 / CentOS 7
- glibc：2.17（兼容麒麟 V10）
- GCC：7.3（devtoolset-7）
- 链接器：mold（源码编译，替代 ld.bfd）
- 架构：amd64 / arm64 / riscv64
- 组件：ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX

**others 镜像**（周边组件编译）：

- 基础：manylinux_2_28 / AlmaLinux 8
- glibc：2.28
- GCC：14.x（gcc-toolset-14）
- 链接器：mold（源码编译）
- 架构：amd64 / arm64
- 组件：EXPLORER_UI, INSIGHT + 全部 connector（dotnet/go/jdbc/node/python/rust/odbc）

设计要点：

- **glibc 版本锁定**：core 镜像锁定 glibc 2.17，确保编译产物可在麒麟 V10 等低版本系统运行——**这是当前 GitHub-hosted Runner（glibc 2.35）无法实现的核心能力**
- **工具链版本统一**：Go、Rust、cmake、protoc 等工具版本集中管理于 `.build-args` 文件，一处修改全局生效
- **中国镜像加速**：Go（goproxy.cn）、Rust（rsproxy.cn）、PyPI（阿里云）均预配置镜像，无需运行时设置
- **mold 链接器**：源码编译并注册为默认链接器（通过 update-alternatives），显著加速链接阶段

#### 6.3.3 统一构建脚本（build.sh）

tsdb-builder 的 `build.sh` 是整个编译流程的**统一入口**，封装了从镜像选择到容器编译的全过程：

```bash
# 镜像构建（由 tsdb-builder 维护者执行）
./build-core-image.sh --version 0.2 --arch amd64
./build-others-image.sh --version 0.2 --arch amd64

# 使用镜像编译 TSDB 组件（由开发者或 CI 执行）
./build.sh --image core engine taosx          # 编译核心组件
./build.sh --image others explorer-ui jdbc    # 编译周边组件
./build.sh --image core --arch arm64 engine   # 指定架构
./build.sh --image core --clean core-all      # 清理缓存后全量编译

# 纯 cmake 参数模式（发版脚本使用）
./build.sh --image core -DBUILD_ENGINE=ON -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_VER_NUMBER=3.4.1.3 -DBUILD_GITINFO=abc123
```

build.sh 核心职责：

1. **镜像路由**：根据 `--image core|others` 解析到 Harbor 单架构标签
2. **容器挂载**：源码目录→`/mnt`，多级缓存目录（Conan/externals/Go/Cargo/pnpm/Maven/NuGet）
3. **cmake 参数编排**：组件快捷名 → `BUILD_*=ON/OFF`，支持 `-D` 直传覆盖
4. **构建容错**：`make -j$(nproc)` 失败后自动降级 `make -j1`（make 3.82 jobserver 兼容）

#### 6.3.4 缓存体系

针对"GitHub Actions cache 10GB 上限、跨 PR 不共享"的痛点，tsdb-builder 设计了**基于 Build Runner 本地持久目录的缓存体系**：

| 缓存目录 | 容器挂载点 | 适用镜像 | 内容 |
| --- | --- | --- | --- |
| `conan2-<arch>/` | `/root/.conan2` | core + others | Conan 包缓存 |
| `externals-core-<arch>/` | `/mnt/.externals` | core | ExternalProject 预构建产物 |
| `externals-others-<arch>/` | `/mnt/.externals` | others | ExternalProject 预构建产物 |
| `go-mod/` | `/root/go/pkg/mod` | core + others | Go 模块缓存 |
| `cargo-registry/` | `/root/.cargo/registry` | core + others | Rust crate 缓存 |
| `cargo-git/` | `/root/.cargo/git` | core + others | Rust git 依赖缓存 |
| `pnpm-store/` | `/mnt/.pnpm-store` | others | pnpm 包缓存 |
| `m2-repository/` | `/root/.m2/repository` | others | Maven 仓库缓存 |
| `nuget/` | `/root/.nuget/packages` | others | NuGet 包缓存 |

缓存根目录默认为 `$HOME/cache/tsdb-builder`（通过 `TSDB_CACHE_DIR` 环境变量可覆盖），**刻意放在源码仓库外部**——缓存跨 git clone 持续有效，首次编译后后续编译可跳过依赖下载。

#### 6.3.5 适配 GitLab CI Build Runner

从现状迁移到 tsdb-builder 方案的关键变化：

| 变化点 | 现状 | tsdb-builder 方案 | 收益 |
| --- | --- | --- | --- |
| 编译环境 | GitHub-hosted Runner apt 安装 / 构建机预装工具链 | 统一使用 tsdb-builder Docker 镜像 | 环境一致、版本可控、可复现 |
| glibc 兼容 | 取决于 Runner OS（2.35+），无保障 | core 镜像锁定 glibc 2.17 | 编译产物可运行于低版本 Linux |
| 依赖缓存 | actions/cache（10GB 上限、跨 PR 不共享） | Runner 本地持久目录，跨 MR 复用 | 缓存命中率高、无容量限制 |
| 多架构 | GitHub-hosted 仅 x86\_64 Linux | 各架构原生 Build Runner + 对应架构镜像 | ARM64 原生编译，无 QEMU 性能损失 |
| 构建入口 | TDengine build.sh / new\_release.sh / container\_build.sh | 统一的 tsdb-builder build.sh | 一套脚本覆盖 CI + 发版 + 本地开发 |

GitLab CI 的 Build Runner 使用 shell executor + `docker run` 方式调用 tsdb-builder 镜像：

```text
GitLab CI Job (shell executor on Build Runner)
  └── tsdb-builder/build.sh --image core engine taosx
        └── docker run harbor.tdengine.net/tsdb-builder/core:latest-amd64
              ├── 挂载源码 → /mnt
              ├── 挂载缓存 → /root/.conan2, /mnt/.externals, ...
              ├── cmake 配置 + make 编译
              └── 产物输出到 /mnt（即宿主机源码目录）
```

#### 6.3.6 镜像标签策略与版本管理

| 标签格式 | 示例 | 用途 |
| --- | --- | --- |
| `<version>-<arch>` | `core:0.2-amd64` | 版本锁定，发版 CI 使用 |
| `latest-<arch>` | `core:latest-arm64` | 滚动更新，MR CI 默认使用 |
| `<version>` | `core:0.2` | 多架构 manifest（amd64+arm64） |
| `latest` | `core:latest` | 多架构 manifest 滚动标签 |

GitLab CI 中通过 `--image core:<version>` 锁定镜像版本，避免编译环境漂移。

#### 6.3.7 CI 集成示例

tsdb-builder 在 GitLab CI 中的典型使用模式：

```yaml
# TDengine/.gitlab-ci.yml 中的 build job 示例
build-linux-amd64:
  stage: build
  tags: [build-linux-x64]
  script:
    # tsdb-builder 作为 git submodule 或独立 clone
    - git clone git@git.tdengine.net:rd-public/platform.git /opt/platform
    - cd $CI_PROJECT_DIR
    # 统一编译入口
    - /opt/platform/tsdb-builder/build.sh --image core --arch amd64 --clean core-all
    # 产物上传
    - bash scripts/upload-artifacts.sh
  artifacts:
    paths: [debug/build/lib/*, debug/build/bin/*]
```

#### 6.3.8 多架构支持

| 架构 | core 镜像 | others 镜像 | Build Runner |
| --- | --- | --- | --- |
| amd64 | ✅ manylinux2014 | ✅ manylinux\_2\_28 | Linux x86\_64 物理机 |
| arm64 | ✅ manylinux2014 | ✅ manylinux\_2\_28 | Linux aarch64 物理机 |
| riscv64 | ✅ Debian trixie（独立 Dockerfile） | ❌ | riscv64 物理机 |

riscv64 使用独立标签（不纳入多架构 manifest），因为 manylinux 系列无 riscv64 支持。

### 6.4 开发工作项

本节列出落地 6.3 设计方案所需的全部开发工作，按仓库维度拆分。

#### 6.4.1 tsdb-builder 仓库（全新创建）

tsdb-builder 是一个从零开始的新仓库，需要完成以下开发工作：

| 工作项 | 优先级 | 说明 |
| --- | --- | --- |
| 编写 Dockerfile.core | P0 | 基于 manylinux2014，安装 GCC 7.3、Go、Rust、cmake、protoc、mold 等，锁定 glibc 2.17 |
| 编写 Dockerfile.others | P0 | 基于 manylinux\_2\_28，安装 GCC 14、Node.js、pnpm、JDK、Maven、.NET SDK、Python 等 |
| 编写 Dockerfile.core-riscv64 | P1 | 基于 Debian trixie，工具链通过 apt 安装，独立于 manylinux 体系 |
| 编写 build.sh（统一构建入口） | P0 | 实现镜像路由、docker run 编排、缓存挂载、cmake 参数编排、组件快捷名映射 |
| 编写 build-core-image.sh / build-others-image.sh | P0 | 实现多架构镜像构建、Harbor 推送、版本标签管理、多架构 manifest 创建 |
| 创建 .build-args 版本配置文件 | P0 | 集中管理所有工具链版本号和镜像加速地址，供 Dockerfile 统一引用 |
| 编写 verify-image.sh | P0 | 镜像构建后的冒烟验证脚本，检查关键工具链是否可用 |
| 首版镜像构建并推送至 Harbor | P0 | 在 amd64 + arm64 构建机上构建 core/others 镜像并推送至 harbor.tdengine.net/tsdb-builder/ |
| 编写 tsdb-builder 自身的 .gitlab-ci.yml | P1 | 实现 Dockerfile 变更时自动触发镜像构建、冒烟测试、推送（详见 6.4.3） |
| 同步推送至 GitLab Container Registry | P1 | 除 Harbor 外，镜像同步推送到 GitLab CR 作为灾备 |
| 支持 `--output-dir` 参数 | P2 | build.sh 允许 CI 指定产物输出目录，适配不同流水线的目录规范 |
| 镜像瘦身优化 | P2 | 审计镜像层大小，移除非必要组件，缩短 CI 拉取耗时 |

#### 6.4.2 TDengine 主仓库适配（关联仓库）

TDengine 主仓库需要新增以下文件，以对接 tsdb-builder 编译环境和 GitLab CI 流水线：

| 工作项 | 说明 |
| --- | --- |
| 新增 `.gitlab-ci.yml` | 定义 MR 流水线的 build/test/report 阶段，build stage 调用 tsdb-builder 的 build.sh |
| 新增 `.gitlab/ci/` 目录 | 拆分 CI 配置（build jobs、test jobs、release jobs） |
| 新增 `scripts/upload-artifacts.sh` | 打包精简编译产物并上传至 Nexus |
| 新增 `scripts/pull-artifacts.sh` | 从 Nexus 下载编译产物 |
| 新增 `scripts/sparse-checkout.sh` | 测试 Runner 稀疏检出测试代码 |

#### 6.4.3 tsdb-builder 自身的 GitLab CI 流水线

为 tsdb-builder 仓库本身添加 CI/CD 流水线：

```yaml
# tsdb-builder/.gitlab-ci.yml
stages:
  - lint
  - build
  - test
  - publish

lint-shellcheck:
  stage: lint
  script:
    - shellcheck build.sh build-core-image.sh build-others-image.sh verify-image.sh

smoke-tests:
  stage: test
  script:
    - for t in tests/smoke/test-*.sh; do bash "$t"; done

build-core-image:
  stage: build
  tags: [build-linux-x64]
  rules:
    - if: $CI_COMMIT_TAG  # tag 触发时构建
    - changes: [Dockerfile.core, .build-args]
  script:
    - ./build-core-image.sh --version ${CI_COMMIT_TAG} --arch amd64

publish-manifests:
  stage: publish
  needs: [build-core-image, build-others-image]
  script:
    - echo "Multi-arch manifest creation"
```

### 6.5 Build Runner 环境要求

#### 6.5.1 软件依赖

| 软件 | 版本要求 | 用途 |
| --- | --- | --- |
| Docker Engine | ≥ 24.0 | 运行 builder 容器 |
| Docker Buildx | ≥ 0.11 | 镜像构建（仅在构建镜像时需要） |
| gitlab-runner | 最新 stable | 执行 CI Job（shell executor） |
| git | ≥ 2.30 | 代码检出 |
| curl | 系统版本 | 产物上传/下载 Nexus |

#### 6.5.2 持久目录

```bash
# Build Runner 初始化脚本【cache目录如不在~下，运行build.sh时需 --cache 指定路径】
mkdir -p /data/cache/tsdb-builder/{conan2-amd64,externals-core-amd64,externals-others-amd64}
mkdir -p /data/cache/tsdb-builder/{go-mod,cargo-registry,cargo-git}
mkdir -p /data/cache/tsdb-builder/{pnpm-store,m2-repository,nuget}
chown -R gitlab-runner:gitlab-runner /data/cache/tsdb-builder
```

#### 6.5.3 Harbor 镜像预拉取

Build Runner 部署时应预拉取常用镜像以避免首次 CI Job 等待：

```bash
docker pull harbor.tdengine.net/tsdb-builder/core:latest-amd64
docker pull harbor.tdengine.net/tsdb-builder/others:latest-amd64
```

### 6.6 风险与注意事项

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| 首次编译缺少 .externals | 构建失败 | CI Job 检查 externals 目录是否存在，首次自动加 `-DBUILD_CONTRIB=ON` |
| 镜像拉取耗时 | CI Job 启动延迟 | Build Runner 预拉取 + 使用版本锁定标签避免每次拉取 |
| 缓存目录权限 | 容器内 root vs 宿主机 gitlab-runner | build.sh 以 `--user root` 运行容器，缓存目录权限需为 777 或 root 拥有 |
| 跨架构编译性能 | 10-20 倍性能下降 | 严禁在 CI 中使用 QEMU 模拟，必须在对应架构 Runner 上原生编译 |
| Nexus 带宽瓶颈 | 多 MR 并发时产物下载拥堵 | Runner 本地缓存 + 限制并发 MR 数量 |

### 6.7 tsdb-builder 实施计划

| 阶段 | 内容 | 依赖 |
| --- | --- | --- |
| 准备阶段 | Build Runner 环境搭建、缓存目录初始化、镜像预拉取 | Runner 硬件就绪 |
| P0：基础对接 | build.sh 在 Build Runner 上验证通过，core/others 全组件编译成功 | 准备阶段完成 |
| P1：CI 集成 | TDengine 仓库 .gitlab-ci.yml 的 build stage 使用 build.sh | P0 完成 |
| P1：自身 CI | tsdb-builder 仓库添加 .gitlab-ci.yml，实现镜像自动构建 | P0 完成 |
| P2：优化 | 镜像瘦身、缓存预热策略、产物上传并行化 | P1 稳定运行后 |
