# TSDB 编译构建优化指南

> **目标读者**：TSDB 研发工程师
> **最后更新**：2026-05-20

---

## 1. 设计目标

本轮编译构建优化围绕两个核心目标展开：

| # | 目标 | 核心动机 |
|---|------|----------|
| 1 | **所有外部依赖切换到内网下载** | 消除对 GitHub/crates.io/PyPI 等公网源的直接依赖，提升下载速度和可靠性。tarball 优先，至少通过内网代理。 |
| 2 | **编译中间产物建立缓存** | 避免重复编译，缩短增量构建和 CI/CD 流水线时长。 |

---

## 2. 整体架构

### 2.1 关键产出物

| 路径 | 作用 |
|------|------|
| `tools/tsdb-builder/build.sh` | 统一构建入口，Docker 容器内编译 |
| `tools/tsdb-builder/.build-args` | 中心化镜像/版本配置（单一数据源） |
| `tools/tsdb-builder/.cargo/config.toml` | Rust 内网 registry 配置 |
| `tools/tsdb-builder/scripts/` | 依赖镜像管理脚本（上传/校验/预热） |
| `tools/setup/` | 本地开发环境配置（与 builder 共享同一镜像策略） |
| `source/taos-community/cmake/external.cmake` | C/C++ ExternalProject 定义及镜像下载宏 |
| `source/taos-community/packaging/setup_env.sh` | CI/裸机环境初始化 |

### 2.2 架构分层

```
┌─────────────────────────────────────────────────────────────┐
│                     build.sh (入口)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │ 镜像选择     │  │ 缓存挂载     │  │ 环境变量注入      │  │
│  │ core/dev/    │  │ externals    │  │ GOPROXY           │  │
│  │ others       │  │ ccache       │  │ CARGO_REGISTRY    │  │
│  │              │  │ go-mod/build │  │ CONAN_REMOTE      │  │
│  │              │  │ cargo-*      │  │ MIRROR_URL        │  │
│  │              │  │ conan2       │  │                   │  │
│  │              │  │ pnpm/m2/nuget│  │                   │  │
│  └──────────────┘  └──────────────┘  └───────────────────┘  │
│                            │                                 │
│                   ┌────────▼─────────┐                      │
│                   │ Docker 容器内编译  │                      │
│                   │ cmake / cargo /  │                      │
│                   │ go build / pnpm  │                      │
│                   └──────────────────┘                      │
└─────────────────────────────────────────────────────────────┘
                             │
                    ┌────────▼──────────┐
                    │ .build-args (配置源)│
                    │ 所有镜像 URL 集中定义│
                    └───────────────────┘
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
    tools/setup/       packaging/        Dockerfile.*
    (本地开发环境)      setup_env.sh      (镜像构建)
                       (CI/裸机)
```

### 2.3 配置层次

`.build-args` 是镜像 URL 和工具版本的**唯一数据源**（single source of truth），被以下系统消费：

- `build.sh` → 传入 Docker `--build-arg` 和容器环境变量
- `tools/setup/config.sh` → 读取并设置开发机镜像配置
- `packaging/setup_env.sh` → CI 裸机环境初始化
- `Dockerfile.*` → 镜像构建时工具安装

当前配置值：

```bash
GO_PROXY=https://nexus.tdengine.net/repository/goproxy/
CARGO_REGISTRY_URL=sparse+https://nora.tdengine.net/cargo/index/
CONAN_REMOTE_URL=https://nexus.tdengine.net/repository/conan/
PYPI_MIRROR=http://mirrors.aliyun.com/pypi/simple/     # Dockerfile 镜像构建用（公网可达）
PYPI_INTERNAL_URL=https://nora.tdengine.net/simple/   # build.sh 容器编译用（内网）
NPM_REGISTRY_URL=https://nora.tdengine.net/npm/
MAVEN_MIRROR_URL=https://nexus.tdengine.net/repository/maven-public/
NUGET_SOURCE_URL=https://nora.tdengine.net/nuget/v3/index.json
```

---

## 3. 数据流向对比

### 3.1 优化前

```
开发者/CI 机器
    │
    ├── cmake ExternalProject ──→ github.com（直接下载 tarball）
    ├── cargo build ────────────→ crates.io（公网 registry）
    ├── go build ───────────────→ proxy.golang.org（公网代理）
    ├── conan install ──────────→ center.conan.io（公网中心仓）
    ├── pnpm install ───────────→ registry.npmjs.org
    ├── mvn install ────────────→ repo1.maven.org
    └── pip install ────────────→ pypi.org
```

**问题**：公网下载慢、不稳定、CI 经常因网络超时失败；每次 clean build 重复下载所有依赖。

### 3.2 优化后

```
开发者/CI 机器
    │
    ├── cmake ExternalProject ──→ GitLab Package Registry（内网 tarball 镜像）
    │                              ↳ 失败回退：github.com（上游）
    ├── cargo build ────────────→ nora.tdengine.net（内网 Cargo registry）
    ├── go build ───────────────→ nexus.tdengine.net/goproxy（内网 Go 代理）
    ├── conan install ──────────→ nexus.tdengine.net/conan（内网 Conan 仓库）
    ├── pnpm install ───────────→ nora.tdengine.net/npm/（build.sh 运行时注入）
    ├── mvn install ────────────→ nexus.tdengine.net/maven-public/（build.sh 注入 settings.xml）
    ├── pip install ────────────→ nora.tdengine.net/simple/（build.sh 运行时覆盖）
    └── dotnet restore ─────────→ nora.tdengine.net/nuget/（build.sh 运行时注入）
    
    缓存层（$HOME/cache/tsdb-builder/）
    ├── externals-{image}-{arch}/  → C/C++ ExternalProject 构建产物
    ├── ccache-{image}-{arch}/     → C/C++ 编译缓存（ccache）
    ├── sccache-{image}-{arch}/   → Rust 编译缓存（sccache，--sccache 启用）
    ├── conan2-{arch}/             → Conan 依赖包缓存
    ├── go-mod/                    → Go 模块缓存
    ├── go-build/                  → Go 编译缓存
    ├── cargo-registry/            → Rust crate 缓存
    ├── cargo-git/                 → Rust git 依赖缓存
    ├── pnpm-store/                → Node.js 包缓存
    ├── m2-repository/             → Maven 仓库缓存
    └── nuget/                     → .NET NuGet 缓存
```

---

## 4. 各语言依赖处理方式详解

### 4.1 总览对比表

| 语言 | 依赖类型 | 内网源 | 下载方式 | 缓存目录 | 编译缓存 | 缓存隔离 |
|------|---------|--------|---------|---------|---------|---------|
| **C/C++** (ExternalProject) | 源码 tarball | GitLab Package Registry | tarball 优先，上游回退 | `externals-{image}-{arch}/` | ccache | 按镜像+架构 |
| **C/C++** (Conan) | 预编译包 | Nexus Conan 仓库 | Conan CLI | `conan2-{arch}/` | — | 按架构 |
| **Rust** | crate + git 依赖 | Nora sparse registry + GitLab 镜像 | Cargo CLI | `cargo-registry/` + `cargo-git/` | sccache (可选) | 全局共享 |
| **Go** | module | Nexus Go Proxy | go mod download | `go-mod/` + `go-build/` | go build cache | 全局共享 |
| **Node.js** | npm 包 | ✅ Nora npm（`build.sh` 运行时注入） | pnpm | `pnpm-store/` | — | 全局共享 |
| **Java** | Maven artifact | ✅ Nexus Maven（`build.sh` 注入 `settings.xml`） | mvn | `m2-repository/` | — | 全局共享 |
| **Python** | PyPI 包 | ✅ Nora PyPI（`build.sh` 运行时覆盖） | pip | pip cache | — | 全局共享 |
| **.NET** | NuGet 包 | ✅ Nora NuGet（`build.sh` 运行时注入） | dotnet restore | `nuget/` | — | 全局共享 |

### 4.2 C/C++ — ExternalProject 依赖

#### 原理

`external.cmake` 中的每个第三方库通过 `ExternalProject_Add()` 管理。优化引入了 `get_from_local_if_exists()` 宏，在设置了 `BUILD_DEPS_MIRROR_URL` 时将下载地址重写为内网镜像：

```cmake
macro(get_from_local_if_exists url)
  if("z${LOCAL_URL}" STREQUAL "z")
    set(_url "${url}")                        # 无镜像，用上游 URL
  else()
    if(${ARGC} GREATER 1)
      set(_url "${LOCAL_URL}/${ARGV1}")        # 优先：内网镜像 + 显式文件名
    else()
      set(_url "${LOCAL_URL}/${_name}")        # 回退：取 URL 末段
    endif()
  endif()
endmacro()
```

#### 镜像管理流程

```
external.cmake (唯一数据源)
       ↓ prepare-externals.sh --cmake 自动提取
GitHub (上游) → 下载 tarball → 计算 SHA256 → 上传到 GitLab Package Registry
                                                       ↓
       externals-manifest.txt ← 更新 SHA256 清单 ← 验证完整性
                                                       ↓
       cmake 编译时: BUILD_DEPS_MIRROR_URL → 从 GitLab 下载
```

**当前镜像管理的 28 个依赖包括**：zlib、lz4、cJSON、xz、xxhash、fast-lzma2、libuv、jemalloc、sqlite、openssl、curl、geos、pcre2、rocksdb、jansson、snappy、libxml2、azure-sdk、mxml、cos-c-sdk、apr、apr-util 等。

#### 编译缓存

| 缓存层级 | 机制 | 缓存目录 | 配置 |
|---------|------|---------|------|
| **源码下载缓存** | ExternalProject `EP_UPDATE_DISCONNECTED=TRUE` | `externals-{image}-{arch}/` | 自动 |
| **构建产物缓存** | `.externals/` 下 install 目录持久化 | 同上 | `BUILD_CONTRIB=OFF` 时复用 |
| **编译结果缓存** | ccache 拦截 gcc/g++ 调用 | `ccache-{image}-{arch}/` | 自动启用，默认 20G |

> **为什么缓存按镜像+架构隔离？** core/dev/others 三个镜像的 GCC 版本不同（7/9/14），
> 编译产物 ABI 不兼容，混用会导致链接错误。

### 4.3 C/C++ — Conan 依赖

#### 原理

`taos-gen` 组件使用 Conan 管理 C++ 依赖（fmt、jemalloc、yaml-cpp、librdkafka 等）。

```
conanfile.txt → conan install → 从 Nexus Conan 仓库下载 → 本地缓存
```

**内网配置**：`build.sh` 每次启动容器后自动添加 Nexus Conan remote：

```bash
conan remote add nexus https://nexus.tdengine.net/repository/conan/ --force
```

缓存目录 `conan2-{arch}/` 按架构隔离，挂载到容器内 `/root/.conan2`。

### 4.4 Rust 依赖

#### 内网下载

Rust 依赖分两类：

| 类型 | 内网源 | 配置方式 |
|------|--------|---------|
| **crates.io 包** | `sparse+https://nora.tdengine.net/cargo/index/` | `.cargo/config.toml` 的 `replace-with` |
| **git 依赖** | GitLab 内部镜像仓库 | `setup-rust-git-mirrors.sh` 创建镜像 |

`.cargo/config.toml` 核心配置：

```toml
[source.crates-io]
replace-with = 'internal'

[source.internal]
registry = "sparse+https://nora.tdengine.net/cargo/index/"

[net]
git-fetch-with-cli = true    # 通过 git CLI 拉取，支持 SSH 鉴权
```

#### 编译缓存

| 缓存层级 | 机制 | 缓存目录 |
|---------|------|---------|
| 依赖下载缓存 | Cargo registry 持久化 | `cargo-registry/` |
| git 依赖缓存 | Cargo git 持久化 | `cargo-git/` |
| 编译产物缓存 | sccache（可选，`--sccache` 启用） | `sccache-{image}-{arch}/` |

#### 缓存预热

`preheat-rust.sh` 脚本在 CI 开始前运行 `cargo fetch`，预下载所有依赖到缓存。

### 4.5 Go 依赖

#### 内网下载

```bash
GOPROXY=https://nexus.tdengine.net/repository/goproxy/,direct
GONOSUMDB=*           # 跳过公网 sumdb 校验
GONOSUMCHECK=*
```

Go 模块先从内网 Nexus 代理获取，只在代理缺失时回退到 `direct`（公网直连）。

#### 编译缓存

| 缓存层级 | 机制 | 缓存目录 |
|---------|------|---------|
| 模块下载缓存 | `$GOPATH/pkg/mod` 持久化 | `go-mod/` |
| 编译缓存 | Go 自带 build cache | `go-build/` |

#### 缓存预热

`preheat-go.sh` 脚本对主要 Go 模块运行 `go mod download`，预填充模块缓存。

### 4.6 Node.js 依赖

| 项目 | 详情 |
|------|------|
| 内网源（容器编译） | ✅ `build.sh` 在 `CONTAINER_SCRIPT` 中执行 `npm config set registry`，URL 来自 `.build-args` 的 `NPM_REGISTRY_URL`（默认 `https://nora.tdengine.net/npm/`）。pnpm/yarn 自动继承 npm registry。 |
| 内网源（宿主机开发） | `https://nora.tdengine.net/npm/`（`modules/node.sh` 配置，URL 来自 `config.sh`） |
| 包管理器 | pnpm（content-addressable 存储，天然去重） |
| 缓存目录 | `pnpm-store/`（挂载到 `/mnt/.pnpm-store`） |
| 适用镜像 | others（Insight、Explorer UI） |

### 4.7 Java 依赖

| 项目 | 详情 |
|------|------|
| 内网源（容器编译） | ✅ `build.sh` 在 `CONTAINER_SCRIPT` 中生成 `/root/.m2/settings.xml`，配置 Maven mirror 指向 `.build-args` 的 `MAVEN_MIRROR_URL`（默认 `https://nexus.tdengine.net/repository/maven-public/`）。与缓存挂载 `m2-repository/` 不冲突。 |
| 内网源（宿主机开发） | Nexus Maven 仓库（`modules/java.sh` 配置 `settings.xml`，URL 来自 `config.sh`） |
| 缓存目录 | `m2-repository/`（挂载到 `/root/.m2/repository`） |
| 适用镜像 | others（connector-jdbc） |

### 4.8 Python 依赖

| 项目 | 详情 |
|------|------|
| 内网源（容器编译） | ✅ `build.sh` 在 `CONTAINER_SCRIPT` 中执行 `pip3 config set global.index-url`，覆盖镜像内烘焙的阿里云地址，URL 来自 `.build-args` 的 `PYPI_INTERNAL_URL`（默认 `https://nora.tdengine.net/simple/`） |
| 内网源（宿主机开发） | `https://nora.tdengine.net/simple/`（`modules/python.sh` 配置，URL 来自 `config.sh`） |
| 配置来源 | `.build-args` → `PYPI_INTERNAL_URL`（容器编译）/ `PYPI_MIRROR`（Dockerfile 镜像构建，阿里云） |
| 主要用途 | connector-python、maturin（Rust-Python 绑定构建） |

> **设计说明**：`PYPI_MIRROR`（阿里云）用于 Dockerfile 镜像构建（公网可达），`PYPI_INTERNAL_URL`（Nora）用于容器编译运行时（内网）。`build.sh` 在容器启动后覆盖 pip config，确保开发编译走内网。

### 4.9 .NET 依赖

| 项目 | 详情 |
|------|------|
| 内网源（容器编译） | ✅ `build.sh` 在 `CONTAINER_SCRIPT` 中执行 `dotnet nuget add source`，URL 来自 `.build-args` 的 `NUGET_SOURCE_URL`（默认 `https://nora.tdengine.net/nuget/v3/index.json`）。 |
| 内网源（宿主机开发） | `https://nora.tdengine.net/nuget/v3/index.json`（`modules/dotnet.sh` 配置，URL 来自 `config.sh`） |
| 缓存目录 | `nuget/`（挂载到 `/root/.nuget/packages`） |
| 适用镜像 | others（connector-dotnet） |

---

## 5. 缓存策略对比表

| 缓存类型 | 目录 | 隔离粒度 | 大小限制 | 清理条件 |
|---------|------|---------|---------|---------|
| ExternalProject 构建产物 | `externals-{image}-{arch}/` | 镜像 × 架构 | 无限制 | 切换镜像类型自动隔离 |
| ccache 编译缓存 | `ccache-{image}-{arch}/` | 镜像 × 架构 | 默认 20G | `CCACHE_MAXSIZE` 自动淘汰 |
| sccache Rust 编译缓存 | `sccache-{image}-{arch}/` | 镜像 × 架构 | 可配置 | 手动清理 |
| Conan 包缓存 | `conan2-{arch}/` | 架构 | 无限制 | 切换 GCC 版本时需删除重建 |
| Go 模块缓存 | `go-mod/` | 全局 | 无限制 | `go clean -modcache` |
| Go 编译缓存 | `go-build/` | 全局 | Go 默认策略 | `go clean -cache` |
| Cargo registry | `cargo-registry/` | 全局 | 无限制 | `cargo cache --autoclean` |
| Cargo git | `cargo-git/` | 全局 | 无限制 | 手动清理 |
| pnpm store | `pnpm-store/` | 全局 | 无限制 | `pnpm store prune` |
| Maven 仓库 | `m2-repository/` | 全局 | 无限制 | 手动清理 |
| NuGet | `nuget/` | 全局 | 无限制 | 手动清理 |

---

## 6. 使用说明

### 6.1 Docker 容器编译（推荐）

```bash
cd /path/to/tsdb

# 编译引擎（使用 core 镜像，自动挂载缓存、配置内网镜像）
./tools/tsdb-builder/build.sh engine

# 编译全部组件
./tools/tsdb-builder/build.sh engine enterprise adapter keeper taosx gen

# 指定架构
./tools/tsdb-builder/build.sh --arch arm64 engine

# 使用 dev 镜像（GCC 9，不需要兼容麒麟 V10）
./tools/tsdb-builder/build.sh --image dev engine

# 启用 Rust sccache
./tools/tsdb-builder/build.sh --sccache taosx

# 清理缓存重新编译
./tools/tsdb-builder/build.sh --clean engine

# 传递额外 CMake 参数
./tools/tsdb-builder/build.sh engine -DBUILD_TEST=ON
```

`build.sh` 自动处理：
- 根据组件选择正确的 Docker 镜像（core/dev/others）
- 挂载所有持久化缓存目录
- 注入内网镜像环境变量（`GOPROXY`、`BUILD_DEPS_MIRROR_URL` 等）
- 配置 ccache 并将其加入 PATH
- 添加 Conan 内网 remote

### 6.2 本地开发环境配置

```bash
# 按组件安装所需工具链并配置内网镜像
./tools/setup/setup-macos.sh engine taosx adapter

# 安装所有语言环境
./tools/setup/setup-linux.sh --all
```

setup 脚本自动：
- 从 `.build-args` 读取镜像 URL（与 Docker 构建保持一致）
- 配置 Go proxy、Cargo registry、Conan remote、npm registry 等
- 检查已有配置，避免重复写入

### 6.3 管理 C/C++ 内网 tarball 镜像

```bash
# 查看当前所有 ExternalProject 依赖
./tools/tsdb-builder/scripts/prepare-externals.sh \
    --cmake source/taos-community/cmake/external.cmake --list

# 验证内网镜像完整性
./tools/tsdb-builder/scripts/prepare-externals.sh \
    --cmake source/taos-community/cmake/external.cmake --verify

# 升级依赖版本后上传新 tarball
# 1) 先修改 external.cmake 中的 URL 和文件名
# 2) 上传到 GitLab Registry
./tools/tsdb-builder/scripts/prepare-externals.sh \
    --cmake source/taos-community/cmake/external.cmake \
    --upload zlib-v1.4.0.tar.gz
```

### 6.4 预热缓存

在 CI/CD 首次运行或缓存失效后：

```bash
# 预热 Go 模块缓存
./tools/tsdb-builder/scripts/preheat-go.sh

# 预热 Rust 依赖缓存
./tools/tsdb-builder/scripts/preheat-rust.sh

# 验证 Go/Rust 是否能完全从内网解析
./tools/tsdb-builder/scripts/validate-internal-deps.sh
./tools/tsdb-builder/scripts/validate-internal-deps.sh --offline
```

---

## 7. 常见问题

### Q1: 首次编译报错找不到 xxhash/zstd 等头文件

**原因**：首次编译（`.externals/` 不存在）必须加 `-DBUILD_CONTRIB=ON` 来构建外部依赖。

**解决**：

```bash
./tools/tsdb-builder/build.sh engine -DBUILD_CONTRIB=ON
```

后续增量编译可省略（默认 `OFF`，直接复用缓存）。

### Q2: 切换 Debug/Release 后编译异常

**原因**：部分缓存（特别是 Conan 生成的 CMake 文件）是 build-type 相关的。

**解决**：使用 `--clean` 参数重新编译：

```bash
./tools/tsdb-builder/build.sh --clean engine
```

### Q3: 切换 core/dev/others 镜像后链接错误

**原因**：不同镜像 GCC 版本不同（7/9/14），编译产物 ABI 不兼容。

**解决**：externals 缓存已按 `{image}-{arch}` 自动隔离，无需手动操作。如果 Conan 缓存出问题：

```bash
rm -rf ~/cache/tsdb-builder/conan2-$(uname -m)/
```

### Q4: 内网镜像下载失败，如何回退到公网？

**原因**：GitLab Package Registry 不可用或 tarball 未上传。

**解决**：
- 临时方案：不传 `BUILD_DEPS_MIRROR_URL`，cmake 会直接从上游 URL 下载
- 永久方案：运行 `prepare-externals.sh --verify` 检查缺失的包并补充上传

### Q5: Cargo 编译时报 crate 找不到

**原因**：内网 Nora registry 可能尚未同步最新的 crate。

**解决**：
1. 检查 Nora 服务是否正常
2. 临时修改 `.cargo/config.toml` 使用公网 rsproxy 备用源
3. 如果是 git 依赖，确认 `setup-rust-git-mirrors.sh` 已创建对应的 GitLab 镜像

### Q6: Go 模块下载超时

**原因**：内网 Nexus Go Proxy 未缓存该模块。

**解决**：`GOPROXY` 已配置 `,direct` 后缀，会自动回退公网。如持续超时，检查 Nexus 服务状态。

### Q7: ccache 命中率低

**排查方法**：

```bash
ccache -s  # 查看命中统计
```

常见原因：
- `CCACHE_BASEDIR` 未正确设置（build.sh 已自动设为 `/mnt`）
- 编译器版本变化（`CCACHE_COMPILERCHECK=content` 已启用，会自动感知）
- 首次编译，缓存尚未建立

### Q8: ExternalProject 缓存如何失效/重建？

```bash
# 清理某个镜像+架构的 ExternalProject 缓存
rm -rf ~/cache/tsdb-builder/externals-core-x86_64/

# 下次编译时加 BUILD_CONTRIB=ON 重建
./tools/tsdb-builder/build.sh engine -DBUILD_CONTRIB=ON
```

### Q9: 如何添加新的 C/C++ ExternalProject 依赖？

1. 在 `external.cmake` 中添加 `get_from_local_if_exists()` 双参数调用
2. 运行 `prepare-externals.sh --cmake ... --upload <filename>` 上传到内网
3. 提交 `external.cmake` 和 `externals-manifest.txt` 的变更

详见 `tools/tsdb-builder/scripts/README.md`。

### Q10: 本地开发环境和 Docker 构建的镜像配置不一致

**设计上已避免**：setup 脚本和 build.sh 均从 `.build-args` 读取镜像 URL。修改 `.build-args` 后，两端自动同步。

---

## 附录 A：缓存目录完整布局

```
$HOME/cache/tsdb-builder/
├── conan2-x86_64/            # Conan C++ 包缓存（amd64）
├── conan2-aarch64/           # Conan C++ 包缓存（arm64）
├── externals-core-x86_64/    # ExternalProject 产物（core, amd64）
├── externals-core-aarch64/   # ExternalProject 产物（core, arm64）
├── externals-dev-x86_64/     # ExternalProject 产物（dev, amd64）
├── externals-dev-aarch64/    # ExternalProject 产物（dev, arm64）
├── externals-others-x86_64/  # ExternalProject 产物（others, amd64）
├── externals-others-aarch64/ # ExternalProject 产物（others, arm64）
├── go-mod/                   # Go 模块缓存
├── go-build/                 # Go 编译缓存
├── cargo-registry/           # Rust crate registry 缓存
├── cargo-git/                # Rust git 依赖缓存
├── ccache-core-x86_64/       # ccache（core, amd64）
├── ccache-core-aarch64/      # ccache（core, arm64）
├── ccache-dev-x86_64/        # ccache（dev, amd64）
├── ccache-others-x86_64/     # ccache（others, amd64）
├── sccache-core-x86_64/      # sccache（可选，Rust 编译缓存）
├── pnpm-store/               # Node.js pnpm 包缓存
├── m2-repository/            # Maven 仓库缓存
└── nuget/                    # .NET NuGet 缓存
```

## 附录 B：内网服务清单

| 服务 | 地址 | 用途 | 配置来源 |
|------|------|------|---------|
| GitLab Package Registry | `https://git.tdengine.net` | C/C++ ExternalProject tarball 镜像 | `build.sh` → `BUILD_DEPS_MIRROR_URL` |
| Nexus Go Proxy | `https://nexus.tdengine.net/repository/goproxy/` | Go 模块代理 | `.build-args` → `GO_PROXY` |
| Nora Cargo Registry | `sparse+https://nora.tdengine.net/cargo/index/` | Rust crate registry | `.build-args` → `CARGO_REGISTRY_URL` |
| Nexus Conan | `https://nexus.tdengine.net/repository/conan/` | C++ Conan 包仓库 | `.build-args` → `CONAN_REMOTE_URL` |
| Nexus Maven | `https://nexus.tdengine.net/repository/maven-public/` | Java Maven 仓库 | `.build-args` → `MAVEN_MIRROR_URL` |
| Nora npm | `https://nora.tdengine.net/npm/` | Node.js npm 包镜像 | `.build-args` → `NPM_REGISTRY_URL` |
| Nora PyPI | `https://nora.tdengine.net/simple/` | Python 包镜像（容器编译） | `.build-args` → `PYPI_INTERNAL_URL` |
| Nora NuGet | `https://nora.tdengine.net/nuget/v3/index.json` | .NET NuGet 包镜像 | `.build-args` → `NUGET_SOURCE_URL` |

> **集中化程度**：所有语言的镜像 URL 均已集中在 `.build-args` 中统一管理，
> `modules/*.sh` 脚本通过 `config.sh` 读取变量，不再硬编码 URL。
