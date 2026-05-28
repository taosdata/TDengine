---
name: "tsdb-build-docker"
description: "使用 tsdb-builder Docker 镜像编译 TDengine TSDB 组件。当用户需要构建/编译 engine、taosx、adapter 等组件，管理 Docker 编译镜像（core/dev/others），排查容器内编译问题，或使用 build.sh 脚本时触发。关键词：build.sh, docker build, tsdb-builder, core image, dev image, others image, 编译 engine, 编译 taosx, 交叉编译, ccache, mold, split-debug, BUILD_CONTRIB, BUILD_ROCKSDB, make 4.2.1, ext_curl, offline packages"
metadata:
  author: Bo Xiao
  version: 1.1.0
  owner_team: engine
  compatibility: "Requires Docker >= 20.10 with buildx support; offline packages at /data/packages/"
---

# tsdb-builder Docker Build Skill

> **Purpose**: 使用 tsdb-builder 提供的 Docker 镜像编译 TDengine TSDB 的所有组件。
> 涵盖三类镜像（core/dev/others）的构建、组件编译、交叉编译、缓存管理和常见问题排查。

## When to use

- 使用 `build.sh` 编译 TSDB 组件（engine, enterprise, adapter, keeper, tools, gen, taosx, explorer-ui, insight, connectors）
- 构建或更新 Docker 编译镜像（`build-core-image.sh`, `build-dev-image.sh`, `build-others-image.sh`）
- 排查容器内编译失败、依赖下载问题、链接错误
- 交叉编译（`--arch arm64` / `--arch riscv64`）
- 管理编译缓存（ccache, conan, externals, cargo）
- 配置 cmake 参数（`BUILD_CONTRIB`, `BUILD_ROCKSDB`, `CMAKE_BUILD_TYPE` 等）
- 调试 split-debug、mold linker、Conan profile 等问题

## Input

收集或确认以下信息：
- **目标组件**: engine / taosx / adapter / explorer-ui / core-all / others-all 等
- **镜像类型**: core（GCC 7, 生产）/ dev（GCC 11/x86_64, GCC 10/arm64, 开发）/ others（GCC 14, connectors）
- **目标架构**: amd64（默认）/ arm64 / riscv64
- **构建类型**: Debug / Release
- **错误日志**: 完整命令 + 首个失败行

缺少关键信息时，询问用户澄清。

## Output

1. 诊断摘要
2. 针对用户环境的精确命令
3. 验证步骤
4. 未解决时提供排查清单

## Safety

- 不要在未经用户确认的情况下执行 `--clean`（会清除 cmake 缓存）
- 不要暴露 `.env` 文件中的 token（`DEPS_MIRROR_TOKEN`）
- Docker 镜像推送需用户先手动 `docker login harbor.tdengine.net`
- `--no-cache` 会大幅增加构建时间，仅在 Dockerfile 变更后使用

## 三类 Docker 镜像

| 镜像 | glibc | 基础 OS | GCC | Make | 用途 |
|---|---|---|---|---|---|
| `core` | 2.17 | manylinux2014 / CentOS 7 | 7.3.1 (devtoolset-7) | **4.2.1** | ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX（Kylin V10 兼容） |
| `core` (riscv64) | 2.41+ | debian:trixie | 14.x | 4.3 | 同上（riscv64 架构） |
| `dev` | 2.17 | manylinux2014 / CentOS 7 | 11.2.1 (x86_64, devtoolset-11) / 10.2.1 (arm64, devtoolset-10) | **4.2.1** | 同 core，但用于日常开发 |
| `others` | 2.28 | manylinux_2_28 / AlmaLinux 8 | 14 (gcc-toolset-14) | 4.2.1 | INSIGHT, EXPLORER_UI + 全部 connectors |

所有镜像支持 `linux/amd64` 和 `linux/arm64`。core 额外支持 `linux/riscv64`（独立 tag）。

**core vs dev**: dev 编译器更高（x86_64: GCC 11, arm64: GCC 10 vs core 统一 GCC 7），用于日常开发；core 保证 Kylin V10 运行时兼容。aarch64 SCL 仓库无 devtoolset-11，arm64 保留基础镜像自带的 devtoolset-10。

> **Make 4.2.1 升级** (2026-05): core 和 dev 镜像从 Make 3.82 升级到 4.2.1，
> 修复了 GNU Make PR #12610 并行调度 bug，彻底解决了 ext_curl 间歇性配置失败问题
> (`configure: error: cannot compute suffix of executables`)。
> others 镜像基于 AlmaLinux 8，系统自带 Make 4.2.1，无此问题。

## 组件与镜像映射

- **core/dev 组件**: `engine`, `enterprise`, `adapter`, `keeper`, `tools`, `gen`, `taosx`
- **others 组件**: `explorer-ui`, `insight`, `dotnet`, `go`, `jdbc`, `node`, `python`, `rust`, `odbc`

`taosx` vs `explorer-ui`:
- `taosx` → `BUILD_TAOSX=ON, BUILD_EXPLORER_UI=OFF`（仅 Rust 二进制，用 **core** 镜像）
- `explorer-ui` → `BUILD_TAOSX=ON, BUILD_EXPLORER_UI=ON`（Rust + pnpm 前端，用 **others** 镜像）

## 关键命令

### 编译组件（开发）

```bash
# --image 是必需的
./build.sh --image core engine taosx
./build.sh --image dev engine taosx                       # 用 dev 镜像 (GCC 11/x86_64, GCC 10/arm64)
./build.sh --image others explorer-ui insight jdbc
./build.sh --image core:3.4.1 engine                      # 指定版本
./build.sh --image core --arch arm64 engine adapter       # 交叉编译
./build.sh --image core --clean --arch amd64 taosx        # 清除 cmake 缓存
./build.sh --image core --src /path/to/TDengine engine    # 指定源码路径
./build.sh --image core --cache /data/cache/tsdb-builder engine  # 指定缓存路径

# cmake 参数透传
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug -DBUILD_VER_NUMBER=3.4.1.3

# Split debug info
./build.sh --image core --split-debug core-all -DCMAKE_BUILD_TYPE=Release

# 强制依赖用 Release（避免 debug 依赖膨胀）
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug -DTD_ALIGN_EXTERNAL=OFF

# 使用内部 GitLab mirror 下载依赖
./build.sh --image core engine -DBUILD_CONTRIB=ON \
    -DBUILD_DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest"

# 组件快捷方式
./build.sh --image core  core-all     # 全部 core 组件
./build.sh --image others others-all  # 全部 others 组件
./build.sh --image others all         # 全部 16 个组件
```

### 编译组件（CI / 全量构建）

```bash
./build.sh --image core --clean core-all
./build.sh --image others --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF
```

### 构建 Docker 镜像

```bash
docker login harbor.tdengine.net

# 推荐：使用预下载的离线包（三套镜像均需要离线包）
# 先运行 scripts/download-packages.sh ~/packages
./build-core-image.sh --version 3.4.1 --packages ~/packages [--arch amd64|arm64|riscv64]
./build-dev-image.sh --version 3.4.1 --packages ~/packages [--arch amd64|arm64]
./build-others-image.sh --version 3.4.1 --packages ~/packages [--arch amd64|arm64]

# 跳过 Docker 层缓存
./build-core-image.sh --version 3.4.1 --no-cache

# 仅本地构建不推送
./build-core-image.sh --version test --local
```

### 验证镜像

```bash
./verify-image.sh core:amd64
./verify-image.sh core:3.4.1-amd64
./verify-image.sh others:arm64
```

## cmake 参数优先级

cmake 参数按最后出现的值生效（last-value-wins）：
1. 组件默认值：未列出的组件 `BUILD_*=OFF`
2. 组件快捷方式：列出的组件 `BUILD_*=ON`
3. pthread 变量（仅 core/dev）
4. `-DKEY=VALUE` CLI 透传 — 最高优先级

## BUILD_CONTRIB / BUILD_ROCKSDB / ROCKSDB_USE_DEPS

| Flag | 用途 |
|---|---|
| `BUILD_CONTRIB` | 外部依赖主开关。`ON` = ExternalProject 下载构建；`OFF` = 复用预构建 |
| `BUILD_ROCKSDB` | RocksDB 编译开关。`ON` = 从源码构建 |
| `ROCKSDB_USE_DEPS` | RocksDB 是否使用 `deps/` 目录预构建二进制 |

**平台默认值：**

| 平台 | `BUILD_CONTRIB` | `BUILD_ROCKSDB` | `ROCKSDB_USE_DEPS` | 行为 |
|---|---|---|---|---|
| Linux | `OFF` | `OFF` | `ON` | 使用 `deps/` 预构建 |
| 非 Linux | `ON` | `ON` | `OFF` | ExternalProject 下载+编译 |

**常见场景：**
```bash
# 从源码构建 RocksDB
./build.sh --image core engine -DBUILD_CONTRIB=ON -DBUILD_ROCKSDB=ON

# 使用 deps/ 预构建（Linux 默认）
./build.sh --image core engine -DBUILD_CONTRIB=OFF

# 使用 .externals/ 缓存
./build.sh --image core engine -DBUILD_CONTRIB=OFF -DROCKSDB_USE_DEPS=OFF
```

> ⚠️ `BUILD_CONTRIB=OFF` + `BUILD_ROCKSDB=ON` 会触发 `FATAL_ERROR`。

## 构建类型与调试信息

| Flag | 默认 | 效果 |
|---|---|---|
| `BUILD_RELEASE` | `OFF` | `OFF` → TSDB 二进制包含 `-g3 -gdwarf-2`；`ON` → 无调试信息 |
| `TD_ALIGN_EXTERNAL` | `ON` | `ON` → 依赖跟随主项目 `CMAKE_BUILD_TYPE`；`OFF` → 依赖始终 Release |

**关键点**：`CMAKE_BUILD_TYPE=Release` 只加 `-O3`，不去除调试信息。去除调试信息用 `BUILD_RELEASE=ON` 或 `--split-debug`。

## 卷挂载（编译时）

`build.sh` 将 TSDB 源码挂载为容器内 `/mnt`，并挂载以下缓存：

| 宿主机路径（`TSDB_CACHE_DIR` 下） | 容器路径 | 范围 |
|---|---|---|
| `conan2-<arch>/` | `/root/.conan2` | core + dev + others |
| `externals-{image}-<arch>/` | `/mnt/.externals` | 按镜像类型隔离 |
| `go-mod/` | `/root/go/pkg/mod` | core + dev + others |
| `cargo-registry/` | `/root/.cargo/registry` | core + dev + others |
| `cargo-git/` | `/root/.cargo/git` | core + dev + others |
| `pnpm-store/` | `/mnt/.pnpm-store` | others only |
| `m2-repository/` | `/root/.m2/repository` | others only |
| `nuget/` | `/root/.nuget/packages` | others only |
| `ccache-{image}-<arch>/` | `/root/.ccache` | 按镜像+架构隔离 |

`TSDB_CACHE_DIR` 默认 `$HOME/cache/tsdb-builder`，位于源码仓库之外。可通过 `export TSDB_CACHE_DIR=...` 覆盖。

**Cargo 子目录挂载原因**：挂载整个 `~/.cargo` 会遮蔽镜像内的 `config.toml`（中国镜像配置）和 `bin/`（rustc/cargo 二进制）。

## 镜像发布

`--version` 必需。每次推送 `<version>-<arch>` 和 `latest-<arch>`。若兄弟架构 tag 已存在，脚本还会尝试更新多架构 manifest（仅 amd64/arm64；riscv64 为独立 tag）。

`--local` 跳过推送，仅本地测试。

## 构建参数管理

所有工具版本和镜像设置集中在 `.build-args` 文件中。两个消费者：
1. 镜像构建脚本 → `--build-arg`
2. `build.sh` → 运行时容器配置（npm/Maven/NuGet/PyPI 内部镜像）

**关键不变量**：`ARG` 变量（Dockerfile 使用）必须用公网 URL；内部 URL 仅由 `build.sh` 在运行时注入。

## 内部网络依赖镜像

**Dockerfile 烘焙（镜像构建时）：**
- Rust/Cargo: 离线 standalone installer（运行时配置 `sparse+https://nora.tdengine.net/cargo/index/`）
- Conan: `https://nexus.tdengine.net/repository/conan/`
- Go: `GOPROXY=https://nexus.tdengine.net/repository/goproxy/`
- pip: 离线 wheel 安装（taospy, taos-ws-py, conan, maturin）
- uv: 离线预编译 musl 二进制
- ccache: 离线源码编译（zstd/hiredis/xxhash 离线依赖）

**运行时注入（容器编译时）：**
- PyPI: `https://nora.tdengine.net/simple/`
- npm/pnpm: `https://nora.tdengine.net/npm/`
- Maven: `https://nexus.tdengine.net/repository/maven-public/`
- NuGet: `https://nora.tdengine.net/nuget/v3/index.json`
- C/C++ ExternalProject: GitLab Package Registry（`DEPS_MIRROR_URL`）

## mold 链接器

所有镜像从源码编译 mold（release 二进制需要更高 glibc）。

**amd64/arm64**：mold 为默认链接器，通过 `update-alternatives` + gcc-toolset alternatives 注册。

**core/dev 特殊**：`-DMOLD_USE_MIMALLOC=OFF`（mimalloc 在 glibc 2.17 arm64 上 segfault）。

**riscv64**：mold **不是**默认链接器（会损坏 Go CGO 二进制的 ELF 布局）。`build.sh` 通过 `-DCMAKE_LINKER=mold` 仅对 cmake C/C++ 目标启用。

## ccache

所有镜像包含 ccache。缓存按 `{image}-{arch}` 隔离。

| 变量 | 默认 | 用途 |
|---|---|---|
| `CCACHE_MAXSIZE` | `20G` | 每目录最大缓存 |
| `CCACHE_REMOTE_STORAGE` | 未设置 | 共享缓存后端（NFS/HTTP，用于 CI） |

## Conan profile 初始化

容器内 cmake 运行前自动检测并修补 Conan 默认 profile：
- `compiler.cppstd=gnu14` → `gnu17`
- `arch` 修正为容器内 `uname -m`（防止交叉架构标志注入）

## Split Debug (`--split-debug`)

分离 DWARF 调试信息：`objcopy --only-keep-debug` → `strip` → `objcopy --add-gnu-debuglink`。

- 可执行文件：`strip -s`（去除所有符号）
- 共享库：`strip --strip-debug`（保留动态符号）
- 排除 Rust 组件（taosx, taos-explorer）

输出：`build/bin/.debug/` 和 `build/lib/.debug/`。

## Make jobserver 回退

`build.sh` 先执行 `make -j$(nproc)`，失败后自动重试 `make -j1`（ExternalProject 在 make 3.82 上的 jobserver 不兼容问题）。

## sqlx 离线模式 (taosx)

`build.sh` 删除容器内 `.env` 和 `taosx.dev.db`。若遇到 `unable to open database file` 错误，需设置 `SQLX_OFFLINE=true`。

## 日志

`build.sh` 将完整输出镜像到 `<src>/build.log`。每次运行打印完整 `docker run` 命令（可直接复制粘贴复现）。

## 常见问题排查

### Docker 认证失败
```bash
docker login harbor.tdengine.net
```

### ext_curl 配置失败（旧镜像）
**症状**: `configure: error: cannot compute suffix of executables: cannot compile and link`

**根因**: GNU Make 3.82 并行调度 bug (PR #12610) + ExternalProject 钻石依赖 + BUILD_IN_SOURCE=TRUE

**解决方案**:
1. **升级镜像**（推荐）：使用最新 core/dev 镜像（Make 4.2.1）
   ```bash
   docker pull harbor.tdengine.net/tsdb-builder/dev:latest-amd64
   ```

2. **清除缓存**（临时）：删除损坏的 ext_curl 缓存
   ```bash
   rm -rf $CACHE_DIR/externals-{dev,core}-*/build/Release/ext_curl
   rm -rf $CACHE_DIR/externals-{dev,core}-*/src/ext_curl
   ./build.sh --image dev --clean engine
   ```

3. **验证 Make 版本**:
   ```bash
   docker run --rm harbor.tdengine.net/tsdb-builder/dev:latest-amd64 make --version
   # 应输出: GNU Make 4.2.1
   ```

详细根因分析参见 `tsdb-build-cmake-invariants` 技能 Rule 14。

### 依赖下载失败
```bash
# 使用内部镜像
./build.sh --image core engine -DBUILD_CONTRIB=ON \
    -DBUILD_DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest"
```

### GCC 14 `-Werror=stringop-overflow`（others 镜像）
```bash
./build.sh --image others others-all -DCMAKE_C_FLAGS="-Wno-error=stringop-overflow"
```

### pthread 链接错误（core/dev 镜像）
`build.sh` 已自动注入 pthread cmake 变量。若仍报错，检查是否使用了正确的镜像。

### sqlx 数据库文件错误
```bash
export SQLX_OFFLINE=true
./build.sh --image core taosx
```

### Cargo SSL 失败（others 镜像）
`build.sh` 已自动设置 `CARGO_NET_GIT_FETCH_WITH_CLI=true`。

## 支持文件

- 脚本目录: `tools/tsdb-builder/`
  - `build.sh` — 组件编译入口
  - `build-core-image.sh` — 构建 core 镜像
  - `build-dev-image.sh` — 构建 dev 镜像
  - `build-others-image.sh` — 构建 others 镜像
  - `verify-image.sh` — 验证镜像
  - `.build-args` — 版本与镜像配置
  - `scripts/prepare-externals.sh` — 依赖 tarball 管理
  - `scripts/download-packages.sh` — 下载离线包（Go, CMake, mold, Make, ccache, sccache, Rust, uv, protoc, tini, bison, ccache deps, pip wheels + maturin）
  
- 文档目录: `tools/tsdb-builder/`
  - `README.md` — 完整使用文档（含离线包加速、故障排查）
  - `COPILOT-INSTRUCTIONS.md` — AI 助手指南
  - `docs/build-optimization-guide.md` — 构建优化指南

## riscv64 注意事项

- 使用独立 Dockerfile（`Dockerfile.core-riscv64`），基于 `debian:trixie`
- glibc 2.41+，GCC 14.x（无 Kylin V10 约束）
- cmake/protoc/mold/tini 使用 apt 安装
- mold 不是默认链接器
- 不需要 buildx
- 无 sccache 预构建
- 独立 tag（不在 amd64/arm64 多架构 manifest 中）

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-build-docker version=1.1.0 author=Bo Xiao`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
