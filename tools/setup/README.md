# tools/setup — TSDB Build Environment Setup

Unified toolchain installation and internal dependency source configuration for
developers building outside the `tsdb-builder` Docker container.

## Quick Start

```bash
# Linux — 按组件配置（自动识别所需语言环境）
./tools/setup/setup-linux.sh --component engine taosx

# macOS
./tools/setup/setup-macos.sh --component engine taosx

# Windows (PowerShell)
.\tools\setup\setup-windows.ps1 -Component engine, taosx

# Check what's missing (no changes)
./tools/setup/setup-macos.sh --check --all              # Linux/macOS
.\tools\setup\setup-windows.ps1 -All -CheckOnly         # Windows
```

## 内网 vs 外网模式

setup 脚本**默认配置内网依赖源**（从 `tools/tsdb-builder/.build-args` 读取 URL）。

如需切换到公网源（例如在外网环境或 GitHub clone 后使用），设置环境变量：

```bash
export TSDB_PUBLIC_DEPS=1
./tools/setup/setup-linux.sh --component adapter
```

| 模式 | `TSDB_PUBLIC_DEPS` | 效果 |
|---|---|---|
| 内网（默认） | 不设置 或 `0` | GOPROXY → Nexus, Cargo → Nora, Conan → Nexus, npm/Maven/NuGet/PyPI → 内网 |
| 外网 | `1` | GOPROXY → proxy.golang.org, Cargo → crates.io, npm/Maven/NuGet/PyPI → 公网 |

> **GitHub 用户无需 setup 脚本**：如果是从 GitHub 克隆单个仓库（如 `taosdata/TDengine`），直接按 README.md 中的 `## Building` 章节操作即可，标准工具默认就使用公网源。`tools/setup/` 仅用于 monorepo 内网开发场景。

## 常见开发场景示例

### 在内网宿主机编译 taos-adapter（Go 组件）

```bash
# 1. 配置 Go 工具链 + 内网 GOPROXY
./tools/setup/setup-linux.sh --component adapter

# 2. 重新加载 shell 配置（首次安装后需要）
source ~/.bashrc

# 3. 编译
cd source/taos-adapter
go build ./...
```

setup 自动完成：安装 Go ≥ 1.23 → 配置 `GOPROXY=https://nexus.tdengine.net/repository/goproxy/,direct` → 设置 `GONOSUMDB`/`GONOSUMCHECK` → 验证内网连通性。

### 在内网宿主机编译 taosx（Rust 组件）

```bash
# 1. 配置 Rust 工具链 + 内网 Cargo registry
./tools/setup/setup-linux.sh --component taosx

# 2. 重新加载 shell 配置
source ~/.bashrc

# 3. 编译
cd source/taos-xservice
cargo build
```

setup 自动完成：安装 Rust ≥ 1.90 → 复制 `.cargo/config.toml`（Nora 内网 registry）→ 安装 protoc → 验证内网连通性。

### 在内网宿主机编译 TDengine 引擎（C/C++ 组件）

```bash
# 1. 配置 C/C++ 工具链 + 内网 Conan remote
./tools/setup/setup-linux.sh --component engine

# 2. 编译（首次需要 BUILD_CONTRIB=ON）
cd source/taos-community
mkdir -p debug && cd debug
cmake .. -DBUILD_CONTRIB=ON
make -j$(nproc)
```

### 在外网宿主机编译（GitHub clone）

```bash
# 切换到公网源
export TSDB_PUBLIC_DEPS=1
./tools/setup/setup-linux.sh --component engine adapter taosx

source ~/.bashrc

# 之后按正常流程编译即可
```

## Usage

### Linux / macOS

```
./tools/setup/setup-{linux,macos}.sh [options]

Options:
  --component NAME [NAME...]   Setup by component (auto-resolves languages)
  --lang NAME [NAME...]        Setup by language: cpp go rust java node python dotnet
  --all                        All language modules
  --check                      Check-only, no modifications
  --yes, -y                    Non-interactive (auto-confirm)
  --help, -h                   Show help
```

### Windows (PowerShell)

```powershell
.\tools\setup\setup-windows.ps1 [options]

Parameters:
  -Component NAME[,NAME...]    Setup by component (auto-resolves languages)
  -Lang NAME[,NAME...]         Setup by language: cpp go rust java node python dotnet
  -All                         All language modules
  -CheckOnly                   Check-only, no modifications
  -Yes                         Non-interactive (auto-confirm)
  -Help                        Show help
```

## Components & Languages

| Component | Languages |
|-----------|-----------|
| engine, enterprise, gen, connector-odbc | cpp |
| adapter, keeper, connector-go | go |
| taosx, connector-rust | rust |
| insight | go, node |
| connector-python | python, rust |
| connector-jdbc | java |
| connector-node | node |
| connector-dotnet | dotnet |

## What Each Module Does

Each module handles two concerns: **toolchain installation** and **internal source configuration**.

| Module | Install | Configure |
|--------|---------|-----------|
| cpp | cmake, gcc/clang, ccache, conan | CMAKE_*_COMPILER_LAUNCHER=ccache, Conan remote → Nexus |
| go | Go SDK | GOPROXY → Nexus, GONOSUMDB for internal modules |
| rust | rustup, protoc, sccache (opt) | ~/.cargo/config.toml → Nora registry |
| java | JDK 17+, Maven | Maven mirror (if available) |
| node | Node.js, pnpm | npm registry (if available) |
| python | python3, pip, maturin | pip index-url (if available) |
| dotnet | .NET SDK | NuGet source (if available) |

## Configuration Source

Mirror URLs and version requirements are read from `tools/tsdb-builder/.build-args` —
the same source used by the Docker build environment. Fallback defaults are
provided if the file is not available.

## Directory Structure

```
tools/setup/
├── setup-linux.sh       # Linux entry point
├── setup-macos.sh       # macOS entry point
├── setup-windows.ps1    # Windows entry point (PowerShell)
├── config.sh            # Component→language mapping, mirror URLs, versions
├── utils/
│   ├── common.sh        # Colors, confirm(), version_gte(), logging
│   └── platform.sh      # OS/arch/distro/pkg-manager detection
├── modules/             # Linux/macOS modules (bash)
│   ├── cpp.sh
│   ├── go.sh
│   ├── rust.sh
│   ├── java.sh
│   ├── node.sh
│   ├── python.sh
│   └── dotnet.sh
├── modules-windows/     # Windows modules (PowerShell)
│   ├── cpp.ps1
│   ├── go.ps1
│   ├── rust.ps1
│   ├── java.ps1
│   ├── node.ps1
│   ├── python.ps1
│   └── dotnet.ps1
└── README.md
```

## Relationship to Other Scripts

| Script | Role |
|--------|------|
| `tools/tsdb-builder/build.sh` | Docker-based full build (container environment) |
| `tools/setup/` | **This** — non-container environment setup |
| `tools/deps/install_deps.sh` | Predecessor (to be deprecated) |
| `source/taos-community/packaging/setup_env.sh` | Full dev-machine bootstrap (different scope) |
