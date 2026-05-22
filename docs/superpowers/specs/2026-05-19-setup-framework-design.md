# Non-Container Build Environment Setup Framework

> **Date**: 2026-05-19
> **Status**: Approved
> **Scope**: `tools/setup/` — unified toolchain installation + internal dependency source configuration

## Problem

TSDB is a monorepo with 14+ components spanning C/C++, Go, Rust, Java, Node.js, Python, and .NET. The containerized build environment (`tools/tsdb-builder/`) handles internal dependency source configuration (GOPROXY, Cargo/Nora, Conan, ccache, sccache), but developers who build outside the container — on their host machine or inside an existing container — lack a unified way to:

1. Install the correct toolchain versions for their target components
2. Configure internal dependency sources (proxies, mirrors, caches)

Existing scripts are fragmented:
- `tools/deps/install_deps.sh` — installs toolchains but no internal source config
- `tools/deps/windows/install_deps_windows.ps1` — Windows-only toolchain install
- `source/taos-community/packaging/setup_env.sh` — heavyweight full dev-machine bootstrap (Docker, Prometheus, NFS, etc.), scope too broad

## Solution

A modular `tools/setup/` framework organized by programming language. Each language module handles both toolchain installation and internal source configuration. Components declare their language dependencies via a mapping table; the entry-point script resolves and executes the required modules.

## Directory Structure

```
tools/setup/
├── setup-linux.sh              # Linux entry point
├── setup-macos.sh              # macOS entry point
├── setup-windows.ps1           # Windows entry point (future)
├── config.sh                   # Component→language mapping + mirror URLs + version requirements
├── utils/
│   ├── platform.sh             # OS/arch/package-manager detection, pkg_install()
│   └── common.sh               # Colors, confirm(), version_gte(), logging
├── modules/
│   ├── cpp.sh                  # cmake, gcc/clang, ccache, conan
│   ├── go.sh                   # go, goproxy, gocache
│   ├── rust.sh                 # rustup, cargo, nora registry, sccache
│   ├── java.sh                 # JDK 17+, Maven
│   ├── node.sh                 # Node.js, pnpm
│   ├── python.sh               # python3, pip, maturin
│   └── dotnet.sh               # .NET SDK
├── modules-windows/            # PowerShell equivalents (future)
│   ├── cpp.ps1
│   ├── go.ps1
│   ├── rust.ps1
│   ├── java.ps1
│   ├── node.ps1
│   ├── python.ps1
│   └── dotnet.ps1
└── README.md
```

## CLI Interface

```bash
# By component (auto-resolves language dependencies)
./tools/setup/setup-linux.sh --component engine        # → cpp, go
./tools/setup/setup-linux.sh --component taosx          # → rust
./tools/setup/setup-linux.sh --component adapter        # → go
./tools/setup/setup-linux.sh --component insight         # → go, node

# By language (direct)
./tools/setup/setup-linux.sh --lang rust go

# All languages
./tools/setup/setup-linux.sh --all

# Check-only (no modifications)
./tools/setup/setup-linux.sh --check --component engine

# Non-interactive (auto-confirm all prompts)
./tools/setup/setup-linux.sh --yes --all

# Combine
./tools/setup/setup-macos.sh --yes --check --all
```

## Component → Language Mapping

Defined in `config.sh`:

| Component | Language Modules |
|-----------|-----------------|
| engine | cpp |
| enterprise | cpp |
| adapter | go |
| keeper | go |
| taosx | rust |
| gen | cpp |
| insight | go, node |
| connector-jdbc | java |
| connector-go | go |
| connector-node | node |
| connector-python | python, rust |
| connector-rust | rust |
| connector-dotnet | dotnet |
| connector-odbc | cpp |

## Module Interface

Every `modules/*.sh` exports three standardized functions:

```bash
mod_<lang>_check()    # Report current state: installed version, config status
mod_<lang>_install()  # Install toolchain (delegates to platform.sh for apt/yum/brew)
mod_<lang>_config()   # Configure internal sources, caches, shell RC entries
```

### Execution flow

```
1. Source utils/common.sh + utils/platform.sh + config.sh
2. Parse arguments → resolve component names to language list (deduplicated)
3. For each language module:
   a. mod_<lang>_check()  → print status (✓ ready / ⚠ missing / ✗ wrong version)
   b. If --check mode: skip to next module
   c. If deficiencies found:
      - mod_<lang>_install()  → install/upgrade toolchain
      - mod_<lang>_config()   → write internal source configuration
4. Print summary (changes made, issues remaining)
```

## Module Details

### cpp.sh

| Phase | Action |
|-------|--------|
| check | cmake version ≥ 3.21, gcc/clang presence, ccache presence, conan presence |
| install | `pkg_install cmake gcc g++` (Linux), `brew install cmake ccache conan` (macOS) |
| config | Write `CMAKE_C_COMPILER_LAUNCHER=ccache` and `CMAKE_CXX_COMPILER_LAUNCHER=ccache` to shell RC; configure Conan remote → `nexus.tdengine.net/repository/conan2/` |

### go.sh

| Phase | Action |
|-------|--------|
| check | go version ≥ 1.23, GOPROXY value |
| install | Download official tarball or `brew install go` |
| config | Write `GOPROXY=<nexus-url>,direct` to shell RC; set `GONOSUMDB` and `GONOSUMCHECK` for internal modules |

### rust.sh

| Phase | Action |
|-------|--------|
| check | rustc version ≥ 1.90.0, cargo presence, ~/.cargo/config.toml content, protoc presence |
| install | `curl rustup-init.sh`, install protoc |
| config | Copy/write `~/.cargo/config.toml` with Nora registry (from `tsdb-builder/.cargo/config.toml`); optionally install sccache |

### java.sh

| Phase | Action |
|-------|--------|
| check | java version ≥ 17, mvn presence |
| install | `pkg_install openjdk-17-jdk maven` or `brew install openjdk@17 maven` |
| config | Configure Maven `settings.xml` mirror if internal Nexus Maven repo exists |

### node.sh

| Phase | Action |
|-------|--------|
| check | node version ≥ 18, pnpm presence |
| install | Install via official script or brew; install pnpm via `corepack enable` |
| config | Configure npm registry mirror if internal registry exists |

### python.sh

| Phase | Action |
|-------|--------|
| check | python3 version ≥ 3.10, pip presence, maturin presence |
| install | `pkg_install python3 python3-pip`; `pip install maturin` |
| config | Configure pip index-url → internal PyPI mirror |

### dotnet.sh

| Phase | Action |
|-------|--------|
| check | dotnet sdk presence |
| install | Install via official script or package manager |
| config | Configure NuGet source if internal registry exists |

## Configuration Data Source

`config.sh` reads from `tools/tsdb-builder/.build-args` as the single source of truth for mirror URLs and version pins:

```bash
BUILDER_DIR="$(cd "$SCRIPT_DIR/../tsdb-builder" && pwd)"
if [[ -f "$BUILDER_DIR/.build-args" ]]; then
    GO_PROXY=$(grep '^GO_PROXY=' "$BUILDER_DIR/.build-args" | cut -d= -f2-)
    CARGO_REGISTRY_URL=$(grep '^CARGO_REGISTRY_URL=' "$BUILDER_DIR/.build-args" | cut -d= -f2-)
    CONAN_REMOTE_URL=$(grep '^CONAN_REMOTE_URL=' "$BUILDER_DIR/.build-args" | cut -d= -f2-)
fi
# Fallback defaults
GO_PROXY="${GO_PROXY:-https://nexus.tdengine.net/repository/goproxy/}"
CARGO_REGISTRY_URL="${CARGO_REGISTRY_URL:-sparse+https://nora.tdengine.net/cargo/index/}"
CONAN_REMOTE_URL="${CONAN_REMOTE_URL:-https://nexus.tdengine.net/repository/conan2/}"
```

For Cargo specifically, `config.sh` also checks for `tools/tsdb-builder/.cargo/config.toml` and copies it directly when available, ensuring exact parity with the container environment.

## Platform Abstraction (`utils/platform.sh`)

```bash
detect_os()       # Returns: linux | macos
detect_arch()     # Returns: amd64 | arm64
detect_distro()   # Returns: ubuntu | debian | centos | rhel | fedora | alma | rocky
detect_pkg_mgr()  # Returns: apt | yum | dnf | brew

pkg_install() {
    case "$PKG_MGR" in
        apt)       sudo apt-get install -y "$@" ;;
        yum)       sudo yum install -y "$@" ;;
        dnf)       sudo dnf install -y "$@" ;;
        brew)      brew install "$@" ;;
    esac
}
```

`setup-linux.sh` and `setup-macos.sh` share all modules. They differ only in:
- Entry-point validation (refuse to run on wrong OS)
- Default paths (brew prefix on macOS, /usr/local vs /usr on Linux)

## Windows Strategy

`setup-windows.ps1` uses the same modular structure with PowerShell:
- Module files in `modules-windows/*.ps1`
- Component→language mapping replicated as PowerShell hashtable (not parsed from bash config.sh)
- Mirror URLs duplicated (avoids complex bash→PS parsing)
- Uses `winget` as primary package manager, fallback to direct downloads

Windows implementation is deferred to a future phase.

## Design Principles

### Idempotency
All modules are safe to run repeatedly:
- `check` before `install` — skip if version requirement already met
- Config file writes use marker detection — skip if already configured
- Backup `.bak` before modifying existing config files

### Error Handling
- Individual module failure does not abort other modules
- Connectivity check failures produce warnings, not errors (developer may be on external network)
- `--check` mode is purely read-only, zero side effects

### Shell RC Management
- Auto-detects `~/.bashrc` or `~/.zshrc` based on `$SHELL`
- Appends with comment markers for traceability: `# TSDB setup (<module>)`
- Deduplicates: checks for existing entries before writing

## Migration Plan

| Phase | Scope |
|-------|-------|
| **Phase 1 (this work)** | Create framework + Linux/macOS entry points + cpp/go/rust modules |
| Phase 2 | Add java/node/python/dotnet modules |
| Phase 3 | Implement `setup-windows.ps1` + Windows modules |
| Phase 4 | Mark `tools/deps/install_deps.sh` and `tools/deps/windows/` as deprecated, redirect to `tools/setup/` |
| Independent | Migrate `setup_env.sh` to `packaging/` with symlink at original location |

## Relationship to Existing Infrastructure

| Script | Relationship |
|--------|-------------|
| `tools/tsdb-builder/build.sh` | Container build — reads same `.build-args` and `.cargo/config.toml`; `tools/setup/` is its non-container counterpart |
| `tools/tsdb-builder/.build-args` | Single source of truth for mirror URLs — both `build.sh` and `tools/setup/` read from here |
| `tools/tsdb-builder/.cargo/config.toml` | Cargo config source — `rust.sh` copies this to `~/.cargo/config.toml` |
| `tools/deps/install_deps.sh` | Predecessor — functionality absorbed into `tools/setup/` modules; to be deprecated |
| `source/taos-community/packaging/setup_env.sh` | Different scope — full dev-machine bootstrap (Docker, monitoring, NFS); not replaced |
| `tools/tsdb-builder/scripts/setup-build-env.sh` | Superseded — delete after `tools/setup/` is operational |
