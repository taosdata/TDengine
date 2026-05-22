# tools/setup — TSDB Build Environment Setup

Unified toolchain installation and internal dependency source configuration for
developers building outside the `tsdb-builder` Docker container.

## Quick Start

```bash
# Linux
./tools/setup/setup-linux.sh --component engine taosx

# macOS
./tools/setup/setup-macos.sh --component engine taosx

# Windows (PowerShell)
.\tools\setup\setup-windows.ps1 -Component engine, taosx

# Check what's missing (no changes)
./tools/setup/setup-macos.sh --check --all              # Linux/macOS
.\tools\setup\setup-windows.ps1 -All -CheckOnly         # Windows
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
