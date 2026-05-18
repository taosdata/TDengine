# tsdb-builder Copilot Instructions

## What This Repo Does

This repo builds and manages three Docker images used to compile TDengine TSDB components:

| Image | glibc | Base OS | Used For |
|---|---|---|---|
| `harbor.tdengine.net/tsdb-builder/core` | 2.17 | manylinux2014 / CentOS 7 | ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX |
| `harbor.tdengine.net/tsdb-builder/core` (riscv64) | 2.41+ | debian:trixie | ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX |
| `harbor.tdengine.net/tsdb-builder/dev` | 2.17 | manylinux2014 / CentOS 7 | ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX (GCC 9.3.1, for development) |
| `harbor.tdengine.net/tsdb-builder/others` | 2.28 | manylinux_2_28 / AlmaLinux 8 | INSIGHT, EXPLORER_UI + all connectors (dotnet/go/jdbc/node/python/rust/odbc) |

All three images support `linux/amd64` and `linux/arm64`. The core image additionally supports `linux/riscv64` (standalone tag, not part of multi-arch manifest). The dev image is identical to core except for the compiler: devtoolset-9 (GCC 9.3.1) instead of devtoolset-7 (GCC 7.3.1). Use dev for daily development; use core when Kylin V10 runtime compatibility is required. Offline install packages are stored on the host at `/data/packages/` and mounted at build time via `--build-context packages=...` (never written into image layers). For riscv64, packages are copied into a temporary build context since `docker buildx` is not required.

The others image also requires 5 TDengine client files in `packages/`: `taos.h`, `taosws.h`, `libtaos.so`, `libtaosnative.so`, `libtaosws.so`. These are copied into the image at `/usr/include/` and `/usr/lib/` for connector compilation (e.g. ODBC).

## Key Commands

### Build Docker images
```bash
# Login first when pushing to Harbor
docker login harbor.tdengine.net

# Defaults: arch=amd64, packages=$HOME/packages
./build-core-image.sh --version 3.4.1 [--arch amd64|arm64|riscv64] [--packages /path/to/packages]
./build-dev-image.sh --version 3.4.1 [--arch amd64|arm64] [--packages /path/to/packages]
./build-others-image.sh --version 3.4.1 [--arch amd64|arm64] [--packages /path/to/packages]

# Examples
./build-core-image.sh --version 3.4.1 --arch arm64
./build-dev-image.sh --version 3.4.1 --arch arm64
./build-core-image.sh --version 3.4.1 --packages /data/packages   # backward-compat path
# Bare positional arch still works: ./build-core-image.sh amd64 --version 3.4.1

# Skip Docker layer cache (recommended after Dockerfile changes)
./build-core-image.sh --version 3.4.1 --no-cache
./build-dev-image.sh --version 3.4.1 --no-cache
./build-others-image.sh --version 3.4.1 --no-cache

# Build locally without pushing (for testing Dockerfile changes)
./build-core-image.sh --version test --local
./build-dev-image.sh --version test --local
./build-others-image.sh --version test --local
```

### Compile TSDB components (development)
```bash
# --image is required for all build.sh calls
./build.sh --image core engine taosx
./build.sh --image dev engine taosx                       # use dev image (GCC 9)
./build.sh --image others explorer-ui insight jdbc
./build.sh --image core:3.4.1 engine
./build.sh --image others --pull-image explorer-ui
./build.sh --image core --arch arm64 engine adapter       # cross-compile
./build.sh --image dev --arch arm64 engine adapter        # cross-compile with dev
./build.sh --image core --clean --arch amd64 taosx        # wipe cmake cache first
./build.sh --image core --src /path/to/TDengine engine    # explicit source path
./build.sh --image core --cache /data/cache/tsdb-builder engine  # explicit cache path

# Component + cmake override (Debug mode, version info, etc.)
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug -DBUILD_VER_NUMBER=3.4.1.3

# Split debug info (strip binaries, save .debug files separately)
./build.sh --image core --split-debug core-all -DCMAKE_BUILD_TYPE=Release

# Force dependencies to Release (no debug info) regardless of main project build type
./build.sh --image core engine -DCMAKE_BUILD_TYPE=Debug -DTD_ALIGN_EXTERNAL=OFF

# Use internal GitLab mirror for dependency downloads
./build.sh --image core engine -DBUILD_CONTRIB=ON \
    -DBUILD_DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest"

# Private GitLab mirror (token in .env file)
./build.sh --image core --clean engine -DBUILD_CONTRIB=ON \
    -DBUILD_DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest"

# Group shortcuts
./build.sh --image core  core-all    # all core components
./build.sh --image others others-all  # all others components
./build.sh --image others all         # all 16 components

# Pure -D mode (no component shortcuts — for release scripts)
cmake_args=(
  -DBUILD_ENGINE=ON -DBUILD_TAOSX=ON
  -DCMAKE_BUILD_TYPE=Release
  -DBUILD_VER_NUMBER=3.4.1.3
  -DBUILD_GITINFO=abc123
  -D"BUILD_VER_DATE=2026-04-13 10:00:00 +0800"
)
./build.sh --image core --arch amd64 "${cmake_args[@]}"
```

### Compile TSDB components (CI / full build)
```bash
# Full core build — clean, all core components
./build.sh --image core --clean core-all

# Full others build — TAOSX Rust binary is produced by the preceding core step;
# ODBC is excluded from CI builds
./build.sh --image others --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF

# Override arch, source, or cache
./build.sh --image core  --arch arm64 --src /data/tsdb --cache /data/cache/tsdb-builder --clean core-all
./build.sh --image others --arch arm64 --src /data/tsdb --clean others-all -DBUILD_TAOSX=OFF -DBUILD_ODBC=OFF
```

### Verify an image
```bash
./verify-image.sh core:amd64                 # latest-amd64 shorthand
./verify-image.sh core:3.4.1-amd64          # versioned shorthand
./verify-image.sh others:arm64
```

## Architecture

### Component → Image Mapping

`build.sh` requires `--image core|dev|others|core:<version>|dev:<version>|others:<version>` to be explicitly specified. The selector resolves to a Harbor single-arch tag based on `--arch`:

- `core` → `harbor.tdengine.net/tsdb-builder/core:latest-<arch>`
- `dev` → `harbor.tdengine.net/tsdb-builder/dev:latest-<arch>`
- `others` → `harbor.tdengine.net/tsdb-builder/others:latest-<arch>`
- `core:<version>` → `harbor.tdengine.net/tsdb-builder/core:<version>-<arch>`
- `dev:<version>` → `harbor.tdengine.net/tsdb-builder/dev:<version>-<arch>`
- `others:<version>` → `harbor.tdengine.net/tsdb-builder/others:<version>-<arch>`

Resolution is local-first by default: if the exact tag exists locally, `build.sh` uses it directly; otherwise it pulls from Harbor. `--pull-image` forces a fresh pull even when the tag already exists locally.

cmake arg priority (last value wins):
1. Component defaults: every component not listed gets `BUILD_*=OFF`
2. Component shortcuts: listed components get `BUILD_*=ON`
3. pthread workaround variables (core/dev image only)
4. `-DKEY=VALUE` CLI passthrough — highest priority, overrides any of the above

When only `-D` flags are provided (no component shortcut names), `build.sh` scans `EXTRA_CMAKE_ARGS` for `BUILD_TAOSX` and `BUILD_EXPLORER_UI` to determine whether to activate the post-build `dist/` cleanup. The externals cache is always mounted regardless of flags; the subdirectory used depends on `--image` (`externals-core-<arch>`, `externals-dev-<arch>`, or `externals-others-<arch>`).

- **core components**: `engine`, `enterprise`, `adapter`, `keeper`, `tools`, `gen`, `taosx`
- **others components**: `explorer-ui`, `insight`, `dotnet`, `go`, `jdbc`, `node`, `python`, `rust`, `odbc`

### `BUILD_CONTRIB`, `BUILD_ROCKSDB`, `ROCKSDB_USE_DEPS` flags

Three cmake flags control external dependency and RocksDB compilation behavior. `build.sh` injects defaults before `EXTRA_CMAKE_ARGS`, so any `-D` passthrough arg takes precedence via cmake's last-value-wins rule.

| Flag | Purpose |
|---|---|
| `BUILD_CONTRIB` | Master switch for external dependencies. `ON` = download & build via ExternalProject; `OFF` = reuse prebuilt artifacts |
| `BUILD_ROCKSDB` | RocksDB compilation switch. `ON` = build RocksDB from source via ExternalProject |
| `ROCKSDB_USE_DEPS` | Whether RocksDB uses prebuilt binaries from `deps/` directory (vs `.externals/` cache) |

**Platform defaults:**

| Platform | `BUILD_CONTRIB` | `BUILD_ROCKSDB` | `ROCKSDB_USE_DEPS` | Behavior |
|---|---|---|---|---|
| Linux | `OFF` | `OFF` | `ON` | Uses prebuilt from `deps/` |
| Non-Linux | `ON` | `ON` | `OFF` | ExternalProject download+compile |

**Full combination matrix:**

| `BUILD_CONTRIB` | `BUILD_ROCKSDB` | `ROCKSDB_USE_DEPS` | Behavior |
|---|---|---|---|
| `ON` | `ON` | ignored | ExternalProject downloads and compiles RocksDB |
| `ON` | `OFF` | `ON` | Other deps compiled, RocksDB from `deps/` |
| `ON` | `OFF` | `OFF` | Other deps compiled, RocksDB from `.externals/` cache |
| `OFF` | `OFF` | `ON` | All from prebuilt, RocksDB from `deps/` |
| `OFF` | `OFF` | `OFF` | All from prebuilt, RocksDB from `.externals/` cache |
| `OFF` | `ON` | `*` | **fatal_error** (cannot compile RocksDB without CONTRIB) |

**Common scenarios:**
```bash
# Build RocksDB from source with GCC 7
./build.sh --image core engine -DBUILD_CONTRIB=ON -DBUILD_ROCKSDB=ON

# Use deps/ prebuilt (Linux default)
./build.sh --image core engine -DBUILD_CONTRIB=OFF

# Use .externals/ cache
./build.sh --image core engine -DBUILD_CONTRIB=OFF -DROCKSDB_USE_DEPS=OFF
```

> Missing prebuilt files now trigger `FATAL_ERROR` immediately instead of the previous silent `"No rule to make target"` failure.

### `taosx` vs `explorer-ui` distinction
- `taosx` → sets `BUILD_TAOSX=ON, BUILD_EXPLORER_UI=OFF` (Rust binary only, uses **core** image)
- `explorer-ui` → sets `BUILD_TAOSX=ON, BUILD_EXPLORER_UI=ON` (Rust binary + pnpm frontend, uses **others** image)

After a `taosx`-only build, `build.sh` removes the placeholder `dist/` directory CMake creates, so a subsequent `explorer-ui` build can run pnpm normally.

### Build argument management
All tool versions and mirror settings are centralized in `.build-args`. All three image build scripts (`build-core-image.sh`, `build-dev-image.sh`, `build-others-image.sh`) read this file and pass every non-comment line as a `--build-arg` flag. Edit `.build-args` to change versions globally — never hardcode versions directly in the Dockerfiles.

### Image publishing
`build-core-image.sh`, `build-dev-image.sh`, and `build-others-image.sh` publish to fixed Harbor repositories:

- `harbor.tdengine.net/tsdb-builder/core`
- `harbor.tdengine.net/tsdb-builder/dev`
- `harbor.tdengine.net/tsdb-builder/others`

`--version` is mandatory. Each run pushes `<version>-<arch>` and `latest-<arch>`. If the sibling architecture tag already exists, the scripts also best-effort update the multi-arch `<version>` and `latest` manifests (amd64/arm64 only; riscv64 is a standalone tag). Harbor login stays manual; on push/pull auth failures, tell users to run `docker login harbor.tdengine.net`.

`--local` skips push, `latest-<arch>` tag, and manifest update — for local testing only.

### Volume mounts during compilation
`build.sh` mounts the TSDB source directory (default: `$(pwd)`) as `/mnt` inside the container, plus these caches:

| Host path (under `TSDB_CACHE_DIR`) | Container path | Scope |
|---|---|---|
| `conan2-<arch>/` | `/root/.conan2` | core + dev + others |
| `externals-core-<arch>/` | `/mnt/.externals` | core image only |
| `externals-dev-<arch>/` | `/mnt/.externals` | dev image only |
| `externals-others-<arch>/` | `/mnt/.externals` | others image only |
| `go-mod/` | `/root/go/pkg/mod` | core + dev + others |
| `cargo-registry/` | `/root/.cargo/registry` | core + dev + others |
| `cargo-git/` | `/root/.cargo/git` | core + dev + others |
| `pnpm-store/` | `/mnt/.pnpm-store` | others only |
| `m2-repository/` | `/root/.m2/repository` | others only |
| `nuget/` | `/root/.nuget/packages` | others only |
| `ccache-core-<arch>/` | `/root/.ccache` | core image only |
| `ccache-dev-<arch>/` | `/root/.ccache` | dev image only |
| `ccache-others-<arch>/` | `/root/.ccache` | others image only |

**Why Cargo uses subdirectories** (`registry/` and `git/`, not all of `~/.cargo`): mounting the full `~/.cargo` would shadow the image-baked `config.toml` (Chinese mirror config) and `bin/` (rustc/cargo binaries).

`TSDB_CACHE_DIR` defaults to `$HOME/cache/tsdb-builder` and is intentionally **outside** the source repo — caches survive `git clean` and re-clones. Override by exporting `TSDB_CACHE_DIR` before running any build script. Subdirectories are created automatically on first run.

### Conan profile initialization
Each `docker run` auto-detects and patches the Conan default profile inside the container before cmake runs:
- If the profile does not yet exist: `conan profile detect --force` is run first.
- `compiler.cppstd=gnu14` → `gnu17` (always patched).
- `arch` is corrected to match `uname -m` inside the container (`aarch64` → `armv8`, `x86_64` → `x86_64`). This prevents Conan from injecting x86-only flags (e.g. `-m64`) when the profile was previously created on a different architecture.

### sqlx offline mode (taosx)
`build.sh` deletes `/mnt/source/taos-xservice/.env` and `/mnt/source/taos-xservice/target/taosx.dev.db` before running cmake, so that `build.rs` recreates them inside the container with the correct `/mnt/...` path. If `SQLX_OFFLINE=true` is not set in the environment, `sqlx::query!` macros will try to open a live SQLite database at compile time and fail. The build scripts do not set this automatically — if you hit `(code: 14) unable to open database file` errors, pass `-e SQLX_OFFLINE=true` to the `docker run` command or export it before invoking the build script.

### `CARGO_NET_GIT_FETCH_WITH_CLI` (others image only)
`build.sh` (when using the others image) passes `-e CARGO_NET_GIT_FETCH_WITH_CLI=true`. This forces Cargo to use system `git` instead of libgit2 for fetching crates, avoiding SSL failures on GitHub-sourced crates in the others image.

### GCC 14 `-Werror=stringop-overflow` (others image only)
The others image uses GCC 14 (AlmaLinux 8), which is stricter than the core image's GCC 7. When building core components (e.g. `BUILD_TOOLS=ON`) inside the others image, GCC 14 may treat a false-positive `stringop-overflow` warning as an error. Add this flag to suppress it:
```bash
./build.sh --image others others-all \
    -DBUILD_CONTRIB=ON \
    -DCMAKE_C_FLAGS="-Wno-error=stringop-overflow"
```

### Make jobserver fallback
`build.sh` runs `make -j$(nproc)` first. If that fails (ExternalProject triggers a jobserver incompatibility on make 3.82), they automatically retry with `make -j1`. This is normal behavior and not a sign of a real failure unless `-j1` also fails.

### Build script logging

`build.sh` mirrors its full stdout+stderr to `<src>/build.log`.

Each script prints a start timestamp at the top and an end timestamp + total elapsed time at the bottom (via an EXIT trap, so timing is always recorded even on failure).

Before launching the container, each script prints the full `docker run` command with every argument on its own line, shell-quoted via `printf '%q'`. The output is valid bash and can be copied from the log and pasted directly into a shell to reproduce the build manually.

### ccache (compilation cache)
All images include ccache. `build.sh` prepends the ccache symlink directory to `PATH` so all gcc/g++ calls go through ccache transparently. Cache is isolated per image+arch (`ccache-{image}-{arch}/`).

| Variable | Default | Purpose |
|---|---|---|
| `CCACHE_MAXSIZE` | `20G` | Max cache size per directory |
| `CCACHE_REMOTE_STORAGE` | unset | Shared cache backend (NFS/HTTP, for CI) |

### External dependency mirror (`BUILD_DEPS_MIRROR_URL`)
Pass `-DBUILD_DEPS_MIRROR_URL=<url>` to cmake (via `build.sh` extra args) to redirect all ExternalProject tarball downloads to an internal mirror. For private GitLab Package Registry, set `DEPS_MIRROR_TOKEN` in `.env` (auto-sourced by `build.sh`); the script creates `/root/.netrc` inside the container and passes `-DCMAKE_NETRC=OPTIONAL`. The token never appears in URLs or build logs.

### Dependency tarball management (`prepare-externals.sh`)
`scripts/prepare-externals.sh` downloads all 22 ExternalProject dependency tarballs, computes SHA256 hashes, and optionally uploads them to GitLab Generic Package Registry (`--upload`). Requires `GITLAB_PROJECT_ID` env var; upload also requires `GITLAB_TOKEN` with `write_package_registry` scope.

### Debug info separation (`--split-debug`)
`build.sh --split-debug` separates DWARF debug info from binaries and shared libraries after compilation completes (inside the container). The workflow for each file: `objcopy --only-keep-debug` → `strip` → `objcopy --add-gnu-debuglink`.

**Strip strategy:**
- Executables (`build/bin/`): `strip -s` (remove all symbols — maximum size reduction)
- Shared libraries (`build/lib/`): `strip --strip-debug` (keep dynamic symbols required for linking)

**Processed files:**
- bin: `taosd`, `taos`, `taosql`, `taosmqtt`, `taosudf`, `taosgen`, `taosadapter`, `taoskeeper`
- lib: `libtaos.so`, `libtaosnative.so`

**Excluded:** `taosx`, `taos-explorer` (Rust — release profile has no debug info by default)

**Output layout:**
```
build/bin/
├── taosd              (strip -s)
├── .debug/
│   └── taosd.debug    (full DWARF debug info)
build/lib/
├── libtaos.so         (strip --strip-debug)
├── .debug/
│   └── libtaos.so.debug
```

GDB loads debug info automatically when `.debug/<name>.debug` is in the same directory as the binary, or manually via `gdb -s .debug/taosd.debug ./taosd core.dump`.

### Build type and debug info flags

Two TSDB-specific cmake options control debug info independently of `CMAKE_BUILD_TYPE`:

| Flag | Default | Effect |
|---|---|---|
| `BUILD_RELEASE` | `OFF` | `OFF` → TSDB binaries include `-g3 -gdwarf-2` debug info; `ON` → no debug info (uses `CMAKE_C_FLAGS_REL`) |
| `TD_ALIGN_EXTERNAL` | `ON` | `ON` → ExternalProject deps follow main project's `CMAKE_BUILD_TYPE`; `OFF` → deps always built as Release |

**Key insight:** `CMAKE_BUILD_TYPE=Release` only adds `-O3` — it does NOT remove debug info from TSDB binaries. To remove debug info, either use `BUILD_RELEASE=ON` or `--split-debug`. To prevent dependency libraries from inheriting Debug build type (avoiding large `.externals/` and slow builds), set `TD_ALIGN_EXTERNAL=OFF`.

### pthread cmake workaround (core/dev image only)
`manylinux2014`'s `FindThreads` tries `-lpthreads` (which doesn't exist). `build.sh` (when using the core or dev image) passes explicit pthread cmake variables to work around this.

### Chinese mirror acceleration
The images are configured to use Chinese mirrors for faster builds in China:
- Go: `GOPROXY=https://goproxy.cn`
- PyPI: `http://mirrors.aliyun.com/pypi/simple/`
- Rust: `rsproxy.cn` (configured in `.cargo/config.toml`, baked into image)

### GCC 7 in core image (Kylin V10 compatibility)
The core image downgrades from devtoolset-10 (GCC 10.2, pre-installed in manylinux2014) to devtoolset-7 (GCC 7.3) for Kylin V10 runtime compatibility. The riscv64 core image uses Debian trixie's system GCC (14.x) instead — Kylin V10 compatibility is not applicable to riscv64. This affects:
- **mold**: requires C++20 (GCC ≥ 10.2), so mold is compiled from source in a separate `mold-builder` stage that still has devtoolset-10
- **mage**: Go runtime crashes under QEMU amd64 emulation, so mage is cross-compiled in the Alpine `builder` stage (native arch)
- **mold as default**: mold is registered as default linker via `update-alternatives` and gcc-toolset alternatives override; GCC 7 does not support `-fuse-ld=mold`, but the alternatives symlink approach works regardless of GCC version

### GCC 9 in dev image (development)
The dev image replaces devtoolset-10 (GCC 10.2) with devtoolset-9 (GCC 9.3.1), providing a higher GCC version than core for daily development without the Kylin V10 compatibility constraint. The same mold and mage workarounds apply as in the core image (mold compiled in `mold-builder` stage with devtoolset-10, mage cross-compiled in Alpine `builder` stage). `build.sh` outputs to `debug-dev/` (vs `debug/` for core) and uses a separate externals cache (`externals-dev-<arch>/`) to prevent cross-image contamination.

### mold source compilation (all images)
All three Dockerfiles compile mold from source in a `mold-builder` multi-stage build instead of downloading release binaries. This is necessary because mold release binaries require higher glibc versions than the base images provide:
- **core/dev**: binary needs glibc ≥ 2.24 (x86_64) / ≥ 2.31 (arm64), image has 2.17
- **others**: binary needs glibc ≥ 2.31 (arm64), image has 2.28

Build flags: `-DMOLD_MOSTLY_STATIC=ON` (static libstdc++), `-DCMAKE_INSTALL_LIBDIR=lib` (consistent install path). The `mold-builder` stage extracts a pre-downloaded mold source tarball from `packages/` (e.g. `mold-2.40.3.tar.gz`) which includes vendored third-party dependencies (zlib, blake3, zstd, mimalloc, tbb). This avoids slow `git clone` during image builds.

### mold as default linker (amd64/arm64 only)
The amd64/arm64 images (core, dev, others) register mold as the default linker via two mechanisms:
1. `update-alternatives --install /usr/bin/ld ld /usr/bin/ld.mold 100` — sets system-level default (priority 100 > bfd 50 > gold 30)
2. `ln -sf /usr/bin/mold /opt/rh/<toolset>/root/etc/alternatives/ld` — overrides the gcc-toolset's independent alternatives directory

Both steps are required because gcc-toolset (devtoolset-7 for core, devtoolset-9 for dev, gcc-toolset-14 for others) maintains its own `alternatives/ld` symlink chain separate from the system one, and toolset paths come first in `PATH`. Without step 2, `ld` still resolves to `ld.bfd` via the toolset's own alternatives.

**Core/dev image special case:** mold is compiled with `-DMOLD_USE_MIMALLOC=OFF` because mimalloc (mold's default allocator) segfaults on glibc 2.17 arm64. System malloc is used instead.

### mold on riscv64: NOT the default linker
The riscv64 core image installs mold but does **not** register it as the default linker. GNU ld remains the system default. This is because mold on riscv64 corrupts the ELF layout of Go CGO binaries — specifically the `pclntab` (PC-line table) section — causing Go components (taosadapter, keeper) to SIGSEGV during runtime initialization.

`build.sh` automatically passes `-DCMAKE_LINKER=mold` when `ARCH=riscv64`, so cmake-managed C/C++ targets (engine, libtaosnative, etc.) still benefit from mold's speed. Go CGO components use `gcc` which invokes the system default `ld` (GNU ld), avoiding the crash.

### `v1/` directory
Legacy single-arch build scripts and Dockerfile kept for reference. Do not use for new work.

### riscv64 core image (Debian trixie)
The riscv64 core image uses a completely separate Dockerfile (`Dockerfile.core-riscv64`) based on `debian:trixie` because:
- manylinux2014 (CentOS 7) has no riscv64 support
- CentOS 7 devtoolset and yum ecosystem do not exist for riscv64
- cmake, protoc, mold, and tini have no official riscv64 binary releases — all are installed from Debian apt repos

Key differences from the amd64/arm64 core image:
- **glibc**: 2.41+ (vs 2.17) — binaries require a modern riscv64 distribution
- **GCC**: 14.x from Debian (vs 7.3 devtoolset-7) — no Kylin V10 compat constraint
- **cmake/protoc/mold/tini**: system packages from apt (versions may differ from `.build-args`)
- **mold NOT default linker**: GNU ld remains default; mold corrupts Go CGO ELF layout on riscv64 (SIGSEGV in pclntab). `build.sh` uses `-DCMAKE_LINKER=mold` for C/C++ targets only
- **No buildx required**: uses plain `docker build` with a temporary build context
- **Standalone tag**: riscv64 images are not included in the amd64/arm64 multi-arch manifest
- **Only Go needed from packages/**: all other tools installed via apt or rustup

## Requirements

- Docker >= 20.10 with `docker buildx` support (not required for riscv64)
- Offline install packages in `/data/packages/` before building images (see README for full list)
- Others connectors may need core client headers — run core build first if connector builds fail with missing deps
