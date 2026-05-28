---
name: "tsdb-build-cmake-invariants"
description: "TSDB 构建系统不变量规则集。当用户编辑 CMakeLists.txt、external.cmake、conan.cmake、options.cmake 或 ExternalProject 相关代码时触发。涵盖 ExternalProject lib/ 路径、RocksDB 缓存验证、构建选项守卫、GTest 兼容、GCC 7 兼容、LOCAL_URL 桥接、GNU Make 4.2.1 升级（修复 ext_curl 并发 bug）等 14 条经验教训规则。"
metadata:
  author: Bo Xiao
  version: 1.1.0
  owner_team: engine
  compatibility: "Applies to TSDB main and 3.3.6 branches"
---

# TSDB Build System Invariants

> **Purpose**: This document records hard-won build system invariants derived from
> production incidents and fixes. Every rule here exists because violating it
> caused a real build failure. **Do NOT remove or weaken any rule without
> verifying the original fix commit.**
>
> **Reference**: 构建环境详情参见 `tsdb-builder/README.md`
> （仓库 `platform/platform`，路径 `tsdb-builder/`）。

## When to use

- Editing `CMakeLists.txt`, `external.cmake`, `conan.cmake`, `options.cmake`
- Adding or modifying `ExternalProject_Add()` calls
- Changing RocksDB build options or linking logic
- Working with GTest setup on aarch64/lib64 platforms
- Writing C++ code in `source/taos-gen/` or `source/taos-community/` that must compile under GCC 7
- Modifying Conan dependency configuration
- Troubleshooting CMake configure-time or link-time failures
- Reviewing PRs that touch the build system

## Trigger keywords

CMakeLists, cmake, ExternalProject, external.cmake, conan.cmake, options.cmake,
BUILD_CONTRIB, BUILD_ROCKSDB, ROCKSDB_USE_DEPS, INIT_EXT, DEP_td_rocksdb,
CMAKE_INSTALL_LIBDIR, lib64, GCC 7, devtoolset-7, manylinux2014, ext_gtest,
ext_rocksdb, ext_curl, LOCAL_URL, BUILD_DEPS_MIRROR_URL, TD_CONFIG_NAME_RESOLVED,
GNU Make, make 3.82, make 4.2.1, parallel build bug, configure error,
cannot compute suffix of executables, diamond dependency, BUILD_IN_SOURCE,
构建失败, 链接错误, link error, build failure, 编译兼容, ext_curl 失败

---

## 0. Build Environment Overview

### Three Docker images

| 镜像 | Registry | 基础 | GCC | Make | glibc | 组件 |
|------|----------|------|-----|------|-------|------|
| **core** | `harbor.tdengine.net/tsdb-builder/core` | manylinux2014 | 7.3 (devtoolset-7) | **4.2.1** | 2.17 | ENGINE, ENTERPRISE, ADAPTER, KEEPER, TOOLS, GEN, TAOSX |
| **dev** | `harbor.tdengine.net/tsdb-builder/dev` | manylinux2014 | 9.3.1 (devtoolset-9) | **4.2.1** | 2.17 | 同 core（日常开发用，不需兼容麒麟 V10） |
| **others** | `harbor.tdengine.net/tsdb-builder/others` | manylinux_2_28 | 14.x | 4.2.1 | 2.28 | INSIGHT, EXPLORER_UI + 全部 connector |

所有镜像支持 `linux/amd64` + `linux/arm64`。core 额外支持 `linux/riscv64`（基于 Debian trixie, glibc 2.41+）。

> **Make 4.2.1 升级** (2026-05): core 和 dev 镜像从 Make 3.82 升级到 4.2.1，
> 修复了 GNU Make PR #12610 并行调度 bug，彻底解决了 ext_curl 间歇性配置失败问题。
> others 镜像基于 AlmaLinux 8，系统自带 Make 4.2.1，无此问题。

### Build output directories

| 镜像 | 构建输出目录 |
|------|-------------|
| core | `<src>/debug/` |
| dev | `<src>/debug-dev/` |
| others | `<src>/debug-others/` |

### Cache directory layout (仓库外，`$HOME/cache/tsdb-builder/`)

| 子目录 | 内容 | 镜像 |
|--------|------|------|
| `conan2-{arch}/` | C/C++ Conan 依赖 | core + dev + others |
| `externals-core-{arch}/` | CMake ExternalProject | core |
| `externals-dev-{arch}/` | CMake ExternalProject | dev |
| `externals-others-{arch}/` | CMake ExternalProject | others |
| `go-mod/` | Go 模块 | 全部 |
| `cargo-registry/` / `cargo-git/` | Rust 缓存 | 全部 |
| `pnpm-store/` | Node.js pnpm | others |
| `m2-repository/` | Maven | others |

> **ExternalProject 缓存按镜像类型隔离**——core/dev/others 的 GCC 版本不同，
> 产物 ABI 不兼容，不可混用。

### First build: `BUILD_CONTRIB=ON` required

首次编译（`.externals/` 不存在）**必须**加 `-DBUILD_CONTRIB=ON`，否则构建
因找不到 xxhash、zstd 等外部依赖头文件而失败。后续增量编译可省略（默认 `OFF`
直接复用缓存）。

### pthread cmake fix (manylinux2014)

manylinux2014 的 `FindThreads` 模块会尝试 `-lpthreads`（不存在），需显式传入
五个 cmake 变量。**`build.sh` 使用 core 或 dev 镜像时已自动处理**，手动调用
cmake 时需自行添加。

### Conan profile auto-correction

`build.sh` 每次启动容器后自动修正 Conan profile：
- `compiler.cppstd=gnu14` → `gnu17`
- `arch` 修正为容器实际架构（`aarch64` → `armv8`）

跨机器迁移或初次检测错误时，删除 `$TSDB_CACHE_DIR/conan2-{arch}/` 重建。

### `.build-args` dual-purpose design

`.build-args` serves two consumers simultaneously:
1. **Image build scripts** (`build-*-image.sh`): pass ALL lines as `--build-arg` to Docker
2. **`build.sh` runtime**: reads specific variables for container compilation config

**Invariant**: Variables that Dockerfiles consume as `ARG` (e.g. `PYPI_MIRROR`) **MUST**
use publicly reachable URLs — image builds may run outside the internal network.
Internal-only URLs (e.g. `PYPI_INTERNAL_URL`, `NPM_REGISTRY_URL`, `MAVEN_MIRROR_URL`,
`NUGET_SOURCE_URL`) are consumed only by `build.sh` at runtime and injected into the
container after startup.

### sccache on riscv64

sccache does **not** publish prebuilt binaries for riscv64. `Dockerfile.core-riscv64`
skips the sccache install step. `build.sh` runtime fallback detects sccache absence and
automatically disables `RUSTC_WRAPPER` — no manual intervention needed.

### GCC 14 stringop-overflow (others 镜像)

others 镜像中编译含 core 组件时，GCC 14 可能将 `stringop-overflow` 误报升级为
编译错误。需追加：

```bash
-DCMAKE_C_FLAGS="-Wno-error=stringop-overflow"
```

---

## 1. ExternalProject Install Path: Always Use Hardcoded `lib/`

| Applies to | main ✅ | 3.3.6 ✅ |
|------------|---------|----------|

### Rule

All `ExternalProject_Add()` calls that install libraries **MUST** pass
`-DCMAKE_INSTALL_LIBDIR:PATH=lib` (or the equivalent `STRING` variant) to
force installation into `lib/` instead of the platform default.

The corresponding `INIT_EXT()` declarations **MUST** reference `lib/<name>`
(never `${CMAKE_INSTALL_LIBDIR}/<name>`).

Any path string that references an ExternalProject install artifact
(validation checks, link paths, etc.) **MUST** use the literal `lib/`,
never `${CMAKE_INSTALL_LIBDIR}`.

### Why

On 64-bit Linux (CentOS 7 / manylinux2014 / aarch64), CMake's
`GNUInstallDirs` module sets `CMAKE_INSTALL_LIBDIR` to `lib64`. If an
`ExternalProject_Add` does not override this, the library is installed into
`lib64/` but `INIT_EXT` expects it in `lib/`, causing a link-time or
cache-validation failure.

### Affected Targets (non-exhaustive)

`ext_rocksdb`, `ext_gtest`, `ext_pcre2`, `ext_jansson`, `ext_lz4`,
`ext_zlib`, `ext_zstd`, `ext_cjson`, `ext_libuv`, `ext_nghttp2`

### Anti-patterns — DO NOT

```cmake
# ❌ WRONG: Uses system variable — may resolve to lib64
set(_check "${_ins}/${CMAKE_INSTALL_LIBDIR}/${ext_rocksdb_static}")

# ❌ WRONG: Missing LIBDIR override — installs to lib64 on aarch64
ExternalProject_Add(ext_foo
    ...
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    # no CMAKE_INSTALL_LIBDIR here!
)

# ❌ WRONG: INIT_EXT referencing system variable
INIT_EXT(ext_foo
    LIB ${CMAKE_INSTALL_LIBDIR}/${ext_foo_static}
)
```

### Correct patterns

```cmake
# ✅ CORRECT: Hardcoded lib/ in validation path
set(_check "${_ins}/lib/${ext_rocksdb_static}")

# ✅ CORRECT: Explicit LIBDIR override
ExternalProject_Add(ext_foo
    ...
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
)

# ✅ CORRECT: INIT_EXT with hardcoded lib/
INIT_EXT(ext_foo
    LIB lib/${ext_foo_static}
)
```

### Reference Commits

- `96dd3146da6` — initial fix for ext_gtest on aarch64
- `e8e7184c634` — force `CMAKE_INSTALL_LIBDIR=lib` for RocksDB ExternalProject
- `d77cd0e017e` — use explicit `lib/` in INIT_EXT LIB for RocksDB (was `${CMAKE_INSTALL_LIBDIR}/`)
- `868c35793cc` — fix cache validation path (replaced `${CMAKE_INSTALL_LIBDIR}` with `lib`)

---

## 2. RocksDB Cache Validation Must Use Resolved Config Name

| Applies to | main ✅ | 3.3.6 ✅ |
|------------|---------|----------|

### Rule

When `BUILD_CONTRIB=OFF` (cache-hit path), the RocksDB cache validation
check **MUST** use `${TD_CONFIG_NAME_RESOLVED}` (a plain string like
`Release`), **NOT** `${TD_CONFIG_NAME}` which may still contain CMake
generator expressions like `$<CONFIG>`.

`TD_CONFIG_NAME_RESOLVED` is defined at the top of `external.cmake`:

```cmake
if(CMAKE_BUILD_TYPE STREQUAL "")
    set(TD_CONFIG_NAME_RESOLVED "Debug")
else()
    set(TD_CONFIG_NAME_RESOLVED "${CMAKE_BUILD_TYPE}")
endif()
```

The validation path **MUST** be:

```cmake
set(_rocksdb_check_path
    "${TD_EXTERNALS_BASE_DIR}/install/ext_rocksdb/${TD_CONFIG_NAME_RESOLVED}/lib/${ext_rocksdb_static}")
```

### Why

Generator expressions (e.g. `$<CONFIG>`) are only expanded at build time,
not at configure time. Using them in `if(NOT EXISTS ...)` at configure time
causes a false-negative: the path contains the literal string `$<CONFIG>`
which never exists on disk, so CMake always reports a spurious FATAL_ERROR.

### Anti-patterns — DO NOT

```cmake
# ❌ WRONG: Generator expression not expanded at configure time
set(_check "${TD_EXTERNALS_BASE_DIR}/install/ext_rocksdb/${TD_CONFIG_NAME}/lib/...")

# ❌ WRONG: Extracting path from ext_rocksdb_libs (contains genex)
list(GET ext_rocksdb_libs 0 _rocksdb_cached_lib)
if(NOT EXISTS "${_rocksdb_cached_lib}")  # genex not resolved!

# ❌ WRONG: System LIBDIR variable in validation path
set(_check ".../${TD_CONFIG_NAME_RESOLVED}/${CMAKE_INSTALL_LIBDIR}/...")
```

### Reference Commits

- `8893247f361` — introduced `TD_CONFIG_NAME_RESOLVED`; fixed false failure from unparsed genex
- `868c35793cc` — replaced `${CMAKE_INSTALL_LIBDIR}` with `lib` in validation path
- `159bedfe32e` — earlier iteration (feature branch)

---

## 3. RocksDB Build Option Guards and Invalid Combination Detection

| Applies to | main ✅ | 3.3.6 ✅ |
|------------|---------|----------|

### Rule

RocksDB build/lookup is controlled by three interdependent options. The
cmake logic **MUST** use these two-level guards:

```
TD_ROCKSDB_USE_EXTERNAL  (outer guard — whether to use ExternalProject at all)
  └─ TD_ROCKSDB_BUILD_FROM_SOURCE  (inner guard — build vs. cache reuse)
```

The option derivation logic (in `options.cmake`):

| `BUILD_CONTRIB` | `BUILD_ROCKSDB` | → `TD_ROCKSDB_BUILD_FROM_SOURCE` | → `TD_ROCKSDB_USE_EXTERNAL` |
|:---:|:---:|:---:|:---:|
| ON | ON | ON | ON |
| ON | OFF | OFF | ON |
| OFF | — | OFF | depends on `ROCKSDB_USE_DEPS` |

**Invalid combinations MUST fail at configure time with clear error messages:**

```cmake
# BUILD_ROCKSDB=ON without BUILD_CONTRIB=ON → FATAL_ERROR
if(BUILD_ROCKSDB AND NOT BUILD_CONTRIB)
    message(FATAL_ERROR "[rocksdb] BUILD_ROCKSDB=ON requires BUILD_CONTRIB=ON.")
endif()

# ROCKSDB_USE_DEPS=ON but deps dir missing → FATAL_ERROR
if(ROCKSDB_USE_DEPS AND NOT EXISTS "${TD_ROCKSDB_DEPS_DIR}")
    message(FATAL_ERROR "[rocksdb] Prebuilt deps not found at: ${TD_ROCKSDB_DEPS_DIR}")
endif()
```

### Complete option combination matrix

| `BUILD_CONTRIB` | `BUILD_ROCKSDB` | `ROCKSDB_USE_DEPS` | 最终行为 |
|---|---|---|---|
| `ON` | `ON` | 忽略 | ExternalProject 下载+编译 RocksDB |
| `ON` | `OFF` | `ON` | 其他组件编译，RocksDB 从 `deps/` 取 |
| `ON` | `OFF` | `OFF` | 其他组件编译，RocksDB 从 `.externals/` 缓存取 |
| `OFF` | `OFF` | `ON` | 全部从预构建取，RocksDB 从 `deps/` 取（**Linux 默认**）|
| `OFF` | `OFF` | `OFF` | 全部从预构建取，RocksDB 从 `.externals/` 缓存取 |
| `OFF` | `ON` | `*` | **FATAL_ERROR**（不允许 CONTRIB=OFF 时编译 RocksDB）|

### Platform defaults

| 平台 | `BUILD_CONTRIB` | `BUILD_ROCKSDB` | `ROCKSDB_USE_DEPS` |
|---|---|---|---|
| Linux | `OFF` | `OFF` | `ON` |
| 非 Linux | `ON` | `ON` | `OFF` |

### Why

- Collapsing into a single flag caused cache-reuse path to skip validation
  or trigger unwanted full builds.
- Silent `message(WARNING ...)` when deps were missing led to confusing
  downstream link errors instead of clear configure-time failures.
- `BUILD_ROCKSDB=ON` without `BUILD_CONTRIB=ON` is invalid and **MUST**
  produce a `FATAL_ERROR` at configure time.

### Reference Commits

- `57902803488` — refactor: clarify RocksDB build/lookup options
- `cfbc0f3d3bb` — GCC 7 兼容性修复及 RocksDB cmake 选项重构

---

## 4. RocksDB Linking: Use Full Library Path, Not target_link_directories

| Applies to | main ✅ | 3.3.6 ✅ |
|------------|---------|----------|

### Rule

When linking prebuilt RocksDB from `deps/`, the `DEP_td_rocksdb` macro
**MUST** use the full path to `librocksdb.a`, **NOT**
`target_link_directories` + `target_link_libraries(... rocksdb)`.

### Anti-patterns — DO NOT

```cmake
# ❌ WRONG: Relies on link directory search — fragile, can pick up wrong lib
target_link_directories(${tgt} PUBLIC "${TD_ROCKSDB_DEPS_DIR}")
target_link_libraries(${tgt} PRIVATE rocksdb)
```

### Correct pattern

```cmake
# ✅ CORRECT: Full path — unambiguous
target_link_libraries(${tgt} PRIVATE "${TD_ROCKSDB_DEPS_DIR}/librocksdb.a")
```

### Why

`target_link_directories` pollutes the link search path and can cause
CMake to pick up a system-installed `librocksdb.so` instead of the intended
static library from `deps/`.

### Reference Commits

- `8893247f361` — changed from `target_link_directories` + short name to full path

---

## 5. GTest lib64 → lib Compatibility (aarch64 / 64-bit Linux)

| Applies to | main ✅ (via `-DCMAKE_INSTALL_LIBDIR:PATH=lib`) | 3.3.6 ✅ (via lib64→lib symlink) |
|------------|---------|----------|

### Rule

`ext_gtest` libraries **MUST** be accessible at the `lib/` path that
`INIT_EXT` declares, even on platforms where GTest installs to `lib64/`.

### Branch-specific implementations

**main branch** — solved by passing `-DCMAKE_INSTALL_LIBDIR:PATH=lib` in
the `ExternalProject_Add` call, so GTest installs directly into `lib/`.

**3.3.6 branch** — solved by creating symlinks from `lib64/` → `lib/`
in the `INSTALL_COMMAND`:

```cmake
INSTALL_COMMAND
    COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${_ins}/lib"
    COMMAND "${CMAKE_COMMAND}" -E create_symlink "${_ins}/lib64/${ext_gtest_static}" "${_ins}/lib/${ext_gtest_static}" || true
    COMMAND "${CMAKE_COMMAND}" -E create_symlink "${_ins}/lib64/${ext_gtest_main}" "${_ins}/lib/${ext_gtest_main}" || true
```

> **Note**: When modifying `ext_gtest` on either branch, do NOT remove the
> mechanism that ensures `lib/` accessibility. If migrating 3.3.6 to the
> main-branch approach (LIBDIR override), the symlink fallback may be removed,
> but not the other way around.

### Reference Commits

- `96dd3146da6` — add `CMAKE_INSTALL_LIBDIR` for ext_gtest (main)
- `dd12723f900` — resolve `BUILD_TEST=ON` compilation errors, added symlinks (3.3.6)

---

## 6. GCC 7 (devtoolset-7 / manylinux2014) C++ Compatibility

| Applies to | main ✅ | 3.3.6 — N/A (taos-gen not present) |
|------------|---------|----------|

### Rule

Code in `source/taos-gen/` and `source/taos-community/` **MUST** compile
under GCC 7 (devtoolset-7, used by the **core** Docker image
`harbor.tdengine.net/tsdb-builder/core`). The core image is the production
build environment and targets 麒麟 V10 compatibility. The **dev** image
(GCC 9.3.1) relaxes some constraints but taos-gen must still avoid
C++17-only features absent in GCC 7.

### 6a. Filesystem compatibility layer

All filesystem operations **MUST** go through `FilesystemCompat.hpp`:

```cpp
// source/taos-gen/src/utils/inc/FilesystemCompat.hpp
#if __has_include(<filesystem>)
  #include <filesystem>
  namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #error "No <filesystem> or <experimental/filesystem> support"
#endif
```

- **MUST** `#include "FilesystemCompat.hpp"` — never `#include <filesystem>` directly
- **MUST** use `fs::` namespace — never `std::filesystem::`
- **MUST** use free function `fs::is_regular_file(entry.path())` — never member `entry.is_regular_file()`
- **MUST** link `-lstdc++fs` when GCC < 9.0 via generator expression in CMakeLists.txt

### 6b. Unavailable C++17 features

| Feature | Replacement | Files affected |
|---------|-------------|----------------|
| `std::filesystem` | `FilesystemCompat.hpp` + `fs::` | 15+ files across taos-gen |
| `std::aligned_alloc` | `posix_memalign(&ptr, alignment, size)` | `MemoryPool.cpp` |
| `<charconv>` / `std::from_chars` | `std::strtoull` / `std::strtoll` / `std::stof` / `std::stod` | `TypeConverter.cpp` |
| Structured bindings `auto& [_, val]` | `auto& kv` + `kv.second` | `ExpressionEngine.cpp`, `StmtV2Data.cpp`, `PluginConfigRegistry.cpp` |
| `std::get` with structured bindings | `auto result = ...; auto& val = std::get<1>(result);` | `TestCSVDataManager.cpp` |

### 6c. Conan / build system constraints

- `conanfile.txt`: cyrus-sasl's `saslauthd` **MUST** be disabled
  (`cyrus-sasl/*:with_saslauthd=False`) — `saslauthd` requires `libcrypt`
  which is absent in the manylinux2014 image.
- Conan profile **MUST** pin `compiler.version` to the actual GCC major
  version of the build environment (e.g. `-s "compiler.version=7"`).
  Cross-image ABI mismatches cause link failures.
- Conan output directory **MUST** be cleaned when switching between
  build types (Release ↔ Debug) to avoid stale artifacts.

### Reference Commits

- `9f97f93ec54` — GCC 7 编译兼容性修复（FilesystemCompat.hpp, charconv, aligned_alloc, structured bindings）
- `cfbc0f3d3bb` — GCC 7 构建兼容性修复及 RocksDB cmake 选项重构（structured bindings → kv.second）
- `81871224d59` — disable cyrus-sasl saslauthd
- `a5f6b796cfc` — `fs::is_regular_file(entry.path())` for experimental::filesystem
- `ac765f084e1` — replace `std::aligned_alloc` with `posix_memalign`
- `e516a0a265d` — replace charconv and fix structured bindings
- `9f622d73f40` — add FilesystemCompat.hpp and `-lstdc++fs` link flag
- `9e277bf0477` — pin conan compiler version, clean between build types

---

## 7. BUILD_TEST=ON Must Not Break the Default Build

| Applies to | main ✅ | 3.3.6 ✅ |
|------------|---------|----------|

### Rule

Enabling `-DBUILD_TEST=ON` **MUST NOT** introduce compilation or link
errors. Test infrastructure (GTest, grant test, etc.) must be properly
guarded and linked.

Specifically:
- `source/taos-internal/source/plugins/grant/test/` must compile when
  `BUILD_TEST=ON`.
- GTest must be findable at the `lib/` path (see Rule 5).
- `libgrant_x64.a` must be auto-generated from the object file at configure
  time if it doesn't exist:

```cmake
IF(NOT EXISTS ${TAOS_GRANT_STUB})
    IF(EXISTS ${GRANT_OBJ_FILE})
        EXECUTE_PROCESS(
            COMMAND ${CMAKE_AR} rcs ${TAOS_GRANT_STUB} ${GRANT_OBJ_FILE}
        )
    ENDIF()
ENDIF()
```

### Reference Commits

- `dd12723f900` — fix BUILD_TEST=ON compilation errors (main): symlinks + grant auto-gen
- `d44a9fb81bc` — fix BUILD_TEST=ON compilation errors (3.3.6)

---

## 8. Enterprise Edition Path Convention: `source/` Not `src/`

| Applies to | main ✅ | 3.3.6 — verify if applicable |
|------------|---------|----------|

### Rule

All CMakeLists.txt references to internal/enterprise directories **MUST**
use `source/` as the subdirectory name, **NOT** the legacy `src/`.

This applies to:
- `source/taos-internal/` subtree (plugins, kit, connector, tests)
- Cross-references from `source/taos-community/` into enterprise paths
- The `TD_ENTERPRISE_DIR` variable (not `CMAKE_SOURCE_DIR/enterprise`)

### Anti-patterns — DO NOT

```cmake
# ❌ WRONG: Legacy src/ path
set(TD_ENTERPRISE_DIR "${CMAKE_SOURCE_DIR}/enterprise")
add_subdirectory(src/plugins/grant)
target_include_directories(... "${TD_ENTERPRISE_DIR}/src/plugins/...")

# ❌ WRONG: Hardcoded contrib path for grant lib
set(TAOS_GRANT_LIB "${CMAKE_SOURCE_DIR}/contrib/grant-lib/...")
```

### Correct patterns

```cmake
# ✅ CORRECT: source/ path, TD_ENTERPRISE_DIR variable
add_subdirectory(source/plugins/grant)
target_include_directories(... "${TD_ENTERPRISE_DIR}/source/plugins/...")

# ✅ CORRECT: TD_GRANT_LIB_DIR variable
set(TD_GRANT_LIB_DIR "${TD_ENTERPRISE_DIR}/source/plugins/grant/lib")
```

### Reference Commits

- `14a9ede8aeb` — fixed 12+ path corrections from `src/` → `source/` across 29 files

---

## 9. CMake Define Correctness

| Applies to | main ✅ | 3.3.6 — verify if applicable |
|------------|---------|----------|

### Rule

All `add_definitions()` and `option()` names **MUST** be spelled correctly
and defined before use.

### Known fixes

| Wrong | Correct | File |
|-------|---------|------|
| `USE_PRCE2` | `USE_PCRE2` | `source/taos-community/cmake/define.cmake` |

Additional requirements:
- `BUILD_S3` option **MUST** be declared in `options.cmake` before use
- `BUILD_LIBSASL` **MUST** be propagated via `add_definitions(-DBUILD_LIBSASL)`
  in `define.cmake` when enabled
- `#include <stdint.h>` **MUST** be present in
  `source/taos-community/include/util/tgeosctx.h` (missing caused compile
  error on some platforms)

### Reference Commits

- `14a9ede8aeb` — fixed `USE_PRCE2` → `USE_PCRE2`, added `BUILD_S3`,
  added `BUILD_LIBSASL` definition, added `stdint.h` include

---

## 10. Conan Macros Must Not Override ExternalProject Definitions

| Applies to | main ✅ | 3.3.6 — verify if applicable |
|------------|---------|----------|

### Rule

When a dependency is managed by `INIT_EXT` / `ExternalProject_Add` in
`external.cmake`, the corresponding `DEP_ext_*` macros in `conan.cmake`
**MUST NOT** provide empty or conflicting overrides.

Empty macros in `conan.cmake` silently shadow the real definitions from
`external.cmake`, causing link failures with missing symbols.

### Anti-patterns — DO NOT

```cmake
# ❌ WRONG in conan.cmake: empty macro shadows external.cmake definition
macro(DEP_ext_pcre2 tgt)
    # Not migrated yet
endmacro()
```

### Correct pattern

```cmake
# ✅ CORRECT: Comment out or remove; let external.cmake's INIT_EXT provide it
# NOTE: pcre2 macros are defined by INIT_EXT in external.cmake
```

### Affected macros

`DEP_ext_pcre2`, `DEP_ext_libs3`, `DEP_ext_azure`, `DEP_ext_cos`

### Reference Commits

- `14a9ede8aeb` — commented out empty conan.cmake macros for pcre2, libs3, azure, cos

---

## 11. Build Option Defaults for Optional Components

| Applies to | main ✅ | 3.3.6 — verify if applicable |
|------------|---------|----------|

### Rule

Components with special build-time dependencies that are **NOT guaranteed
to be present** in all CI/CD environments **MUST** default to `OFF`.

| Component | Default | Reason |
|-----------|---------|--------|
| `BUILD_INSIGHT` | OFF | Requires Node.js 22+ |
| `BUILD_ODBC` | OFF | Requires flex |
| `BUILD_DOTNET` | OFF | Requires .NET SDK |

### Reference Commits

- `14a9ede8aeb` — changed defaults from ON → OFF for insight, odbc, dotnet

---

## 12. `DEP_td_rocksdb` Must Be Uniform Across All Targets

| Applies to | main ✅ | 3.3.6 ✅ |
|------------|---------|----------|

### Rule

All targets that depend on RocksDB **MUST** use the `DEP_td_rocksdb()`
macro. Do NOT have separate linking logic for different `BUILD_CONTRIB`
settings within individual `CMakeLists.txt` files.

### Anti-patterns — DO NOT

```cmake
# ❌ WRONG: Conditional linking in consumer CMakeLists.txt
if(BUILD_CONTRIB)
    DEP_td_rocksdb(new-stream)
else()
    target_link_libraries(new-stream PRIVATE rocksdb)
endif()
```

### Correct pattern

```cmake
# ✅ CORRECT: Always use the macro — it handles all paths internally
DEP_td_rocksdb(new-stream)
```

### Why

The `DEP_td_rocksdb` macro already contains the three-way branching
(ExternalProject / deps / cached). Duplicating this logic in consumers
creates maintenance burden and inconsistencies.

### Reference Commits

- `8893247f361` — unified `new-stream/CMakeLists.txt` to always use `DEP_td_rocksdb()`

---

## 13. ExternalProject Mirror Bridge: LOCAL_URL CACHE Variable

| Applies to | main ✅ | 3.3.6 — verify if applicable |
|------------|---------|----------|

### Rule

The `BUILD_DEPS_MIRROR_URL` → `LOCAL_URL` bridge in `external.cmake` **MUST**
check for empty string, **NOT** use `DEFINED`:

```cmake
# ✅ CORRECT: Check empty string, FORCE overwrite CACHE
if(DEFINED BUILD_DEPS_MIRROR_URL AND "${LOCAL_URL}" STREQUAL "")
  set(LOCAL_URL "${BUILD_DEPS_MIRROR_URL}" CACHE STRING "local archives storage to use" FORCE)
endif()
```

### Why

`set(LOCAL_URL "" CACHE STRING ...)` at the top of `external.cmake` makes
`LOCAL_URL` "defined" (even with empty value). The condition
`NOT DEFINED LOCAL_URL` is always FALSE, so `BUILD_DEPS_MIRROR_URL` passed
from `build.sh` via `-D` never propagates to `LOCAL_URL`. This causes all
`get_from_local_if_exists()` calls to fall back to the original GitHub URLs.

Additionally, plain `set(LOCAL_URL ...)` (without `CACHE ... FORCE`) cannot
overwrite a CACHE variable — CMake silently ignores the assignment.

### Anti-patterns — DO NOT

```cmake
# ❌ WRONG: LOCAL_URL is already DEFINED by CACHE declaration (empty but defined)
if(DEFINED BUILD_DEPS_MIRROR_URL AND NOT DEFINED LOCAL_URL)
  set(LOCAL_URL "${BUILD_DEPS_MIRROR_URL}")
endif()

# ❌ WRONG: Plain set cannot overwrite CACHE variable
if(DEFINED BUILD_DEPS_MIRROR_URL AND "${LOCAL_URL}" STREQUAL "")
  set(LOCAL_URL "${BUILD_DEPS_MIRROR_URL}")  # silently ignored!
endif()
```

### Reference Commits

- `156ec64d507` — fix LOCAL_URL bridge: check empty string + FORCE overwrite CACHE

---

## 14. GNU Make 4.2.1 Upgrade (ext_curl Parallel Build Fix)

**Problem**: Intermittent `ext_curl` configure failures (~20-30% failure rate):

```
configure: error: in `/mnt/.externals/build/Release/ext_curl/src/ext_curl':
configure: error: cannot compute suffix of executables: cannot compile and link
```

**Root Cause**: GNU Make 3.82 (CentOS 7 default) has a known parallel scheduling bug ([PR #12610](https://savannah.gnu.org/support/?109593)):

1. **Bug**: PHONY targets in diamond dependency graphs are scheduled multiple times
2. **ext_curl pattern**: CMake ExternalProject creates a diamond:
   ```
   ext_curl → ext_curl-configure (direct)
   ext_curl → ext_curl-complete → ext_curl-configure (indirect)
   ```
3. **BUILD_IN_SOURCE=TRUE**: Two concurrent `./configure` run in the same directory
4. **Conflict**: One process deletes `conftest.c` that the other is compiling

**Evidence**: `config.log` shows non-monotonic line numbers (27079 → 4405 → 27245), proving concurrent execution. In single-threaded autoconf, AC_PROG_CC always runs before SSL checks.

**Solution**: Upgrade to Make 4.2.1 (fixes PR #12610, released 2016)

**Implementation** (2026-05):
- dev/core: Compile Make 4.2.1 from source in `make-builder` stage
- Fully compatible with glibc 2.17 (manylinux2014)
- others: AlmaLinux 8 already has Make 4.2.1, no upgrade needed

**Verification**:
```bash
docker run --rm harbor.tdengine.net/tsdb-builder/dev:latest make --version
# Expected: GNU Make 4.2.1
```

**Workaround** (if using old images):
```bash
# Clear corrupted ext_curl cache
rm -rf $CACHE_DIR/externals-{dev,core}-*/build/Release/ext_curl
rm -rf $CACHE_DIR/externals-{dev,core}-*/src/ext_curl
```

**DO NOT**:
- ❌ Downgrade to Make 3.81 or earlier (missing features)
- ❌ Use `-j1` as permanent solution (slow, doesn't fix root cause)
- ❌ Patch ext_curl to use BUILD_IN_SOURCE=FALSE (breaks other logic)

**MUST**:
- ✅ Use latest dev/core images (with Make 4.2.1)
- ✅ Verify Make version in Dockerfile if building custom images
- ✅ Clear cache when switching between old/new images

**Impact**: After upgrade, ext_curl build success rate improved from ~70-80% to 100%.

**Reference**: `tools/tsdb-builder/` (platform/platform repo) contains Dockerfile upgrades and documentation.

---

## Quick Reference: Branch Comparison

| Rule | main | 3.3.6 | Key Difference |
|------|------|-------|----------------|
| 1. Hardcoded `lib/` in ExternalProject | ✅ `-DCMAKE_INSTALL_LIBDIR:PATH=lib` | ✅ Same + RocksDB uses `_rocksdb_libdir_flag` variable | 3.3.6 uses a variable for RocksDB LIBDIR; main uses direct arg |
| 2. RocksDB cache validation | ✅ `TD_CONFIG_NAME_RESOLVED` + `lib/` | ✅ Same | Identical logic |
| 3. RocksDB option guards | ✅ `TD_ROCKSDB_USE_EXTERNAL` / `TD_ROCKSDB_BUILD_FROM_SOURCE` | ✅ Same | Identical logic |
| 4. RocksDB linking (full path) | ✅ Full path to `librocksdb.a` | ✅ Same | Identical logic |
| 5. GTest lib64 compat | ✅ LIBDIR override | ✅ Symlink fallback | Different mechanism, same goal |
| 6. GCC 7 compat (taos-gen) | ✅ Required | N/A | taos-gen only exists on main |
| 7. BUILD_TEST=ON | ✅ Fixed | ✅ Fixed | Both branches patched independently |
| 8. Enterprise paths `source/` | ✅ Fixed | Verify | May differ in structure |
| 9. CMake define correctness | ✅ Fixed | Verify | `USE_PCRE2` typo may exist |
| 10. Conan macro overrides | ✅ Fixed | Verify | May differ in conan.cmake |
| 11. Build option defaults | ✅ insight/odbc/dotnet=OFF | Verify | May differ |
| 12. `DEP_td_rocksdb` uniform | ✅ Always via macro | ✅ Same | Identical logic |
| 13. LOCAL_URL CACHE bridge | ✅ Empty-string check + FORCE | Verify | CACHE var was always defined, blocking bridge |
| 14. GNU Make 4.2.1 upgrade | ✅ **Make 4.2.1** (2026-05) | Verify | ext_curl parallel bug fixed |

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-build-cmake-invariants version=1.0.0 author=Bo Xiao`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
