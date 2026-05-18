# C/C++ Build Cache Strategy Design

## Problem

All C/C++ build caches in tsdb-builder are local-only. Each developer and CI runner maintains its own `.externals/`, `conan2/`, and cmake build directories. This causes:

- **First build on a new machine takes 10–30 min** downloading and compiling ~28 ExternalProject dependencies from GitHub (Linux; 6 more are Windows-only)
- **No cross-machine reuse** of compiled object files — the same `.o` files are rebuilt independently on every machine
- **Branch switching invalidates cmake cache** — switching between feature branches triggers unnecessary recompilation of unchanged files
- **External downloads are slow and unreliable** — GitHub access from China is often throttled or blocked

## Scope

C/C++ components only: **engine**, **tools**, **gen**. Other language ecosystems (Go, Rust, Java, Node.js) are out of scope for this design.

## Architecture Overview

Three-layer caching strategy, implemented in two phases:

```
┌────────────────────────────────────────────────────┐
│  Phase 1: ccache (compilation cache)               │
│  ├─ Local ccache volume per developer              │
│  └─ Shared ccache via NFS or HTTP remote_storage   │
├────────────────────────────────────────────────────┤
│  Phase 2: External dependencies on GitLab          │
│  ├─ All ExternalProject deps as tarballs           │
│  ├─ GitLab Generic Package Registry storage        │
│  └─ LOCAL_URL cmake variable for redirection       │
└────────────────────────────────────────────────────┘
```

## Phase 1: ccache — Compilation Cache

### 1.1 Goal

Avoid recompiling unchanged `.o` files across builds, branches, and machines. ccache hashes preprocessor output + compiler flags to detect cache hits even when build directories differ.

### 1.2 Step 1 — Local ccache

**Docker image changes** (Dockerfile.core, Dockerfile.dev, Dockerfile.others):

- Install ccache ≥ 4.4 (required for `remote_storage` in Step 2)
  - **core/dev images** (manylinux2014 / CentOS 7): system `ccache` is too old (3.x). Install from EPEL or compile from source using the existing devtoolset
  - **others image** (AlmaLinux 8): `dnf install ccache` provides ≥ 4.x
  - **riscv64 core image** (Debian trixie): `apt install ccache` provides ≥ 4.x
- Create `/usr/lib64/ccache/` symlinks for `gcc`, `g++`, `cc`, `c++`

**build.sh changes**:

- Mount ccache cache volume: `$TSDB_CACHE_DIR/ccache-<image>-<arch>/` → `/root/.cache/ccache`
- Prepend `/usr/lib64/ccache` to `PATH` inside the container
- Set `CCACHE_MAXSIZE=20G` (configurable via env var)
- Set `CCACHE_BASEDIR=/mnt` so paths are normalized regardless of host source location

**Cache isolation**: per-image + per-arch (like `.externals/`), because different GCC versions produce different object files.

| Host path | Container path | Isolation |
|---|---|---|
| `ccache-core-amd64/` | `/root/.cache/ccache` | core + amd64 |
| `ccache-dev-arm64/` | `/root/.cache/ccache` | dev + arm64 |
| `ccache-others-amd64/` | `/root/.cache/ccache` | others + amd64 |

**Debug/Release 无需额外隔离**：ccache 的缓存 key 包含完整编译参数（`-O0 -g3` vs `-O3`），不同编译类型产生不同 hash，不会互相覆盖或误命中，可以安全共存于同一目录。但两种类型共存会占用约 2 倍空间，因此 `CCACHE_MAXSIZE` 设为 20G（而非 10G）以容纳两种编译类型。如果未来发现缓存驱逐频繁（hit rate 下降），可按 build type 拆分目录。

### 1.3 Step 2 — Shared ccache

#### 工作机制

ccache `remote_storage` 采用 **L1 (local) + L2 (remote) 双层按需查找**模式，而非全量同步：

```
编译 foo.c 时:
  1. ccache 计算 hash（基于预处理后输出 + 编译器路径 + 编译参数）
  2. 查 L1 (本地 ccache):
     → 命中 → 直接使用本地缓存，零网络开销
     → 未命中 → 继续第 3 步
  3. 查 L2 (remote storage):
     → 命中 → 下载该单个 .o 文件（~200KB），存入 L1 供后续使用
     → 未命中 → 正常编译，结果同时写入 L1 和 L2
```

关键点：
- **不是**全量下载整个远程缓存
- **不是**编译前预先扫描所有文件
- 每个 `.c` 文件编译时仅触发**一次 GET/一次文件读取**
- 本地已命中时完全不访问远端

#### TSDB 编译规模估算

| 指标 | 数值 |
|---|---|
| TSDB 自身 C/C++ 源文件数 | ~1632 个 |
| TSDB 自身代码量 | ~112 万行 |
| ExternalProject 编译单元（估算） | ~3000–5000 个 |
| 单个 .o 文件 ccache 压缩后大小 | 50KB – 2MB（均值约 200KB） |
| 全量单一编译类型缓存大小 | ~1–3 GB |
| Debug + Release 合计 | ~3–6 GB |

#### NFS 负载评估（5 并发 CI）

| 场景 | 网络 IO |
|---|---|
| 5 并发完全冷启动（最坏情况） | 5 × 1632 × 200KB ≈ 1.5 GB 总读取，分散在 3–5 分钟内 |
| 峰值读取带宽 | ~50–100 MB/s（千兆内网轻松承载） |
| 增量构建（仅改动文件 miss） | 极少量 IO（绝大部分 L1 命中） |

**结论**：5 个并发 CI 下 NFS **完全可以胜任**。NFS 瓶颈出现在 50+ 并发（元数据操作/stale file handle 概率上升）。ccache 使用原子 rename 写入，在 NFS 上是安全的。

#### 方案选择

| 并发规模 | 推荐方案 |
|---|---|
| ≤ 10 并发 CI | NFS (`CCACHE_REMOTE_STORAGE=file:///nfs/ccache`) |
| 10–50 并发 CI | HTTP (`CCACHE_REMOTE_STORAGE=http://<host>:<port>`) |
| 50+ 并发 CI | Redis backend (`CCACHE_REMOTE_STORAGE=redis://<host>`) |

**Option A: NFS mount** (当前推荐)

- Mount a shared NFS directory as ccache's secondary storage
- `CCACHE_REMOTE_STORAGE=file:///nfs/shared-ccache/<image>-<arch>/`
- 保持 per-image + per-arch 隔离（与本地缓存相同策略）
- Pros: 零额外基础设施，千兆内网即可
- Cons: 50+ 并发时可能出现元数据瓶颈

**Option B: HTTP remote storage** (未来升级路径)

- Deploy a lightweight HTTP storage (e.g., `ccache-remote`, MinIO, or Redis)
- `CCACHE_REMOTE_STORAGE=http://<host>:<port>|redis://<host>`
- Pros: 高并发、跨网段、无文件锁问题
- Cons: 需要额外部署和维护一个服务

**Recommendation**: 以 NFS (Option A) 起步，当前 5 并发 CI 远未到瓶颈。当 CI 规模扩大或观测到 NFS 延迟升高时，迁移至 HTTP/Redis。迁移仅需更改 `CCACHE_REMOTE_STORAGE` 环境变量，对 build.sh 和 Dockerfile 零改动。

## Phase 2: External Dependencies on GitLab

### 2.1 Goal

Host all ExternalProject source downloads on internal GitLab, eliminating GitHub dependency during builds. Unify all dependencies (currently split between `git clone` and `URL` download) to tarball-based downloads.

### 2.2 Current State

`external.cmake` (1806 lines) defines ~28 Linux ExternalProject dependencies (plus 6 Windows-only and test deps) using two download mechanisms:

| Mechanism | Count | cmake macro | Controlled by |
|---|---|---|---|
| `GIT_REPOSITORY` + `GIT_TAG` | ~24 | `get_from_local_repo_if_exists()` | `LOCAL_REPO` |
| `URL` + `URL_HASH` | 7 | `get_from_local_if_exists()` | `LOCAL_URL` |

All dependencies are version-locked — either to a specific tag (e.g., `v1.3.1`) or a commit SHA (e.g., `12c4bf1986c2...`). None track `main` or `master`.

### 2.3 Design Decision: Unify to Tarballs

Since all versions are locked, `git clone` provides no benefit over tarball download. Tarballs are:

- **Faster**: single HTTP GET vs git protocol negotiation + object packing
- **Smaller**: no `.git/` metadata, typically 30–50% less data
- **More deterministic**: tarball + SHA256 hash = bit-exact reproducibility
- **Simpler to host**: flat files in Package Registry vs 24+ mirror repos

**All ~24 `GIT_REPOSITORY` deps will be converted to `URL` downloads**, joining the existing 7 tarball deps. This eliminates the `LOCAL_REPO` variable entirely.

### 2.4 GitLab Generic Package Registry

A single GitLab project (`infra/build-deps`) hosts all tarballs in its Generic Package Registry.

**Naming convention**: `<repo_name>-<version>.tar.gz`

Each tarball is named uniquely by combining the repository name and the pinned version/commit. This avoids collisions that would occur with raw GitHub archive filenames (e.g., multiple deps might have `v1.0.0.tar.gz`). For commit-pinned deps, use at least 12 characters of the SHA to minimize collision risk.

Examples:
```
zlib-v1.3.1.tar.gz
lz4-v1.10.0.tar.gz
cJSON-12c4bf1986c2.tar.gz      # commit SHA, 12 chars
xxHash-de9d6577907d.tar.gz
openssl-3.1.3.tar.gz           # existing tarball deps keep current names
rocksdb-v8.1.1.tar.gz
curl-8.2.1.tar.gz
apr-1.7.6.tar.gz
apr-util-1.6.3.tar.gz
azure-storage-blobs-12.13.0-beta.1.tar.gz
```

**Upload**:
```bash
curl --header "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
     --upload-file zlib-v1.3.1.tar.gz \
     "https://git.tdengine.net/api/v4/projects/${PID}/packages/generic/externals/latest/zlib-v1.3.1.tar.gz"
```

**Download URL pattern**:
```
https://git.tdengine.net/api/v4/projects/${PID}/packages/generic/externals/latest/<filename>
```

**Canonical source**: the GitLab-hosted tarball is the canonical immutable artifact. The upstream GitHub URL serves as fallback only. GitHub auto-generated archives may change byte layout over time, so `URL_HASH` values are computed from and validated against the GitLab copy.

### 2.5 cmake Changes

#### 2.5.1 Macro enhancement

The existing `get_from_local_if_exists` macro extracts the last path component from the upstream URL to construct the mirror URL. This causes a filename mismatch when tarballs are renamed for uniqueness (e.g., upstream `v1.3.1.tar.gz` vs mirror `zlib-v1.3.1.tar.gz`).

**Solution**: add an optional second parameter for the explicit mirror filename:

```cmake
macro(get_from_local_if_exists url)                       # {
  if("z${LOCAL_URL}" STREQUAL "z")
    set(_url "${url}")
  else()
    if(${ARGC} GREATER 1)
      # Explicit mirror filename provided
      set(_url "${LOCAL_URL}/${ARGV1}")
    else()
      # Legacy behavior: extract filename from URL
      string(FIND ${url} "/" _pos REVERSE)
      math(EXPR _pos "${_pos} + 1")
      string(SUBSTRING ${url} ${_pos} -1 _name)
      set(_url "${LOCAL_URL}/${_name}")
    endif()
  endif()
endmacro()                                                # }
```

This also fixes a minor existing bug where the extracted filename included a leading `/`, producing double-slash URLs (`${LOCAL_URL}//filename`).

#### 2.5.2 Per-dependency conversion

Each `GIT_REPOSITORY` + `GIT_TAG` call becomes `URL` + `URL_HASH`:

```cmake
# Before
get_from_local_repo_if_exists("https://github.com/madler/zlib.git")
ExternalProject_Add(ext_zlib
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.3.1
    GIT_SHALLOW TRUE
    PREFIX "${_base}"
    ...)

# After
get_from_local_if_exists(
    "https://github.com/madler/zlib/archive/refs/tags/v1.3.1.tar.gz"
    "zlib-v1.3.1.tar.gz"
)
ExternalProject_Add(ext_zlib
    URL ${_url}
    URL_HASH SHA256=<computed_hash>
    PREFIX "${_base}"
    ...)
```

For commit-SHA-pinned deps, use the full SHA in the upstream URL:
```cmake
# Before
get_from_local_repo_if_exists("https://github.com/DaveGamble/cJSON.git")
ExternalProject_Add(ext_cjson
    GIT_REPOSITORY ${_git_url}
    GIT_TAG 12c4bf1986c288950a3d06da757109a6aa1ece38
    GIT_SHALLOW FALSE
    ...)

# After
get_from_local_if_exists(
    "https://github.com/DaveGamble/cJSON/archive/12c4bf1986c288950a3d06da757109a6aa1ece38.tar.gz"
    "cJSON-12c4bf1986c2.tar.gz"
)
ExternalProject_Add(ext_cjson
    URL ${_url}
    URL_HASH SHA256=<computed_hash>
    ...)
```

#### 2.5.3 Macro cleanup

- `get_from_local_repo_if_exists()`: remove entirely (no longer used)
- `get_from_local_if_exists()`: enhanced with optional mirror filename parameter (see 2.5.1)
- `LOCAL_REPO` cmake variable: deprecated, remove from documentation

#### 2.5.4 Source directory naming and high-risk dependencies

When ExternalProject downloads a tarball, the extracted directory name depends on the archive format. GitHub's `/archive/refs/tags/<tag>.tar.gz` produces directories like `<repo>-<tag>/`. If the renamed tarball (`<repo>-<version>.tar.gz`) produces a different directory name, ExternalProject handles this automatically via its `SOURCE_DIR` management. No manual `SOURCE_SUBDIR` changes should be needed.

**High-risk dependencies** — these require extra care during migration due to non-standard build or patching:

| Dependency | Risk Factor | Notes |
|---|---|---|
| `ext_xxhash` | `BUILD_IN_SOURCE TRUE`, custom `PATCH_COMMAND` | Verify patch applies cleanly to tarball source layout |
| `ext_lzma2` | `BUILD_IN_SOURCE TRUE`, custom Makefile patch | Patch references specific file paths |
| `ext_tz` | `BUILD_IN_SOURCE TRUE`, custom Makefile patch | Same as above |
| `ext_jemalloc` | Runs `./autogen.sh` as configure command | autogen.sh must be present in tarball root |
| `ext_avro` | `SOURCE_SUBDIR lang/c`, extensive patching | Subdirectory structure must match git layout |
| `ext_snappy` | Explicitly sets `GIT_SUBMODULES ""` | Tarball won't include any submodule content — verify build works without them |
| `ext_geos` | Large repo, cmake-based | Straightforward but large download |

**Validation required**: for each dependency, verify that the build still finds source files correctly after the tarball switch. Some deps with `BUILD_IN_SOURCE TRUE` or `PATCH_COMMAND` may need the `DOWNLOAD_EXTRACT_TIMESTAMP` option.

### 2.6 build.sh Changes

Add `LOCAL_URL` to cmake args when the user opts in (via env var or flag):

```bash
# In the cmake args assembly section of build.sh
if [[ -n "${DEPS_MIRROR_URL:-}" ]]; then
    EXTRA_CMAKE_ARGS+=("-DLOCAL_URL=${DEPS_MIRROR_URL}")
fi
```

**Default behavior**: when `DEPS_MIRROR_URL` is not set, cmake uses the original upstream URLs (GitHub). This ensures the change is backward-compatible for external contributors and environments without GitLab access.

**CI configuration**: CI runners set `DEPS_MIRROR_URL` in their environment:
```bash
export DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/${PID}/packages/generic/externals/latest"
```

### 2.7 Authentication

GitLab Generic Package Registry access depends on project visibility settings. The key question is whether anonymous downloads work from inside Docker containers.

**Option A: Public/Internal project with anonymous package access** (recommended)

1. Set `infra/build-deps` project visibility to "Internal" (visible to all logged-in GitLab users)
2. Enable **"Allow anyone to pull from Package Registry"** in Project → Settings → General → Visibility → Package Registry
3. **Validation step** (must be done before implementation):
   ```bash
   # Test from inside a Docker container with no auth
   docker run --rm quay.io/pypa/manylinux2014_x86_64 \
       curl -fsSL -o /dev/null -w "%{http_code}" \
       "https://git.tdengine.net/api/v4/projects/${PID}/packages/generic/externals/latest/zlib-v1.3.1.tar.gz"
   # Expected: 200
   ```
4. If anonymous access works: no tokens needed — `LOCAL_URL` works directly
5. If anonymous access doesn't work (GitLab "Internal" ≠ fully anonymous): use Option B

**Option B: Token-based access**

- Create a GitLab Deploy Token with `read_package_registry` scope
- Pass via cmake `HTTP_HEADER` in ExternalProject (avoids leaking tokens to logs/process lists):
  ```cmake
  ExternalProject_Add(ext_zlib
      URL ${_url}
      URL_HASH SHA256=<hash>
      HTTP_HEADER "PRIVATE-TOKEN: ${GITLAB_DOWNLOAD_TOKEN}"
      ...)
  ```
- `GITLAB_DOWNLOAD_TOKEN` injected via `build.sh` from env var
- **Security**: `HTTP_HEADER` is NOT printed in cmake output, unlike embedding token in the URL

**Recommendation**: Option A. Internal visibility is sufficient for an internal build system. Validate anonymous access first; fall back to Option B only if needed.

### 2.8 Tarball Preparation Script

A one-time script to download all upstream tarballs, rename them, compute SHA256 hashes, and upload to GitLab:

```bash
#!/bin/bash
# scripts/prepare-externals.sh
# Downloads upstream sources, renames to <repo>-<version>.tar.gz,
# computes SHA256, and uploads to GitLab Package Registry.

GITLAB_URL="https://git.tdengine.net"
PROJECT_ID="<PID>"
PACKAGE="externals"
VERSION="latest"

declare -A DEPS=(
    # [local_name]="upstream_url"
    ["zlib-v1.3.1.tar.gz"]="https://github.com/madler/zlib/archive/refs/tags/v1.3.1.tar.gz"
    ["lz4-v1.10.0.tar.gz"]="https://github.com/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz"
    # ... all 31 deps
)

for name in "${!DEPS[@]}"; do
    url="${DEPS[$name]}"
    echo "Downloading ${name} from ${url}..."
    curl -L -o "${name}" "${url}"

    sha256=$(sha256sum "${name}" | cut -d' ' -f1)
    echo "  SHA256: ${sha256}"

    echo "Uploading to GitLab..."
    curl --header "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
         --upload-file "${name}" \
         "${GITLAB_URL}/api/v4/projects/${PROJECT_ID}/packages/generic/${PACKAGE}/${VERSION}/${name}"
done
```

This script also generates a manifest file mapping each tarball to its SHA256, which is referenced during cmake conversion.

## Complete Dependency Inventory

### Linux dependencies (scope of this design)

| # | Dependency | Current version | Pinned by | Current method |
|---|---|---|---|---|
| 1 | zlib | v1.3.1 | tag | git clone |
| 2 | lz4 | v1.10.0 | tag | git clone |
| 3 | cJSON | 12c4bf1 | commit | git clone |
| 4 | xz (liblzma) | v5.8.1 | tag | git clone |
| 5 | xxHash | de9d657 | commit | git clone |
| 6 | fast-lzma2 | ded964d | commit | git clone |
| 7 | libuv | v1.49.2 | tag | git clone |
| 8 | tz | 2025a | tag | git clone |
| 9 | jemalloc | 5.3.0 | tag | git clone |
| 10 | sqlite | version-3.36.0 | tag | git clone |
| 11 | geos | 3.12.0 | tag | git clone |
| 12 | libdwarf-code | libdwarf-0.3.1 | tag | git clone |
| 13 | libdwarf-addr2line | 9d76b42 | commit | git clone |
| 14 | pcre2 | pcre2-10.45 | tag | git clone |
| 15 | jansson | 61fc3d0 | commit | git clone |
| 16 | snappy | 32ded45 | commit | git clone |
| 17 | avro | 7b106b1 | commit | git clone |
| 18 | libxml2 | v2.14.0 | tag | git clone |
| 19 | libs3 | 98f667b | commit | git clone |
| 20 | mxml | v2.12 | tag | git clone |
| 21 | cos-c-sdk-v5 | v5.0.16 | tag | git clone |
| 22 | cyrus-sasl | cyrus-sasl-2.1.27 | tag | git clone |
| 23 | openssl | 3.1.3 | tag | tarball (already) |
| 24 | curl | 8.2.1 | tag | tarball (already) |
| 25 | rocksdb | v8.1.1 | tag | tarball (already) |
| 26 | azure-sdk-for-cpp | 12.13.0-beta.1 | tag | tarball (already) |
| 27 | apr | 1.7.6 | tag | tarball (already) |
| 28 | apr-util | 1.6.3 | tag | tarball (already) |

### Windows-only dependencies (not migrated in Phase 2)

| Dependency | Version | Notes |
|---|---|---|
| pthread-win32 | 3309f4d | commit-pinned |
| win-iconv | 9f98392 | commit-pinned |
| libgnurx-msvc | 1a6514d | gitee.com, commit-pinned |
| wcwidth-cjk | a1b1e2c | commit-pinned |
| wingetopt | e8531ed | commit-pinned |
| crashdump | 149b43c | commit-pinned |

### Other (out of scope)

| Dependency | Location | Notes |
|---|---|---|
| googletest | external.cmake | test-only, behind `BUILD_DEPENDENCY_TESTS` |
| cpp-stub | external.cmake | test-only |
| ODBC deps (cjson, iconv, libwebsockets) | taos-connector-odbc/cmake/macros.cmake | separate cmake, does not use `LOCAL_URL` macro |
| contrib/ vendored (lemon, TSZ, libaes, libmqtt) | in-tree source | not ExternalProject, no download needed |

## Migration Plan

### Phase 1: ccache

1. Install ccache ≥ 4.4 in Dockerfile.core, Dockerfile.dev, Dockerfile.others, Dockerfile.core-riscv64
   - core/dev: compile from source in a builder stage (CentOS 7 EPEL only has 3.x)
   - others: `dnf install ccache` (AlmaLinux 8 has 4.x+)
   - riscv64: `apt install ccache` (Debian trixie has 4.x+)
2. Add ccache volume mount and env vars to build.sh
3. Validate: build twice, second build should show high cache hit ratio
4. (Later) Set up NFS-based shared ccache across CI runners

### Phase 2: External dependencies

**Order matters** — macro contract must be defined before uploads:

1. Create GitLab project `infra/build-deps`, enable Package Registry
2. Validate anonymous download from inside Docker (see §2.7)
3. Enhance `get_from_local_if_exists` macro (add optional mirror filename param, fix double-slash bug)
4. Run `prepare-externals.sh` to download, rename, compute SHA256, and upload all tarballs
5. Convert cmake `external.cmake`: change ~22 deps from `GIT_REPOSITORY` → `URL` (6 already use `URL`)
   - Start with low-risk deps (zlib, lz4, cJSON, etc.)
   - High-risk deps last (xxhash, jemalloc, avro — see risk table in §2.5.4)
6. Add `URL_HASH SHA256=<hash>` to all ExternalProject entries
7. Remove `get_from_local_repo_if_exists` macro and `LOCAL_REPO` references
8. Add `DEPS_MIRROR_URL` support to build.sh
9. Validate: full build with `BUILD_CONTRIB=ON -DLOCAL_URL=<gitlab_url>` on both amd64 and arm64
10. Update README.md documentation

## Out of Scope

- Non-C/C++ caches (Go modules, Cargo registry, pnpm, Maven, NuGet)
- Conan 2 cache sharing (uses existing Nexus remote)
- ODBC connector dependencies (separate cmake file, different build flow)
- Windows-only dependencies (6 deps, no Linux CI impact)
- Prebuilt `.externals/` sharing (may be addressed in a future design — ccache makes this less critical since object files are cached even without prebuilt deps)
