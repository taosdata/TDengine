# C/C++ Build Cache Strategy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Accelerate C/C++ builds by adding ccache compilation caching and migrating all ExternalProject dependencies to GitLab-hosted tarballs.

**Architecture:** Two-phase approach. Phase 1 installs ccache in all Docker builder images and wires it through `build.sh` volume mounts. Phase 2 enhances the cmake `get_from_local_if_exists` macro to support uniquely-named mirror tarballs, uploads all ~28 dependency tarballs to GitLab's Generic Package Registry, and converts every `GIT_REPOSITORY` call in `external.cmake` to `URL` + `URL_HASH`.

**Tech Stack:** Docker, bash, cmake (ExternalProject), ccache ≥ 4.4, GitLab Generic Package Registry

**Design Spec:** `docs/superpowers/specs/2026-05-14-cpp-cache-strategy-design.md`

---

## Repositories Involved

| Repo | Root | Changes |
|---|---|---|
| **tsdb-builder** | `/Users/xiaobo/work/gitlab/platform/tsdb-builder` | Dockerfiles, build.sh, scripts, docs |
| **taos-community** | `/Users/xiaobo/tsdb/source/taos-community` | `cmake/external.cmake` |

## File Map

### Phase 1 — ccache

| Action | File | Responsibility |
|---|---|---|
| Modify | `Dockerfile.core:280–299` | Add ccache build-from-source + symlinks (after mold, before protoc/tini) |
| Modify | `Dockerfile.dev:283–302` | Same as core (devtoolset-9 instead of devtoolset-7) |
| Modify | `Dockerfile.others:327–345` | `dnf install ccache` + symlinks |
| Modify | `Dockerfile.core-riscv64:150–170` | `apt install ccache` + symlinks |
| Modify | `.build-args` | Add `CCACHE_VERSION=4.10.2` |
| Modify | `build.sh:468–472` | Add ccache cache directory creation |
| Modify | `build.sh:611–623` | Add ccache volume mount to DOCKER_MAIN_ARGS |
| Modify | `build.sh:474–552` | Add ccache PATH prepend + env vars in CONTAINER_SCRIPT |
| Modify | `verify-image.sh` | Add ccache version check |

### Phase 2 — External Dependencies

| Action | File | Responsibility |
|---|---|---|
| Modify | `taos-community/cmake/external.cmake:249–257` | Enhance `get_from_local_if_exists` macro |
| Modify | `taos-community/cmake/external.cmake:228–247` | Remove `get_from_local_repo_if_exists` macro |
| Modify | `taos-community/cmake/external.cmake:259–1783` | Convert ~22 git deps to URL+URL_HASH |
| Modify | `build.sh:383–389` | Add DEPS_MIRROR_URL → LOCAL_URL passthrough |
| Create | `scripts/prepare-externals.sh` | One-time tarball download + upload script |
| Create | `scripts/externals-manifest.txt` | SHA256 manifest of all uploaded tarballs |

### Documentation

| Action | File |
|---|---|
| Modify | `README.md` | Add ccache docs, DEPS_MIRROR_URL usage |

---

## Phase 1: ccache

### Task 1: Add CCACHE_VERSION to .build-args

**Files:**
- Modify: `.build-args`

- [ ] **Step 1: Add ccache version**

Add `CCACHE_VERSION=4.10.2` to the Development Tools Versions section of `.build-args`:

```
# Development Tools Versions
GO_VERSION=1.23.4
MAVEN_VERSION=3.8.4
CMAKE_VERSION=3.21.5
...
CCACHE_VERSION=4.10.2
```

Insert the line after the `TINI_VERSION` line and before `NODE_VERSION`:

```bash
cd /Users/xiaobo/work/gitlab/platform/tsdb-builder
sed -i '' '/^TINI_VERSION=/a\
CCACHE_VERSION=4.10.2' .build-args
```

- [ ] **Step 2: Verify**

```bash
grep CCACHE_VERSION .build-args
# Expected: CCACHE_VERSION=4.10.2
```

- [ ] **Step 3: Commit**

```bash
git add .build-args
git commit -m "build: add CCACHE_VERSION=4.10.2 to .build-args"
```

---

### Task 2: Install ccache in Dockerfile.core

**Files:**
- Modify: `Dockerfile.core`

The core image is based on manylinux2014 (CentOS 7). System ccache is 3.x (too old for `remote_storage`). We compile ccache from source using the pre-installed devtoolset-10 in a builder stage.

- [ ] **Step 1: Add CCACHE_VERSION ARG**

Near the top of `Dockerfile.core`, in the ARG declarations section (around line 40–50), add:

```dockerfile
ARG CCACHE_VERSION=4.10.2
```

Place it next to the other version ARGs (e.g., after `ARG MOLD_VERSION`).

- [ ] **Step 2: Add ccache builder stage**

After the `mold-builder` stage and before the main `FROM stage2-${TARGETARCH}` line (line 106), add a new build stage:

```dockerfile
# ── ccache builder (compile from source for CentOS 7) ────────────────────────
# CentOS 7 system ccache is 3.x; we need ≥ 4.4 for remote_storage support.
# devtoolset-10 (pre-installed in manylinux2014) provides C++17 needed by ccache.
FROM stage2-${TARGETARCH} AS ccache-builder
ARG CCACHE_VERSION
RUN source scl_source enable devtoolset-10 && \
    cd /tmp && \
    curl -fsSL "https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz" \
        -o ccache.tar.gz && \
    tar xf ccache.tar.gz && \
    cd ccache-${CCACHE_VERSION} && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DZSTD_FROM_INTERNET=ON \
          -DHIREDIS_FROM_INTERNET=ON \
          -DENABLE_TESTING=OFF \
          .. && \
    make -j$(nproc) && \
    make install DESTDIR=/opt/ccache-install && \
    rm -rf /tmp/ccache*
```

- [ ] **Step 3: COPY ccache binary and create symlinks**

In the main image, after the mold section (after line 289) and before the protoc/tini section, add:

```dockerfile
# ccache: compiled from source in ccache-builder stage (≥ 4.4 for remote_storage)
COPY --from=ccache-builder /opt/ccache-install/usr/local/bin/ccache /usr/local/bin/ccache
RUN mkdir -p /usr/lib64/ccache && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/gcc && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/g++ && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/cc && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/c++
```

- [ ] **Step 4: Verify Dockerfile syntax**

```bash
cd /Users/xiaobo/work/gitlab/platform/tsdb-builder
docker buildx build --check -f Dockerfile.core .
```

Expected: no syntax errors.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile.core
git commit -m "build(core): install ccache 4.x compiled from source

CentOS 7 (manylinux2014) only has ccache 3.x in repos. Compile from
source in a ccache-builder stage using devtoolset-10 (C++17 required).
ccache ≥ 4.4 is needed for remote_storage support (shared caching).

Symlinks in /usr/lib64/ccache/ allow PATH-based interception of gcc/g++."
```

---

### Task 3: Install ccache in Dockerfile.dev

**Files:**
- Modify: `Dockerfile.dev`

The dev Dockerfile is nearly identical to core (devtoolset-9 instead of devtoolset-7). Apply the same ccache changes.

- [ ] **Step 1: Add CCACHE_VERSION ARG**

Add `ARG CCACHE_VERSION=4.10.2` near the other version ARGs.

- [ ] **Step 2: Add ccache-builder stage**

Add the same `ccache-builder` stage as in Task 2 Step 2. The stage uses devtoolset-10 (which is also present in the dev base image before it's replaced with devtoolset-9), so the code is identical.

```dockerfile
FROM stage2-${TARGETARCH} AS ccache-builder
ARG CCACHE_VERSION
RUN source scl_source enable devtoolset-10 && \
    cd /tmp && \
    curl -fsSL "https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz" \
        -o ccache.tar.gz && \
    tar xf ccache.tar.gz && \
    cd ccache-${CCACHE_VERSION} && \
    mkdir build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DZSTD_FROM_INTERNET=ON \
          -DHIREDIS_FROM_INTERNET=ON \
          -DENABLE_TESTING=OFF \
          .. && \
    make -j$(nproc) && \
    make install DESTDIR=/opt/ccache-install && \
    rm -rf /tmp/ccache*
```

- [ ] **Step 3: COPY ccache binary and create symlinks**

Same as Task 2 Step 3:

```dockerfile
COPY --from=ccache-builder /opt/ccache-install/usr/local/bin/ccache /usr/local/bin/ccache
RUN mkdir -p /usr/lib64/ccache && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/gcc && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/g++ && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/cc && \
    ln -sf /usr/local/bin/ccache /usr/lib64/ccache/c++
```

- [ ] **Step 4: Verify Dockerfile syntax**

```bash
docker buildx build --check -f Dockerfile.dev .
```

- [ ] **Step 5: Commit**

```bash
git add Dockerfile.dev
git commit -m "build(dev): install ccache 4.x compiled from source"
```

---

### Task 4: Install ccache in Dockerfile.others

**Files:**
- Modify: `Dockerfile.others`

The others image uses AlmaLinux 8, which has ccache 4.x in EPEL.

- [ ] **Step 1: Install ccache via dnf**

In `Dockerfile.others`, in the main yum/dnf package install RUN block (the large `dnf install` around line 159–180), add `ccache` to the package list.

If there's no EPEL repo enabled, install it first:

```dockerfile
RUN dnf install -y epel-release && \
    dnf install -y ccache && \
    mkdir -p /usr/lib64/ccache && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/gcc && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/g++ && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/cc && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/c++
```

Add this block after the mold section and before the environment configuration section, matching the placement in core/dev.

- [ ] **Step 2: Verify ccache version is ≥ 4.4**

Add a version check in the verify section at the end of the Dockerfile:

```bash
ccache --version | head -1
# Expected: ccache version 4.x.y (must be ≥ 4.4)
```

- [ ] **Step 3: Verify Dockerfile syntax**

```bash
docker buildx build --check -f Dockerfile.others .
```

- [ ] **Step 4: Commit**

```bash
git add Dockerfile.others
git commit -m "build(others): install ccache from EPEL (AlmaLinux 8)"
```

---

### Task 5: Install ccache in Dockerfile.core-riscv64

**Files:**
- Modify: `Dockerfile.core-riscv64`

Debian trixie has ccache 4.x in the standard repos.

- [ ] **Step 1: Add ccache to apt install**

In the main `apt-get install` block, add `ccache` to the package list:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
    ...existing packages... \
    ccache \
    && rm -rf /var/lib/apt/lists/*
```

- [ ] **Step 2: Create symlinks**

```dockerfile
RUN mkdir -p /usr/lib64/ccache && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/gcc && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/g++ && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/cc && \
    ln -sf /usr/bin/ccache /usr/lib64/ccache/c++
```

- [ ] **Step 3: Commit**

```bash
git add Dockerfile.core-riscv64
git commit -m "build(riscv64): install ccache from Debian repos"
```

---

### Task 6: Wire ccache into build.sh

**Files:**
- Modify: `build.sh`

Three changes needed: (1) create cache directory, (2) mount volume, (3) configure ccache env inside container.

- [ ] **Step 1: Add ccache cache directory creation**

At line 468–472 (`mkdir -p` block), add the ccache directory:

```bash
# ── docker run ────────────────────────────────────────────────────────────────
mkdir -p "${TSDB_CACHE_DIR}/conan2-${ARCH}" \
         "${TSDB_CACHE_DIR}/go-mod" \
         "${TSDB_CACHE_DIR}/cargo-registry" \
         "${TSDB_CACHE_DIR}/cargo-git" \
         "${TSDB_CACHE_DIR}/ccache-${USE_IMAGE}-${ARCH}"
```

- [ ] **Step 2: Add ccache volume mount to DOCKER_MAIN_ARGS**

In the `DOCKER_MAIN_ARGS` array (around line 611–624), add the ccache volume mount before the `"${IMAGE}"` line:

```bash
declare -a DOCKER_MAIN_ARGS=(
    "--rm"
    $( [[ "$ARCH" != "riscv64" ]] && echo "--platform=${PLATFORM}" )
    "${EXTRA_ENV_ARGS[@]}"
    "${EXTERNALS_MOUNT_ARGS[@]}"
    "${PNPM_STORE_ARGS[@]}"
    "${JVM_MOUNT_ARGS[@]}"
    "--volume=${TSDB_DIR}:/mnt"
    "--volume=${TSDB_CACHE_DIR}/conan2-${ARCH}:/root/.conan2"
    "--volume=${TSDB_CACHE_DIR}/go-mod:/root/go/pkg/mod"
    "--volume=${TSDB_CACHE_DIR}/cargo-registry:/root/.cargo/registry"
    "--volume=${TSDB_CACHE_DIR}/cargo-git:/root/.cargo/git"
    "--volume=${TSDB_CACHE_DIR}/ccache-${USE_IMAGE}-${ARCH}:/root/.cache/ccache"
    "${IMAGE}"
)
```

- [ ] **Step 3: Add ccache env configuration in CONTAINER_SCRIPT**

At the beginning of `CONTAINER_SCRIPT` (line 474), right after `set -eo pipefail`, add:

```bash
CONTAINER_SCRIPT="
set -eo pipefail

# ── ccache configuration ─────────────────────────────────────────────────────
# Prepend ccache symlink directory so gcc/g++ calls go through ccache
export PATH=/usr/lib64/ccache:\${PATH}
export CCACHE_MAXSIZE=${CCACHE_MAXSIZE:-20G}
export CCACHE_BASEDIR=/mnt
export CCACHE_COMPILERCHECK=content
# If CCACHE_REMOTE_STORAGE is set in host env, pass it through for shared caching
if [ -n \"\${CCACHE_REMOTE_STORAGE:-}\" ]; then
    export CCACHE_REMOTE_STORAGE
fi
ccache --zero-stats >/dev/null 2>&1

... (rest of existing CONTAINER_SCRIPT) ...
"
```

- [ ] **Step 4: Add ccache stats output at end of CONTAINER_SCRIPT**

Before the closing `"` of CONTAINER_SCRIPT (before the `if [ '${NEEDS_DIST_CLEANUP}' = 'true' ]` block), add:

```bash
echo ''
echo '── ccache statistics ──────────────────────────────────────────────────────'
ccache --show-stats
```

- [ ] **Step 5: Pass CCACHE env vars through docker run**

Add CCACHE env passthrough in the `EXTRA_ENV_ARGS` setup section (around line 360–373). After the existing env args setup:

```bash
# ccache environment passthrough
EXTRA_ENV_ARGS+=(
    "--env=CCACHE_MAXSIZE=${CCACHE_MAXSIZE:-20G}"
)
if [[ -n "${CCACHE_REMOTE_STORAGE:-}" ]]; then
    EXTRA_ENV_ARGS+=("--env=CCACHE_REMOTE_STORAGE=${CCACHE_REMOTE_STORAGE}")
fi
```

- [ ] **Step 6: Verify no syntax errors**

```bash
bash -n build.sh
# Expected: no output (no syntax errors)
```

- [ ] **Step 7: Commit**

```bash
git add build.sh
git commit -m "build: wire ccache into build.sh

- Create per-image+per-arch ccache cache directory
- Mount ccache volume at /root/.cache/ccache
- Prepend /usr/lib64/ccache to PATH for transparent interception
- Set CCACHE_BASEDIR=/mnt for path normalization across hosts
- Set CCACHE_MAXSIZE=20G (configurable via env var)
- Print ccache stats after each build
- Pass through CCACHE_REMOTE_STORAGE for shared caching"
```

---

### Task 7: Add ccache verification to verify-image.sh

**Files:**
- Modify: `verify-image.sh`

- [ ] **Step 1: Add ccache check**

In `verify-image.sh`, find the section where tools are verified (e.g., `cmake --version`, `gcc --version`). Add a ccache version check:

```bash
echo "── ccache ──"
ccache --version | head -1
```

- [ ] **Step 2: Commit**

```bash
git add verify-image.sh
git commit -m "build: add ccache version check to verify-image.sh"
```

---

### Task 8: Validate Phase 1 ccache integration

**Files:**
- None (validation only)

This task validates the ccache integration end-to-end. It requires building at least one Docker image first. If image rebuild is not feasible, defer this validation to after images are published.

- [ ] **Step 1: Build a test image (core, amd64)**

```bash
./build-core-image.sh --version test --arch amd64 --no-cache
```

- [ ] **Step 2: Verify ccache inside the image**

```bash
./verify-image.sh core:test-amd64
# Expected: ccache version output shows 4.10.2
```

- [ ] **Step 3: Cold build (ccache empty)**

```bash
# Clean any existing ccache
rm -rf ~/cache/tsdb-builder/ccache-core-amd64/*

./build.sh --image core:test engine
# Expected: build succeeds, ccache stats show 0 hits at end
```

- [ ] **Step 4: Warm build (ccache populated)**

```bash
# Wipe cmake build dir but keep ccache
rm -rf ~/tsdb/debug/build

./build.sh --image core:test engine
# Expected: ccache stats show high hit rate (>90%)
# Expected: build time significantly reduced
```

- [ ] **Step 5: Verify cache isolation**

```bash
ls ~/cache/tsdb-builder/ | grep ccache
# Expected: ccache-core-amd64/ exists, no other ccache dirs yet
```

---

## Phase 2: External Dependencies on GitLab

### Task 9: Enhance get_from_local_if_exists macro

**Files:**
- Modify: `~/tsdb/source/taos-community/cmake/external.cmake:249–257`

This task modifies the cmake macro to accept an optional second parameter for the mirror filename, fixing the double-slash bug and enabling uniquely-named tarballs.

- [ ] **Step 1: Replace the macro**

In `external.cmake`, replace lines 249–257 (the `get_from_local_if_exists` macro) with:

```cmake
macro(get_from_local_if_exists url)                       # {
  if("z${LOCAL_URL}" STREQUAL "z")
    set(_url "${url}")
  else()
    if(${ARGC} GREATER 1)
      # Explicit mirror filename provided (e.g. "zlib-v1.3.1.tar.gz")
      set(_url "${LOCAL_URL}/${ARGV1}")
    else()
      # Legacy behavior: extract filename from URL (last path segment)
      string(FIND ${url} "/" _pos REVERSE)
      math(EXPR _pos "${_pos} + 1")
      string(SUBSTRING ${url} ${_pos} -1 _name)
      set(_url "${LOCAL_URL}/${_name}")
    endif()
  endif()
endmacro()                                                # }
```

Key changes:
- Added `if(${ARGC} GREATER 1)` branch for explicit mirror filename via `${ARGV1}`
- Fixed double-slash bug: added `math(EXPR _pos "${_pos} + 1")` to skip the `/`
- Legacy one-argument behavior preserved for backward compatibility

- [ ] **Step 2: Verify cmake parses correctly**

```bash
cd ~/tsdb/debug
cmake .. -DLOCAL_URL="" 2>&1 | head -5
# Expected: no cmake errors (LOCAL_URL empty = no-op, uses upstream URLs)
```

- [ ] **Step 3: Commit (in taos-community repo)**

```bash
cd ~/tsdb/source/taos-community
git add cmake/external.cmake
git commit -m "cmake: enhance get_from_local_if_exists with explicit mirror filename

Add optional second parameter for mirror filename. When LOCAL_URL is set
and a mirror filename is provided, the macro constructs the URL as
\${LOCAL_URL}/<mirror_filename> instead of extracting from the upstream URL.

This enables uniquely-named tarballs (e.g. zlib-v1.3.1.tar.gz) that
avoid filename collisions across dependencies.

Also fixes a minor bug where the extracted filename included a leading
slash, producing double-slash URLs (\${LOCAL_URL}//filename)."
```

---

### Task 10: Create prepare-externals.sh script

**Files:**
- Create: `scripts/prepare-externals.sh`

A one-time script to download all upstream source tarballs, compute SHA256 hashes, and upload to GitLab Package Registry.

- [ ] **Step 1: Create the script**

Create `scripts/prepare-externals.sh` in the tsdb-builder repo:

```bash
#!/bin/bash
# ============================================================================
# prepare-externals.sh — Download, rename, hash, and upload ExternalProject
# source tarballs to GitLab Generic Package Registry.
#
# Usage:
#   export GITLAB_TOKEN="glpat-xxxx"
#   ./scripts/prepare-externals.sh [--upload]
#
# Without --upload, downloads and computes hashes only (dry run).
# With --upload, also pushes tarballs to GitLab Package Registry.
# ============================================================================
set -euo pipefail

GITLAB_URL="${GITLAB_URL:-https://git.tdengine.net}"
PROJECT_ID="${GITLAB_PROJECT_ID:?Set GITLAB_PROJECT_ID to the infra/build-deps project ID}"
PACKAGE_NAME="externals"
PACKAGE_VERSION="latest"

DO_UPLOAD=false
if [[ "${1:-}" == "--upload" ]]; then
    DO_UPLOAD=true
    if [[ -z "${GITLAB_TOKEN:-}" ]]; then
        echo "ERROR: GITLAB_TOKEN must be set for upload"
        exit 1
    fi
fi

WORKDIR="$(mktemp -d)"
MANIFEST="scripts/externals-manifest.txt"
trap 'rm -rf "$WORKDIR"' EXIT

echo "Working directory: $WORKDIR"
echo ""

# ── Dependency list ──────────────────────────────────────────────────────────
# Format: local_filename|upstream_url
# All upstream URLs must produce a valid tar.gz archive.
DEPS=(
    # --- Currently GIT_REPOSITORY deps (to be converted) ---
    "zlib-v1.3.1.tar.gz|https://github.com/madler/zlib/archive/refs/tags/v1.3.1.tar.gz"
    "lz4-v1.10.0.tar.gz|https://github.com/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz"
    "cJSON-12c4bf1986c2.tar.gz|https://github.com/DaveGamble/cJSON/archive/12c4bf1986c288950a3d06da757109a6aa1ece38.tar.gz"
    "xz-v5.8.1.tar.gz|https://github.com/tukaani-project/xz/archive/refs/tags/v5.8.1.tar.gz"
    "xxHash-de9d6577907d.tar.gz|https://github.com/Cyan4973/xxHash/archive/de9d6577907d80b190cc3a07be460741b0e2a980.tar.gz"
    "fast-lzma2-ded964d92c27.tar.gz|https://github.com/conor42/fast-lzma2/archive/ded964d92c27be8f712e3a5b1e42e07a886c2145.tar.gz"
    "libuv-v1.49.2.tar.gz|https://github.com/libuv/libuv/archive/refs/tags/v1.49.2.tar.gz"
    "tz-2025a.tar.gz|https://github.com/eggert/tz/archive/refs/tags/2025a.tar.gz"
    "jemalloc-5.3.0.tar.gz|https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz"
    "sqlite-version-3.36.0.tar.gz|https://github.com/nicedoc/sqlite/archive/refs/tags/version-3.36.0.tar.gz"
    "geos-3.12.0.tar.gz|https://github.com/libgeos/geos/archive/refs/tags/3.12.0.tar.gz"
    "libdwarf-code-libdwarf-0.3.1.tar.gz|https://github.com/davea42/libdwarf-code/archive/refs/tags/libdwarf-0.3.1.tar.gz"
    "libdwarf-addr2line-9d76b42cce85.tar.gz|https://github.com/nicedoc/libdwarf-addr2line/archive/9d76b42cce85b15fc57e45d20d20c94c09806f05.tar.gz"
    "pcre2-pcre2-10.45.tar.gz|https://github.com/PCRE2Project/pcre2/archive/refs/tags/pcre2-10.45.tar.gz"
    "jansson-61fc3d0c1bf0.tar.gz|https://github.com/akheron/jansson/archive/61fc3d0c1bf0f0e8a2e5db56fa9d9e8e23ee91cb.tar.gz"
    "snappy-32ded457c0b1.tar.gz|https://github.com/google/snappy/archive/32ded457c0b1fe78ceb8397632c416568d6714a0.tar.gz"
    "avro-7b106b1dff01.tar.gz|https://github.com/nicedoc/avro/archive/7b106b1dff01c6b03e1f44ba91b9c7f63e413c29.tar.gz"
    "libxml2-v2.14.0.tar.gz|https://github.com/GNOME/libxml2/archive/refs/tags/v2.14.0.tar.gz"
    "libs3-98f667b0a3ec.tar.gz|https://github.com/nicedoc/libs3/archive/98f667b0a3ec6eb2e7f2ebddf9da3abaf90ddf87.tar.gz"
    "mxml-v2.12.tar.gz|https://github.com/nicedoc/mxml/archive/refs/tags/v2.12.tar.gz"
    "cos-c-sdk-v5-v5.0.16.tar.gz|https://github.com/nicedoc/cos-c-sdk-v5/archive/refs/tags/v5.0.16.tar.gz"
    "cyrus-sasl-cyrus-sasl-2.1.27.tar.gz|https://github.com/cyrusimap/cyrus-sasl/archive/refs/tags/cyrus-sasl-2.1.27.tar.gz"
    # --- Already URL-based deps (re-host for consistency) ---
    # These already use URL in external.cmake; re-host them on GitLab too.
    # Add them to this list when ready.
)

# ── Download + hash ──────────────────────────────────────────────────────────
> "$MANIFEST"
echo "# externals-manifest.txt — SHA256 hashes for GitLab-hosted tarballs" >> "$MANIFEST"
echo "# Generated by prepare-externals.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$MANIFEST"
echo "" >> "$MANIFEST"

FAIL_COUNT=0
for entry in "${DEPS[@]}"; do
    name="${entry%%|*}"
    url="${entry##*|}"

    echo "Downloading: ${name}"
    echo "  From: ${url}"

    if ! curl -fsSL -o "${WORKDIR}/${name}" "${url}"; then
        echo "  ERROR: download failed"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi

    sha256=$(sha256sum "${WORKDIR}/${name}" | cut -d' ' -f1)
    size=$(du -h "${WORKDIR}/${name}" | cut -f1)
    echo "  SHA256: ${sha256}  (${size})"
    echo "${sha256}  ${name}" >> "$MANIFEST"

    if $DO_UPLOAD; then
        echo "  Uploading to GitLab..."
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            --header "PRIVATE-TOKEN: ${GITLAB_TOKEN}" \
            --upload-file "${WORKDIR}/${name}" \
            "${GITLAB_URL}/api/v4/projects/${PROJECT_ID}/packages/generic/${PACKAGE_NAME}/${PACKAGE_VERSION}/${name}")
        if [[ "$HTTP_CODE" =~ ^2 ]]; then
            echo "  Uploaded OK (HTTP ${HTTP_CODE})"
        else
            echo "  ERROR: upload failed (HTTP ${HTTP_CODE})"
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
    fi
    echo ""
done

echo "Manifest written to: ${MANIFEST}"
echo "Total deps: ${#DEPS[@]}, failures: ${FAIL_COUNT}"

if [[ $FAIL_COUNT -gt 0 ]]; then
    echo "WARNING: ${FAIL_COUNT} failures occurred"
    exit 1
fi
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/prepare-externals.sh
```

- [ ] **Step 3: Test dry run (download only, no upload)**

```bash
./scripts/prepare-externals.sh
# Expected: all 22 tarballs download successfully
# Expected: scripts/externals-manifest.txt created with SHA256 hashes
```

- [ ] **Step 4: Commit**

```bash
git add scripts/prepare-externals.sh scripts/externals-manifest.txt
git commit -m "scripts: add prepare-externals.sh for GitLab tarball hosting

Downloads all ExternalProject upstream source tarballs, renames them
to <repo>-<version>.tar.gz, computes SHA256 hashes, and optionally
uploads to GitLab Generic Package Registry.

Run with --upload to push to GitLab (requires GITLAB_TOKEN).
Without --upload, performs a dry run (download + hash only)."
```

---

### Task 11: Add DEPS_MIRROR_URL support to build.sh

**Files:**
- Modify: `build.sh`

- [ ] **Step 1: Add DEPS_MIRROR_URL to cmake args**

In `build.sh`, find the section where `CMAKE_ARGS` is assembled (around line 298–330). After the existing cmake arg assembly (e.g., after the `CMAKE_LINKER=mold` line around 325), add:

```bash
# External dependency mirror URL (GitLab Package Registry)
if [[ -n "${DEPS_MIRROR_URL:-}" ]]; then
    CMAKE_ARGS="${CMAKE_ARGS} -DLOCAL_URL=${DEPS_MIRROR_URL}"
    echo "[INFO] Using dependency mirror: ${DEPS_MIRROR_URL}"
fi
```

- [ ] **Step 2: Verify no syntax errors**

```bash
bash -n build.sh
```

- [ ] **Step 3: Commit**

```bash
git add build.sh
git commit -m "build: add DEPS_MIRROR_URL passthrough to cmake LOCAL_URL

When DEPS_MIRROR_URL env var is set, passes it as -DLOCAL_URL to cmake,
redirecting all ExternalProject tarball downloads to the internal mirror.

Usage:
  export DEPS_MIRROR_URL='https://git.tdengine.net/api/v4/projects/<PID>/packages/generic/externals/latest'
  ./build.sh --image core engine"
```

---

### Task 12: Convert low-risk git deps to URL (batch 1 of 3)

**Files:**
- Modify: `~/tsdb/source/taos-community/cmake/external.cmake`

Convert the first batch of straightforward dependencies. These have no custom `PATCH_COMMAND`, no `BUILD_IN_SOURCE`, and standard cmake builds.

Deps in this batch: `zlib`, `lz4`, `cJSON`, `xz`, `libuv`, `sqlite`, `geos`, `pcre2`, `libxml2`, `mxml`, `cyrus-sasl`

- [ ] **Step 1: Convert each dependency**

For each dependency, apply this pattern. Example for zlib (around line 259):

```cmake
# Before:
get_from_local_repo_if_exists("https://github.com/madler/zlib.git")
ExternalProject_Add(ext_zlib
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.3.1
    GIT_SHALLOW TRUE
    PREFIX "${_base}"
    ...)

# After:
get_from_local_if_exists(
    "https://github.com/madler/zlib/archive/refs/tags/v1.3.1.tar.gz"
    "zlib-v1.3.1.tar.gz"
)
ExternalProject_Add(ext_zlib
    URL ${_url}
    URL_HASH SHA256=<hash_from_manifest>
    PREFIX "${_base}"
    ...)
```

Replace `<hash_from_manifest>` with the corresponding SHA256 from `scripts/externals-manifest.txt` generated in Task 10.

Remove `GIT_SHALLOW TRUE` and `GIT_SUBMODULES` flags (not applicable to URL downloads).

Apply this transformation to all 11 deps in this batch. Look up each dep's section in `external.cmake` by searching for its `ExternalProject_Add` name.

- [ ] **Step 2: Test build with upstream URLs**

```bash
cd ~/tsdb
rm -rf debug/build
./build.sh --image core engine -DBUILD_CONTRIB=ON
# Expected: all deps download from GitHub as tarballs
```

- [ ] **Step 3: Commit**

```bash
cd ~/tsdb/source/taos-community
git add cmake/external.cmake
git commit -m "cmake: convert 11 low-risk deps from git clone to URL tarball

Convert zlib, lz4, cJSON, xz, libuv, sqlite, geos, pcre2, libxml2,
mxml, cyrus-sasl from GIT_REPOSITORY to URL + URL_HASH (SHA256).

All deps use the enhanced get_from_local_if_exists macro with explicit
mirror filenames for LOCAL_URL redirection."
```

---

### Task 13: Convert medium-risk git deps to URL (batch 2 of 3)

**Files:**
- Modify: `~/tsdb/source/taos-community/cmake/external.cmake`

Deps in this batch: `libdwarf-code`, `libdwarf-addr2line`, `jansson`, `cos-c-sdk-v5`, `libs3`, `snappy`

These are commit-SHA-pinned or have minor nuances (e.g., snappy with `GIT_SUBMODULES ""`).

- [ ] **Step 1: Convert each dependency**

Same pattern as Task 12. For commit-SHA-pinned deps, use the full SHA in the upstream URL:

```cmake
get_from_local_if_exists(
    "https://github.com/DaveGamble/cJSON/archive/12c4bf1986c288950a3d06da757109a6aa1ece38.tar.gz"
    "cJSON-12c4bf1986c2.tar.gz"
)
```

For `snappy`: remove `GIT_SUBMODULES ""` — tarballs don't have submodules.

- [ ] **Step 2: Test build**

```bash
cd ~/tsdb
rm -rf debug/build
./build.sh --image core engine -DBUILD_CONTRIB=ON
```

- [ ] **Step 3: Commit**

```bash
cd ~/tsdb/source/taos-community
git add cmake/external.cmake
git commit -m "cmake: convert 6 medium-risk deps from git clone to URL tarball

Convert libdwarf-code, libdwarf-addr2line, jansson, cos-c-sdk-v5,
libs3, snappy from GIT_REPOSITORY to URL + URL_HASH."
```

---

### Task 14: Convert high-risk git deps to URL (batch 3 of 3)

**Files:**
- Modify: `~/tsdb/source/taos-community/cmake/external.cmake`

Deps in this batch: `xxHash`, `fast-lzma2`, `tz`, `jemalloc`, `avro`

These have `BUILD_IN_SOURCE TRUE`, custom `PATCH_COMMAND`, or `SOURCE_SUBDIR`. Each needs individual validation.

- [ ] **Step 1: Convert xxHash**

Has `BUILD_IN_SOURCE TRUE` and custom `PATCH_COMMAND`. The patch modifies source files in-place.

After conversion, verify:
- The `PATCH_COMMAND` still applies cleanly to the tarball-extracted source layout
- File paths in the patch match the directory structure inside the archive

```cmake
get_from_local_if_exists(
    "https://github.com/Cyan4973/xxHash/archive/de9d6577907d80b190cc3a07be460741b0e2a980.tar.gz"
    "xxHash-de9d6577907d.tar.gz"
)
```

- [ ] **Step 2: Convert fast-lzma2**

Has `BUILD_IN_SOURCE TRUE` and Makefile patch. Verify patch paths.

- [ ] **Step 3: Convert tz**

Has `BUILD_IN_SOURCE TRUE` and Makefile patch. Verify patch paths.

- [ ] **Step 4: Convert jemalloc**

Runs `./autogen.sh` as configure command. Verify `autogen.sh` is present in the tarball root directory (GitHub archives include it).

- [ ] **Step 5: Convert avro**

Uses `SOURCE_SUBDIR lang/c` and has extensive patching. Verify:
- The archive contains the `lang/c` subdirectory
- All `PATCH_COMMAND` sed commands match the directory layout

- [ ] **Step 6: Test full build**

```bash
cd ~/tsdb
rm -rf debug/build
./build.sh --image core engine -DBUILD_CONTRIB=ON
# Expected: all deps build successfully from tarballs
```

- [ ] **Step 7: Commit**

```bash
cd ~/tsdb/source/taos-community
git add cmake/external.cmake
git commit -m "cmake: convert 5 high-risk deps from git clone to URL tarball

Convert xxHash, fast-lzma2, tz, jemalloc, avro from GIT_REPOSITORY
to URL + URL_HASH. These deps have BUILD_IN_SOURCE, PATCH_COMMAND,
or SOURCE_SUBDIR requiring careful validation."
```

---

### Task 15: Remove deprecated macro and LOCAL_REPO

**Files:**
- Modify: `~/tsdb/source/taos-community/cmake/external.cmake:228–247`

- [ ] **Step 1: Remove get_from_local_repo_if_exists**

Delete lines 228–247 (the entire `get_from_local_repo_if_exists` macro). At this point, no ExternalProject should reference `_git_url` or `LOCAL_REPO` anymore.

- [ ] **Step 2: Verify no remaining references**

```bash
cd ~/tsdb/source/taos-community
grep -rn "get_from_local_repo_if_exists\|LOCAL_REPO\|_git_url" cmake/
# Expected: no matches (or only comments/documentation)
```

- [ ] **Step 3: Test build**

```bash
cd ~/tsdb
rm -rf debug/build
./build.sh --image core engine -DBUILD_CONTRIB=ON
```

- [ ] **Step 4: Commit**

```bash
cd ~/tsdb/source/taos-community
git add cmake/external.cmake
git commit -m "cmake: remove deprecated get_from_local_repo_if_exists macro

All deps now use URL + URL_HASH via get_from_local_if_exists.
The LOCAL_REPO variable and git-clone redirect macro are no longer needed."
```

---

### Task 16: Full validation — upstream and mirror

**Files:**
- None (validation only)

- [ ] **Step 1: Test with upstream URLs (no mirror)**

```bash
cd ~/tsdb
rm -rf debug/build
unset DEPS_MIRROR_URL
./build.sh --image core engine -DBUILD_CONTRIB=ON
# Expected: all deps download from GitHub, build succeeds
```

- [ ] **Step 2: Upload tarballs to GitLab**

```bash
cd /Users/xiaobo/work/gitlab/platform/tsdb-builder
export GITLAB_TOKEN="glpat-xxxx"
export GITLAB_PROJECT_ID="<PID>"
./scripts/prepare-externals.sh --upload
# Expected: all tarballs uploaded successfully
```

- [ ] **Step 3: Test with mirror URLs**

```bash
cd ~/tsdb
rm -rf debug/build
export DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/<PID>/packages/generic/externals/latest"
./build.sh --image core engine -DBUILD_CONTRIB=ON
# Expected: all deps download from GitLab mirror, build succeeds
```

- [ ] **Step 4: Cross-arch validation (arm64)**

```bash
cd ~/tsdb
rm -rf debug/build
./build.sh --image core --arch arm64 engine -DBUILD_CONTRIB=ON
# Expected: build succeeds on arm64
```

---

### Task 17: Update README.md documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add ccache section**

In the "Volume mounts during compilation" table in README.md, add the ccache row:

```markdown
| `ccache-<image>-<arch>/` | `/root/.cache/ccache` | core + dev + others |
```

Add a new subsection after the table:

```markdown
### ccache (compilation cache)

ccache is installed in all builder images. It transparently caches compiled object
files, providing significant speedup for incremental builds and branch switching.

Cache isolation is per-image + per-arch (same as `.externals/`). Debug and Release
builds share the same ccache directory — ccache includes compiler flags in the hash
key, so different build types never collide.

Environment variables:
- `CCACHE_MAXSIZE`: max cache size (default: 20G)
- `CCACHE_REMOTE_STORAGE`: shared cache backend (optional, for CI)
  - NFS: `file:///nfs/shared-ccache/<image>-<arch>/`
  - HTTP: `http://<host>:<port>`
```

- [ ] **Step 2: Add DEPS_MIRROR_URL section**

Add to the "Key Commands" or a new "Mirror Configuration" section:

```markdown
### External dependency mirror

All ExternalProject source downloads can be redirected to an internal mirror by
setting `DEPS_MIRROR_URL`:

\`\`\`bash
export DEPS_MIRROR_URL="https://git.tdengine.net/api/v4/projects/<PID>/packages/generic/externals/latest"
./build.sh --image core engine -DBUILD_CONTRIB=ON
\`\`\`

When not set, cmake uses the original upstream URLs (GitHub). This is the default
for external contributors and environments without internal GitLab access.
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add ccache and DEPS_MIRROR_URL documentation"
```
